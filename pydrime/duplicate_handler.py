"""Duplicate file detection and handling utilities."""

from pathlib import Path, PurePosixPath
from typing import Any, Optional

import click

from .api import DrimeClient
from .exceptions import DrimeAPIError
from .file_entries_manager import FileEntriesManager
from .output import OutputFormatter
from .utils import format_size as _format_size


class DuplicateHandler:
    """Handles duplicate file detection and resolution during uploads."""

    def __init__(
        self,
        client: DrimeClient,
        out: OutputFormatter,
        workspace_id: int,
        on_duplicate: str = "ask",
        parent_id: Optional[int] = None,
    ):
        """Initialize duplicate handler.

        Args:
            client: Drime API client
            out: Output formatter
            workspace_id: Workspace ID
            on_duplicate: Action for duplicates ('ask', 'replace', 'rename', 'skip')
            parent_id: Base parent folder ID for uploads (None for root)
        """
        self.client = client
        self.out = out
        self.workspace_id = workspace_id
        self.on_duplicate = on_duplicate
        self.parent_id = parent_id
        # Set when user chooses or from on_duplicate if not 'ask'
        self.chosen_action = None
        self.apply_to_all = on_duplicate != "ask"

        # If not asking, set the action directly
        if self.apply_to_all:
            self.chosen_action = on_duplicate

        self.files_to_skip: set[str] = set()
        self.rename_map: dict[str, str] = {}
        self.entries_to_delete: list[int] = []  # Entry IDs to delete for replace action

        # Performance optimization: cache for folder ID lookups
        self._folder_id_cache: dict[str, Optional[int]] = {}

        # Mapping of rel_path -> list of duplicate entry IDs on server
        # Used to track which specific files have server duplicates
        self._duplicate_rel_paths: dict[str, list[int]] = {}

        # File entries manager for pagination and search
        self.entries_manager = FileEntriesManager(client, workspace_id)

    def validate_and_handle_duplicates(
        self, files_to_upload: list[tuple[Path, str]]
    ) -> None:
        """Validate uploads and handle duplicates.

        Args:
            files_to_upload: List of (file_path, relative_path) tuples
        """
        # Early optimization: If all files share a common root folder,
        # check if that folder exists with a single API call
        # Avoids expensive duplicate checking when uploading to non-existent folders
        common_root = self._get_common_root_folder(files_to_upload)
        if common_root and self.parent_id is not None:
            # Check if the common root folder exists in the parent
            folder_exists = self._quick_folder_check(common_root)
            if not folder_exists:
                # Folder doesn't exist - no duplicates possible!
                if not self.out.quiet:
                    self.out.success(
                        "✓ No duplicates found (target folder doesn't exist)"
                    )
                return

        # Prepare validation files
        if not self.out.quiet:
            self.out.info("Checking for duplicates...")

        validation_files = [
            {
                "name": PurePosixPath(rel_path).name,
                "size": file_path.stat().st_size,
                "relativePath": str(PurePosixPath(rel_path).parent)
                if PurePosixPath(rel_path).parent != PurePosixPath(".")
                else "",
            }
            for file_path, rel_path in files_to_upload
        ]

        try:
            validation_result = self.client.validate_uploads(
                files=validation_files, workspace_id=self.workspace_id
            )
            duplicates = validation_result.get("duplicates", [])

            # Filter out folders from duplicates
            folder_duplicates = duplicates.copy()
            duplicates = self._filter_folder_duplicates(duplicates, files_to_upload)

            # Check for file-level duplicates in filtered folders
            filtered_folders = set(folder_duplicates) - set(duplicates)
            if filtered_folders:
                if not self.out.quiet:
                    folder_count = len(filtered_folders)
                    self.out.info(
                        f"Checking {folder_count} folder(s) for file duplicates..."
                    )
                file_duplicates = self._check_file_duplicates_in_folders(
                    files_to_upload, filtered_folders
                )
                duplicates.extend(file_duplicates)
        except DrimeAPIError:
            # If validation fails, continue without duplicate detection
            duplicates = []

        if not duplicates:
            if not self.out.quiet:
                self.out.success("✓ No duplicates found")
            return

        # Look up IDs for all duplicates
        if not self.out.quiet:
            self.out.info(f"Looking up details for {len(duplicates)} duplicate(s)...")
        duplicate_info = self._lookup_duplicate_ids(duplicates, files_to_upload)

        # Show duplicate summary with sizes
        if not self.out.quiet:
            self._display_duplicate_summary(duplicates, duplicate_info, files_to_upload)

        # Handle each duplicate
        for duplicate_name in duplicates:
            self._handle_single_duplicate(
                duplicate_name, duplicate_info, files_to_upload
            )

        # Show final summary after all actions taken
        if not self.out.quiet and duplicates:
            self._display_action_summary(duplicates, files_to_upload)

    def _display_duplicate_summary(
        self,
        duplicates: list[str],
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]],
        files_to_upload: list[tuple[Path, str]],
    ) -> None:
        """Display summary of duplicates with sizes before prompting user.

        Args:
            duplicates: List of duplicate names
            duplicate_info: Dict mapping duplicate name to list of (id, path)
            files_to_upload: List of (file_path, relative_path) tuples
        """
        self.out.warning(f"\nFound {len(duplicates)} duplicate(s):")
        self.out.print("")

        total_size = 0
        for dup in duplicates:
            # Find the file size from files_to_upload
            file_size = 0
            for file_path, rel_path in files_to_upload:
                if PurePosixPath(rel_path).name == dup:
                    file_size = file_path.stat().st_size
                    break

            total_size += file_size
            size_str = _format_size(file_size)

            # Show ID and path if available
            if dup in duplicate_info and duplicate_info[dup]:
                ids = [str(id) for id, _ in duplicate_info[dup]]
                if len(ids) == 1:
                    self.out.warning(f"  • {dup} ({size_str}, ID: {ids[0]})")
                else:
                    ids_str = ", ".join(ids)
                    self.out.warning(f"  • {dup} ({size_str}, IDs: {ids_str})")
            else:
                self.out.warning(f"  • {dup} ({size_str})")

        self.out.print("")
        self.out.info(f"Total duplicate size: {_format_size(total_size)}")
        self.out.print("")

    def _display_action_summary(
        self,
        duplicates: list[str],
        files_to_upload: list[tuple[Path, str]],
    ) -> None:
        """Display summary of actions taken after duplicate handling.

        Args:
            duplicates: List of duplicate names
            files_to_upload: List of (file_path, relative_path) tuples
        """
        self.out.print("")
        self.out.info("Duplicate handling summary:")

        skipped = 0
        renamed = 0
        replaced = 0
        skipped_size = 0
        renamed_size = 0
        replaced_size = 0

        for dup in duplicates:
            # Find the file size
            file_size = 0
            for file_path, rel_path in files_to_upload:
                if PurePosixPath(rel_path).name == dup:
                    file_size = file_path.stat().st_size
                    break

            # Check which action was taken
            if dup in self.files_to_skip:
                skipped += 1
                skipped_size += file_size
            elif dup in self.rename_map:
                renamed += 1
                renamed_size += file_size
            else:
                # Check if any entry was marked for deletion (replace)
                replaced += 1
                replaced_size += file_size

        if skipped > 0:
            self.out.info(f"  Skipped: {skipped} file(s), {_format_size(skipped_size)}")
        if renamed > 0:
            self.out.info(f"  Renamed: {renamed} file(s), {_format_size(renamed_size)}")
        if replaced > 0:
            self.out.info(
                f"  Replaced: {replaced} file(s), {_format_size(replaced_size)}"
            )
        self.out.print("")

    def _filter_folder_duplicates(
        self, duplicates: list[str], files_to_upload: list[tuple[Path, str]]
    ) -> list[str]:
        """Filter out folders from duplicates list.

        Args:
            duplicates: List of duplicate names
            files_to_upload: List of (file_path, relative_path) tuples

        Returns:
            Filtered list of duplicates (files only)
        """
        if not duplicates:
            return []

        # Collect all folder names from our upload paths
        folders_in_upload = set()
        for _, rel_path in files_to_upload:
            path_parts = PurePosixPath(rel_path).parts
            # Add all parent folder names (exclude the filename)
            for i in range(len(path_parts) - 1):
                folder_name = path_parts[i]
                folders_in_upload.add(folder_name)

        # Batch check which duplicates are folders on the server
        # Do a single API call to check all at once
        duplicates_to_check = [d for d in duplicates if d not in folders_in_upload]

        if not duplicates_to_check:
            return []

        folder_set = self._batch_check_folders(duplicates_to_check)

        # Filter duplicates - keep only non-folders
        duplicates_to_keep = []
        for dup_name in duplicates:
            # Skip if it's a folder in our upload paths
            if dup_name in folders_in_upload:
                continue

            # Skip if it's a folder on server
            if dup_name in folder_set:
                continue

            duplicates_to_keep.append(dup_name)

        return duplicates_to_keep

    def _batch_check_folders(self, names: list[str]) -> set[str]:
        """Batch check which names are folders on the server.

        Args:
            names: List of names to check

        Returns:
            Set of names that are folders
        """
        folder_names = set()
        names_to_check = []

        # First pass: check cache
        for name in names:
            cache_key = f"is_folder:{name}"
            if cache_key in self._folder_id_cache:
                if self._folder_id_cache[cache_key]:
                    folder_names.add(name)
            else:
                names_to_check.append(name)

        if not names_to_check:
            return folder_names

        # Check each name individually using scoped searches
        # This is more efficient than fetching all entries when parent has many files
        if not self.out.quiet and names_to_check:
            self.out.progress_message(
                f"  Checking {len(names_to_check)} items to determine "
                f"if they are folders..."
            )

        for idx, name in enumerate(names_to_check, 1):
            try:
                if not self.out.quiet:
                    self.out.progress_message(
                        f"    Checking '{name}' ({idx}/{len(names_to_check)})..."
                    )
                cache_key = f"is_folder:{name}"
                # Use find_folder_by_name with parent_id for scoped search
                folder = self.entries_manager.find_folder_by_name(
                    name, parent_id=self.parent_id
                )
                if folder:
                    folder_names.add(name)
                    self._folder_id_cache[cache_key] = folder.id
                    if not self.out.quiet:
                        self.out.progress_message(f"      → '{name}' is a folder")
                else:
                    self._folder_id_cache[cache_key] = None
                    if not self.out.quiet:
                        self.out.progress_message(f"      → '{name}' is not a folder")
            except DrimeAPIError:
                if not self.out.quiet:
                    self.out.progress_message(
                        f"      → Error checking '{name}', skipping"
                    )
                pass

        return folder_names

    def _check_file_duplicates_in_folders(
        self, files_to_upload: list[tuple[Path, str]], folder_names: set[str]
    ) -> list[str]:
        """Check for file duplicates within existing folders.

        When the API returns folder names as duplicates, we need to manually check
        if files we're uploading to those folders already exist.

        Args:
            files_to_upload: List of (file_path, relative_path) tuples
            folder_names: Set of folder names that exist on server

        Returns:
            List of duplicate file names
        """
        file_duplicates = []

        # Group files by their parent folder
        files_by_folder: dict[str, list[tuple[Path, str]]] = {}
        for file_path, rel_path in files_to_upload:
            parent = str(PurePosixPath(rel_path).parent)
            if parent == ".":
                continue  # Skip root-level files

            # Get the top-level folder name
            top_folder = PurePosixPath(rel_path).parts[0]
            if top_folder in folder_names:
                if top_folder not in files_by_folder:
                    files_by_folder[top_folder] = []
                files_by_folder[top_folder].append((file_path, rel_path))

        # For each folder, check if files exist
        for idx, (folder_name, files) in enumerate(files_by_folder.items(), 1):
            try:
                if not self.out.quiet:
                    total_folders = len(files_by_folder)
                    self.out.progress_message(
                        f"  Scanning folder '{folder_name}' ({idx}/{total_folders})..."
                    )

                # Get the folder ID
                folder_entry = self.entries_manager.find_folder_by_name(
                    folder_name, parent_id=self.parent_id
                )
                if not folder_entry:
                    continue

                # Build a cache of all files in this folder tree (fetch once)
                if not self.out.quiet:
                    self.out.progress_message(
                        f"    Building file index for '{folder_name}'..."
                    )
                file_path_cache = self._build_file_path_cache(folder_entry.id)

                if not self.out.quiet:
                    self.out.progress_message(
                        f"    Found {len(file_path_cache)} existing files in cache"
                    )

                # Check each file we're uploading against the cache
                total_files = len(files)
                for file_idx, (_file_path, rel_path) in enumerate(files, 1):
                    # Show progress for each file being checked
                    if not self.out.quiet and file_idx % 50 == 0:
                        # Show progress every 50 files to avoid spam
                        file_name = PurePosixPath(rel_path).name
                        self.out.progress_message(
                            f"    Checked {file_idx}/{total_files} files..."
                        )

                    # Get the path relative to the top folder
                    rel_to_folder = str(
                        PurePosixPath(rel_path).relative_to(folder_name)
                    )

                    # Check if this specific file exists in cache
                    if rel_to_folder in file_path_cache:
                        # Just add the filename, not the full path
                        file_name = PurePosixPath(rel_path).name
                        if file_name not in file_duplicates:
                            file_duplicates.append(file_name)

                if not self.out.quiet:
                    self.out.progress_message(
                        f"    Completed checking {total_files} files"
                    )

            except (DrimeAPIError, ValueError):
                # If we can't check, skip it
                pass

        return file_duplicates

    def _build_file_path_cache(self, base_folder_id: int) -> set[str]:
        """Build a cache of all file paths in a folder tree.

        Recursively fetches all files and builds a set of relative paths.
        This is done ONCE per folder to avoid repeated API calls.

        Args:
            base_folder_id: Base folder ID to start from

        Returns:
            Set of relative file paths (e.g., {"file.txt", "subdir/file2.txt"})
        """
        file_paths = set()
        visited_folders = set()

        def _scan_folder(folder_id: int, current_path: str = "") -> None:
            """Recursively scan a folder and add file paths to the cache."""
            # Prevent infinite loops
            if folder_id in visited_folders:
                return
            visited_folders.add(folder_id)

            try:
                # Fetch all entries in this folder
                entries = self.entries_manager.get_all_in_folder(
                    folder_id=folder_id, use_cache=False
                )

                for entry in entries:
                    if entry.is_folder:
                        # Recursively scan subfolders
                        subfolder_path = (
                            f"{current_path}/{entry.name}"
                            if current_path
                            else entry.name
                        )
                        _scan_folder(entry.id, subfolder_path)
                    else:
                        # Add file path to cache
                        file_path = (
                            f"{current_path}/{entry.name}"
                            if current_path
                            else entry.name
                        )
                        file_paths.add(file_path)

            except (DrimeAPIError, ValueError):
                # If we can't fetch, skip this folder
                pass

        # Start scanning from the base folder
        _scan_folder(base_folder_id)
        return file_paths

    def _file_exists_at_path(self, base_folder_id: int, rel_path: str) -> bool:
        """Check if a specific file exists at a relative path within a folder.

        This is much more efficient than fetching all files recursively.

        Args:
            base_folder_id: Base folder ID to start from
            rel_path: Relative path to the file (e.g., "subdir/file.txt")

        Returns:
            True if the file exists at that path
        """
        try:
            path_parts = PurePosixPath(rel_path).parts

            # Navigate through folders to get to the parent folder
            current_folder_id = base_folder_id
            for idx, part in enumerate(path_parts[:-1]):  # All except filename
                if not self.out.quiet:
                    self.out.progress_message(
                        f"      Navigating to subfolder '{part}' "
                        f"({idx + 1}/{len(path_parts) - 1})..."
                    )
                # Find the subfolder
                folder = self.entries_manager.find_folder_by_name(
                    part, parent_id=current_folder_id
                )
                if not folder:
                    if not self.out.quiet:
                        self.out.progress_message(
                            f"      Subfolder '{part}' not found, file doesn't exist"
                        )
                    return False  # Folder doesn't exist, so file can't exist
                current_folder_id = folder.id

            # Now check if the file exists in the final parent folder
            filename = path_parts[-1]
            if not self.out.quiet:
                self.out.progress_message(
                    f"      Fetching entries in final folder to check for "
                    f"'{filename}'..."
                )
            entries = self.entries_manager.get_all_in_folder(
                folder_id=current_folder_id, use_cache=False
            )

            if not self.out.quiet:
                self.out.progress_message(
                    f"      Checking {len(entries)} entries for match..."
                )

            # Check if any entry matches the filename (and is not a folder)
            for entry in entries:
                if entry.name == filename and not entry.is_folder:
                    if not self.out.quiet:
                        self.out.progress_message(
                            f"      Found duplicate: '{filename}'"
                        )
                    return True

            if not self.out.quiet:
                self.out.progress_message(f"      No match found for '{filename}'")
            return False
        except (DrimeAPIError, ValueError, IndexError):
            # If we can't determine, assume it doesn't exist
            if not self.out.quiet:
                self.out.progress_message(
                    f"      Error checking path '{rel_path}', skipping..."
                )
            return False

    def _is_existing_folder(self, name: str) -> bool:
        """Check if name is an existing folder on server.

        Args:
            name: File/folder name

        Returns:
            True if name is an existing folder
        """
        try:
            folder = self.entries_manager.find_folder_by_name(name, parent_id=None)
            return folder is not None
        except DrimeAPIError:
            pass
        return False

    def _resolve_parent_folder_id(self, folder_path: str) -> Optional[int]:
        """Resolve a folder path to its ID with caching.

        Args:
            folder_path: Relative folder path (e.g., "backup/data")

        Returns:
            Folder ID if found, None otherwise
        """
        # Check cache first
        if folder_path in self._folder_id_cache:
            return self._folder_id_cache[folder_path]

        # Start from the base parent_id
        current_parent_id = self.parent_id

        # Split path and navigate through folders
        path_parts = PurePosixPath(folder_path).parts
        current_path = ""

        for idx, folder_name in enumerate(path_parts):
            # Build incremental path for caching
            current_path = folder_name if idx == 0 else f"{current_path}/{folder_name}"

            # Check if we already have this path cached
            if current_path in self._folder_id_cache:
                current_parent_id = self._folder_id_cache[current_path]
                if current_parent_id is None:
                    self._folder_id_cache[folder_path] = None
                    return None
                continue

            try:
                # Search for folder in current parent
                folder_entry = self.entries_manager.find_folder_by_name(
                    folder_name, parent_id=current_parent_id
                )

                if not folder_entry:
                    self._folder_id_cache[current_path] = None
                    self._folder_id_cache[folder_path] = None
                    return None

                current_parent_id = folder_entry.id
                self._folder_id_cache[current_path] = current_parent_id
            except DrimeAPIError:
                self._folder_id_cache[current_path] = None
                self._folder_id_cache[folder_path] = None
                return None

        # Cache the final result
        self._folder_id_cache[folder_path] = current_parent_id
        return current_parent_id

    def _build_dup_to_paths_map(
        self,
        duplicates: list[str],
        files_to_upload: list[tuple[Path, str]],
    ) -> dict[str, list[str]]:
        """Build a map of duplicate names to their target paths.

        Args:
            duplicates: List of duplicate names
            files_to_upload: List of (file_path, relative_path) tuples

        Returns:
            Dict mapping duplicate name to list of relative paths
        """
        dup_to_paths: dict[str, list[str]] = {}
        for _file_path, rel_path in files_to_upload:
            name = PurePosixPath(rel_path).name
            if name in duplicates:
                if name not in dup_to_paths:
                    dup_to_paths[name] = []
                dup_to_paths[name].append(rel_path)
        return dup_to_paths

    def _group_by_parent_folder(
        self,
        dup_to_paths: dict[str, list[str]],
    ) -> dict[tuple[Optional[int], str], list[str]]:
        """Group duplicate files by their target parent folder.

        Args:
            dup_to_paths: Dict mapping duplicate name to list of relative paths

        Returns:
            Dict with (parent_id, dup_name) as key and list of rel_paths as value
        """
        by_parent: dict[tuple[Optional[int], str], list[str]] = {}

        for dup_name, rel_paths in dup_to_paths.items():
            for rel_path in rel_paths:
                parent_path = PurePosixPath(rel_path).parent

                # Resolve parent folder ID
                target_parent_id = self.parent_id
                if parent_path != PurePosixPath("."):
                    target_parent_id = self._resolve_parent_folder_id(str(parent_path))

                key = (target_parent_id, dup_name)
                if key not in by_parent:
                    by_parent[key] = []
                by_parent[key].append(rel_path)

        return by_parent

    def _add_entry_to_duplicate_info(
        self,
        entry: Any,
        dup_name: str,
        rel_paths: list[str],
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]],
    ) -> None:
        """Add an entry to duplicate_info and _duplicate_rel_paths.

        Args:
            entry: The file entry to add
            dup_name: Name of the duplicate file
            rel_paths: List of relative paths that match this duplicate
            duplicate_info: Dict to update with (id, path) tuples
        """
        if dup_name not in duplicate_info:
            duplicate_info[dup_name] = []
        duplicate_info[dup_name].append(
            (entry.id, entry.path if hasattr(entry, "path") else None)
        )
        # Associate entry with the rel_paths
        for rp in rel_paths:
            if rp not in self._duplicate_rel_paths:
                self._duplicate_rel_paths[rp] = []
            self._duplicate_rel_paths[rp].append(entry.id)

    def _lookup_duplicate_ids(
        self,
        duplicates: list[str],
        files_to_upload: list[tuple[Path, str]],
    ) -> dict[str, list[tuple[int, Optional[str]]]]:
        """Look up IDs for all duplicates in their target upload folders.

        Args:
            duplicates: List of duplicate names
            files_to_upload: List of (file_path, relative_path) tuples

        Returns:
            Dict mapping duplicate name to list of (id, server_path) tuples
        """
        # Build mapping of duplicate names to their target paths
        dup_to_paths = self._build_dup_to_paths_map(duplicates, files_to_upload)

        # Group files by their target parent folder for batching API calls
        by_parent = self._group_by_parent_folder(dup_to_paths)

        # Initialize result structures
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]] = {}
        self._duplicate_rel_paths = {}

        # Group keys by parent_id for batching
        parent_to_keys: dict[Optional[int], list[tuple[Optional[int], str]]] = {}
        for key in by_parent:
            parent_id = key[0]
            if parent_id not in parent_to_keys:
                parent_to_keys[parent_id] = []
            parent_to_keys[parent_id].append(key)

        # Process each parent folder
        for parent_id, keys in parent_to_keys.items():
            try:
                if parent_id is None:
                    self._lookup_duplicates_global(keys, by_parent, duplicate_info)
                else:
                    self._lookup_duplicates_in_folder(
                        parent_id, keys, by_parent, duplicate_info
                    )
            except DrimeAPIError:
                # If batch fails, continue without these duplicates
                pass

        return duplicate_info

    def _lookup_duplicates_global(
        self,
        keys: list[tuple[Optional[int], str]],
        by_parent: dict[tuple[Optional[int], str], list[str]],
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]],
    ) -> None:
        """Look up duplicates using global search (when parent folder is unknown).

        Args:
            keys: List of (parent_id, dup_name) keys to look up
            by_parent: Mapping of keys to relative paths
            duplicate_info: Dict to update with results
        """
        for key in keys:
            dup_name = key[1]
            rel_paths = by_parent[key]
            try:
                matching_entries = self.entries_manager.search_by_name(
                    dup_name, exact_match=True
                )
                for entry in matching_entries:
                    self._add_entry_to_duplicate_info(
                        entry, dup_name, rel_paths, duplicate_info
                    )
            except DrimeAPIError:
                pass

    def _lookup_duplicates_in_folder(
        self,
        parent_id: int,
        keys: list[tuple[Optional[int], str]],
        by_parent: dict[tuple[Optional[int], str], list[str]],
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]],
    ) -> None:
        """Look up duplicates within a specific folder.

        Args:
            parent_id: Folder ID to search in
            keys: List of (parent_id, dup_name) keys to look up
            by_parent: Mapping of keys to relative paths
            duplicate_info: Dict to update with results
        """
        # Batch: Get all entries in this parent folder at once
        file_entries_list = self.entries_manager.get_all_in_folder(parent_id)

        # Build a map for quick lookup
        entries_by_name: dict[str, list] = {}
        for entry in file_entries_list:
            if entry.name not in entries_by_name:
                entries_by_name[entry.name] = []
            entries_by_name[entry.name].append(entry)

        # Match duplicates with entries
        for key in keys:
            dup_name = key[1]
            rel_paths = by_parent[key]
            if dup_name in entries_by_name:
                for entry in entries_by_name[dup_name]:
                    self._add_entry_to_duplicate_info(
                        entry, dup_name, rel_paths, duplicate_info
                    )

    def _handle_single_duplicate(
        self,
        duplicate_name: str,
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]],
        files_to_upload: list[tuple[Path, str]],
    ) -> None:
        """Handle a single duplicate file.

        Args:
            duplicate_name: Name of duplicate file
            duplicate_info: Dict of duplicate IDs
            files_to_upload: List of files being uploaded
        """
        # Find which specific files have this duplicate name AND are actually duplicates
        # Use _duplicate_rel_paths to get only files that matched server entries
        matching_rel_paths = []
        for _file_path, rel_path in files_to_upload:
            path_obj = PurePosixPath(rel_path)
            if path_obj.name == duplicate_name:
                # Check if this specific path was identified as having a
                # server duplicate
                if hasattr(self, "_duplicate_rel_paths") and self._duplicate_rel_paths:
                    if rel_path in self._duplicate_rel_paths:
                        matching_rel_paths.append(rel_path)
                else:
                    # Fallback: if no path mapping, use the old behavior
                    matching_rel_paths.append(rel_path)

        # Prompt user if needed
        if not self.apply_to_all:
            # Show ID in the prompt if available
            if duplicate_name in duplicate_info and duplicate_info[duplicate_name]:
                ids_str = ", ".join(
                    f"ID: {id}" for id, _ in duplicate_info[duplicate_name]
                )
                self.out.warning(f"Duplicate detected: '{duplicate_name}' ({ids_str})")
            else:
                self.out.warning(f"Duplicate detected: '{duplicate_name}'")

            # Show which paths are affected if multiple
            if len(matching_rel_paths) > 1:
                self.out.info(f"  Affects {len(matching_rel_paths)} files:")
                for rp in matching_rel_paths[:5]:  # Show first 5
                    self.out.info(f"    - {rp}")
                if len(matching_rel_paths) > 5:
                    self.out.info(f"    ... and {len(matching_rel_paths) - 5} more")

            self.chosen_action = click.prompt(
                "Action",
                type=click.Choice(["replace", "rename", "skip"]),
            )

            apply_choice = click.prompt(
                "Apply this choice to all duplicates?",
                type=click.Choice(["y", "n"]),
                default="n",
            )
            self.apply_to_all = apply_choice.lower() == "y"

        # Execute the chosen action - only for files that are actual duplicates
        if self.chosen_action == "skip":
            # Skip only the files that are actual duplicates
            for rel_path in matching_rel_paths:
                self._handle_skip(duplicate_name, files_to_upload, rel_path)
        elif self.chosen_action == "rename":
            self._handle_rename(duplicate_name, files_to_upload)
        elif self.chosen_action == "replace":
            self._handle_replace(duplicate_name, duplicate_info)

    def _handle_skip(
        self,
        duplicate_name: str,
        files_to_upload: list[tuple[Path, str]],
        specific_rel_path: Optional[str] = None,
    ) -> None:
        """Mark files matching duplicate for skipping.

        Args:
            duplicate_name: Name of duplicate file
            files_to_upload: List of files being uploaded
            specific_rel_path: If provided, only skip this specific path
                              (used when duplicate_name matches multiple files)
        """
        if specific_rel_path:
            # Only skip the specific file, not all files with the same name
            self.files_to_skip.add(specific_rel_path)
            if not self.out.quiet:
                self.out.info(f"Will skip: {specific_rel_path}")
        else:
            for _file_path, rel_path in files_to_upload:
                path_obj = Path(rel_path)
                # Check if filename or parent folder matches duplicate
                if path_obj.name == duplicate_name or duplicate_name in path_obj.parts:
                    self.files_to_skip.add(rel_path)
            if not self.out.quiet:
                self.out.info(f"Will skip files matching: {duplicate_name}")

    def _handle_rename(
        self, duplicate_name: str, files_to_upload: list[tuple[Path, str]]
    ) -> None:
        """Get available name and mark for renaming.

        Args:
            duplicate_name: Name of duplicate file
            files_to_upload: List of files being uploaded
        """
        try:
            new_name = self.client.get_available_name(
                duplicate_name, workspace_id=self.workspace_id
            )

            # Store the rename mapping for this duplicate
            self.rename_map[duplicate_name] = new_name

            if not self.out.quiet:
                self.out.info(f"Will rename '{duplicate_name}' → '{new_name}'")

        except DrimeAPIError as e:
            self.out.error(f"Could not get available name for '{duplicate_name}': {e}")
            self.out.error("Skipping this file.")
            # Mark for skipping instead of aborting
            self._handle_skip(duplicate_name, files_to_upload)

    def _handle_replace(
        self,
        duplicate_name: str,
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]],
    ) -> None:
        """Handle replace action for duplicates.

        The upload will use force_upload=True to ensure files are uploaded
        even if they're identical to existing files. The cloud service will
        automatically overwrite the existing file entry during upload.

        Args:
            duplicate_name: Name of duplicate file
            duplicate_info: Dict of duplicate IDs
        """
        # Track entries that will be replaced (for summary display)
        if duplicate_name in duplicate_info and duplicate_info[duplicate_name]:
            for entry_id, _ in duplicate_info[duplicate_name]:
                self.entries_to_delete.append(entry_id)

            if not self.out.quiet:
                ids_str = ", ".join(
                    str(entry_id) for entry_id, _ in duplicate_info[duplicate_name]
                )
                self.out.info(
                    f"Will replace existing '{duplicate_name}' (ID: {ids_str})"
                )
        else:
            if not self.out.quiet:
                self.out.info(f"Will replace existing '{duplicate_name}'")

    def apply_renames(self, rel_path: str) -> str:
        """Apply rename mappings to a relative path.

        Args:
            rel_path: Relative path to apply renames to

        Returns:
            Updated path with renames applied (always with forward slashes)
        """
        upload_path = rel_path
        # Use PurePosixPath to ensure forward slashes on all platforms
        path_obj = PurePosixPath(rel_path)

        # Check if the filename needs renaming
        if path_obj.name in self.rename_map:
            new_filename = self.rename_map[path_obj.name]
            if path_obj.parent != PurePosixPath("."):
                upload_path = str(path_obj.parent / new_filename)
            else:
                upload_path = new_filename

        # Check if any parent folder in the path needs renaming
        parts = list(path_obj.parts)
        renamed_parts = [self.rename_map.get(part, part) for part in parts]
        if renamed_parts != list(parts):
            upload_path = str(PurePosixPath(*renamed_parts))

        return upload_path

    def _get_common_root_folder(
        self, files_to_upload: list[tuple[Path, str]]
    ) -> Optional[str]:
        """Get the common root folder if all files share one.

        Args:
            files_to_upload: List of (file_path, relative_path) tuples

        Returns:
            Common root folder name, or None if files don't share a common root
        """
        if not files_to_upload:
            return None

        # Get the first part of each relative path
        root_folders = set()
        for _, rel_path in files_to_upload:
            parts = PurePosixPath(rel_path).parts
            if len(parts) > 0:
                root_folders.add(parts[0])

        # If all files share the same root folder, return it
        if len(root_folders) == 1:
            return root_folders.pop()

        return None

    def _quick_folder_check(self, folder_name: str) -> bool:
        """Quick check if a folder exists in the parent folder.

        Single API call to check if folder exists.

        Args:
            folder_name: Folder name to check

        Returns:
            True if folder exists, False otherwise
        """
        try:
            folder = self.entries_manager.find_folder_by_name(
                folder_name, parent_id=self.parent_id
            )
            return folder is not None
        except DrimeAPIError:
            # If API call fails, assume folder exists to be safe
            return True
