"""Duplicate file detection and handling utilities."""

from pathlib import Path, PurePosixPath
from typing import Optional

import click

from .api import DrimeClient
from .exceptions import DrimeAPIError
from .file_entries_manager import FileEntriesManager
from .models import _format_size
from .output import OutputFormatter


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
        self.entries_to_delete: list[int] = []

        # Performance optimization: cache for folder ID lookups
        self._folder_id_cache: dict[str, Optional[int]] = {}

        # File entries manager for pagination and search
        self.entries_manager = FileEntriesManager(client, workspace_id)

    def validate_and_handle_duplicates(
        self, files_to_upload: list[tuple[Path, str]]
    ) -> None:
        """Validate uploads and handle duplicates.

        Args:
            files_to_upload: List of (file_path, relative_path) tuples
        """
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
        for name in names_to_check:
            try:
                cache_key = f"is_folder:{name}"
                # Use find_folder_by_name with parent_id for scoped search
                folder = self.entries_manager.find_folder_by_name(
                    name, parent_id=self.parent_id
                )
                if folder:
                    folder_names.add(name)
                    self._folder_id_cache[cache_key] = folder.id
                else:
                    self._folder_id_cache[cache_key] = None
            except DrimeAPIError:
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
                    total = len(files_by_folder)
                    self.out.progress_message(
                        f"  Scanning folder '{folder_name}' ({idx}/{total})..."
                    )

                # Get the folder ID
                folder_entry = self.entries_manager.find_folder_by_name(
                    folder_name, parent_id=self.parent_id
                )
                if not folder_entry:
                    continue

                # Check each file we're uploading individually
                # Instead of fetching all existing files, only check specific paths
                for _file_path, rel_path in files:
                    # Get the path relative to the top folder
                    rel_to_folder = str(
                        PurePosixPath(rel_path).relative_to(folder_name)
                    )

                    # Check if this specific file exists
                    if self._file_exists_at_path(folder_entry.id, rel_to_folder):
                        # Just add the filename, not the full path
                        file_name = PurePosixPath(rel_path).name
                        if file_name not in file_duplicates:
                            file_duplicates.append(file_name)

            except (DrimeAPIError, ValueError):
                # If we can't check, skip it
                pass

        return file_duplicates

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
            for part in path_parts[:-1]:  # All parts except the filename
                # Find the subfolder
                folder = self.entries_manager.find_folder_by_name(
                    part, parent_id=current_folder_id
                )
                if not folder:
                    return False  # Folder doesn't exist, so file can't exist
                current_folder_id = folder.id

            # Now check if the file exists in the final parent folder
            filename = path_parts[-1]
            entries = self.entries_manager.get_all_in_folder(
                folder_id=current_folder_id, use_cache=False
            )

            # Check if any entry matches the filename (and is not a folder)
            for entry in entries:
                if entry.name == filename and not entry.is_folder:
                    return True

            return False
        except (DrimeAPIError, ValueError, IndexError):
            # If we can't determine, assume it doesn't exist
            return False

    def _get_files_in_folder_recursive(
        self, folder_id: int, visited: Optional[set[int]] = None
    ) -> set[str]:
        """Get all file paths in a folder recursively.

        DEPRECATED: This method is slow for large folders.
        Use _file_exists_at_path instead.

        Args:
            folder_id: Folder ID to search
            visited: Set of visited folder IDs to prevent infinite recursion

        Returns:
            Set of relative file paths within the folder
        """
        # Use FileEntriesManager for pagination handling
        manager = FileEntriesManager(self.client, self.workspace_id)
        entries_with_paths = manager.get_all_recursive(
            folder_id=folder_id, visited=visited
        )

        # Convert to set of paths (files only, folders already filtered)
        return {path for _, path in entries_with_paths}

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
            Dict mapping duplicate name to list of (id, path) tuples
        """
        # Pre-build a map of duplicate names to their target paths
        # This avoids repeated searches through files_to_upload
        dup_to_path: dict[str, str] = {}
        for _file_path, rel_path in files_to_upload:
            name = PurePosixPath(rel_path).name
            if name in duplicates and name not in dup_to_path:
                dup_to_path[name] = rel_path

        # Group duplicates by their target parent folder
        # to batch API calls for files in the same folder
        by_parent: dict[Optional[int], list[str]] = {}
        dup_to_parent: dict[str, Optional[int]] = {}

        for dup_name in duplicates:
            if dup_name not in dup_to_path:
                continue

            target_rel_path = dup_to_path[dup_name]
            parent_path = PurePosixPath(target_rel_path).parent

            # Resolve parent folder ID
            target_parent_id = self.parent_id
            if parent_path != PurePosixPath("."):
                target_parent_id = self._resolve_parent_folder_id(str(parent_path))

            dup_to_parent[dup_name] = target_parent_id
            if target_parent_id not in by_parent:
                by_parent[target_parent_id] = []
            by_parent[target_parent_id].append(dup_name)

        # Now fetch entries grouped by parent folder
        duplicate_info = {}

        for parent_id, dup_names in by_parent.items():
            try:
                if parent_id is None:
                    # Fallback to global search for each
                    for dup_name in dup_names:
                        try:
                            matching_entries = self.entries_manager.search_by_name(
                                dup_name, exact_match=True
                            )
                            if matching_entries:
                                duplicate_info[dup_name] = [
                                    (
                                        e.id,
                                        e.path if hasattr(e, "path") else None,
                                    )
                                    for e in matching_entries
                                ]
                        except DrimeAPIError:
                            pass
                else:
                    # Batch: Get all entries in this parent folder at once
                    file_entries_list = self.entries_manager.get_all_in_folder(
                        parent_id
                    )

                    # Build a map for quick lookup
                    entries_by_name: dict[str, list] = {}
                    for entry in file_entries_list:
                        if entry.name not in entries_by_name:
                            entries_by_name[entry.name] = []
                        entries_by_name[entry.name].append(entry)

                    # Match duplicates with entries
                    for dup_name in dup_names:
                        if dup_name in entries_by_name:
                            duplicate_info[dup_name] = [
                                (
                                    e.id,
                                    e.path if hasattr(e, "path") else None,
                                )
                                for e in entries_by_name[dup_name]
                            ]
            except DrimeAPIError:
                # If batch fails, continue without these duplicates
                pass

        return duplicate_info

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

        # Execute the chosen action
        if self.chosen_action == "skip":
            self._handle_skip(duplicate_name, files_to_upload)
        elif self.chosen_action == "rename":
            self._handle_rename(duplicate_name, files_to_upload)
        elif self.chosen_action == "replace":
            self._handle_replace(duplicate_name, duplicate_info)

    def _handle_skip(
        self, duplicate_name: str, files_to_upload: list[tuple[Path, str]]
    ) -> None:
        """Mark files matching duplicate for skipping.

        Args:
            duplicate_name: Name of duplicate file
            files_to_upload: List of files being uploaded
        """
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
        """Mark existing entries for deletion.

        Args:
            duplicate_name: Name of duplicate file
            duplicate_info: Dict of duplicate IDs
        """
        if duplicate_name in duplicate_info and duplicate_info[duplicate_name]:
            for entry_id, _ in duplicate_info[duplicate_name]:
                self.entries_to_delete.append(entry_id)
                if not self.out.quiet:
                    self.out.info(
                        f"Will delete existing '{duplicate_name}' "
                        f"(ID: {entry_id}) before upload"
                    )
        else:
            # Fall back to searching if we don't have the info
            self._search_and_mark_for_deletion(duplicate_name)

    def _search_and_mark_for_deletion(self, duplicate_name: str) -> None:
        """Search for duplicate and mark for deletion.

        Args:
            duplicate_name: Name of duplicate file
        """
        try:
            matching_entries = self.entries_manager.search_by_name(
                duplicate_name, exact_match=True
            )

            if matching_entries:
                for entry in matching_entries:
                    self.entries_to_delete.append(entry.id)
                    if not self.out.quiet:
                        self.out.info(
                            f"Will delete existing '{duplicate_name}' "
                            f"(ID: {entry.id}) before upload"
                        )
            else:
                if not self.out.quiet:
                    self.out.warning(
                        f"Could not find exact match for '{duplicate_name}' "
                        "to delete - will attempt upload anyway"
                    )
        except DrimeAPIError as e:
            if not self.out.quiet:
                self.out.warning(
                    f"Could not search for existing '{duplicate_name}': {e}"
                )
                self.out.warning("Will attempt upload anyway")

    def delete_marked_entries(self) -> bool:
        """Delete entries marked for replacement.

        Returns:
            True if successful or no entries to delete, False on error
        """
        if not self.entries_to_delete:
            return True

        try:
            total_entries = len(self.entries_to_delete)
            if not self.out.quiet:
                self.out.info(f"Moving {total_entries} existing entries to trash...")

            # Batch delete operations to avoid API limits
            # Use smaller batches (10) to be safe
            batch_size = 10
            deleted_count = 0

            for i in range(0, total_entries, batch_size):
                batch = self.entries_to_delete[i : i + batch_size]
                self.client.delete_file_entries(batch, delete_forever=False)
                deleted_count += len(batch)

                if not self.out.quiet and total_entries > batch_size:
                    self.out.info(f"  Deleted {deleted_count}/{total_entries}...")

            if not self.out.quiet:
                self.out.success(f"✓ Moved {total_entries} entries to trash")
            return True
        except DrimeAPIError as e:
            self.out.error(f"Failed to delete existing entries: {e}")
            self.out.error("Aborting upload to avoid conflicts")
            return False

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
