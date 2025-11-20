"""Duplicate file detection and handling utilities."""

from pathlib import Path, PurePosixPath
from typing import Optional

import click

from .api import DrimeClient
from .exceptions import DrimeAPIError
from .models import FileEntriesResult
from .output import OutputFormatter


class DuplicateHandler:
    """Handles duplicate file detection and resolution during uploads."""

    def __init__(
        self,
        client: DrimeClient,
        out: OutputFormatter,
        workspace_id: int,
        on_duplicate: str = "ask",
    ):
        """Initialize duplicate handler.

        Args:
            client: Drime API client
            out: Output formatter
            workspace_id: Workspace ID
            on_duplicate: Action for duplicates ('ask', 'replace', 'rename', 'skip')
        """
        self.client = client
        self.out = out
        self.workspace_id = workspace_id
        self.on_duplicate = on_duplicate
        # Set when user chooses or from on_duplicate if not 'ask'
        self.chosen_action = None
        self.apply_to_all = on_duplicate != "ask"

        # If not asking, set the action directly
        if self.apply_to_all:
            self.chosen_action = on_duplicate

        self.files_to_skip: set[str] = set()
        self.rename_map: dict[str, str] = {}
        self.entries_to_delete: list[int] = []

    def validate_and_handle_duplicates(
        self, files_to_upload: list[tuple[Path, str]]
    ) -> None:
        """Validate uploads and handle duplicates.

        Args:
            files_to_upload: List of (file_path, relative_path) tuples
        """
        # Prepare validation files
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
                file_duplicates = self._check_file_duplicates_in_folders(
                    files_to_upload, filtered_folders
                )
                duplicates.extend(file_duplicates)
        except DrimeAPIError:
            # If validation fails, continue without duplicate detection
            duplicates = []

        if not duplicates:
            return

        # Look up IDs for all duplicates
        duplicate_info = self._lookup_duplicate_ids(duplicates)

        # Show duplicate warning
        if not self.out.quiet:
            self.out.warning(f"\nFound {len(duplicates)} duplicate(s):")
            for dup in duplicates:
                if dup in duplicate_info and duplicate_info[dup]:
                    ids_str = ", ".join(f"ID: {id}" for id, _ in duplicate_info[dup])
                    self.out.warning(f"  • {dup} ({ids_str})")
                else:
                    self.out.warning(f"  • {dup}")
            self.out.print("")

        # Handle each duplicate
        for duplicate_name in duplicates:
            self._handle_single_duplicate(
                duplicate_name, duplicate_info, files_to_upload
            )

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

        # Filter duplicates
        duplicates_to_keep = []
        for dup_name in duplicates:
            # Skip if it's a folder in our upload paths
            if dup_name in folders_in_upload:
                continue

            # Check if the duplicate is an existing folder on the server
            is_folder = self._is_existing_folder(dup_name)

            # Only keep non-folder duplicates
            if not is_folder:
                duplicates_to_keep.append(dup_name)

        return duplicates_to_keep

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
        for folder_name, files in files_by_folder.items():
            try:
                # Get the folder ID
                search_result = self.client.get_file_entries(
                    query=folder_name, workspace_id=self.workspace_id
                )
                if not search_result or not search_result.get("data"):
                    continue

                folder_entries = FileEntriesResult.from_api_response(search_result)
                folder_entry = None
                for entry in folder_entries.entries:
                    if entry.name == folder_name and entry.is_folder:
                        folder_entry = entry
                        break

                if not folder_entry:
                    continue

                # Get files in this folder recursively
                existing_files = self._get_files_in_folder_recursive(folder_entry.id)

                # Check each file we're uploading
                for _, rel_path in files:
                    # Get the path relative to the top folder
                    rel_to_folder = str(
                        PurePosixPath(rel_path).relative_to(folder_name)
                    )

                    if rel_to_folder in existing_files:
                        # Just add the filename, not the full path
                        file_name = PurePosixPath(rel_path).name
                        if file_name not in file_duplicates:
                            file_duplicates.append(file_name)

            except (DrimeAPIError, ValueError):
                # If we can't check, skip it
                pass

        return file_duplicates

    def _get_files_in_folder_recursive(
        self, folder_id: int, visited: Optional[set[int]] = None
    ) -> set[str]:
        """Get all file paths in a folder recursively.

        Args:
            folder_id: Folder ID to search
            visited: Set of visited folder IDs to prevent infinite recursion

        Returns:
            Set of relative file paths within the folder
        """
        if visited is None:
            visited = set()

        # Prevent infinite recursion
        if folder_id in visited:
            return set()
        visited.add(folder_id)

        files = set()

        try:
            result = self.client.get_file_entries(
                parent_ids=[folder_id], workspace_id=self.workspace_id
            )
            entries = FileEntriesResult.from_api_response(result)

            for entry in entries.entries:
                if entry.is_folder:
                    # Recursively get files in subfolder
                    subfolder_files = self._get_files_in_folder_recursive(
                        entry.id, visited
                    )
                    # Prefix with folder name
                    for f in subfolder_files:
                        files.add(f"{entry.name}/{f}")
                else:
                    files.add(entry.name)

        except DrimeAPIError:
            pass

        return files

    def _is_existing_folder(self, name: str) -> bool:
        """Check if name is an existing folder on server.

        Args:
            name: File/folder name

        Returns:
            True if name is an existing folder
        """
        try:
            search_result = self.client.get_file_entries(
                query=name, workspace_id=self.workspace_id
            )
            if search_result and search_result.get("data"):
                file_entries = FileEntriesResult.from_api_response(search_result)
                # Check if any exact match is a folder
                for entry in file_entries.entries:
                    if entry.name == name and entry.is_folder:
                        return True
        except DrimeAPIError:
            pass
        return False

    def _lookup_duplicate_ids(
        self, duplicates: list[str]
    ) -> dict[str, list[tuple[int, Optional[str]]]]:
        """Look up IDs for all duplicates.

        Args:
            duplicates: List of duplicate names

        Returns:
            Dict mapping duplicate name to list of (id, path) tuples
        """
        duplicate_info = {}
        for dup_name in duplicates:
            try:
                search_result = self.client.get_file_entries(
                    query=dup_name,
                    workspace_id=self.workspace_id,
                )
                if search_result and search_result.get("data"):
                    file_entries = FileEntriesResult.from_api_response(search_result)
                    # Find exact matches (case-sensitive)
                    matching_entries = [
                        e for e in file_entries.entries if e.name == dup_name
                    ]
                    if matching_entries:
                        duplicate_info[dup_name] = [
                            (e.id, e.path if hasattr(e, "path") else None)
                            for e in matching_entries
                        ]
            except DrimeAPIError:
                # If we can't look up the ID, continue without it
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
                default="rename",
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
            search_result = self.client.get_file_entries(
                query=duplicate_name,
                workspace_id=self.workspace_id,
            )

            if search_result and search_result.get("data"):
                file_entries = FileEntriesResult.from_api_response(search_result)

                # Find exact matches (case-sensitive)
                matching_entries = [
                    e for e in file_entries.entries if e.name == duplicate_name
                ]

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
            else:
                if not self.out.quiet:
                    self.out.warning(
                        f"Could not search for existing '{duplicate_name}' "
                        "- will attempt upload anyway"
                    )
        except DrimeAPIError as e:
            self.out.warning(f"Could not search for existing '{duplicate_name}': {e}")
            self.out.warning("Will attempt upload anyway")

    def delete_marked_entries(self) -> bool:
        """Delete entries marked for replacement.

        Returns:
            True if successful or no entries to delete, False on error
        """
        if not self.entries_to_delete:
            return True

        try:
            if not self.out.quiet:
                self.out.info(
                    f"Moving {len(self.entries_to_delete)} existing entries to trash..."
                )
            self.client.delete_file_entries(
                self.entries_to_delete, delete_forever=False
            )
            if not self.out.quiet:
                self.out.success(
                    f"✓ Moved {len(self.entries_to_delete)} entries to trash"
                )
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
            Updated path with renames applied
        """
        upload_path = rel_path
        path_obj = Path(rel_path)

        # Check if the filename needs renaming
        if path_obj.name in self.rename_map:
            new_filename = self.rename_map[path_obj.name]
            if path_obj.parent != Path("."):
                upload_path = str(path_obj.parent / new_filename)
            else:
                upload_path = new_filename

        # Check if any parent folder in the path needs renaming
        parts = list(path_obj.parts)
        renamed_parts = [self.rename_map.get(part, part) for part in parts]
        if renamed_parts != list(parts):
            upload_path = str(Path(*renamed_parts))

        return upload_path
