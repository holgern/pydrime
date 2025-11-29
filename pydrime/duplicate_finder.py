"""Utilities for finding duplicate files in Drime Cloud storage."""

import re
from collections import defaultdict
from typing import Optional

from .file_entries_manager import FileEntriesManager
from .models import FileEntry
from .output import OutputFormatter


def get_base_name(filename: str) -> str:
    """Extract base name from a filename, removing duplicate suffixes like (1), (2).

    Examples:
        "test.txt" -> "test.txt"
        "test (1).txt" -> "test.txt"
        "test (2).txt" -> "test.txt"
        "test (1) (2).txt" -> "test.txt"
        "document (copy).txt" -> "document (copy).txt"  # Only removes numeric suffixes

    Args:
        filename: The filename to process

    Returns:
        The base filename without numeric duplicate suffixes
    """
    # Pattern matches " (N)" where N is one or more digits, before the extension
    # We need to handle files with and without extensions
    pattern = r"^(.+?)(?: \(\d+\))+(\.[^.]+)?$"
    match = re.match(pattern, filename)
    if match:
        base = match.group(1)
        ext = match.group(2) or ""
        return base + ext
    return filename


class DuplicateFileFinder:
    """Finds duplicate files with same name but different IDs in the same folder."""

    def __init__(
        self,
        entries_manager: FileEntriesManager,
        out: OutputFormatter,
    ):
        """Initialize duplicate file finder.

        Args:
            entries_manager: File entries manager for fetching files
            out: Output formatter for messages
        """
        self.entries_manager = entries_manager
        self.out = out

    def find_duplicates(
        self, folder_id: Optional[int] = None, recursive: bool = False
    ) -> dict[str, list[FileEntry]]:
        """Find duplicate files in a folder.

        Duplicates are identified by:
        1. Files with the exact same name in the same folder but different IDs
        2. Files with rename suffixes like "file (1).txt" matching "file.txt"

        Args:
            folder_id: Folder ID to scan (None for root)
            recursive: Whether to scan recursively into subfolders

        Returns:
            Dictionary mapping unique keys to lists of duplicate FileEntry objects.
            Only includes entries where there are 2+ files with the same base name
            in the same folder.
        """
        if not self.out.quiet:
            self.out.info("Scanning for duplicate files...")

        duplicates: dict[str, list[FileEntry]] = {}

        if recursive:
            # Scan each folder individually to find duplicates within each folder
            self._scan_folder_recursive(folder_id, duplicates)
        else:
            # Scan only the specified folder
            self._scan_single_folder(folder_id, duplicates)

        if not self.out.quiet:
            total_duplicates = sum(len(entries) - 1 for entries in duplicates.values())
            self.out.info(
                f"Found {len(duplicates)} duplicate file groups "
                f"({total_duplicates} files to remove)"
            )

        return duplicates

    def _scan_single_folder(
        self,
        folder_id: Optional[int],
        duplicates: dict[str, list[FileEntry]],
        folder_name: Optional[str] = None,
    ) -> None:
        """Scan a single folder for duplicates.

        Args:
            folder_id: Folder ID to scan (None for root)
            duplicates: Dictionary to store found duplicates
            folder_name: Display name of the folder being scanned
        """
        if not self.out.quiet:
            if folder_name:
                self.out.info(f"Checking folder: {folder_name}")
            elif folder_id is None:
                self.out.info("Checking folder: / (root)")
            else:
                self.out.info(f"Checking folder: ID={folder_id}")

        entries = self.entries_manager.get_all_in_folder(
            folder_id=folder_id, use_cache=False
        )

        # Filter out folders, only keep files
        files = [entry for entry in entries if not entry.is_folder]

        # Group by (base_name, parent_id) - files with same base name in same folder
        # Base name strips suffixes like (1), (2) etc.
        file_groups: dict[tuple[str, Optional[int]], list[FileEntry]] = defaultdict(
            list
        )

        for file_entry in files:
            base_name = get_base_name(file_entry.name)
            key = (base_name, file_entry.parent_id)
            file_groups[key].append(file_entry)

        # Filter to only groups with duplicates (2+ files with same base name)
        for key, entries in file_groups.items():
            if len(entries) > 1:
                # Verify they have different IDs (true duplicates)
                unique_ids = {e.id for e in entries}
                if len(unique_ids) > 1:
                    base_name, parent_id = key
                    display_key = f"{base_name} in folder_id={parent_id}"
                    duplicates[display_key] = entries

    def _scan_folder_recursive(
        self,
        folder_id: Optional[int],
        duplicates: dict[str, list[FileEntry]],
        visited: Optional[set[int]] = None,
        folder_path: str = "/",
    ) -> None:
        """Recursively scan folders for duplicates.

        Args:
            folder_id: Folder ID to scan (None for root)
            duplicates: Dictionary to store found duplicates
            visited: Set of visited folder IDs (for cycle detection)
            folder_path: Current folder path for display
        """
        if visited is None:
            visited = set()

        # Prevent infinite recursion
        if folder_id is not None and folder_id in visited:
            return
        if folder_id is not None:
            visited.add(folder_id)

        if not self.out.quiet:
            self.out.info(f"Checking folder: {folder_path}")

        # Get all entries in this folder
        entries = self.entries_manager.get_all_in_folder(
            folder_id=folder_id, use_cache=False
        )

        # Separate files and folders
        files = [entry for entry in entries if not entry.is_folder]
        folders = [entry for entry in entries if entry.is_folder]

        # Find duplicates in current folder using base names
        file_groups: dict[tuple[str, Optional[int]], list[FileEntry]] = defaultdict(
            list
        )

        for file_entry in files:
            base_name = get_base_name(file_entry.name)
            key = (base_name, file_entry.parent_id)
            file_groups[key].append(file_entry)

        # Add groups with duplicates (2+ files with same base name and different IDs)
        for key, entries_list in file_groups.items():
            if len(entries_list) > 1:
                unique_ids = {e.id for e in entries_list}
                if len(unique_ids) > 1:
                    base_name, parent_id = key
                    display_key = f"{base_name} in folder_id={parent_id}"
                    duplicates[display_key] = entries_list

        # Recursively scan subfolders
        for folder in folders:
            subfolder_path = (
                f"{folder_path}{folder.name}/"
                if folder_path == "/"
                else f"{folder_path}/{folder.name}"
            )
            self._scan_folder_recursive(folder.id, duplicates, visited, subfolder_path)

    def display_duplicates(
        self, duplicates: dict[str, list[FileEntry]], show_details: bool = True
    ) -> None:
        """Display duplicate files to the user.

        Args:
            duplicates: Dictionary of duplicate file groups
            show_details: Whether to show detailed information
        """
        if not duplicates:
            self.out.info("No duplicate files found.")
            return

        self.out.info(f"\nFound {len(duplicates)} groups of duplicate files:\n")

        for display_key, entries in duplicates.items():
            # Sort by ID so older files come first
            entries_sorted = sorted(entries, key=lambda e: e.id)

            self.out.info(f"Duplicate group: {display_key}")
            self.out.info(f"  {len(entries)} copies found:")

            for i, entry in enumerate(entries_sorted, start=1):
                created = entry.created_at[:10] if entry.created_at else "unknown"
                status = "★ KEEP (oldest)" if i == 1 else "  DELETE"
                self.out.info(
                    f"    {status} - ID: {entry.id}, Created: {created}, "
                    f"Path: {entry.path or '(no path)'}"
                )

            self.out.info("")

    def get_entries_to_delete(
        self, duplicates: dict[str, list[FileEntry]], keep_oldest: bool = True
    ) -> list[FileEntry]:
        """Get list of duplicate entries to delete.

        Args:
            duplicates: Dictionary of duplicate file groups
            keep_oldest: If True, keep the oldest file (by ID). If False, keep newest.

        Returns:
            List of FileEntry objects to delete
        """
        to_delete = []

        for entries in duplicates.values():
            # Sort by ID
            entries_sorted = sorted(entries, key=lambda e: e.id)

            if keep_oldest:
                # Keep first (oldest), delete rest
                to_delete.extend(entries_sorted[1:])
            else:
                # Keep last (newest), delete rest
                to_delete.extend(entries_sorted[:-1])

        return to_delete
