"""Utilities for finding duplicate files in Drime Cloud storage."""

from collections import defaultdict
from typing import Optional

from .file_entries_manager import FileEntriesManager
from .models import FileEntry
from .output import OutputFormatter


class DuplicateFileFinder:
    """Finds duplicate files based on name, size, and parent folder."""

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

        Duplicates are identified by having identical:
        - filename (name field)
        - file size (file_size field)
        - parent folder (parent_id field)

        Args:
            folder_id: Folder ID to scan (None for root)
            recursive: Whether to scan recursively into subfolders

        Returns:
            Dictionary mapping unique keys to lists of duplicate FileEntry objects.
            Only includes entries where there are 2+ files with the same key.
        """
        if not self.out.quiet:
            self.out.info("Scanning for duplicate files...")

        # Get all files
        if recursive:
            all_entries_with_paths = self.entries_manager.get_all_recursive(
                folder_id=folder_id
            )
            all_entries = [entry for entry, _ in all_entries_with_paths]
        else:
            all_entries = self.entries_manager.get_all_in_folder(
                folder_id=folder_id, use_cache=False
            )

        # Filter out folders, only keep files
        files = [entry for entry in all_entries if not entry.is_folder]

        # Group by (name, size, parent_id)
        file_groups: dict[tuple[str, int, Optional[int]], list[FileEntry]] = (
            defaultdict(list)
        )

        for file_entry in files:
            key = (file_entry.name, file_entry.file_size, file_entry.parent_id)
            file_groups[key].append(file_entry)

        # Filter to only groups with duplicates (2+ files)
        duplicates: dict[str, list[FileEntry]] = {}
        for key, entries in file_groups.items():
            if len(entries) > 1:
                # Create a readable key for display
                name, size, parent_id = key
                display_key = f"{name} ({size} bytes) in folder_id={parent_id}"
                duplicates[display_key] = entries

        if not self.out.quiet:
            self.out.info(f"Found {len(duplicates)} duplicate file groups")

        return duplicates

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
