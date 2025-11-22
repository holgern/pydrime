"""Tests for duplicate file finder."""

from unittest.mock import MagicMock

from pydrime.duplicate_finder import DuplicateFileFinder
from pydrime.models import FileEntry
from pydrime.output import OutputFormatter


class TestDuplicateFileFinder:
    """Tests for DuplicateFileFinder class."""

    def test_find_duplicates_no_duplicates(self):
        """Test finding duplicates when there are none."""
        mock_entries_manager = MagicMock()
        mock_entries_manager.get_all_in_folder.return_value = [
            FileEntry(
                id=1,
                name="file1.txt",
                file_name="file1.txt",
                mime="text/plain",
                file_size=100,
                parent_id=0,
                created_at="2023-01-01",
                type="text",
                extension="txt",
                hash="hash1",
                url="",
            ),
            FileEntry(
                id=2,
                name="file2.txt",
                file_name="file2.txt",
                mime="text/plain",
                file_size=200,
                parent_id=0,
                created_at="2023-01-02",
                type="text",
                extension="txt",
                hash="hash2",
                url="",
            ),
        ]

        out = OutputFormatter(json_output=False, quiet=False)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = finder.find_duplicates(folder_id=None, recursive=False)

        assert len(duplicates) == 0

    def test_find_duplicates_with_duplicates(self):
        """Test finding duplicates when there are some."""
        mock_entries_manager = MagicMock()
        mock_entries_manager.get_all_in_folder.return_value = [
            FileEntry(
                id=1,
                name="file.txt",
                file_name="file.txt",
                mime="text/plain",
                file_size=100,
                parent_id=0,
                created_at="2023-01-01",
                type="text",
                extension="txt",
                hash="hash1",
                url="",
            ),
            FileEntry(
                id=2,
                name="file.txt",
                file_name="file.txt",
                mime="text/plain",
                file_size=100,
                parent_id=0,
                created_at="2023-01-02",
                type="text",
                extension="txt",
                hash="hash2",
                url="",
            ),
            FileEntry(
                id=3,
                name="file.txt",
                file_name="file.txt",
                mime="text/plain",
                file_size=100,
                parent_id=0,
                created_at="2023-01-03",
                type="text",
                extension="txt",
                hash="hash3",
                url="",
            ),
        ]

        out = OutputFormatter(json_output=False, quiet=True)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = finder.find_duplicates(folder_id=None, recursive=False)

        assert len(duplicates) == 1
        # Should have all 3 files in the duplicate group
        duplicate_group = list(duplicates.values())[0]
        assert len(duplicate_group) == 3
        assert all(entry.name == "file.txt" for entry in duplicate_group)
        assert all(entry.file_size == 100 for entry in duplicate_group)

    def test_find_duplicates_filters_folders(self):
        """Test that folders are excluded from duplicate detection."""
        mock_entries_manager = MagicMock()
        mock_entries_manager.get_all_in_folder.return_value = [
            FileEntry(
                id=1,
                name="Documents",
                file_name="Documents",
                mime="",
                file_size=0,
                parent_id=0,
                created_at="2023-01-01",
                type="folder",
                extension=None,
                hash="hash1",
                url="",
            ),
            FileEntry(
                id=2,
                name="file.txt",
                file_name="file.txt",
                mime="text/plain",
                file_size=100,
                parent_id=0,
                created_at="2023-01-01",
                type="text",
                extension="txt",
                hash="hash2",
                url="",
            ),
        ]

        out = OutputFormatter(json_output=False, quiet=True)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = finder.find_duplicates(folder_id=None, recursive=False)

        assert len(duplicates) == 0

    def test_find_duplicates_different_parent_ids(self):
        """Test that files with same name/size but different parents are not duplicates."""
        mock_entries_manager = MagicMock()
        mock_entries_manager.get_all_in_folder.return_value = [
            FileEntry(
                id=1,
                name="file.txt",
                file_name="file.txt",
                mime="text/plain",
                file_size=100,
                parent_id=0,
                created_at="2023-01-01",
                type="text",
                extension="txt",
                hash="hash1",
                url="",
            ),
            FileEntry(
                id=2,
                name="file.txt",
                file_name="file.txt",
                mime="text/plain",
                file_size=100,
                parent_id=10,  # Different parent
                created_at="2023-01-02",
                type="text",
                extension="txt",
                hash="hash2",
                url="",
            ),
        ]

        out = OutputFormatter(json_output=False, quiet=True)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = finder.find_duplicates(folder_id=None, recursive=False)

        assert len(duplicates) == 0

    def test_get_entries_to_delete_keep_oldest(self):
        """Test getting entries to delete, keeping oldest."""
        mock_entries_manager = MagicMock()
        out = OutputFormatter(json_output=False, quiet=True)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = {
            "file.txt (100 bytes) in folder_id=0": [
                FileEntry(
                    id=1,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-01",
                    type="text",
                    extension="txt",
                    hash="hash1",
                    url="",
                ),
                FileEntry(
                    id=2,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-02",
                    type="text",
                    extension="txt",
                    hash="hash2",
                    url="",
                ),
                FileEntry(
                    id=3,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-03",
                    type="text",
                    extension="txt",
                    hash="hash3",
                    url="",
                ),
            ]
        }

        to_delete = finder.get_entries_to_delete(duplicates, keep_oldest=True)

        assert len(to_delete) == 2
        assert to_delete[0].id == 2
        assert to_delete[1].id == 3

    def test_get_entries_to_delete_keep_newest(self):
        """Test getting entries to delete, keeping newest."""
        mock_entries_manager = MagicMock()
        out = OutputFormatter(json_output=False, quiet=True)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = {
            "file.txt (100 bytes) in folder_id=0": [
                FileEntry(
                    id=1,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-01",
                    type="text",
                    extension="txt",
                    hash="hash1",
                    url="",
                ),
                FileEntry(
                    id=2,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-02",
                    type="text",
                    extension="txt",
                    hash="hash2",
                    url="",
                ),
                FileEntry(
                    id=3,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-03",
                    type="text",
                    extension="txt",
                    hash="hash3",
                    url="",
                ),
            ]
        }

        to_delete = finder.get_entries_to_delete(duplicates, keep_oldest=False)

        assert len(to_delete) == 2
        assert to_delete[0].id == 1
        assert to_delete[1].id == 2

    def test_display_duplicates_no_duplicates(self):
        """Test displaying when there are no duplicates."""
        mock_entries_manager = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        # Should not raise an error
        finder.display_duplicates({})

    def test_display_duplicates_with_duplicates(self):
        """Test displaying duplicates."""
        mock_entries_manager = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = {
            "file.txt (100 bytes) in folder_id=0": [
                FileEntry(
                    id=1,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-01",
                    type="text",
                    extension="txt",
                    hash="hash1",
                    url="",
                    path="/file.txt",
                ),
                FileEntry(
                    id=2,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-02",
                    type="text",
                    extension="txt",
                    hash="hash2",
                    url="",
                    path="/file.txt",
                ),
            ]
        }

        # Should not raise an error
        finder.display_duplicates(duplicates)

    def test_find_duplicates_recursive(self):
        """Test finding duplicates recursively."""
        mock_entries_manager = MagicMock()
        mock_entries_manager.get_all_recursive.return_value = [
            (
                FileEntry(
                    id=1,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-01",
                    type="text",
                    extension="txt",
                    hash="hash1",
                    url="",
                ),
                "file.txt",
            ),
            (
                FileEntry(
                    id=2,
                    name="file.txt",
                    file_name="file.txt",
                    mime="text/plain",
                    file_size=100,
                    parent_id=0,
                    created_at="2023-01-02",
                    type="text",
                    extension="txt",
                    hash="hash2",
                    url="",
                ),
                "subfolder/file.txt",
            ),
        ]

        out = OutputFormatter(json_output=False, quiet=True)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = finder.find_duplicates(folder_id=None, recursive=True)

        assert len(duplicates) == 1
        # Should have both files in the duplicate group
        duplicate_group = list(duplicates.values())[0]
        assert len(duplicate_group) == 2
