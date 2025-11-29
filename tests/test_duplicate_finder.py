"""Tests for duplicate file finder."""

from unittest.mock import MagicMock

import pytest

from pydrime.duplicate_finder import DuplicateFileFinder, get_base_name
from pydrime.models import FileEntry
from pydrime.output import OutputFormatter


class TestGetBaseName:
    """Tests for the get_base_name function."""

    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("test.txt", "test.txt"),
            ("test (1).txt", "test.txt"),
            ("test (2).txt", "test.txt"),
            ("test (10).txt", "test.txt"),
            ("test (1) (2).txt", "test.txt"),
            ("document (copy).txt", "document (copy).txt"),  # Only removes numeric
            ("file", "file"),  # No extension
            ("file (1)", "file"),  # No extension with suffix
            ("my.file.txt", "my.file.txt"),  # Multiple dots
            ("my.file (1).txt", "my.file.txt"),  # Multiple dots with suffix
            ("photo (1) (2) (3).jpg", "photo.jpg"),  # Multiple suffixes
            ("test (abc).txt", "test (abc).txt"),  # Non-numeric suffix preserved
        ],
    )
    def test_get_base_name(self, filename: str, expected: str):
        """Test get_base_name extracts base name correctly."""
        assert get_base_name(filename) == expected


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

    def test_find_duplicates_with_renamed_files(self):
        """Test finding duplicates with renamed suffixes like (1), (2)."""
        mock_entries_manager = MagicMock()
        mock_entries_manager.get_all_in_folder.return_value = [
            FileEntry(
                id=1,
                name="document.pdf",
                file_name="document.pdf",
                mime="application/pdf",
                file_size=500,
                parent_id=0,
                created_at="2023-01-01",
                type="document",
                extension="pdf",
                hash="hash1",
                url="",
            ),
            FileEntry(
                id=2,
                name="document (1).pdf",
                file_name="document (1).pdf",
                mime="application/pdf",
                file_size=500,
                parent_id=0,
                created_at="2023-01-02",
                type="document",
                extension="pdf",
                hash="hash2",
                url="",
            ),
            FileEntry(
                id=3,
                name="document (2).pdf",
                file_name="document (2).pdf",
                mime="application/pdf",
                file_size=500,
                parent_id=0,
                created_at="2023-01-03",
                type="document",
                extension="pdf",
                hash="hash3",
                url="",
            ),
        ]

        out = OutputFormatter(json_output=False, quiet=True)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = finder.find_duplicates(folder_id=None, recursive=False)

        # All 3 files should be grouped as duplicates (same base name "document.pdf")
        assert len(duplicates) == 1
        duplicate_group = list(duplicates.values())[0]
        assert len(duplicate_group) == 3
        # Verify all three files are included
        names = {entry.name for entry in duplicate_group}
        assert names == {"document.pdf", "document (1).pdf", "document (2).pdf"}

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
        """Test files with same name/size but different parents aren't duplicates."""
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
        """Test finding duplicates recursively across subfolders."""
        mock_entries_manager = MagicMock()

        # Root folder contains a subfolder and some files
        root_contents = [
            FileEntry(
                id=100,
                name="subfolder",
                file_name="subfolder",
                mime="",
                file_size=0,
                parent_id=None,
                created_at="2023-01-01",
                type="folder",
                extension=None,
                hash="",
                url="",
            ),
            FileEntry(
                id=1,
                name="file.txt",
                file_name="file.txt",
                mime="text/plain",
                file_size=100,
                parent_id=None,
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
                parent_id=None,
                created_at="2023-01-02",
                type="text",
                extension="txt",
                hash="hash2",
                url="",
            ),
        ]

        # Subfolder contains duplicates too
        subfolder_contents = [
            FileEntry(
                id=3,
                name="doc.pdf",
                file_name="doc.pdf",
                mime="application/pdf",
                file_size=200,
                parent_id=100,
                created_at="2023-01-01",
                type="document",
                extension="pdf",
                hash="hash3",
                url="",
            ),
            FileEntry(
                id=4,
                name="doc.pdf",
                file_name="doc.pdf",
                mime="application/pdf",
                file_size=200,
                parent_id=100,
                created_at="2023-01-02",
                type="document",
                extension="pdf",
                hash="hash4",
                url="",
            ),
        ]

        # Mock get_all_in_folder to return different results based on folder_id
        def get_folder_contents(folder_id=None, use_cache=False):
            if folder_id is None:
                return root_contents
            elif folder_id == 100:
                return subfolder_contents
            return []

        mock_entries_manager.get_all_in_folder.side_effect = get_folder_contents

        out = OutputFormatter(json_output=False, quiet=True)
        finder = DuplicateFileFinder(mock_entries_manager, out)

        duplicates = finder.find_duplicates(folder_id=None, recursive=True)

        # Should find 2 duplicate groups: one in root, one in subfolder
        assert len(duplicates) == 2

        # Verify each group has 2 entries
        for group in duplicates.values():
            assert len(group) == 2
