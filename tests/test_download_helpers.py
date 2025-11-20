"""Tests for download helper functions."""

from unittest.mock import MagicMock

import pytest

from pydrime.download_helpers import (
    download_file_with_progress,
    download_folder_recursive,
    download_single_file,
    get_entry_from_hash,
    get_unique_filename,
    resolve_identifier_to_hash,
)
from pydrime.exceptions import DrimeAPIError, DrimeNotFoundError
from pydrime.models import FileEntry


@pytest.fixture
def mock_client():
    """Create a mock DrimeClient."""
    return MagicMock()


@pytest.fixture
def mock_output():
    """Create a mock OutputFormatter."""
    out = MagicMock()
    out.quiet = False
    return out


class TestResolveIdentifierToHash:
    """Tests for resolve_identifier_to_hash function."""

    def test_resolve_by_name(self, mock_client, mock_output):
        """Test resolving entry by name."""
        mock_client.resolve_entry_identifier.return_value = 12345

        result = resolve_identifier_to_hash(
            mock_client,
            "myfile.txt",
            current_folder=100,
            workspace=1,
            out=mock_output,
        )

        assert result == "MTIzNDV8"  # normalize_to_hash("12345")
        mock_client.resolve_entry_identifier.assert_called_once_with(
            identifier="myfile.txt",
            parent_id=100,
            workspace_id=1,
        )
        mock_output.info.assert_called_once()

    def test_resolve_by_id(self, mock_client, mock_output):
        """Test resolving entry by numeric ID."""
        mock_client.resolve_entry_identifier.return_value = 12345

        result = resolve_identifier_to_hash(
            mock_client,
            "12345",
            current_folder=None,
            workspace=1,
            out=mock_output,
        )

        assert result == "MTIzNDV8"
        # Should not log when using numeric ID
        mock_output.info.assert_not_called()

    def test_resolve_file_id(self, mock_client, mock_output):
        """Test resolving file ID format."""
        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )

        result = resolve_identifier_to_hash(
            mock_client,
            "12345",  # Numeric ID
            current_folder=None,
            workspace=1,
            out=mock_output,
        )

        # Should convert ID to hash
        assert result == "MTIzNDV8"
        # Should log when converting ID to hash
        mock_output.info.assert_called_once()

    def test_resolve_hash_directly(self, mock_client, mock_output):
        """Test passing hash directly."""
        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )

        result = resolve_identifier_to_hash(
            mock_client,
            "abcd1234",
            current_folder=None,
            workspace=1,
            out=mock_output,
        )

        # Should return as-is if not found by name and not a numeric ID
        assert result == "abcd1234"

    def test_resolve_quiet_mode(self, mock_client, mock_output):
        """Test quiet mode suppresses info messages."""
        mock_output.quiet = True
        mock_client.resolve_entry_identifier.return_value = 12345

        result = resolve_identifier_to_hash(
            mock_client,
            "myfile.txt",
            current_folder=None,
            workspace=1,
            out=mock_output,
        )

        assert result == "MTIzNDV8"
        mock_output.info.assert_not_called()


class TestGetEntryFromHash:
    """Tests for get_entry_from_hash function."""

    def test_get_file_entry(self, mock_client, mock_output):
        """Test getting a file entry."""
        file_data = {
            "id": 123,
            "hash": "abc123",
            "name": "test.txt",
            "is_folder": False,
        }
        mock_client.get_file_entries.return_value = {"data": [file_data]}

        result = get_entry_from_hash(mock_client, "abc123", "test.txt", mock_output)

        assert result is not None
        assert result.name == "test.txt"
        assert result.hash == "abc123"
        mock_output.error.assert_not_called()

    def test_get_folder_entry(self, mock_client, mock_output):
        """Test getting a folder entry."""
        folder_data = {
            "id": 456,
            "hash": "def456",
            "name": "myfolder",
            "type": "folder",  # This is what makes is_folder True
        }
        # First call returns empty (no files matching query)
        # Second call returns folder
        mock_client.get_file_entries.side_effect = [
            {"data": []},
            {"folder": folder_data},
        ]

        result = get_entry_from_hash(mock_client, "def456", "myfolder", mock_output)

        assert result is not None
        assert result.name == "myfolder"
        assert result.is_folder is True

    def test_entry_not_found(self, mock_client, mock_output):
        """Test handling entry not found."""
        mock_client.get_file_entries.side_effect = [
            {"data": []},  # No file found
            {},  # No folder found
        ]

        result = get_entry_from_hash(
            mock_client, "notfound", "missing.txt", mock_output
        )

        assert result is None
        mock_output.error.assert_called_once_with("Entry not found: missing.txt")


class TestGetUniqueFilename:
    """Tests for get_unique_filename function."""

    def test_no_conflict(self, tmp_path):
        """Test when file doesn't exist."""
        file_path = tmp_path / "test.txt"

        result = get_unique_filename(file_path)

        assert result == file_path

    def test_single_conflict(self, tmp_path):
        """Test with one existing file."""
        file_path = tmp_path / "test.txt"
        file_path.touch()

        result = get_unique_filename(file_path)

        assert result == tmp_path / "test (1).txt"
        assert not result.exists()

    def test_multiple_conflicts(self, tmp_path):
        """Test with multiple existing files."""
        base = tmp_path / "test.txt"
        base.touch()
        (tmp_path / "test (1).txt").touch()
        (tmp_path / "test (2).txt").touch()

        result = get_unique_filename(base)

        assert result == tmp_path / "test (3).txt"
        assert not result.exists()

    def test_with_extension(self, tmp_path):
        """Test file with extension."""
        file_path = tmp_path / "document.pdf"
        file_path.touch()

        result = get_unique_filename(file_path)

        assert result == tmp_path / "document (1).pdf"

    def test_without_extension(self, tmp_path):
        """Test file without extension."""
        file_path = tmp_path / "README"
        file_path.touch()

        result = get_unique_filename(file_path)

        assert result == tmp_path / "README (1)"


class TestDownloadFileWithProgress:
    """Tests for download_file_with_progress function."""

    def test_download_with_progress_bar(self, mock_client, mock_output, tmp_path):
        """Test download with progress bar enabled."""
        output_path = tmp_path / "test.txt"
        mock_client.download_file.return_value = output_path

        result = download_file_with_progress(
            mock_client,
            "abc123",
            output_path,
            "test.txt",
            show_progress=True,
            no_progress=False,
            out=mock_output,
        )

        assert result == output_path
        mock_client.download_file.assert_called_once()
        # Progress callback should be provided
        call_kwargs = mock_client.download_file.call_args[1]
        assert "progress_callback" in call_kwargs

    def test_download_without_progress_bar(self, mock_client, mock_output, tmp_path):
        """Test download without progress bar."""
        output_path = tmp_path / "test.txt"
        mock_client.download_file.return_value = output_path

        result = download_file_with_progress(
            mock_client,
            "abc123",
            output_path,
            "test.txt",
            show_progress=False,
            no_progress=False,
            out=mock_output,
        )

        assert result == output_path
        mock_output.progress_message.assert_called_once_with("Downloading test.txt...")
        mock_output.success.assert_called_once()

    def test_download_no_progress_flag(self, mock_client, mock_output, tmp_path):
        """Test download with no_progress flag set."""
        output_path = tmp_path / "test.txt"
        mock_client.download_file.return_value = output_path

        result = download_file_with_progress(
            mock_client,
            "abc123",
            output_path,
            "test.txt",
            show_progress=True,
            no_progress=True,  # This should disable progress
            out=mock_output,
        )

        assert result == output_path
        # Should not show progress messages when no_progress is True
        mock_output.progress_message.assert_not_called()

    def test_download_quiet_mode(self, mock_client, mock_output, tmp_path):
        """Test download in quiet mode."""
        mock_output.quiet = True
        output_path = tmp_path / "test.txt"
        mock_client.download_file.return_value = output_path

        result = download_file_with_progress(
            mock_client,
            "abc123",
            output_path,
            "test.txt",
            show_progress=False,
            no_progress=False,
            out=mock_output,
        )

        assert result == output_path
        mock_output.success.assert_not_called()


class TestDownloadFolderRecursive:
    """Tests for download_folder_recursive function."""

    def test_download_empty_folder(self, mock_client, mock_output, tmp_path):
        """Test downloading an empty folder."""
        folder_entry = FileEntry(
            id=123,
            hash="abc123",
            name="emptyfolder",
            file_name="emptyfolder",
            mime="",
            file_size=0,
            parent_id=None,
            created_at="2024-01-01",
            type="folder",
            extension=None,
            url="",
        )

        mock_client.get_file_entries.return_value = {"data": []}

        downloaded_files = []
        download_folder_recursive(
            mock_client,
            folder_entry,
            tmp_path / "emptyfolder",
            "abc123",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        assert (tmp_path / "emptyfolder").exists()
        assert downloaded_files == []

    def test_download_folder_with_files(self, mock_client, mock_output, tmp_path):
        """Test downloading a folder with files."""
        folder_entry = FileEntry(
            id=123,
            hash="abc123",
            name="myfolder",
            file_name="myfolder",
            mime="",
            file_size=0,
            parent_id=None,
            created_at="2024-01-01",
            type="folder",
            extension=None,
            url="",
        )

        file_entry = {
            "id": 456,
            "hash": "def456",
            "name": "file.txt",
            "is_folder": False,
        }

        mock_client.get_file_entries.return_value = {"data": [file_entry]}
        mock_client.download_file.return_value = tmp_path / "myfolder" / "file.txt"

        downloaded_files = []
        download_folder_recursive(
            mock_client,
            folder_entry,
            tmp_path / "myfolder",
            "abc123",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        assert (tmp_path / "myfolder").exists()
        assert len(downloaded_files) == 1
        assert downloaded_files[0]["hash"] == "def456"

    def test_download_nested_folders(self, mock_client, mock_output, tmp_path):
        """Test downloading nested folder structure."""
        root_folder = FileEntry(
            id=100,
            hash="root",
            name="root",
            file_name="root",
            mime="",
            file_size=0,
            parent_id=None,
            created_at="2024-01-01",
            type="folder",
            extension=None,
            url="",
        )

        subfolder_data = {
            "id": 200,
            "hash": "sub",
            "name": "subfolder",
            "type": "folder",
        }

        file_data = {
            "id": 300,
            "hash": "file",
            "name": "deep.txt",
            "type": "file",
        }

        # First call: root folder contains subfolder
        # Second call: subfolder contains file
        mock_client.get_file_entries.side_effect = [
            {"data": [subfolder_data]},
            {"data": [file_data]},
        ]

        mock_client.download_file.return_value = (
            tmp_path / "root" / "subfolder" / "deep.txt"
        )

        downloaded_files = []
        download_folder_recursive(
            mock_client,
            root_folder,
            tmp_path / "root",
            "root",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        assert (tmp_path / "root").exists()
        assert (tmp_path / "root" / "subfolder").exists()
        assert len(downloaded_files) == 1

    def test_download_folder_file_conflict(self, mock_client, mock_output, tmp_path):
        """Test handling when a file exists with folder name."""
        folder_entry = FileEntry(
            id=123,
            hash="abc123",
            name="conflict",
            file_name="conflict",
            mime="",
            file_size=0,
            parent_id=None,
            created_at="2024-01-01",
            type="folder",
            extension=None,
            url="",
        )

        # Create a file with the folder name
        conflict_file = tmp_path / "conflict"
        conflict_file.touch()

        downloaded_files = []
        download_folder_recursive(
            mock_client,
            folder_entry,
            conflict_file,
            "abc123",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        # Should error and not create folder
        mock_output.error.assert_called_once()
        assert not conflict_file.is_dir()

    def test_download_folder_api_error(self, mock_client, mock_output, tmp_path):
        """Test handling API errors during folder download."""
        folder_entry = FileEntry(
            id=123,
            hash="abc123",
            name="errorfolder",
            file_name="errorfolder",
            mime="",
            file_size=0,
            parent_id=None,
            created_at="2024-01-01",
            type="folder",
            extension=None,
            url="",
        )

        mock_client.get_file_entries.side_effect = DrimeAPIError("API Error")

        downloaded_files = []
        download_folder_recursive(
            mock_client,
            folder_entry,
            tmp_path / "errorfolder",
            "abc123",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        mock_output.error.assert_called_once()
        assert "Error downloading folder contents" in str(
            mock_output.error.call_args[0][0]
        )


class TestDownloadSingleFile:
    """Tests for download_single_file function."""

    def test_download_to_directory(self, mock_client, mock_output, tmp_path):
        """Test downloading file to a directory."""
        dest_dir = tmp_path / "downloads"
        dest_dir.mkdir()

        expected_path = dest_dir / "test.txt"
        mock_client.download_file.return_value = expected_path

        downloaded_files = []
        download_single_file(
            mock_client,
            "abc123",
            "test.txt",
            dest_dir,
            "test.txt",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        assert len(downloaded_files) == 1
        assert downloaded_files[0]["hash"] == "abc123"
        assert downloaded_files[0]["path"] == str(expected_path)

    def test_download_to_specific_path(self, mock_client, mock_output, tmp_path):
        """Test downloading file to specific path."""
        dest_path = tmp_path / "custom.txt"
        mock_client.download_file.return_value = dest_path

        downloaded_files = []
        download_single_file(
            mock_client,
            "abc123",
            "test.txt",
            dest_path,
            "test.txt",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        assert len(downloaded_files) == 1

    def test_download_skip_duplicate(self, mock_client, mock_output, tmp_path):
        """Test skipping duplicate files."""
        existing_file = tmp_path / "test.txt"
        existing_file.touch()

        downloaded_files = []
        download_single_file(
            mock_client,
            "abc123",
            "test.txt",
            existing_file,
            "test.txt",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        # Should skip and not download
        mock_client.download_file.assert_not_called()
        assert len(downloaded_files) == 1
        assert downloaded_files[0]["skipped"] is True
        mock_output.info.assert_called_with(
            f"Skipped (already exists): {existing_file}"
        )

    def test_download_rename_duplicate(self, mock_client, mock_output, tmp_path):
        """Test renaming duplicate files."""
        existing_file = tmp_path / "test.txt"
        existing_file.touch()

        renamed_path = tmp_path / "test (1).txt"
        mock_client.download_file.return_value = renamed_path

        downloaded_files = []
        download_single_file(
            mock_client,
            "abc123",
            "test.txt",
            existing_file,
            "test.txt",
            downloaded_files,
            mock_output,
            on_duplicate="rename",
            no_progress=False,
        )

        # Should download with renamed path
        mock_client.download_file.assert_called_once()
        call_args = mock_client.download_file.call_args[0]
        assert call_args[1] == renamed_path

        mock_output.info.assert_called_with("Renaming to avoid duplicate: test (1).txt")

    def test_download_replace_duplicate(self, mock_client, mock_output, tmp_path):
        """Test replacing duplicate files."""
        existing_file = tmp_path / "test.txt"
        existing_file.touch()

        mock_client.download_file.return_value = existing_file

        downloaded_files = []
        download_single_file(
            mock_client,
            "abc123",
            "test.txt",
            existing_file,
            "test.txt",
            downloaded_files,
            mock_output,
            on_duplicate="replace",
            no_progress=False,
        )

        # Should download and replace
        mock_client.download_file.assert_called_once()
        assert len(downloaded_files) == 1

    def test_download_directory_conflict(self, mock_client, mock_output, tmp_path):
        """Test handling directory with same name as file."""
        # Create a directory in the parent
        parent_dir = tmp_path
        existing_dir = parent_dir / "test.txt"
        existing_dir.mkdir()

        renamed_path = parent_dir / "test (1).txt"
        mock_client.download_file.return_value = renamed_path

        downloaded_files = []
        download_single_file(
            mock_client,
            "abc123",
            "test.txt",
            parent_dir,  # Pass parent, so output_path becomes parent / "test.txt"
            "test.txt",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        # Should automatically rename when directory exists
        mock_output.info.assert_called()
        assert "Directory exists with same name" in str(
            mock_output.info.call_args[0][0]
        )

    def test_download_with_output_override(self, mock_client, mock_output, tmp_path):
        """Test download with output override."""
        override_path = tmp_path / "override.txt"
        mock_client.download_file.return_value = override_path

        downloaded_files = []
        download_single_file(
            mock_client,
            "abc123",
            "test.txt",
            None,  # No dest_path
            "test.txt",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
            output_override=str(override_path),
            single_file=True,
        )

        assert len(downloaded_files) == 1

    def test_download_api_error(self, mock_client, mock_output, tmp_path):
        """Test handling API errors during download."""
        mock_client.download_file.side_effect = DrimeAPIError("Download failed")

        downloaded_files = []
        download_single_file(
            mock_client,
            "abc123",
            "test.txt",
            tmp_path / "test.txt",
            "test.txt",
            downloaded_files,
            mock_output,
            on_duplicate="skip",
            no_progress=False,
        )

        # Should handle error gracefully
        mock_output.error.assert_called_once()
        assert "Error downloading file" in str(mock_output.error.call_args[0][0])
        assert len(downloaded_files) == 0
