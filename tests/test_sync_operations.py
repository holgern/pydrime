"""Tests for pydrime/sync/operations.py."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pydrime.sync.operations import (
    LOCAL_TRASH_DIR_NAME,
    SyncOperations,
    get_local_trash_path,
    move_to_local_trash,
    rename_local_file,
)
from pydrime.sync.scanner import LocalFile, RemoteFile


class TestGetLocalTrashPath:
    """Tests for get_local_trash_path function."""

    def test_returns_correct_path(self, tmp_path: Path):
        """Test that the trash path is returned correctly."""
        result = get_local_trash_path(tmp_path)
        assert result == tmp_path / LOCAL_TRASH_DIR_NAME

    def test_returns_path_object(self, tmp_path: Path):
        """Test that the result is a Path object."""
        result = get_local_trash_path(tmp_path)
        assert isinstance(result, Path)


class TestMoveToLocalTrash:
    """Tests for move_to_local_trash function."""

    def test_moves_file_to_trash(self, tmp_path: Path):
        """Test that file is moved to trash directory."""
        # Create a test file
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        result = move_to_local_trash(test_file, tmp_path)

        # Original file should not exist
        assert not test_file.exists()
        # Result should be in trash directory
        assert result.exists()
        assert LOCAL_TRASH_DIR_NAME in str(result)
        assert result.read_text() == "test content"

    def test_preserves_relative_path(self, tmp_path: Path):
        """Test that relative path structure is preserved in trash."""
        # Create a nested file
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        test_file = subdir / "nested_file.txt"
        test_file.write_text("nested content")

        result = move_to_local_trash(test_file, tmp_path)

        # The relative path should be preserved
        assert "subdir" in str(result)
        assert result.name == "nested_file.txt"

    def test_raises_file_not_found(self, tmp_path: Path):
        """Test that FileNotFoundError is raised for non-existent file."""
        nonexistent = tmp_path / "nonexistent.txt"

        with pytest.raises(FileNotFoundError, match="File not found"):
            move_to_local_trash(nonexistent, tmp_path)

    def test_file_not_under_sync_root(self, tmp_path: Path):
        """Test handling when file is not under sync_root (lines 58-60)."""
        # Create two separate directories
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        sync_root = tmp_path / "sync"
        sync_root.mkdir()

        # Create file outside sync_root
        test_file = other_dir / "outside_file.txt"
        test_file.write_text("outside content")

        result = move_to_local_trash(test_file, sync_root)

        # File should be moved, using just the filename
        assert not test_file.exists()
        assert result.exists()
        assert result.name == "outside_file.txt"
        assert result.read_text() == "outside content"

    def test_timestamp_in_path(self, tmp_path: Path):
        """Test that timestamp is added to trash path."""
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        with patch("pydrime.sync.operations.datetime") as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20230101_120000"
            result = move_to_local_trash(test_file, tmp_path)

        assert "20230101_120000" in str(result)


class TestRenameLocalFile:
    """Tests for rename_local_file function (lines 96-108)."""

    def test_renames_file(self, tmp_path: Path):
        """Test basic file rename."""
        old_path = tmp_path / "old_name.txt"
        old_path.write_text("content")
        new_path = tmp_path / "new_name.txt"

        result = rename_local_file(old_path, new_path)

        assert result == new_path
        assert not old_path.exists()
        assert new_path.exists()
        assert new_path.read_text() == "content"

    def test_moves_to_different_directory(self, tmp_path: Path):
        """Test moving file to a different directory."""
        old_path = tmp_path / "old_name.txt"
        old_path.write_text("content")
        new_dir = tmp_path / "subdir"
        new_path = new_dir / "moved_file.txt"

        result = rename_local_file(old_path, new_path)

        assert result == new_path
        assert not old_path.exists()
        assert new_path.exists()
        assert new_dir.exists()

    def test_creates_parent_directories(self, tmp_path: Path):
        """Test that parent directories are created as needed."""
        old_path = tmp_path / "file.txt"
        old_path.write_text("content")
        new_path = tmp_path / "a" / "b" / "c" / "file.txt"

        result = rename_local_file(old_path, new_path)

        assert result == new_path
        assert new_path.exists()
        assert (tmp_path / "a" / "b" / "c").is_dir()

    def test_raises_file_not_found_for_missing_source(self, tmp_path: Path):
        """Test FileNotFoundError for non-existent source."""
        old_path = tmp_path / "nonexistent.txt"
        new_path = tmp_path / "new.txt"

        with pytest.raises(FileNotFoundError, match="Source file not found"):
            rename_local_file(old_path, new_path)

    def test_raises_file_exists_for_existing_target(self, tmp_path: Path):
        """Test FileExistsError when target already exists."""
        old_path = tmp_path / "old.txt"
        old_path.write_text("old content")
        new_path = tmp_path / "new.txt"
        new_path.write_text("existing content")

        with pytest.raises(FileExistsError, match="Target path already exists"):
            rename_local_file(old_path, new_path)


class TestSyncOperationsInit:
    """Tests for SyncOperations initialization."""

    def test_stores_client(self):
        """Test that client is stored."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)
        assert ops.client is mock_client


class TestSyncOperationsUploadFile:
    """Tests for SyncOperations.upload_file method."""

    def test_calls_client_upload(self, tmp_path: Path):
        """Test that upload_file calls client with correct parameters."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        test_file = tmp_path / "test.txt"
        test_file.write_text("test")
        local_file = LocalFile(
            path=test_file,
            relative_path="test.txt",
            size=4,
            mtime=1000.0,
        )

        progress_cb = MagicMock()
        ops.upload_file(
            local_file=local_file,
            remote_path="remote/test.txt",
            workspace_id=5,
            chunk_size=1024,
            multipart_threshold=2048,
            progress_callback=progress_cb,
        )

        mock_client.upload_file.assert_called_once_with(
            file_path=test_file,
            relative_path="remote/test.txt",
            workspace_id=5,
            chunk_size=1024,
            use_multipart_threshold=2048,
            progress_callback=progress_cb,
        )


class TestSyncOperationsDownloadFile:
    """Tests for SyncOperations.download_file method."""

    def test_calls_client_download(self, tmp_path: Path):
        """Test that download_file calls client with correct parameters."""
        mock_client = MagicMock()
        mock_client.download_file.return_value = tmp_path / "downloaded.txt"
        ops = SyncOperations(mock_client)

        mock_entry = MagicMock()
        mock_entry.hash = "abc123"
        remote_file = RemoteFile(entry=mock_entry, relative_path="remote/file.txt")

        local_path = tmp_path / "subdir" / "downloaded.txt"
        progress_cb = MagicMock()

        ops.download_file(
            remote_file=remote_file,
            local_path=local_path,
            progress_callback=progress_cb,
        )

        # Parent directory should be created
        assert local_path.parent.exists()

        mock_client.download_file.assert_called_once_with(
            hash_value="abc123",
            output_path=local_path,
            progress_callback=progress_cb,
        )


class TestSyncOperationsDeleteRemote:
    """Tests for SyncOperations.delete_remote method (line 194)."""

    def test_calls_client_delete(self):
        """Test that delete_remote calls client delete_file_entries."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        mock_entry = MagicMock()
        mock_entry.id = 12345
        remote_file = RemoteFile(entry=mock_entry, relative_path="file.txt")

        ops.delete_remote(remote_file, permanent=False)

        mock_client.delete_file_entries.assert_called_once_with(
            entry_ids=[12345],
            delete_forever=False,
        )

    def test_permanent_delete(self):
        """Test permanent delete passes correct flag."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        mock_entry = MagicMock()
        mock_entry.id = 12345
        remote_file = RemoteFile(entry=mock_entry, relative_path="file.txt")

        ops.delete_remote(remote_file, permanent=True)

        mock_client.delete_file_entries.assert_called_once_with(
            entry_ids=[12345],
            delete_forever=True,
        )


class TestSyncOperationsDeleteLocal:
    """Tests for SyncOperations.delete_local method."""

    def test_moves_to_trash(self, tmp_path: Path):
        """Test that file is moved to trash when use_trash=True."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        test_file = tmp_path / "to_delete.txt"
        test_file.write_text("content")
        local_file = LocalFile(
            path=test_file,
            relative_path="to_delete.txt",
            size=7,
            mtime=1000.0,
        )

        ops.delete_local(local_file, use_trash=True, sync_root=tmp_path)

        assert not test_file.exists()
        # File should be in trash
        trash_dir = tmp_path / LOCAL_TRASH_DIR_NAME
        assert trash_dir.exists()

    def test_permanent_delete(self, tmp_path: Path):
        """Test that file is permanently deleted when use_trash=False."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        test_file = tmp_path / "to_delete.txt"
        test_file.write_text("content")
        local_file = LocalFile(
            path=test_file,
            relative_path="to_delete.txt",
            size=7,
            mtime=1000.0,
        )

        ops.delete_local(local_file, use_trash=False)

        assert not test_file.exists()
        # No trash directory should be created
        trash_dir = tmp_path / LOCAL_TRASH_DIR_NAME
        assert not trash_dir.exists()


class TestSyncOperationsRenameLocal:
    """Tests for SyncOperations.rename_local method (lines 250-251)."""

    def test_renames_file(self, tmp_path: Path):
        """Test renaming a local file."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        test_file = tmp_path / "old_name.txt"
        test_file.write_text("content")
        local_file = LocalFile(
            path=test_file,
            relative_path="old_name.txt",
            size=7,
            mtime=1000.0,
        )

        result = ops.rename_local(
            local_file=local_file,
            new_relative_path="new_name.txt",
            sync_root=tmp_path,
        )

        expected_path = tmp_path / "new_name.txt"
        assert result == expected_path
        assert expected_path.exists()
        assert not test_file.exists()

    def test_renames_to_subdirectory(self, tmp_path: Path):
        """Test renaming file to a subdirectory."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        test_file = tmp_path / "file.txt"
        test_file.write_text("content")
        local_file = LocalFile(
            path=test_file,
            relative_path="file.txt",
            size=7,
            mtime=1000.0,
        )

        result = ops.rename_local(
            local_file=local_file,
            new_relative_path="subdir/file.txt",
            sync_root=tmp_path,
        )

        expected_path = tmp_path / "subdir" / "file.txt"
        assert result == expected_path
        assert expected_path.exists()


class TestSyncOperationsRenameRemote:
    """Tests for SyncOperations.rename_remote method (lines 276-283)."""

    def test_rename_same_folder(self):
        """Test simple rename in the same folder."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        mock_entry = MagicMock()
        mock_entry.id = 12345
        remote_file = RemoteFile(entry=mock_entry, relative_path="old_name.txt")

        ops.rename_remote(remote_file, new_name="new_name.txt")

        # Only update should be called, not move
        mock_client.move_file_entries.assert_not_called()
        mock_client.update_file_entry.assert_called_once_with(
            entry_id=12345,
            name="new_name.txt",
        )

    def test_move_to_different_folder(self):
        """Test moving to a different folder (lines 276-280)."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        mock_entry = MagicMock()
        mock_entry.id = 12345
        remote_file = RemoteFile(entry=mock_entry, relative_path="folder/file.txt")

        ops.rename_remote(
            remote_file,
            new_name="file.txt",
            new_parent_id=67890,
        )

        # Both move and rename should be called
        mock_client.move_file_entries.assert_called_once_with(
            entry_ids=[12345],
            destination_id=67890,
        )
        mock_client.update_file_entry.assert_called_once_with(
            entry_id=12345,
            name="file.txt",
        )

    def test_move_and_rename(self):
        """Test moving to different folder with new name."""
        mock_client = MagicMock()
        ops = SyncOperations(mock_client)

        mock_entry = MagicMock()
        mock_entry.id = 12345
        remote_file = RemoteFile(entry=mock_entry, relative_path="folder/old.txt")

        ops.rename_remote(
            remote_file,
            new_name="new.txt",
            new_parent_id=99999,
        )

        # Move first, then rename
        mock_client.move_file_entries.assert_called_once_with(
            entry_ids=[12345],
            destination_id=99999,
        )
        mock_client.update_file_entry.assert_called_once_with(
            entry_id=12345,
            name="new.txt",
        )
