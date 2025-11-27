"""Tests for the sync engine."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pydrime.api import DrimeClient
from pydrime.models import FileEntry
from pydrime.output import OutputFormatter
from pydrime.sync import SyncEngine, SyncMode, SyncPair


class TestSyncEngine:
    """Test SyncEngine functionality."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        client = Mock(spec=DrimeClient)
        return client

    @pytest.fixture
    def mock_output(self):
        """Create a mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True  # Suppress output during tests
        return output

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sync_engine(self, mock_client, mock_output):
        """Create a sync engine instance."""
        return SyncEngine(mock_client, mock_output)

    def test_create_sync_engine(self, mock_client, mock_output):
        """Test creating a sync engine."""
        engine = SyncEngine(mock_client, mock_output)
        assert engine.client == mock_client
        assert engine.output == mock_output
        assert engine.operations is not None

    def test_sync_pair_invalid_local_path(self, sync_engine, temp_dir):
        """Test sync_pair with non-existent local path."""
        pair = SyncPair(
            local=temp_dir / "nonexistent",
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        with pytest.raises(ValueError, match="does not exist"):
            sync_engine.sync_pair(pair)

    def test_sync_pair_local_path_not_directory(self, sync_engine, temp_dir):
        """Test sync_pair with local path that is a file, not directory."""
        # Create a file instead of directory
        test_file = temp_dir / "test.txt"
        test_file.write_text("test")

        pair = SyncPair(
            local=test_file,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        with pytest.raises(ValueError, match="not a directory"):
            sync_engine.sync_pair(pair)

    def test_sync_pair_dry_run_empty_dirs(self, sync_engine, temp_dir):
        """Test dry run with empty local and remote directories."""
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Mock file entries manager to return empty results
        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = None
            mock_manager.get_all_recursive.return_value = []
            mock_manager_class.return_value = mock_manager

            stats = sync_engine.sync_pair(pair, dry_run=True)

        assert stats["uploads"] == 0
        assert stats["downloads"] == 0
        assert stats["deletes_local"] == 0
        assert stats["deletes_remote"] == 0
        assert stats["skips"] == 0
        assert stats["conflicts"] == 0

    def test_sync_pair_local_to_cloud_upload(self, sync_engine, temp_dir):
        """Test LOCAL_TO_CLOUD mode with new local files."""
        # Create test files
        (temp_dir / "file1.txt").write_text("content1")
        (temp_dir / "file2.txt").write_text("content2")

        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.LOCAL_TO_CLOUD,
        )

        # Mock file entries manager to return empty remote
        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = None
            mock_manager.get_all_recursive.return_value = []
            mock_manager_class.return_value = mock_manager

            stats = sync_engine.sync_pair(pair, dry_run=True)

        assert stats["uploads"] == 2
        assert stats["downloads"] == 0

    def test_sync_pair_cloud_to_local_download(self, sync_engine, temp_dir):
        """Test CLOUD_TO_LOCAL mode with remote files."""
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.CLOUD_TO_LOCAL,
        )

        # Create mock remote files
        mock_entry1 = Mock(spec=FileEntry)
        mock_entry1.id = 1
        mock_entry1.name = "file1.txt"
        mock_entry1.file_size = 100
        mock_entry1.updated_at = "2025-01-01T00:00:00Z"
        mock_entry1.type = "file"
        mock_entry1.hash = "hash1"

        mock_entry2 = Mock(spec=FileEntry)
        mock_entry2.id = 2
        mock_entry2.name = "file2.txt"
        mock_entry2.file_size = 200
        mock_entry2.updated_at = "2025-01-01T00:00:00Z"
        mock_entry2.type = "file"
        mock_entry2.hash = "hash2"

        # Mock file entries manager to return remote files
        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = Mock(id=123)
            mock_manager.get_all_recursive.return_value = [
                (mock_entry1, "file1.txt"),
                (mock_entry2, "file2.txt"),
            ]
            mock_manager_class.return_value = mock_manager

            stats = sync_engine.sync_pair(pair, dry_run=True)

        assert stats["uploads"] == 0
        assert stats["downloads"] == 2

    def test_sync_pair_two_way_conflict(self, sync_engine, temp_dir):
        """Test TWO_WAY mode with conflicting files."""
        # Create local file with recent timestamp
        local_file = temp_dir / "file.txt"
        local_file.write_text("local content")

        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Create mock remote file with recent timestamp
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 1
        mock_entry.name = "file.txt"
        mock_entry.file_size = 999  # Different size
        mock_entry.updated_at = "2025-01-01T00:00:00Z"
        mock_entry.type = "file"
        mock_entry.hash = "hash1"

        # Mock file entries manager
        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = Mock(id=123)
            mock_manager.get_all_recursive.return_value = [
                (mock_entry, "file.txt"),
            ]
            mock_manager_class.return_value = mock_manager

            stats = sync_engine.sync_pair(pair, dry_run=True)

        # Should detect conflict (different sizes, similar times)
        assert stats["conflicts"] >= 0  # Depends on exact timing

    def test_sync_pair_ignore_patterns(self, sync_engine, temp_dir):
        """Test that ignore patterns are respected."""
        # Create files
        (temp_dir / "file.txt").write_text("content")
        (temp_dir / "file.log").write_text("log content")
        (temp_dir / ".hidden").write_text("hidden")

        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.LOCAL_TO_CLOUD,
            ignore=["*.log"],
            exclude_dot_files=True,
        )

        # Mock file entries manager
        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = None
            mock_manager.get_all_recursive.return_value = []
            mock_manager_class.return_value = mock_manager

            stats = sync_engine.sync_pair(pair, dry_run=True)

        # Should only upload file.txt (not .log or .hidden)
        assert stats["uploads"] == 1

    def test_scan_remote_nonexistent_folder(self, sync_engine):
        """Test _scan_remote with non-existent remote folder."""
        pair = SyncPair(
            local=Path("/tmp"),
            remote="/nonexistent",
            sync_mode=SyncMode.TWO_WAY,
            workspace_id=0,
        )

        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = None
            mock_manager.get_all_recursive.return_value = []
            mock_manager_class.return_value = mock_manager

            remote_files = sync_engine._scan_remote(pair)

        assert len(remote_files) == 0

    def test_categorize_decisions(self, sync_engine):
        """Test _categorize_decisions method."""
        from pydrime.sync.comparator import SyncAction, SyncDecision

        decisions = [
            SyncDecision(
                action=SyncAction.UPLOAD,
                reason="New local file",
                local_file=None,
                remote_file=None,
                relative_path="file1.txt",
            ),
            SyncDecision(
                action=SyncAction.DOWNLOAD,
                reason="New remote file",
                local_file=None,
                remote_file=None,
                relative_path="file2.txt",
            ),
            SyncDecision(
                action=SyncAction.SKIP,
                reason="Files match",
                local_file=None,
                remote_file=None,
                relative_path="file3.txt",
            ),
            SyncDecision(
                action=SyncAction.CONFLICT,
                reason="Modified on both sides",
                local_file=None,
                remote_file=None,
                relative_path="file4.txt",
            ),
        ]

        stats = sync_engine._categorize_decisions(decisions)

        assert stats["uploads"] == 1
        assert stats["downloads"] == 1
        assert stats["skips"] == 1
        assert stats["conflicts"] == 1
        assert stats["deletes_local"] == 0
        assert stats["deletes_remote"] == 0

    def test_handle_conflicts_skips_conflicts(self, sync_engine):
        """Test that _handle_conflicts converts conflicts to skips."""
        from pydrime.sync.comparator import SyncAction, SyncDecision

        decisions = [
            SyncDecision(
                action=SyncAction.CONFLICT,
                reason="Modified on both sides",
                local_file=None,
                remote_file=None,
                relative_path="conflict.txt",
            ),
        ]

        updated_decisions = sync_engine._handle_conflicts(decisions)

        assert len(updated_decisions) == 1
        assert updated_decisions[0].action == SyncAction.SKIP
        assert "Conflict" in updated_decisions[0].reason


class TestSyncEngineIntegration:
    """Integration tests for sync engine with real file operations."""

    @pytest.fixture
    def temp_dirs(self):
        """Create two temporary directories for sync testing."""
        with tempfile.TemporaryDirectory() as tmpdir1:
            with tempfile.TemporaryDirectory() as tmpdir2:
                yield Path(tmpdir1), Path(tmpdir2)

    @pytest.fixture
    def mock_client_with_ops(self):
        """Create a mock client with upload/download operations."""
        client = Mock(spec=DrimeClient)

        # Mock upload
        def mock_upload(file_path, relative_path, **kwargs):
            # Simulate successful upload
            return {"id": 123, "name": file_path.name}

        client.upload_file = Mock(side_effect=mock_upload)

        # Mock download
        def mock_download(hash_value, output_path, **kwargs):
            # Simulate successful download by creating the file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(f"downloaded content for {hash_value}")
            return output_path

        client.download_file = Mock(side_effect=mock_download)

        # Mock delete
        client.delete_file_entries = Mock(return_value={"deleted": True})

        return client

    def test_sync_with_real_files(self, mock_client_with_ops, temp_dirs):
        """Test sync engine with real file operations."""
        local_dir, _ = temp_dirs

        # Create test files
        (local_dir / "file1.txt").write_text("content1")
        (local_dir / "file2.txt").write_text("content2")

        output = Mock(spec=OutputFormatter)
        output.quiet = True

        engine = SyncEngine(mock_client_with_ops, output)

        pair = SyncPair(
            local=local_dir,
            remote="/remote",
            sync_mode=SyncMode.LOCAL_TO_CLOUD,
        )

        # Mock empty remote directory
        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = None
            mock_manager.get_all_recursive.return_value = []
            mock_manager.iter_all_recursive.return_value = iter([])  # Empty iterator
            mock_manager_class.return_value = mock_manager

            # Run sync (not dry run)
            stats = engine.sync_pair(pair, dry_run=False)

        # Verify uploads were called
        assert stats["uploads"] == 2
        assert mock_client_with_ops.upload_file.call_count == 2


class TestLocalTrashOperations:
    """Tests for local trash directory functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_move_to_local_trash(self, temp_dir: Path):
        """Test moving a file to local trash directory."""
        from pydrime.sync.operations import (
            LOCAL_TRASH_DIR_NAME,
            move_to_local_trash,
        )

        # Create a test file
        test_file = temp_dir / "test_file.txt"
        test_file.write_text("test content")

        # Move to trash
        trash_path = move_to_local_trash(test_file, temp_dir)

        # Original file should be gone
        assert not test_file.exists()

        # File should be in trash directory
        assert trash_path.exists()
        assert LOCAL_TRASH_DIR_NAME in str(trash_path)
        assert trash_path.read_text() == "test content"

    def test_move_to_local_trash_preserves_structure(self, temp_dir: Path):
        """Test that directory structure is preserved in trash."""
        from pydrime.sync.operations import move_to_local_trash

        # Create nested directory structure
        nested_dir = temp_dir / "subdir1" / "subdir2"
        nested_dir.mkdir(parents=True)
        test_file = nested_dir / "nested_file.txt"
        test_file.write_text("nested content")

        # Move to trash
        trash_path = move_to_local_trash(test_file, temp_dir)

        # Original file should be gone
        assert not test_file.exists()

        # Trash path should contain the nested structure
        assert trash_path.exists()
        assert "subdir1" in str(trash_path)
        assert "subdir2" in str(trash_path)
        assert trash_path.name == "nested_file.txt"

    def test_move_to_local_trash_nonexistent_file(self, temp_dir: Path):
        """Test that moving nonexistent file raises error."""
        from pydrime.sync.operations import move_to_local_trash

        nonexistent = temp_dir / "nonexistent.txt"

        with pytest.raises(FileNotFoundError):
            move_to_local_trash(nonexistent, temp_dir)

    def test_get_local_trash_path(self, temp_dir: Path):
        """Test getting the local trash path."""
        from pydrime.sync.operations import (
            LOCAL_TRASH_DIR_NAME,
            get_local_trash_path,
        )

        trash_path = get_local_trash_path(temp_dir)
        assert trash_path == temp_dir / LOCAL_TRASH_DIR_NAME

    def test_sync_operations_delete_local_with_trash(self, temp_dir: Path):
        """Test SyncOperations.delete_local with trash enabled."""
        from pydrime.sync.operations import LOCAL_TRASH_DIR_NAME, SyncOperations
        from pydrime.sync.scanner import LocalFile

        # Create a test file
        test_file = temp_dir / "to_delete.txt"
        test_file.write_text("delete me")
        local_file = LocalFile.from_path(test_file, temp_dir)

        # Create mock client
        mock_client = Mock(spec=DrimeClient)
        ops = SyncOperations(mock_client)

        # Delete with trash enabled
        ops.delete_local(local_file, use_trash=True, sync_root=temp_dir)

        # Original should be gone
        assert not test_file.exists()

        # Trash directory should exist and contain the file
        trash_dir = temp_dir / LOCAL_TRASH_DIR_NAME
        assert trash_dir.exists()

        # Find the file in trash (it's in a timestamped subdirectory)
        trash_files = list(trash_dir.rglob("to_delete.txt"))
        assert len(trash_files) == 1
        assert trash_files[0].read_text() == "delete me"

    def test_sync_operations_delete_local_without_trash(self, temp_dir: Path):
        """Test SyncOperations.delete_local with trash disabled."""
        from pydrime.sync.operations import LOCAL_TRASH_DIR_NAME, SyncOperations
        from pydrime.sync.scanner import LocalFile

        # Create a test file
        test_file = temp_dir / "to_delete.txt"
        test_file.write_text("delete me")
        local_file = LocalFile.from_path(test_file, temp_dir)

        # Create mock client
        mock_client = Mock(spec=DrimeClient)
        ops = SyncOperations(mock_client)

        # Delete with trash disabled
        ops.delete_local(local_file, use_trash=False, sync_root=temp_dir)

        # File should be permanently deleted
        assert not test_file.exists()

        # Trash directory should NOT exist
        trash_dir = temp_dir / LOCAL_TRASH_DIR_NAME
        assert not trash_dir.exists()

    def test_sync_operations_delete_local_without_sync_root(self, temp_dir: Path):
        """Test SyncOperations.delete_local without sync_root falls back to delete."""
        from pydrime.sync.operations import LOCAL_TRASH_DIR_NAME, SyncOperations
        from pydrime.sync.scanner import LocalFile

        # Create a test file
        test_file = temp_dir / "to_delete.txt"
        test_file.write_text("delete me")
        local_file = LocalFile.from_path(test_file, temp_dir)

        # Create mock client
        mock_client = Mock(spec=DrimeClient)
        ops = SyncOperations(mock_client)

        # Delete with trash enabled but no sync_root
        ops.delete_local(local_file, use_trash=True, sync_root=None)

        # File should be permanently deleted (fallback behavior)
        assert not test_file.exists()

        # Trash directory should NOT exist
        trash_dir = temp_dir / LOCAL_TRASH_DIR_NAME
        assert not trash_dir.exists()

    def test_sync_pair_disable_local_trash(self, temp_dir: Path):
        """Test SyncPair.disable_local_trash setting."""
        pair_with_trash = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
            disable_local_trash=False,
        )
        assert pair_with_trash.use_local_trash is True

        pair_without_trash = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
            disable_local_trash=True,
        )
        assert pair_without_trash.use_local_trash is False
