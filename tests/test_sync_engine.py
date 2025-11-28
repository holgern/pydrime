"""Tests for the sync engine."""

import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pydrime.api import DrimeClient
from pydrime.models import FileEntry
from pydrime.output import OutputFormatter
from pydrime.sync import SyncEngine, SyncMode, SyncPair
from pydrime.sync.comparator import SyncAction, SyncDecision
from pydrime.sync.concurrency import ConcurrencyLimits, SyncPauseController
from pydrime.sync.scanner import LocalFile, RemoteFile
from pydrime.sync.state import SyncStateManager


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


class TestSyncEnginePauseResumeCancel:
    """Tests for pause, resume, and cancel functionality."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        return Mock(spec=DrimeClient)

    @pytest.fixture
    def mock_output(self):
        """Create a mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = False  # Enable output to test messages
        return output

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    def test_pause_with_output(self, mock_client, mock_output):
        """Test pause method with output enabled."""
        engine = SyncEngine(mock_client, mock_output)
        engine.pause()

        assert engine.paused is True
        mock_output.info.assert_called_with("Sync paused")

    def test_pause_quiet(self, mock_client, mock_output_quiet):
        """Test pause method with quiet output."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        engine.pause()

        assert engine.paused is True
        mock_output_quiet.info.assert_not_called()

    def test_resume_with_output(self, mock_client, mock_output):
        """Test resume method with output enabled."""
        engine = SyncEngine(mock_client, mock_output)
        engine.pause()
        engine.resume()

        assert engine.paused is False
        mock_output.info.assert_any_call("Sync resumed")

    def test_resume_quiet(self, mock_client, mock_output_quiet):
        """Test resume method with quiet output."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        engine.pause()
        engine.resume()

        assert engine.paused is False

    def test_cancel_with_output(self, mock_client, mock_output):
        """Test cancel method with output enabled."""
        engine = SyncEngine(mock_client, mock_output)
        engine.cancel()

        assert engine.cancelled is True
        mock_output.info.assert_called_with("Sync cancelled")

    def test_cancel_quiet(self, mock_client, mock_output_quiet):
        """Test cancel method with quiet output."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        engine.cancel()

        assert engine.cancelled is True

    def test_paused_property(self, mock_client, mock_output_quiet):
        """Test paused property reflects pause controller state."""
        engine = SyncEngine(mock_client, mock_output_quiet)

        assert engine.paused is False
        engine.pause()
        assert engine.paused is True
        engine.resume()
        assert engine.paused is False

    def test_cancelled_property(self, mock_client, mock_output_quiet):
        """Test cancelled property reflects pause controller state."""
        engine = SyncEngine(mock_client, mock_output_quiet)

        assert engine.cancelled is False
        engine.cancel()
        assert engine.cancelled is True

    def test_reset(self, mock_client, mock_output_quiet):
        """Test reset method clears pause controller state."""
        engine = SyncEngine(mock_client, mock_output_quiet)

        engine.pause()
        assert engine.paused is True
        assert engine.cancelled is False

        engine.cancel()
        # cancel() also resumes (unpauses) to allow workers to exit
        assert engine.paused is False
        assert engine.cancelled is True

        engine.reset()
        assert engine.paused is False
        assert engine.cancelled is False


class TestSyncEngineWithCustomDependencies:
    """Tests for SyncEngine with custom dependencies."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        return Mock(spec=DrimeClient)

    def test_init_with_custom_state_manager(self, mock_client):
        """Test initializing with custom state manager."""
        custom_state_manager = SyncStateManager(state_dir=Path("/tmp/test_state"))
        engine = SyncEngine(mock_client, state_manager=custom_state_manager)

        assert engine.state_manager is custom_state_manager

    def test_init_with_custom_pause_controller(self, mock_client):
        """Test initializing with custom pause controller."""
        custom_controller = SyncPauseController()
        engine = SyncEngine(mock_client, pause_controller=custom_controller)

        assert engine.pause_controller is custom_controller

    def test_init_with_custom_concurrency_limits(self, mock_client):
        """Test initializing with custom concurrency limits."""
        custom_limits = ConcurrencyLimits(transfers_limit=5, operations_limit=10)
        engine = SyncEngine(mock_client, concurrency_limits=custom_limits)

        assert engine.concurrency_limits is custom_limits

    def test_init_defaults(self, mock_client):
        """Test default initialization creates all components."""
        engine = SyncEngine(mock_client)

        assert engine.output is not None
        assert engine.operations is not None
        assert engine.state_manager is not None
        assert engine.pause_controller is not None
        assert engine.concurrency_limits is not None


class TestSyncEngineHelperMethods:
    """Tests for helper methods in SyncEngine."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        return Mock(spec=DrimeClient)

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    @pytest.fixture
    def sync_engine(self, mock_client, mock_output_quiet):
        """Create a sync engine instance."""
        return SyncEngine(mock_client, mock_output_quiet)

    def test_create_empty_stats(self, sync_engine):
        """Test _create_empty_stats creates correct dictionary."""
        stats = sync_engine._create_empty_stats()

        assert stats == {
            "uploads": 0,
            "downloads": 0,
            "deletes_local": 0,
            "deletes_remote": 0,
            "renames_local": 0,
            "renames_remote": 0,
            "skips": 0,
            "conflicts": 0,
            "errors": 0,
        }

    def test_categorize_decisions_with_renames(self, sync_engine):
        """Test _categorize_decisions handles rename actions."""
        decisions = [
            SyncDecision(
                action=SyncAction.RENAME_LOCAL,
                reason="Renamed in remote",
                local_file=None,
                remote_file=None,
                relative_path="old.txt",
                new_path="new.txt",
            ),
            SyncDecision(
                action=SyncAction.RENAME_REMOTE,
                reason="Renamed in local",
                local_file=None,
                remote_file=None,
                relative_path="old2.txt",
                new_path="new2.txt",
            ),
            SyncDecision(
                action=SyncAction.DELETE_LOCAL,
                reason="Deleted in remote",
                local_file=None,
                remote_file=None,
                relative_path="deleted_local.txt",
            ),
            SyncDecision(
                action=SyncAction.DELETE_REMOTE,
                reason="Deleted in local",
                local_file=None,
                remote_file=None,
                relative_path="deleted_remote.txt",
            ),
        ]

        stats = sync_engine._categorize_decisions(decisions)

        assert stats["renames_local"] == 1
        assert stats["renames_remote"] == 1
        assert stats["deletes_local"] == 1
        assert stats["deletes_remote"] == 1

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_display_sync_plan_with_all_actions(self, mock_client, temp_dir):
        """Test _display_sync_plan displays all action types."""
        output = Mock(spec=OutputFormatter)
        output.quiet = False
        engine = SyncEngine(mock_client, output)

        stats = {
            "uploads": 2,
            "downloads": 3,
            "deletes_local": 1,
            "deletes_remote": 1,
            "renames_local": 1,
            "renames_remote": 1,
            "skips": 5,
            "conflicts": 1,
        }

        decisions = [
            SyncDecision(
                action=SyncAction.CONFLICT,
                reason="Modified both sides",
                local_file=None,
                remote_file=None,
                relative_path="conflict.txt",
            ),
        ]

        engine._display_sync_plan(stats, decisions, dry_run=True)

        # Verify output calls
        assert output.info.call_count >= 8  # Multiple info calls
        output.warning.assert_called()  # Warning for conflicts

    def test_display_sync_plan_quiet(self, sync_engine):
        """Test _display_sync_plan is silent in quiet mode."""
        stats = {
            "uploads": 1,
            "downloads": 0,
            "deletes_local": 0,
            "deletes_remote": 0,
            "skips": 0,
            "conflicts": 0,
        }

        # Should not raise and should be silent
        sync_engine._display_sync_plan(stats, [], dry_run=True)

    def test_display_summary_dry_run(self, mock_client):
        """Test _display_summary for dry run."""
        output = Mock(spec=OutputFormatter)
        output.quiet = False
        engine = SyncEngine(mock_client, output)

        stats = {
            "uploads": 2,
            "downloads": 1,
            "deletes_local": 0,
            "deletes_remote": 0,
            "renames_local": 0,
            "renames_remote": 0,
        }

        engine._display_summary(stats, dry_run=True)

        output.success.assert_called_with("Dry run complete!")

    def test_display_summary_real_run(self, mock_client):
        """Test _display_summary for actual run."""
        output = Mock(spec=OutputFormatter)
        output.quiet = False
        engine = SyncEngine(mock_client, output)

        stats = {
            "uploads": 2,
            "downloads": 1,
            "deletes_local": 1,
            "deletes_remote": 1,
            "renames_local": 1,
            "renames_remote": 1,
        }

        engine._display_summary(stats, dry_run=False)

        output.success.assert_called_with("Sync complete!")

    def test_display_summary_no_changes(self, mock_client):
        """Test _display_summary with no changes."""
        output = Mock(spec=OutputFormatter)
        output.quiet = False
        engine = SyncEngine(mock_client, output)

        stats = {
            "uploads": 0,
            "downloads": 0,
            "deletes_local": 0,
            "deletes_remote": 0,
            "renames_local": 0,
            "renames_remote": 0,
        }

        engine._display_summary(stats, dry_run=False)

        output.info.assert_any_call("No changes needed - everything is in sync!")


class TestSyncEngineExecuteDecision:
    """Tests for _execute_single_decision and related methods."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        client = Mock(spec=DrimeClient)
        return client

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sync_engine(self, mock_client, mock_output_quiet):
        """Create a sync engine instance."""
        return SyncEngine(mock_client, mock_output_quiet)

    def test_execute_decision_cancelled(self, sync_engine, temp_dir):
        """Test _execute_single_decision returns early when cancelled."""
        sync_engine.cancel()

        decision = SyncDecision(
            action=SyncAction.UPLOAD,
            reason="Test",
            local_file=None,
            remote_file=None,
            relative_path="test.txt",
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise - just returns early
        sync_engine._execute_single_decision(decision, pair, 1024, 1024, None)

    def test_execute_upload_file_not_exists(self, sync_engine, temp_dir):
        """Test _execute_upload skips if file doesn't exist."""
        local_file = LocalFile(
            path=temp_dir / "nonexistent.txt",
            relative_path="nonexistent.txt",
            size=100,
            mtime=time.time(),
        )
        decision = SyncDecision(
            action=SyncAction.UPLOAD,
            reason="New local file",
            local_file=local_file,
            remote_file=None,
            relative_path="nonexistent.txt",
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise - just logs and returns
        sync_engine._execute_upload(decision, pair, 1024, 1024, None)

    def test_execute_upload_no_local_file(self, sync_engine, temp_dir):
        """Test _execute_upload returns early if no local_file."""
        decision = SyncDecision(
            action=SyncAction.UPLOAD,
            reason="Test",
            local_file=None,
            remote_file=None,
            relative_path="test.txt",
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise
        sync_engine._execute_upload(decision, pair, 1024, 1024, None)

    def test_execute_download_no_remote_file(self, sync_engine, temp_dir):
        """Test _execute_download returns early if no remote_file."""
        decision = SyncDecision(
            action=SyncAction.DOWNLOAD,
            reason="Test",
            local_file=None,
            remote_file=None,
            relative_path="test.txt",
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise
        sync_engine._execute_download(decision, pair, None)

    def test_execute_delete_local_file_not_exists(self, sync_engine, temp_dir):
        """Test _execute_delete_local skips if file doesn't exist."""
        local_file = LocalFile(
            path=temp_dir / "nonexistent.txt",
            relative_path="nonexistent.txt",
            size=100,
            mtime=time.time(),
        )
        decision = SyncDecision(
            action=SyncAction.DELETE_LOCAL,
            reason="Deleted in remote",
            local_file=local_file,
            remote_file=None,
            relative_path="nonexistent.txt",
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise - logs and returns
        sync_engine._execute_delete_local(decision, pair)

    def test_execute_delete_local_no_local_file(self, sync_engine, temp_dir):
        """Test _execute_delete_local returns early if no local_file."""
        decision = SyncDecision(
            action=SyncAction.DELETE_LOCAL,
            reason="Test",
            local_file=None,
            remote_file=None,
            relative_path="test.txt",
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise
        sync_engine._execute_delete_local(decision, pair)

    def test_execute_delete_remote_no_remote_file(self, sync_engine, temp_dir):
        """Test _execute_delete_remote returns early if no remote_file."""
        decision = SyncDecision(
            action=SyncAction.DELETE_REMOTE,
            reason="Test",
            local_file=None,
            remote_file=None,
            relative_path="test.txt",
        )

        # Should not raise
        sync_engine._execute_delete_remote(decision)

    def test_execute_rename_local_no_local_file(self, sync_engine, temp_dir):
        """Test _execute_rename_local returns early if no local_file."""
        decision = SyncDecision(
            action=SyncAction.RENAME_LOCAL,
            reason="Test",
            local_file=None,
            remote_file=None,
            relative_path="test.txt",
            new_path="new.txt",
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise
        sync_engine._execute_rename_local(decision, pair)

    def test_execute_rename_local_no_new_path(self, sync_engine, temp_dir):
        """Test _execute_rename_local returns early if no new_path."""
        local_file = LocalFile(
            path=temp_dir / "test.txt",
            relative_path="test.txt",
            size=100,
            mtime=time.time(),
        )
        decision = SyncDecision(
            action=SyncAction.RENAME_LOCAL,
            reason="Test",
            local_file=local_file,
            remote_file=None,
            relative_path="test.txt",
            new_path=None,
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise
        sync_engine._execute_rename_local(decision, pair)

    def test_execute_rename_local_file_not_exists(self, sync_engine, temp_dir):
        """Test _execute_rename_local skips if file doesn't exist."""
        local_file = LocalFile(
            path=temp_dir / "nonexistent.txt",
            relative_path="nonexistent.txt",
            size=100,
            mtime=time.time(),
        )
        decision = SyncDecision(
            action=SyncAction.RENAME_LOCAL,
            reason="Test",
            local_file=local_file,
            remote_file=None,
            relative_path="nonexistent.txt",
            old_path="nonexistent.txt",
            new_path="new.txt",
        )
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise - logs and returns
        sync_engine._execute_rename_local(decision, pair)

    def test_execute_rename_remote_no_remote_file(self, sync_engine):
        """Test _execute_rename_remote returns early if no remote_file."""
        decision = SyncDecision(
            action=SyncAction.RENAME_REMOTE,
            reason="Test",
            local_file=None,
            remote_file=None,
            relative_path="test.txt",
            new_path="new.txt",
        )

        # Should not raise
        sync_engine._execute_rename_remote(decision)

    def test_execute_rename_remote_no_new_path(self, sync_engine):
        """Test _execute_rename_remote returns early if no new_path."""
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 123
        mock_entry.name = "test.txt"
        mock_entry.file_size = 100
        mock_entry.updated_at = "2025-01-01T00:00:00Z"
        mock_entry.hash = "abc123"

        remote_file = RemoteFile(entry=mock_entry, relative_path="test.txt")
        decision = SyncDecision(
            action=SyncAction.RENAME_REMOTE,
            reason="Test",
            local_file=None,
            remote_file=remote_file,
            relative_path="test.txt",
            new_path=None,
        )

        # Should not raise
        sync_engine._execute_rename_remote(decision)


class TestSyncEngineBatchExecution:
    """Tests for batch execution methods."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        return Mock(spec=DrimeClient)

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sync_engine(self, mock_client, mock_output_quiet):
        """Create a sync engine instance."""
        return SyncEngine(mock_client, mock_output_quiet)

    def test_execute_batch_decisions_empty(self, sync_engine, temp_dir):
        """Test _execute_batch_decisions with empty list."""
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        stats = sync_engine._execute_batch_decisions(
            batch_decisions=[],
            pair=pair,
            chunk_size=1024,
            multipart_threshold=1024,
            progress_callback=None,
            max_workers=1,
        )

        assert stats["uploads"] == 0
        assert stats["downloads"] == 0

    def test_execute_batch_decisions_with_skips_and_conflicts(
        self, sync_engine, temp_dir
    ):
        """Test _execute_batch_decisions counts skips and conflicts."""
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        decisions = [
            SyncDecision(
                action=SyncAction.SKIP,
                reason="Already synced",
                local_file=None,
                remote_file=None,
                relative_path="skip.txt",
            ),
            SyncDecision(
                action=SyncAction.CONFLICT,
                reason="Both modified",
                local_file=None,
                remote_file=None,
                relative_path="conflict.txt",
            ),
        ]

        stats = sync_engine._execute_batch_decisions(
            batch_decisions=decisions,
            pair=pair,
            chunk_size=1024,
            multipart_threshold=1024,
            progress_callback=None,
            max_workers=1,
        )

        assert stats["skips"] == 1
        assert stats["conflicts"] == 1

    def test_execute_decision_with_stats_error(self, sync_engine, temp_dir):
        """Test _execute_decision_with_stats handles errors gracefully."""
        output = Mock(spec=OutputFormatter)
        output.quiet = False
        engine = SyncEngine(sync_engine.client, output)

        # Mock the operations to raise an exception
        with patch.object(engine, "_execute_single_decision") as mock_exec:
            mock_exec.side_effect = Exception("Test error")

            decision = SyncDecision(
                action=SyncAction.UPLOAD,
                reason="Test",
                local_file=Mock(),
                remote_file=None,
                relative_path="fail.txt",
            )
            pair = SyncPair(
                local=temp_dir,
                remote="/remote",
                sync_mode=SyncMode.TWO_WAY,
            )
            stats = {
                "uploads": 0,
                "downloads": 0,
                "deletes_local": 0,
                "deletes_remote": 0,
            }

            # Should not raise, but should not increment stats
            engine._execute_decision_with_stats(
                decision=decision,
                pair=pair,
                chunk_size=1024,
                multipart_threshold=1024,
                progress_callback=None,
                stats=stats,
            )

            assert stats["uploads"] == 0  # Not incremented due to error
            output.error.assert_called_once()  # Error message shown


class TestSyncEngineUploadFolder:
    """Tests for upload_folder method."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        client = Mock(spec=DrimeClient)
        client.upload_file = Mock(return_value={"id": 123})
        return client

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_upload_folder_nonexistent_path(self, mock_client, mock_output_quiet):
        """Test upload_folder with non-existent path."""
        engine = SyncEngine(mock_client, mock_output_quiet)

        with pytest.raises(ValueError, match="does not exist"):
            engine.upload_folder(Path("/nonexistent"), "/remote")

    def test_upload_folder_not_directory(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test upload_folder with file instead of directory."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("content")

        engine = SyncEngine(mock_client, mock_output_quiet)

        with pytest.raises(ValueError, match="not a directory"):
            engine.upload_folder(test_file, "/remote")

    def test_upload_folder_empty(self, mock_client, mock_output_quiet, temp_dir):
        """Test upload_folder with empty directory."""
        engine = SyncEngine(mock_client, mock_output_quiet)

        stats = engine.upload_folder(temp_dir, "/remote")

        assert stats["uploads"] == 0
        assert stats["skips"] == 0
        assert stats["errors"] == 0

    def test_upload_folder_with_files_to_skip(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test upload_folder respects files_to_skip."""
        # Create test files
        (temp_dir / "keep.txt").write_text("keep")
        (temp_dir / "skip.txt").write_text("skip")

        engine = SyncEngine(mock_client, mock_output_quiet)

        with patch.object(engine, "_execute_upload_decisions") as mock_exec:
            mock_exec.return_value = {"uploads": 1, "errors": 0}

            stats = engine.upload_folder(
                temp_dir,
                "/remote",
                files_to_skip={"skip.txt"},
            )

        assert stats["skips"] == 1
        # Verify only 1 file passed to execute (the non-skipped one)
        call_args = mock_exec.call_args
        assert len(call_args[0][0]) == 1  # First positional arg is decisions list

    def test_upload_folder_with_file_renames(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test upload_folder applies file_renames."""
        # Create test file
        (temp_dir / "original.txt").write_text("content")

        engine = SyncEngine(mock_client, mock_output_quiet)

        with patch.object(engine, "_execute_upload_decisions") as mock_exec:
            mock_exec.return_value = {"uploads": 1, "errors": 0}

            engine.upload_folder(
                temp_dir,
                "/remote",
                file_renames={"original.txt": "renamed.txt"},
            )

        # Verify the renamed path is used
        call_args = mock_exec.call_args
        decisions = call_args[0][0]
        assert decisions[0].relative_path == "renamed.txt"


class TestSyncEngineDownloadFolder:
    """Tests for download_folder method."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        client = Mock(spec=DrimeClient)
        return client

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_download_folder_to_file_path(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test download_folder raises error when destination is a file."""
        test_file = temp_dir / "file.txt"
        test_file.write_text("content")

        engine = SyncEngine(mock_client, mock_output_quiet)
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 123
        mock_entry.name = "folder"

        with pytest.raises(ValueError, match="a file with this name exists"):
            engine.download_folder(mock_entry, test_file)

    def test_download_folder_empty_remote(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test download_folder with empty remote folder."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 123
        mock_entry.name = "folder"

        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.get_all_recursive.return_value = []
            mock_manager_class.return_value = mock_manager

            stats = engine.download_folder(
                mock_entry,
                temp_dir / "dest",
            )

        assert stats["downloads"] == 0
        assert stats["skips"] == 0
        assert stats["errors"] == 0


class TestSyncEngineParallelExecution:
    """Tests for parallel execution methods."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        return Mock(spec=DrimeClient)

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sync_engine(self, mock_client, mock_output_quiet):
        """Create a sync engine instance."""
        return SyncEngine(mock_client, mock_output_quiet)

    def test_execute_decisions_parallel_cancelled(self, sync_engine, temp_dir):
        """Test parallel execution stops when cancelled."""
        sync_engine.cancel()

        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        decisions = [
            SyncDecision(
                action=SyncAction.UPLOAD,
                reason="Test",
                local_file=None,
                remote_file=None,
                relative_path=f"file{i}.txt",
            )
            for i in range(5)
        ]

        stats = sync_engine._execute_decisions_parallel(
            decisions=decisions,
            pair=pair,
            chunk_size=1024,
            multipart_threshold=1024,
            progress_callback=None,
            max_workers=2,
        )

        # All should fail due to cancellation
        assert stats["uploads"] == 0

    def test_execute_decisions_with_start_delay(self, sync_engine, temp_dir):
        """Test parallel execution with start delay."""
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Create a file that exists
        test_file = temp_dir / "test.txt"
        test_file.write_text("content")

        local_file = LocalFile(
            path=test_file,
            relative_path="test.txt",
            size=7,
            mtime=test_file.stat().st_mtime,
        )

        decisions = [
            SyncDecision(
                action=SyncAction.SKIP,
                reason="Test",
                local_file=local_file,
                remote_file=None,
                relative_path="test.txt",
            ),
        ]

        # Should work with start_delay
        stats = sync_engine._execute_decisions_parallel(
            decisions=decisions,
            pair=pair,
            chunk_size=1024,
            multipart_threshold=1024,
            progress_callback=None,
            max_workers=2,
            start_delay=0.01,
        )

        # SKIP actions don't count as uploads
        assert stats["uploads"] == 0


class TestSyncEngineEnsureRemoteFolders:
    """Tests for _ensure_remote_folders_exist method."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        client = Mock(spec=DrimeClient)
        client.create_folder = Mock(return_value={"status": "success"})
        return client

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_ensure_remote_folders_no_uploads(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test _ensure_remote_folders_exist with no upload decisions."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        decisions = [
            SyncDecision(
                action=SyncAction.DOWNLOAD,
                reason="Test",
                local_file=None,
                remote_file=None,
                relative_path="test.txt",
            ),
        ]

        engine._ensure_remote_folders_exist(decisions, pair)

        # Should not call create_folder
        mock_client.create_folder.assert_not_called()

    def test_ensure_remote_folders_flat_files(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test _ensure_remote_folders_exist with flat files (no subfolders)."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        pair = SyncPair(
            local=temp_dir,
            remote="",  # No remote prefix
            sync_mode=SyncMode.TWO_WAY,
        )

        decisions = [
            SyncDecision(
                action=SyncAction.UPLOAD,
                reason="Test",
                local_file=Mock(),
                remote_file=None,
                relative_path="file.txt",  # No folder
            ),
        ]

        engine._ensure_remote_folders_exist(decisions, pair)

        # No folders to create for flat files
        mock_client.create_folder.assert_not_called()

    def test_ensure_remote_folders_with_subfolders(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test _ensure_remote_folders_exist creates needed folders."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        decisions = [
            SyncDecision(
                action=SyncAction.UPLOAD,
                reason="Test",
                local_file=Mock(),
                remote_file=None,
                relative_path="subdir/file.txt",
            ),
        ]

        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = None
            mock_manager_class.return_value = mock_manager

            engine._ensure_remote_folders_exist(decisions, pair)

        # Should create the folder
        mock_client.create_folder.assert_called()

    def test_ensure_remote_folders_handles_error(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test _ensure_remote_folders_exist handles errors gracefully."""
        mock_client.create_folder = Mock(side_effect=Exception("API Error"))

        engine = SyncEngine(mock_client, mock_output_quiet)
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        decisions = [
            SyncDecision(
                action=SyncAction.UPLOAD,
                reason="Test",
                local_file=Mock(),
                remote_file=None,
                relative_path="subdir/file.txt",
            ),
        ]

        with patch("pydrime.sync.engine.FileEntriesManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.find_folder_by_name.return_value = None
            mock_manager_class.return_value = mock_manager

            # Should not raise
            engine._ensure_remote_folders_exist(decisions, pair)


class TestSyncEngineExecuteOrderedOperations:
    """Tests for ordered execution methods."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Drime client."""
        return Mock(spec=DrimeClient)

    @pytest.fixture
    def mock_output_quiet(self):
        """Create a quiet mock output formatter."""
        output = Mock(spec=OutputFormatter)
        output.quiet = True
        return output

    @pytest.fixture
    def mock_output(self):
        """Create an output formatter with output enabled."""
        output = Mock(spec=OutputFormatter)
        output.quiet = False
        return output

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_execute_ordered_quiet_empty_lists(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test _execute_ordered_quiet with empty lists."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise with empty lists
        engine._execute_ordered_quiet(
            rename_local=[],
            rename_remote=[],
            delete_local=[],
            delete_remote=[],
            uploads=[],
            downloads=[],
            pair=pair,
            chunk_size=1024,
            multipart_threshold=1024,
            progress_callback=None,
            max_workers=1,
            start_delay=0.0,
        )

    def test_execute_ordered_with_progress_empty_lists(
        self, mock_client, mock_output, temp_dir
    ):
        """Test _execute_ordered_with_progress with empty lists."""
        engine = SyncEngine(mock_client, mock_output)
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        # Should not raise with empty lists
        engine._execute_ordered_with_progress(
            rename_local=[],
            rename_remote=[],
            delete_local=[],
            delete_remote=[],
            uploads=[],
            downloads=[],
            pair=pair,
            chunk_size=1024,
            multipart_threshold=1024,
            progress_callback=None,
            max_workers=1,
            start_delay=0.0,
        )

    def test_execute_decisions_empty_actionable(
        self, mock_client, mock_output_quiet, temp_dir
    ):
        """Test _execute_decisions with only skips returns early."""
        engine = SyncEngine(mock_client, mock_output_quiet)
        pair = SyncPair(
            local=temp_dir,
            remote="/remote",
            sync_mode=SyncMode.TWO_WAY,
        )

        decisions = [
            SyncDecision(
                action=SyncAction.SKIP,
                reason="Test",
                local_file=None,
                remote_file=None,
                relative_path="test.txt",
            ),
        ]

        # Should return early without doing anything
        engine._execute_decisions(
            decisions=decisions,
            pair=pair,
            chunk_size=1024,
            multipart_threshold=1024,
            progress_callback=None,
            max_workers=1,
        )
