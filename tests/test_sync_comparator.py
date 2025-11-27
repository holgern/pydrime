"""Tests for the FileComparator class."""

import time
from pathlib import Path
from unittest.mock import Mock

from pydrime.models import FileEntry
from pydrime.sync.comparator import FileComparator, SyncAction, SyncDecision
from pydrime.sync.modes import SyncMode
from pydrime.sync.scanner import LocalFile, RemoteFile
from pydrime.sync.state import (
    LocalItemState,
    LocalTree,
    RemoteItemState,
    RemoteTree,
)


class TestHandleLocalOnly:
    """Tests for handling local-only files (files that don't exist remotely)."""

    def _create_local_file(self, relative_path: str = "test.txt") -> LocalFile:
        """Create a LocalFile for testing."""
        return LocalFile(
            path=Path(f"/local/{relative_path}"),
            relative_path=relative_path,
            size=100,
            mtime=1234567890.0,
        )

    def test_local_only_two_way_uploads(self):
        """TWO_WAY mode should upload local-only files."""
        comparator = FileComparator(SyncMode.TWO_WAY)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file, {}, {})

        assert decision.action == SyncAction.UPLOAD
        assert decision.reason == "New local file"

    def test_local_only_local_to_cloud_uploads(self):
        """LOCAL_TO_CLOUD mode should upload local-only files."""
        comparator = FileComparator(SyncMode.LOCAL_TO_CLOUD)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file, {}, {})

        assert decision.action == SyncAction.UPLOAD
        assert decision.reason == "New local file"

    def test_local_only_local_backup_uploads(self):
        """LOCAL_BACKUP mode should upload local-only files."""
        comparator = FileComparator(SyncMode.LOCAL_BACKUP)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file, {}, {})

        assert decision.action == SyncAction.UPLOAD
        assert decision.reason == "New local file"

    def test_local_only_cloud_to_local_deletes_local(self):
        """CLOUD_TO_LOCAL mode should delete local-only files (deleted from cloud)."""
        comparator = FileComparator(SyncMode.CLOUD_TO_LOCAL)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file, {}, {})

        assert decision.action == SyncAction.DELETE_LOCAL
        assert decision.reason == "File deleted from cloud"

    def test_local_only_cloud_backup_skips(self):
        """CLOUD_BACKUP mode should skip local-only files (no upload, no delete)."""
        comparator = FileComparator(SyncMode.CLOUD_BACKUP)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file, {}, {})

        assert decision.action == SyncAction.SKIP
        assert "prevents action" in decision.reason


class TestHandleRemoteOnly:
    """Tests for handling remote-only files (files that don't exist locally)."""

    def _create_remote_file(self, relative_path: str = "test.txt") -> RemoteFile:
        """Create a RemoteFile for testing."""
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 123
        mock_entry.name = relative_path.split("/")[-1]
        mock_entry.file_size = 100
        mock_entry.hash = "abc123"
        mock_entry.updated_at = "2024-01-01T00:00:00Z"
        return RemoteFile(
            entry=mock_entry,
            relative_path=relative_path,
        )

    def test_remote_only_two_way_downloads(self):
        """TWO_WAY mode should download remote-only files."""
        comparator = FileComparator(SyncMode.TWO_WAY)
        remote_file = self._create_remote_file()

        decision = comparator._handle_remote_only("test.txt", remote_file, {}, {})

        assert decision.action == SyncAction.DOWNLOAD
        assert decision.reason == "New remote file"

    def test_remote_only_cloud_to_local_downloads(self):
        """CLOUD_TO_LOCAL mode should download remote-only files."""
        comparator = FileComparator(SyncMode.CLOUD_TO_LOCAL)
        remote_file = self._create_remote_file()

        decision = comparator._handle_remote_only("test.txt", remote_file, {}, {})

        assert decision.action == SyncAction.DOWNLOAD
        assert decision.reason == "New remote file"

    def test_remote_only_local_to_cloud_deletes_remote(self):
        """LOCAL_TO_CLOUD mode should delete remote-only files (deleted locally)."""
        comparator = FileComparator(SyncMode.LOCAL_TO_CLOUD)
        remote_file = self._create_remote_file()

        decision = comparator._handle_remote_only("test.txt", remote_file, {}, {})

        assert decision.action == SyncAction.DELETE_REMOTE
        assert decision.reason == "File deleted locally"

    def test_remote_only_local_backup_skips(self):
        """LOCAL_BACKUP mode should skip remote-only files (no download, no delete)."""
        comparator = FileComparator(SyncMode.LOCAL_BACKUP)
        remote_file = self._create_remote_file()

        decision = comparator._handle_remote_only("test.txt", remote_file, {}, {})

        assert decision.action == SyncAction.SKIP
        assert "prevents action" in decision.reason


class TestCompareExistingFiles:
    """Tests for comparing files that exist in both locations."""

    def _create_local_file(
        self,
        relative_path: str = "test.txt",
        size: int = 100,
        mtime: float = 1234567890.0,
        file_id: int = 0,
    ) -> LocalFile:
        """Create a LocalFile for testing."""
        return LocalFile(
            path=Path(f"/local/{relative_path}"),
            relative_path=relative_path,
            size=size,
            mtime=mtime,
            file_id=file_id,
        )

    def _create_remote_file(
        self,
        relative_path: str = "test.txt",
        size: int = 100,
        updated_at: str = "2024-01-01T00:00:00Z",
        id: int = 123,
    ) -> RemoteFile:
        """Create a RemoteFile for testing."""
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = id
        mock_entry.name = relative_path.split("/")[-1]
        mock_entry.file_size = size
        mock_entry.hash = "abc123"
        mock_entry.updated_at = updated_at
        return RemoteFile(
            entry=mock_entry,
            relative_path=relative_path,
        )

    def test_same_size_files_skip(self):
        """Files with same size should be skipped (considered identical)."""
        comparator = FileComparator(SyncMode.TWO_WAY)
        local_file = self._create_local_file(size=100)
        remote_file = self._create_remote_file(size=100)

        decision = comparator._compare_existing_files(
            "test.txt", local_file, remote_file
        )

        assert decision.action == SyncAction.SKIP
        assert "identical" in decision.reason

    def test_local_newer_uploads(self):
        """Local file newer should trigger upload in TWO_WAY mode."""
        comparator = FileComparator(SyncMode.TWO_WAY)
        local_file = self._create_local_file(
            size=200,
            mtime=time.time(),  # Recent local
        )
        remote_file = self._create_remote_file(
            size=100,
            updated_at="2020-01-01T00:00:00Z",  # Old remote
        )

        decision = comparator._compare_existing_files(
            "test.txt", local_file, remote_file
        )

        assert decision.action == SyncAction.UPLOAD
        assert "newer" in decision.reason

    def test_remote_newer_downloads(self):
        """Remote file newer should trigger download in TWO_WAY mode."""
        comparator = FileComparator(SyncMode.TWO_WAY)
        local_file = self._create_local_file(
            size=100,
            mtime=946684800.0,  # Jan 1, 2000
        )
        remote_file = self._create_remote_file(
            size=200,
            updated_at="2024-01-01T00:00:00Z",  # Recent remote
        )

        decision = comparator._compare_existing_files(
            "test.txt", local_file, remote_file
        )

        assert decision.action == SyncAction.DOWNLOAD
        assert "newer" in decision.reason

    def test_same_time_different_size_conflict(self):
        """Same timestamp but different sizes should be conflict."""
        comparator = FileComparator(SyncMode.TWO_WAY)
        # Use a fixed timestamp
        mtime = 1704067200.0  # Jan 1, 2024, 00:00:00 UTC
        local_file = self._create_local_file(size=100, mtime=mtime)

        # Create remote with same mtime (within tolerance)
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 123
        mock_entry.name = "test.txt"
        mock_entry.file_size = 200  # Different size
        mock_entry.hash = "abc123"
        mock_entry.updated_at = (
            "2024-01-01T00:00:01Z"  # 1 second diff, within tolerance
        )
        remote_file = RemoteFile(entry=mock_entry, relative_path="test.txt")

        decision = comparator._compare_existing_files(
            "test.txt", local_file, remote_file
        )

        assert decision.action == SyncAction.CONFLICT
        assert "Same timestamp but different sizes" in decision.reason

    def test_no_remote_mtime_upload_allowed(self):
        """No remote mtime with upload allowed should upload."""
        comparator = FileComparator(SyncMode.TWO_WAY)
        local_file = self._create_local_file(size=200)

        # Create remote with no mtime
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 123
        mock_entry.name = "test.txt"
        mock_entry.file_size = 100  # Different size
        mock_entry.hash = "abc123"
        mock_entry.updated_at = None  # No mtime
        remote_file = RemoteFile(entry=mock_entry, relative_path="test.txt")

        decision = comparator._compare_existing_files(
            "test.txt", local_file, remote_file
        )

        assert decision.action == SyncAction.UPLOAD
        assert "mtime unavailable" in decision.reason

    def test_no_remote_mtime_upload_not_allowed(self):
        """No remote mtime with upload not allowed should skip."""
        comparator = FileComparator(SyncMode.CLOUD_BACKUP)  # No upload allowed
        local_file = self._create_local_file(size=200)

        # Create remote with no mtime
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 123
        mock_entry.name = "test.txt"
        mock_entry.file_size = 100  # Different size
        mock_entry.hash = "abc123"
        mock_entry.updated_at = None  # No mtime
        remote_file = RemoteFile(entry=mock_entry, relative_path="test.txt")

        decision = comparator._compare_existing_files(
            "test.txt", local_file, remote_file
        )

        assert decision.action == SyncAction.SKIP
        assert "cannot determine" in decision.reason

    def test_local_newer_upload_not_allowed_skips(self):
        """Local file newer but upload not allowed should skip."""
        comparator = FileComparator(SyncMode.CLOUD_TO_LOCAL)  # No upload
        local_file = self._create_local_file(
            size=200,
            mtime=time.time(),  # Recent local
        )
        remote_file = self._create_remote_file(
            size=100,
            updated_at="2020-01-01T00:00:00Z",  # Old remote
        )

        decision = comparator._compare_existing_files(
            "test.txt", local_file, remote_file
        )

        assert decision.action == SyncAction.SKIP
        assert "prevents action" in decision.reason

    def test_remote_newer_download_not_allowed_skips(self):
        """Remote file newer but download not allowed should skip."""
        comparator = FileComparator(SyncMode.LOCAL_TO_CLOUD)  # No download
        local_file = self._create_local_file(
            size=100,
            mtime=946684800.0,  # Old local
        )
        remote_file = self._create_remote_file(
            size=200,
            updated_at="2024-01-01T00:00:00Z",  # Recent remote
        )

        decision = comparator._compare_existing_files(
            "test.txt", local_file, remote_file
        )

        assert decision.action == SyncAction.SKIP
        assert "prevents action" in decision.reason


class TestCompareFilesWithPreviousState:
    """Tests for compare_files with previous sync state (TWO_WAY mode)."""

    def _create_local_file(
        self,
        relative_path: str = "test.txt",
        size: int = 100,
        mtime: float = 1234567890.0,
        file_id: int = 1,
    ) -> LocalFile:
        """Create a LocalFile for testing."""
        return LocalFile(
            path=Path(f"/local/{relative_path}"),
            relative_path=relative_path,
            size=size,
            mtime=mtime,
            file_id=file_id,
        )

    def _create_remote_file(
        self,
        relative_path: str = "test.txt",
        size: int = 100,
        id: int = 1,
    ) -> RemoteFile:
        """Create a RemoteFile for testing."""
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = id
        mock_entry.name = relative_path.split("/")[-1]
        mock_entry.file_size = size
        mock_entry.hash = "abc123"
        mock_entry.updated_at = "2024-01-01T00:00:00Z"
        return RemoteFile(
            entry=mock_entry,
            relative_path=relative_path,
        )

    def test_local_only_was_previously_synced_deletes_local(self):
        """Previously synced file only in local should be deleted."""
        previous_synced = {"test.txt"}
        comparator = FileComparator(
            SyncMode.TWO_WAY, previous_synced_files=previous_synced
        )
        local_file = self._create_local_file()

        local_files = {"test.txt": local_file}
        remote_files = {}  # Not in remote anymore

        decisions = comparator.compare_files(local_files, remote_files)

        assert len(decisions) == 1
        assert decisions[0].action == SyncAction.DELETE_LOCAL
        assert "deleted from cloud" in decisions[0].reason

    def test_remote_only_was_previously_synced_deletes_remote(self):
        """Previously synced file only in remote should be deleted."""
        previous_synced = {"test.txt"}
        comparator = FileComparator(
            SyncMode.TWO_WAY, previous_synced_files=previous_synced
        )
        remote_file = self._create_remote_file()

        local_files = {}  # Not in local anymore
        remote_files = {"test.txt": remote_file}

        decisions = comparator.compare_files(local_files, remote_files)

        assert len(decisions) == 1
        assert decisions[0].action == SyncAction.DELETE_REMOTE
        assert "deleted locally" in decisions[0].reason


class TestCompareFilesWithRenames:
    """Tests for rename detection in compare_files."""

    def _create_local_file(
        self,
        relative_path: str = "test.txt",
        size: int = 100,
        mtime: float = 1234567890.0,
        file_id: int = 1,
    ) -> LocalFile:
        """Create a LocalFile for testing."""
        return LocalFile(
            path=Path(f"/local/{relative_path}"),
            relative_path=relative_path,
            size=size,
            mtime=mtime,
            file_id=file_id,
        )

    def _create_remote_file(
        self,
        relative_path: str = "test.txt",
        size: int = 100,
        id: int = 1,
    ) -> RemoteFile:
        """Create a RemoteFile for testing."""
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = id
        mock_entry.name = relative_path.split("/")[-1]
        mock_entry.file_size = size
        mock_entry.hash = "abc123"
        mock_entry.updated_at = "2024-01-01T00:00:00Z"
        return RemoteFile(
            entry=mock_entry,
            relative_path=relative_path,
        )

    def test_local_rename_detected(self):
        """Local file renamed should be detected and trigger remote rename."""
        # Previous state: file was at "old.txt"
        previous_local_tree = LocalTree()
        previous_local_tree.add_item(
            LocalItemState(file_id=1, path="old.txt", size=100, mtime=1234567890.0)
        )

        comparator = FileComparator(
            SyncMode.TWO_WAY,
            previous_local_tree=previous_local_tree,
        )

        # Current state: file is now at "new.txt" (same file_id)
        local_file = self._create_local_file(relative_path="new.txt", file_id=1)
        remote_file = self._create_remote_file(
            relative_path="old.txt", id=1
        )  # Still at old path

        local_files = {"new.txt": local_file}
        remote_files = {"old.txt": remote_file}

        decisions = comparator.compare_files(local_files, remote_files)

        # Should detect rename and create RENAME_REMOTE decision
        rename_decisions = [
            d for d in decisions if d.action == SyncAction.RENAME_REMOTE
        ]
        assert len(rename_decisions) == 1
        assert rename_decisions[0].old_path == "old.txt"
        assert rename_decisions[0].new_path == "new.txt"

    def test_remote_rename_detected(self):
        """Remote file renamed should be detected and trigger local rename."""
        # Previous state: file was at "old.txt"
        previous_remote_tree = RemoteTree()
        previous_remote_tree.add_item(
            RemoteItemState(id=1, path="old.txt", size=100, mtime=1234567890.0)
        )

        comparator = FileComparator(
            SyncMode.TWO_WAY,
            previous_remote_tree=previous_remote_tree,
        )

        # Current state: file is now at "new.txt" (same id)
        remote_file = self._create_remote_file(relative_path="new.txt", id=1)
        local_file = self._create_local_file(
            relative_path="old.txt", file_id=99
        )  # Different file_id, still at old path

        local_files = {"old.txt": local_file}
        remote_files = {"new.txt": remote_file}

        decisions = comparator.compare_files(local_files, remote_files)

        # Should detect rename and create RENAME_LOCAL decision
        rename_decisions = [d for d in decisions if d.action == SyncAction.RENAME_LOCAL]
        assert len(rename_decisions) == 1
        assert rename_decisions[0].old_path == "old.txt"
        assert rename_decisions[0].new_path == "new.txt"

    def test_detect_renames_local(self):
        """Test _detect_renames for local file renames."""
        previous_local_tree = LocalTree()
        previous_local_tree.add_item(
            LocalItemState(file_id=1, path="old_name.txt", size=100, mtime=1234567890.0)
        )
        previous_local_tree.add_item(
            LocalItemState(file_id=2, path="unchanged.txt", size=50, mtime=1234567890.0)
        )

        comparator = FileComparator(
            SyncMode.TWO_WAY,
            previous_local_tree=previous_local_tree,
        )

        # Current: file_id=1 moved to new path, file_id=2 unchanged
        local_file_renamed = self._create_local_file(
            relative_path="new_name.txt", file_id=1
        )
        local_file_unchanged = self._create_local_file(
            relative_path="unchanged.txt", file_id=2
        )

        local_files = {
            "new_name.txt": local_file_renamed,
            "unchanged.txt": local_file_unchanged,
        }
        remote_files = {}

        comparator._detect_renames(local_files, remote_files)

        assert 1 in comparator._local_renames
        assert comparator._local_renames[1] == "new_name.txt"
        assert 2 not in comparator._local_renames  # Unchanged

    def test_detect_renames_remote(self):
        """Test _detect_renames for remote file renames."""
        previous_remote_tree = RemoteTree()
        previous_remote_tree.add_item(
            RemoteItemState(id=100, path="old_name.txt", size=100, mtime=1234567890.0)
        )
        previous_remote_tree.add_item(
            RemoteItemState(id=200, path="unchanged.txt", size=50, mtime=1234567890.0)
        )

        comparator = FileComparator(
            SyncMode.TWO_WAY,
            previous_remote_tree=previous_remote_tree,
        )

        # Current: id=100 moved to new path, id=200 unchanged
        remote_file_renamed = self._create_remote_file(
            relative_path="new_name.txt", id=100
        )
        remote_file_unchanged = self._create_remote_file(
            relative_path="unchanged.txt", id=200
        )

        local_files = {}
        remote_files = {
            "new_name.txt": remote_file_renamed,
            "unchanged.txt": remote_file_unchanged,
        }

        comparator._detect_renames(local_files, remote_files)

        assert 100 in comparator._remote_renames
        assert comparator._remote_renames[100] == "new_name.txt"
        assert 200 not in comparator._remote_renames  # Unchanged


class TestCompareSingleFile:
    """Tests for _compare_single_file method."""

    def test_no_files_returns_skip(self):
        """No local or remote file should return SKIP."""
        comparator = FileComparator(SyncMode.TWO_WAY)

        decision = comparator._compare_single_file("test.txt", None, None)

        assert decision.action == SyncAction.SKIP
        assert "No file found" in decision.reason

    def test_handled_rename_old_path_skipped(self):
        """Paths already handled as old paths of renames should be skipped."""
        comparator = FileComparator(SyncMode.TWO_WAY)
        # Mark a path as handled
        comparator._handled_rename_old_paths.add("old.txt")

        def _create_local_file(relative_path: str) -> LocalFile:
            return LocalFile(
                path=Path(f"/local/{relative_path}"),
                relative_path=relative_path,
                size=100,
                mtime=1234567890.0,
            )

        local_files = {"old.txt": _create_local_file("old.txt")}
        remote_files = {}

        decisions = comparator.compare_files(local_files, remote_files)

        # Should be skipped (not in decisions)
        assert len(decisions) == 0


class TestCloudBackupMode:
    """Tests for CLOUD_BACKUP mode (download only, no upload, no delete)."""

    def _create_local_file(self, relative_path: str = "test.txt") -> LocalFile:
        """Create a LocalFile for testing."""
        return LocalFile(
            path=Path(f"/local/{relative_path}"),
            relative_path=relative_path,
            size=100,
            mtime=1234567890.0,
        )

    def _create_remote_file(self, relative_path: str = "test.txt") -> RemoteFile:
        """Create a RemoteFile for testing."""
        mock_entry = Mock(spec=FileEntry)
        mock_entry.id = 123
        mock_entry.name = relative_path.split("/")[-1]
        mock_entry.file_size = 100
        mock_entry.hash = "abc123"
        mock_entry.updated_at = "2024-01-01T00:00:00Z"
        return RemoteFile(entry=mock_entry, relative_path=relative_path)

    def test_cloud_backup_downloads_remote_only(self):
        """CLOUD_BACKUP should download remote-only files."""
        comparator = FileComparator(SyncMode.CLOUD_BACKUP)
        remote_file = self._create_remote_file()

        decision = comparator._handle_remote_only("test.txt", remote_file, {}, {})

        assert decision.action == SyncAction.DOWNLOAD
        assert decision.reason == "New remote file"

    def test_cloud_backup_skips_local_only(self):
        """CLOUD_BACKUP should skip local-only files."""
        comparator = FileComparator(SyncMode.CLOUD_BACKUP)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file, {}, {})

        assert decision.action == SyncAction.SKIP


class TestSyncDecisionDataclass:
    """Tests for SyncDecision dataclass."""

    def test_sync_decision_creation(self):
        """Test creating a SyncDecision with all fields."""
        decision = SyncDecision(
            action=SyncAction.RENAME_LOCAL,
            reason="File renamed",
            local_file=None,
            remote_file=None,
            relative_path="new.txt",
            old_path="old.txt",
            new_path="new.txt",
        )

        assert decision.action == SyncAction.RENAME_LOCAL
        assert decision.reason == "File renamed"
        assert decision.old_path == "old.txt"
        assert decision.new_path == "new.txt"

    def test_sync_decision_defaults(self):
        """Test SyncDecision with default optional fields."""
        decision = SyncDecision(
            action=SyncAction.UPLOAD,
            reason="Test",
            local_file=None,
            remote_file=None,
            relative_path="test.txt",
        )

        assert decision.old_path is None
        assert decision.new_path is None


class TestSyncActionEnum:
    """Tests for SyncAction enum."""

    def test_all_actions_exist(self):
        """Test all expected actions exist."""
        assert SyncAction.UPLOAD == "upload"
        assert SyncAction.DOWNLOAD == "download"
        assert SyncAction.DELETE_LOCAL == "delete_local"
        assert SyncAction.DELETE_REMOTE == "delete_remote"
        assert SyncAction.RENAME_LOCAL == "rename_local"
        assert SyncAction.RENAME_REMOTE == "rename_remote"
        assert SyncAction.SKIP == "skip"
        assert SyncAction.CONFLICT == "conflict"

    def test_action_string_conversion(self):
        """Test actions can be converted to strings."""
        assert str(SyncAction.UPLOAD) == "SyncAction.UPLOAD"
        assert SyncAction.UPLOAD.value == "upload"
