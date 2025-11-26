"""Tests for the FileComparator class."""

from pathlib import Path
from unittest.mock import Mock

from pydrime.models import FileEntry
from pydrime.sync.comparator import FileComparator, SyncAction
from pydrime.sync.modes import SyncMode
from pydrime.sync.scanner import LocalFile, RemoteFile


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

        decision = comparator._handle_local_only("test.txt", local_file)

        assert decision.action == SyncAction.UPLOAD
        assert decision.reason == "New local file"

    def test_local_only_local_to_cloud_uploads(self):
        """LOCAL_TO_CLOUD mode should upload local-only files."""
        comparator = FileComparator(SyncMode.LOCAL_TO_CLOUD)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file)

        assert decision.action == SyncAction.UPLOAD
        assert decision.reason == "New local file"

    def test_local_only_local_backup_uploads(self):
        """LOCAL_BACKUP mode should upload local-only files."""
        comparator = FileComparator(SyncMode.LOCAL_BACKUP)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file)

        assert decision.action == SyncAction.UPLOAD
        assert decision.reason == "New local file"

    def test_local_only_cloud_to_local_deletes_local(self):
        """CLOUD_TO_LOCAL mode should delete local-only files (deleted from cloud)."""
        comparator = FileComparator(SyncMode.CLOUD_TO_LOCAL)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file)

        assert decision.action == SyncAction.DELETE_LOCAL
        assert decision.reason == "File deleted from cloud"

    def test_local_only_cloud_backup_skips(self):
        """CLOUD_BACKUP mode should skip local-only files (no upload, no delete)."""
        comparator = FileComparator(SyncMode.CLOUD_BACKUP)
        local_file = self._create_local_file()

        decision = comparator._handle_local_only("test.txt", local_file)

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

        decision = comparator._handle_remote_only("test.txt", remote_file)

        assert decision.action == SyncAction.DOWNLOAD
        assert decision.reason == "New remote file"

    def test_remote_only_cloud_to_local_downloads(self):
        """CLOUD_TO_LOCAL mode should download remote-only files."""
        comparator = FileComparator(SyncMode.CLOUD_TO_LOCAL)
        remote_file = self._create_remote_file()

        decision = comparator._handle_remote_only("test.txt", remote_file)

        assert decision.action == SyncAction.DOWNLOAD
        assert decision.reason == "New remote file"

    def test_remote_only_local_to_cloud_deletes_remote(self):
        """LOCAL_TO_CLOUD mode should delete remote-only files (deleted locally)."""
        comparator = FileComparator(SyncMode.LOCAL_TO_CLOUD)
        remote_file = self._create_remote_file()

        decision = comparator._handle_remote_only("test.txt", remote_file)

        assert decision.action == SyncAction.DELETE_REMOTE
        assert decision.reason == "File deleted locally"

    def test_remote_only_local_backup_skips(self):
        """LOCAL_BACKUP mode should skip remote-only files (no download, no delete)."""
        comparator = FileComparator(SyncMode.LOCAL_BACKUP)
        remote_file = self._create_remote_file()

        decision = comparator._handle_remote_only("test.txt", remote_file)

        assert decision.action == SyncAction.SKIP
        assert "prevents action" in decision.reason
