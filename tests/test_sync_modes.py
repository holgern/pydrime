"""Unit tests for sync modes."""

import pytest

from pydrime.sync.modes import SyncMode


class TestSyncMode:
    """Tests for SyncMode enum."""

    def test_sync_mode_values(self):
        """Test that all sync modes have correct values."""
        assert SyncMode.TWO_WAY.value == "twoWay"
        assert SyncMode.LOCAL_TO_CLOUD.value == "localToCloud"
        assert SyncMode.LOCAL_BACKUP.value == "localBackup"
        assert SyncMode.CLOUD_TO_LOCAL.value == "cloudToLocal"
        assert SyncMode.CLOUD_BACKUP.value == "cloudBackup"

    def test_from_string_full_names(self):
        """Test parsing full sync mode names."""
        assert SyncMode.from_string("twoWay") == SyncMode.TWO_WAY
        assert SyncMode.from_string("localToCloud") == SyncMode.LOCAL_TO_CLOUD
        assert SyncMode.from_string("localBackup") == SyncMode.LOCAL_BACKUP
        assert SyncMode.from_string("cloudToLocal") == SyncMode.CLOUD_TO_LOCAL
        assert SyncMode.from_string("cloudBackup") == SyncMode.CLOUD_BACKUP

    def test_from_string_abbreviations(self):
        """Test parsing abbreviated sync mode names."""
        assert SyncMode.from_string("tw") == SyncMode.TWO_WAY
        assert SyncMode.from_string("ltc") == SyncMode.LOCAL_TO_CLOUD
        assert SyncMode.from_string("lb") == SyncMode.LOCAL_BACKUP
        assert SyncMode.from_string("ctl") == SyncMode.CLOUD_TO_LOCAL
        assert SyncMode.from_string("cb") == SyncMode.CLOUD_BACKUP

    def test_from_string_case_insensitive(self):
        """Test that parsing is case-insensitive."""
        assert SyncMode.from_string("TWOWAY") == SyncMode.TWO_WAY
        assert SyncMode.from_string("TW") == SyncMode.TWO_WAY
        assert SyncMode.from_string("LocalToCloud") == SyncMode.LOCAL_TO_CLOUD

    def test_from_string_invalid(self):
        """Test that invalid mode strings raise ValueError."""
        with pytest.raises(ValueError, match="Invalid sync mode"):
            SyncMode.from_string("invalid")

    def test_allows_upload(self):
        """Test allows_upload property."""
        assert SyncMode.TWO_WAY.allows_upload is True
        assert SyncMode.LOCAL_TO_CLOUD.allows_upload is True
        assert SyncMode.LOCAL_BACKUP.allows_upload is True
        assert SyncMode.CLOUD_TO_LOCAL.allows_upload is False
        assert SyncMode.CLOUD_BACKUP.allows_upload is False

    def test_allows_download(self):
        """Test allows_download property."""
        assert SyncMode.TWO_WAY.allows_download is True
        assert SyncMode.LOCAL_TO_CLOUD.allows_download is False
        assert SyncMode.LOCAL_BACKUP.allows_download is False
        assert SyncMode.CLOUD_TO_LOCAL.allows_download is True
        assert SyncMode.CLOUD_BACKUP.allows_download is True

    def test_allows_local_delete(self):
        """Test allows_local_delete property."""
        assert SyncMode.TWO_WAY.allows_local_delete is True
        assert SyncMode.LOCAL_TO_CLOUD.allows_local_delete is False
        assert SyncMode.LOCAL_BACKUP.allows_local_delete is False
        assert SyncMode.CLOUD_TO_LOCAL.allows_local_delete is True
        assert SyncMode.CLOUD_BACKUP.allows_local_delete is False

    def test_allows_remote_delete(self):
        """Test allows_remote_delete property."""
        assert SyncMode.TWO_WAY.allows_remote_delete is True
        assert SyncMode.LOCAL_TO_CLOUD.allows_remote_delete is True
        assert SyncMode.LOCAL_BACKUP.allows_remote_delete is False
        assert SyncMode.CLOUD_TO_LOCAL.allows_remote_delete is False
        assert SyncMode.CLOUD_BACKUP.allows_remote_delete is False

    def test_is_bidirectional(self):
        """Test is_bidirectional property."""
        assert SyncMode.TWO_WAY.is_bidirectional is True
        assert SyncMode.LOCAL_TO_CLOUD.is_bidirectional is False
        assert SyncMode.LOCAL_BACKUP.is_bidirectional is False
        assert SyncMode.CLOUD_TO_LOCAL.is_bidirectional is False
        assert SyncMode.CLOUD_BACKUP.is_bidirectional is False

    def test_str_representation(self):
        """Test string representation."""
        assert str(SyncMode.TWO_WAY) == "twoWay"
        assert str(SyncMode.LOCAL_TO_CLOUD) == "localToCloud"
