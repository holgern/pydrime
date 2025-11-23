"""Unit tests for sync pairs."""

from pathlib import Path

import pytest

from pydrime.sync.modes import SyncMode
from pydrime.sync.pair import SyncPair


class TestSyncPair:
    """Tests for SyncPair class."""

    def test_create_sync_pair(self):
        """Test creating a basic sync pair."""
        pair = SyncPair(
            local=Path("/home/user/Documents"),
            remote="/Documents",
            sync_mode=SyncMode.TWO_WAY,
        )

        assert pair.local == Path("/home/user/Documents")
        assert pair.remote == "Documents"  # Normalized without leading/trailing slashes
        assert pair.sync_mode == SyncMode.TWO_WAY
        assert pair.alias is None
        assert pair.workspace_id == 0

    def test_sync_pair_with_options(self):
        """Test creating sync pair with all options."""
        pair = SyncPair(
            local=Path("/home/user/Documents"),
            remote="/Documents",
            sync_mode=SyncMode.LOCAL_TO_CLOUD,
            alias="documents",
            disable_local_trash=True,
            ignore=["*.log", "*.tmp"],
            exclude_dot_files=True,
            workspace_id=5,
        )

        assert pair.alias == "documents"
        assert pair.disable_local_trash is True
        assert pair.ignore == ["*.log", "*.tmp"]
        assert pair.exclude_dot_files is True
        assert pair.workspace_id == 5

    def test_sync_pair_normalization(self):
        """Test that paths are normalized."""
        pair = SyncPair(
            local="/home/user/Documents",  # String converted to Path
            remote="/Documents/",  # Trailing slash removed
            sync_mode="twoWay",  # String converted to SyncMode
        )

        assert isinstance(pair.local, Path)
        assert pair.remote == "Documents"
        assert pair.sync_mode == SyncMode.TWO_WAY

    def test_from_dict(self):
        """Test creating sync pair from dictionary."""
        data = {
            "local": "/home/user/Documents",
            "remote": "/Documents",
            "syncMode": "twoWay",
            "alias": "documents",
            "disableLocalTrash": True,
            "ignore": ["*.log"],
            "excludeDotFiles": True,
            "workspaceId": 5,
        }

        pair = SyncPair.from_dict(data)

        assert pair.local == Path("/home/user/Documents")
        assert pair.remote == "Documents"
        assert pair.sync_mode == SyncMode.TWO_WAY
        assert pair.alias == "documents"
        assert pair.disable_local_trash is True
        assert pair.ignore == ["*.log"]
        assert pair.exclude_dot_files is True
        assert pair.workspace_id == 5

    def test_from_dict_minimal(self):
        """Test creating sync pair from dictionary with minimal fields."""
        data = {
            "local": "/home/user/Documents",
            "remote": "/Documents",
            "syncMode": "twoWay",
        }

        pair = SyncPair.from_dict(data)

        assert pair.local == Path("/home/user/Documents")
        assert pair.remote == "Documents"
        assert pair.sync_mode == SyncMode.TWO_WAY
        assert pair.alias is None
        assert pair.disable_local_trash is False
        assert pair.ignore == []
        assert pair.exclude_dot_files is False
        assert pair.workspace_id == 0

    def test_from_dict_missing_required_field(self):
        """Test that missing required fields raise ValueError."""
        data = {
            "local": "/home/user/Documents",
            # Missing remote and syncMode
        }

        with pytest.raises(ValueError, match="Missing required fields"):
            SyncPair.from_dict(data)

    def test_to_dict(self):
        """Test converting sync pair to dictionary."""
        pair = SyncPair(
            local=Path("/home/user/Documents"),
            remote="/Documents",
            sync_mode=SyncMode.TWO_WAY,
            alias="documents",
            disable_local_trash=True,
            ignore=["*.log"],
            exclude_dot_files=True,
            workspace_id=5,
        )

        data = pair.to_dict()

        assert data == {
            "local": "/home/user/Documents",
            "remote": "Documents",
            "syncMode": "twoWay",
            "alias": "documents",
            "disableLocalTrash": True,
            "ignore": ["*.log"],
            "excludeDotFiles": True,
            "workspaceId": 5,
        }

    def test_parse_literal_simple(self):
        """Test parsing simple literal sync pair."""
        pair = SyncPair.parse_literal("/home/user/docs:/Documents")

        assert pair.local == Path("/home/user/docs")
        assert pair.remote == "Documents"
        assert pair.sync_mode == SyncMode.TWO_WAY  # Default

    def test_parse_literal_with_mode(self):
        """Test parsing literal sync pair with mode."""
        pair = SyncPair.parse_literal("/home/user/docs:localToCloud:/Documents")

        assert pair.local == Path("/home/user/docs")
        assert pair.remote == "Documents"
        assert pair.sync_mode == SyncMode.LOCAL_TO_CLOUD

    def test_parse_literal_with_abbreviation(self):
        """Test parsing literal sync pair with abbreviated mode."""
        pair = SyncPair.parse_literal("/home/user/docs:ltc:/Documents")

        assert pair.local == Path("/home/user/docs")
        assert pair.remote == "Documents"
        assert pair.sync_mode == SyncMode.LOCAL_TO_CLOUD

    def test_parse_literal_invalid_format(self):
        """Test that invalid literal format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid sync pair literal"):
            SyncPair.parse_literal("invalid")

        with pytest.raises(ValueError, match="Invalid sync pair literal"):
            SyncPair.parse_literal("too:many:colons:here")

    def test_parse_literal_empty_paths(self):
        """Test that empty paths raise ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            SyncPair.parse_literal(":/Documents")

        with pytest.raises(ValueError, match="cannot be empty"):
            SyncPair.parse_literal("/home/user/docs:")

    def test_str_representation(self):
        """Test string representation."""
        pair = SyncPair(
            local=Path("/home/user/docs"),
            remote="/Documents",
            sync_mode=SyncMode.TWO_WAY,
        )

        assert "twoWay" in str(pair)
        assert "/home/user/docs" in str(pair)
        assert "Documents" in str(pair)

    def test_str_representation_with_alias(self):
        """Test string representation with alias."""
        pair = SyncPair(
            local=Path("/home/user/docs"),
            remote="/Documents",
            sync_mode=SyncMode.TWO_WAY,
            alias="documents",
        )

        result = str(pair)
        assert "documents" in result

    def test_repr(self):
        """Test repr representation."""
        pair = SyncPair(
            local=Path("/home/user/docs"),
            remote="/Documents",
            sync_mode=SyncMode.TWO_WAY,
        )

        result = repr(pair)
        assert "SyncPair" in result
        assert "local=" in result
        assert "remote=" in result
        assert "sync_mode=" in result


class TestSyncPairLiteralParsing:
    """Comprehensive tests for literal sync pair parsing with all 5 sync modes."""

    def test_parse_literal_two_way_shorthand(self):
        """Test parsing two-way sync with shorthand notation."""
        pair = SyncPair.parse_literal("./local:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.TWO_WAY

    def test_parse_literal_two_way_full_name(self):
        """Test parsing two-way sync with full mode name."""
        pair = SyncPair.parse_literal("./local:twoWay:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.TWO_WAY

    def test_parse_literal_two_way_abbreviation(self):
        """Test parsing two-way sync with abbreviation."""
        pair = SyncPair.parse_literal("./local:tw:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.TWO_WAY

    def test_parse_literal_local_to_cloud_full_name(self):
        """Test parsing local-to-cloud sync with full mode name."""
        pair = SyncPair.parse_literal("./local:localToCloud:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.LOCAL_TO_CLOUD

    def test_parse_literal_local_to_cloud_abbreviation(self):
        """Test parsing local-to-cloud sync with abbreviation."""
        pair = SyncPair.parse_literal("./local:ltc:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.LOCAL_TO_CLOUD

    def test_parse_literal_local_backup_full_name(self):
        """Test parsing local backup sync with full mode name."""
        pair = SyncPair.parse_literal("./local:localBackup:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.LOCAL_BACKUP

    def test_parse_literal_local_backup_abbreviation(self):
        """Test parsing local backup sync with abbreviation."""
        pair = SyncPair.parse_literal("./local:lb:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.LOCAL_BACKUP

    def test_parse_literal_cloud_to_local_full_name(self):
        """Test parsing cloud-to-local sync with full mode name."""
        pair = SyncPair.parse_literal("./local:cloudToLocal:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.CLOUD_TO_LOCAL

    def test_parse_literal_cloud_to_local_abbreviation(self):
        """Test parsing cloud-to-local sync with abbreviation."""
        pair = SyncPair.parse_literal("./local:ctl:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.CLOUD_TO_LOCAL

    def test_parse_literal_cloud_backup_full_name(self):
        """Test parsing cloud backup sync with full mode name."""
        pair = SyncPair.parse_literal("./local:cloudBackup:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.CLOUD_BACKUP

    def test_parse_literal_cloud_backup_abbreviation(self):
        """Test parsing cloud backup sync with abbreviation."""
        pair = SyncPair.parse_literal("./local:cb:/remote")
        assert str(pair.local) == "local"
        assert pair.remote == "remote"
        assert pair.sync_mode == SyncMode.CLOUD_BACKUP
