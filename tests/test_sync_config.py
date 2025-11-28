"""Unit tests for sync config loading."""

import json

import pytest

from pydrime.sync.config import SyncConfigError, load_sync_pairs_from_json


class TestLoadSyncPairsFromJson:
    """Tests for load_sync_pairs_from_json function."""

    def test_load_valid_config(self, tmp_path):
        """Test loading a valid sync config file."""
        # Create a valid local directory
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "twoWay",
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        result = load_sync_pairs_from_json(config_file)

        assert len(result) == 1
        assert result[0]["local"] == str(local_dir)
        assert result[0]["remote"] == "/remote/path"
        assert result[0]["syncMode"] == "twoWay"
        # Check defaults are applied (None means use default workspace)
        assert result[0]["workspace"] is None
        assert result[0]["disableLocalTrash"] is False
        assert result[0]["ignore"] == []
        assert result[0]["excludeDotFiles"] is False

    def test_load_config_with_all_options(self, tmp_path):
        """Test loading a config with all optional fields."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "localToCloud",
                "workspace": 5,
                "disableLocalTrash": True,
                "ignore": ["*.tmp", "*.log"],
                "excludeDotFiles": True,
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        result = load_sync_pairs_from_json(config_file)

        assert len(result) == 1
        assert result[0]["workspace"] == 5
        assert result[0]["disableLocalTrash"] is True
        assert result[0]["ignore"] == ["*.tmp", "*.log"]
        assert result[0]["excludeDotFiles"] is True

    def test_load_multiple_pairs(self, tmp_path):
        """Test loading multiple sync pairs."""
        local_dir1 = tmp_path / "folder1"
        local_dir1.mkdir()
        local_dir2 = tmp_path / "folder2"
        local_dir2.mkdir()

        config = [
            {
                "local": str(local_dir1),
                "remote": "/remote/path1",
                "syncMode": "twoWay",
            },
            {
                "local": str(local_dir2),
                "remote": "/remote/path2",
                "syncMode": "cloudToLocal",
            },
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        result = load_sync_pairs_from_json(config_file)

        assert len(result) == 2
        assert result[0]["local"] == str(local_dir1)
        assert result[1]["local"] == str(local_dir2)

    def test_invalid_json_raises_error(self, tmp_path):
        """Test that invalid JSON raises SyncConfigError."""
        config_file = tmp_path / "invalid.json"
        config_file.write_text("not valid json {")

        with pytest.raises(SyncConfigError, match="Invalid JSON"):
            load_sync_pairs_from_json(config_file)

    def test_nonexistent_file_raises_error(self, tmp_path):
        """Test that nonexistent file raises SyncConfigError."""
        config_file = tmp_path / "nonexistent.json"

        with pytest.raises(SyncConfigError, match="Cannot read file"):
            load_sync_pairs_from_json(config_file)

    def test_not_a_list_raises_error(self, tmp_path):
        """Test that non-list JSON raises SyncConfigError."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"local": "/path", "remote": "/remote"}))

        with pytest.raises(SyncConfigError, match="expected a list"):
            load_sync_pairs_from_json(config_file)

    def test_empty_list_raises_error(self, tmp_path):
        """Test that empty list raises SyncConfigError."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps([]))

        with pytest.raises(SyncConfigError, match="No sync pairs found"):
            load_sync_pairs_from_json(config_file)

    def test_pair_not_dict_raises_error(self, tmp_path):
        """Test that non-dict pair raises SyncConfigError."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(["not a dict"]))

        with pytest.raises(SyncConfigError, match="expected a dictionary"):
            load_sync_pairs_from_json(config_file)

    def test_missing_required_fields_raises_error(self, tmp_path):
        """Test that missing required fields raises SyncConfigError."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps([{"local": "/path"}]))

        with pytest.raises(SyncConfigError, match="missing required fields"):
            load_sync_pairs_from_json(config_file)

    def test_invalid_sync_mode_raises_error(self, tmp_path):
        """Test that invalid sync mode raises SyncConfigError."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "invalidMode",
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(SyncConfigError, match="Invalid sync mode"):
            load_sync_pairs_from_json(config_file)

    def test_local_path_not_exists_raises_error(self, tmp_path):
        """Test that nonexistent local path raises SyncConfigError."""
        config = [
            {
                "local": str(tmp_path / "nonexistent"),
                "remote": "/remote/path",
                "syncMode": "twoWay",
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(SyncConfigError, match="local path does not exist"):
            load_sync_pairs_from_json(config_file)

    def test_local_path_not_directory_raises_error(self, tmp_path):
        """Test that local path being a file raises SyncConfigError."""
        local_file = tmp_path / "file.txt"
        local_file.write_text("content")

        config = [
            {
                "local": str(local_file),
                "remote": "/remote/path",
                "syncMode": "twoWay",
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(SyncConfigError, match="not a directory"):
            load_sync_pairs_from_json(config_file)

    def test_invalid_workspace_type_raises_error(self, tmp_path):
        """Test that invalid workspace type raises SyncConfigError."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        # workspace can be int, str, or None - but not a list or dict
        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "twoWay",
                "workspace": ["not", "valid"],
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(
            SyncConfigError, match="workspace must be an integer or string"
        ):
            load_sync_pairs_from_json(config_file)

    def test_workspace_can_be_string(self, tmp_path):
        """Test that workspace can be a string (workspace name)."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "twoWay",
                "workspace": "My Team Workspace",
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        result = load_sync_pairs_from_json(config_file)
        assert result[0]["workspace"] == "My Team Workspace"

    def test_workspace_can_be_int(self, tmp_path):
        """Test that workspace can be an integer (workspace ID)."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "twoWay",
                "workspace": 5,
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        result = load_sync_pairs_from_json(config_file)
        assert result[0]["workspace"] == 5

    def test_invalid_disable_local_trash_type_raises_error(self, tmp_path):
        """Test that non-boolean disableLocalTrash raises SyncConfigError."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "twoWay",
                "disableLocalTrash": "not_a_bool",
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(
            SyncConfigError, match="disableLocalTrash must be a boolean"
        ):
            load_sync_pairs_from_json(config_file)

    def test_invalid_ignore_type_raises_error(self, tmp_path):
        """Test that non-list ignore raises SyncConfigError."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "twoWay",
                "ignore": "not_a_list",
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(SyncConfigError, match="ignore must be a list"):
            load_sync_pairs_from_json(config_file)

    def test_invalid_exclude_dot_files_type_raises_error(self, tmp_path):
        """Test that non-boolean excludeDotFiles raises SyncConfigError."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        config = [
            {
                "local": str(local_dir),
                "remote": "/remote/path",
                "syncMode": "twoWay",
                "excludeDotFiles": "not_a_bool",
            }
        ]

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(SyncConfigError, match="excludeDotFiles must be a boolean"):
            load_sync_pairs_from_json(config_file)

    def test_all_sync_modes_are_valid(self, tmp_path):
        """Test that all valid sync modes work."""
        local_dir = tmp_path / "sync_folder"
        local_dir.mkdir()

        valid_modes = [
            "twoWay",
            "localToCloud",
            "localBackup",
            "cloudToLocal",
            "cloudBackup",
        ]

        for mode in valid_modes:
            config = [
                {
                    "local": str(local_dir),
                    "remote": "/remote/path",
                    "syncMode": mode,
                }
            ]

            config_file = tmp_path / "config.json"
            config_file.write_text(json.dumps(config))

            result = load_sync_pairs_from_json(config_file)
            assert result[0]["syncMode"] == mode


class TestSyncConfigError:
    """Tests for SyncConfigError exception."""

    def test_sync_config_error_is_exception(self):
        """Test that SyncConfigError is an Exception."""
        error = SyncConfigError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"
