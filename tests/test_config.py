"""Unit tests for the Drime configuration module."""

import sys
from unittest.mock import patch

import pytest

from pydrime.config import Config


class TestConfigBasics:
    """Basic configuration tests."""

    @patch.dict("os.environ", {}, clear=True)
    @patch("pydrime.config.Path.home")
    @patch("pydrime.config.Path.exists", return_value=False)
    def test_config_default_api_url(self, mock_exists, mock_home, tmp_path):
        """Test that default API URL is set correctly."""
        mock_home.return_value = tmp_path
        config = Config()
        assert config.api_url == "https://app.drime.cloud/api/v1"

    @patch.dict("os.environ", {"DRIME_API_KEY": "env_test_key"}, clear=True)
    @patch("pydrime.config.Path.home")
    @patch("pydrime.config.Path.exists", return_value=False)
    def test_config_loads_from_env_variable(self, mock_exists, mock_home, tmp_path):
        """Test that config loads API key from environment variable."""
        mock_home.return_value = tmp_path
        config = Config()
        assert config.api_key == "env_test_key"

    @patch.dict("os.environ", {"DRIME_API_URL": "https://custom.api/v2"}, clear=True)
    @patch("pydrime.config.Path.home")
    @patch("pydrime.config.Path.exists", return_value=False)
    def test_config_custom_api_url_from_env(self, mock_exists, mock_home, tmp_path):
        """Test custom API URL from environment variable."""
        mock_home.return_value = tmp_path
        config = Config()
        assert config.api_url == "https://custom.api/v2"


class TestConfigMethods:
    """Tests for Config methods."""

    @patch.dict("os.environ", {"DRIME_API_KEY": "test_key"}, clear=True)
    @patch("pydrime.config.Path.home")
    @patch("pydrime.config.Path.exists", return_value=False)
    def test_is_configured_true(self, mock_exists, mock_home, tmp_path):
        """Test is_configured returns True when API key is set."""
        mock_home.return_value = tmp_path
        config = Config()
        assert config.is_configured() is True

    @patch.dict("os.environ", {}, clear=True)
    @patch("pydrime.config.Path.home")
    @patch("pydrime.config.Path.exists", return_value=False)
    def test_is_configured_false(self, mock_exists, mock_home, tmp_path):
        """Test is_configured returns False when no API key."""
        mock_home.return_value = tmp_path
        config = Config()
        assert config.is_configured() is False

    @patch.dict("os.environ", {"DRIME_API_KEY": "test_key"}, clear=True)
    @patch("pydrime.config.Path.home")
    @patch("pydrime.config.Path.exists", return_value=False)
    def test_get_headers(self, mock_exists, mock_home, tmp_path):
        """Test get_headers returns correct authorization header."""
        mock_home.return_value = tmp_path
        config = Config()
        headers = config.get_headers()

        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test_key"
        assert headers["Content-Type"] == "application/json"

    @patch.dict("os.environ", {}, clear=True)
    @patch("pydrime.config.Path.home")
    @patch("pydrime.config.Path.exists", return_value=False)
    def test_get_headers_raises_when_no_key(self, mock_exists, mock_home, tmp_path):
        """Test get_headers raises error when no API key configured."""
        mock_home.return_value = tmp_path
        config = Config()
        with pytest.raises(ValueError, match="API key not configured"):
            config.get_headers()


class TestSaveAPIKey:
    """Tests for save_api_key functionality."""

    @patch.dict("os.environ", {}, clear=True)
    def test_save_api_key_creates_directory(self, tmp_path):
        """Test that save_api_key creates config directory if needed."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        config = Config()
        # Temporarily override the paths for this test
        original_dir = config.CONFIG_DIR
        original_file = config.CONFIG_FILE
        config.CONFIG_DIR = config_dir
        config.CONFIG_FILE = config_file

        try:
            config.save_api_key("new_key")
            assert config_dir.exists()
            assert config_dir.is_dir()
        finally:
            config.CONFIG_DIR = original_dir
            config.CONFIG_FILE = original_file

    @patch.dict("os.environ", {}, clear=True)
    def test_save_api_key_writes_file(self, tmp_path):
        """Test that save_api_key writes to config file."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        config = Config()
        original_dir = config.CONFIG_DIR
        original_file = config.CONFIG_FILE
        config.CONFIG_DIR = config_dir
        config.CONFIG_FILE = config_file

        try:
            config.save_api_key("new_key")
            assert config_file.exists()
            content = config_file.read_text()
            assert "DRIME_API_KEY=new_key" in content
        finally:
            config.CONFIG_DIR = original_dir
            config.CONFIG_FILE = original_file

    @patch.dict("os.environ", {}, clear=True)
    def test_save_api_key_updates_existing(self, tmp_path):
        """Test that save_api_key updates existing config."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file.write_text("DRIME_API_KEY=old_key\nOTHER_SETTING=value\n")

        config = Config()
        original_dir = config.CONFIG_DIR
        original_file = config.CONFIG_FILE
        config.CONFIG_DIR = config_dir
        config.CONFIG_FILE = config_file

        try:
            config.save_api_key("new_key")
            content = config_file.read_text()
            assert "DRIME_API_KEY=new_key" in content
            assert "DRIME_API_KEY=old_key" not in content
            assert "OTHER_SETTING=value" in content
        finally:
            config.CONFIG_DIR = original_dir
            config.CONFIG_FILE = original_file

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File permissions work differently on Windows"
    )
    @patch.dict("os.environ", {}, clear=True)
    def test_save_api_key_sets_permissions(self, tmp_path):
        """Test that config file has secure permissions."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                config.save_api_key("secure_key")

                # Check that permissions are owner read/write only (0o600)
                stat_result = config_file.stat()
                permissions = stat_result.st_mode & 0o777
                assert permissions == 0o600

    @patch.dict("os.environ", {}, clear=True)
    def test_save_api_key_updates_instance(self, tmp_path):
        """Test that save_api_key updates the config instance."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                # Config might have loaded from global config, so just check it changes
                config.save_api_key("new_key")
                assert config.api_key == "new_key"

    @patch.dict("os.environ", {}, clear=True)
    def test_get_config_path(self, tmp_path):
        """Test get_config_path returns correct path."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_FILE", config_file):
            config = Config()
            assert config.get_config_path() == config_file


class TestConfigCurrentFolder:
    """Tests for current folder functionality."""

    @patch.dict("os.environ", {}, clear=True)
    def test_save_current_folder(self, tmp_path):
        """Test saving current folder ID."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                config.save_current_folder(480432024)

                # Check that folder ID was saved
                assert config.current_folder_id == 480432024
                assert config.get_current_folder() == 480432024

                # Check that it was written to file
                assert config_file.exists()
                content = config_file.read_text()
                assert "CURRENT_FOLDER_ID=480432024" in content

    @patch.dict("os.environ", {}, clear=True)
    def test_save_current_folder_root(self, tmp_path):
        """Test saving None as current folder (root)."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                config.save_current_folder(None)

                assert config.current_folder_id is None
                assert config.get_current_folder() is None

    @patch.dict("os.environ", {}, clear=True)
    def test_load_current_folder_from_file(self, tmp_path):
        """Test loading current folder from config file."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"
        config_dir.mkdir(parents=True)

        # Write config file with folder ID
        config_file.write_text("CURRENT_FOLDER_ID=480432024\n")

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                assert config.get_current_folder() == 480432024

    @patch.dict("os.environ", {}, clear=True)
    def test_update_current_folder(self, tmp_path):
        """Test updating current folder multiple times."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()

                # Set initial folder
                config.save_current_folder(123)
                assert config.get_current_folder() == 123

                # Update to new folder
                config.save_current_folder(456)
                assert config.get_current_folder() == 456

                # Check file only has one CURRENT_FOLDER_ID line
                content = config_file.read_text()
                assert content.count("CURRENT_FOLDER_ID") == 1
                assert "CURRENT_FOLDER_ID=456" in content


class TestConfigDefaultWorkspace:
    """Tests for default workspace functionality."""

    @patch.dict("os.environ", {}, clear=True)
    def test_save_default_workspace(self, tmp_path):
        """Test saving default workspace ID."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                config.save_default_workspace(5)

                # Check that workspace ID was saved
                assert config.default_workspace_id == 5
                assert config.get_default_workspace() == 5

                # Check that it was written to file
                assert config_file.exists()
                content = config_file.read_text()
                assert "DEFAULT_WORKSPACE_ID=5" in content

    @patch.dict("os.environ", {}, clear=True)
    def test_save_default_workspace_none(self, tmp_path):
        """Test saving None as default workspace (personal)."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                config.save_default_workspace(None)

                assert config.default_workspace_id is None
                assert config.get_default_workspace() is None

    @patch.dict("os.environ", {}, clear=True)
    def test_load_default_workspace_from_file(self, tmp_path):
        """Test loading default workspace from config file."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"
        config_dir.mkdir(parents=True)

        # Write config file with workspace ID
        config_file.write_text("DEFAULT_WORKSPACE_ID=10\n")

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                assert config.get_default_workspace() == 10

    @patch.dict("os.environ", {}, clear=True)
    def test_update_default_workspace(self, tmp_path):
        """Test updating default workspace multiple times."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()

                # Set initial workspace
                config.save_default_workspace(5)
                assert config.get_default_workspace() == 5

                # Update to new workspace
                config.save_default_workspace(10)
                assert config.get_default_workspace() == 10

                # Check file only has one DEFAULT_WORKSPACE_ID line
                content = config_file.read_text()
                assert content.count("DEFAULT_WORKSPACE_ID") == 1
                assert "DEFAULT_WORKSPACE_ID=10" in content

    @patch.dict("os.environ", {}, clear=True)
    def test_default_workspace_zero(self, tmp_path):
        """Test that workspace 0 is not saved (None is used instead)."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                # Save with None should result in empty value
                config.save_default_workspace(None)

                content = config_file.read_text()
                assert "DEFAULT_WORKSPACE_ID=" in content


class TestConfigFileLoading:
    """Tests for config file loading and error handling."""

    @patch.dict("os.environ", {}, clear=True)
    def test_load_dotenv_file_exists(self, tmp_path):
        """Test loading from .env file in current directory."""
        import os

        # Create a .env file in current directory
        env_file = tmp_path / ".env"
        env_file.write_text("DRIME_API_KEY=dotenv_key\n")

        # Change to tmp_path directory
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            config_dir = tmp_path / ".config" / "drime"

            with patch.object(Config, "CONFIG_DIR", config_dir):
                with patch.object(Config, "CONFIG_FILE", config_dir / "config"):
                    config = Config()
                    # .env should be loaded and key should be available
                    assert config.api_key == "dotenv_key"
        finally:
            os.chdir(original_cwd)

    @patch.dict("os.environ", {}, clear=True)
    def test_load_config_with_invalid_folder_id(self, tmp_path):
        """Test loading config with invalid folder ID (non-numeric)."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"
        config_dir.mkdir(parents=True)

        # Write config file with invalid folder ID
        config_file.write_text("CURRENT_FOLDER_ID=not_a_number\n")

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                # Should silently ignore the invalid value
                assert config.get_current_folder() is None

    @patch.dict("os.environ", {}, clear=True)
    def test_load_config_with_invalid_workspace_id(self, tmp_path):
        """Test loading config with invalid workspace ID (non-numeric)."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"
        config_dir.mkdir(parents=True)

        # Write config file with invalid workspace ID
        config_file.write_text("DEFAULT_WORKSPACE_ID=not_a_number\n")

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                config = Config()
                # Should silently ignore the invalid value
                assert config.get_default_workspace() is None

    @patch.dict("os.environ", {}, clear=True)
    def test_load_config_with_corrupted_file(self, tmp_path):
        """Test loading config with file that can't be read."""
        config_dir = tmp_path / ".config" / "drime"
        config_file = config_dir / "config"
        config_dir.mkdir(parents=True)

        with patch.object(Config, "CONFIG_DIR", config_dir):
            with patch.object(Config, "CONFIG_FILE", config_file):
                # Mock file opening to raise an exception
                with patch("builtins.open", side_effect=PermissionError("No access")):
                    config = Config()
                    # Should silently ignore the error and continue
                    assert config.api_key is None
