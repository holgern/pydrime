"""Unit tests for the Drime CLI commands."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from pydrime.cli import main
from pydrime.exceptions import DrimeAPIError, DrimeNotFoundError


@pytest.fixture
def runner():
    """Provide a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_config():
    """Mock the config module."""
    with patch("pydrime.cli.config") as mock:
        mock.is_configured.return_value = False
        mock.api_key = None
        yield mock


class TestMainGroup:
    """Tests for the main CLI group."""

    def test_main_help(self, runner):
        """Test main help shows all commands."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "PyDrime" in result.output
        assert "--api-key" in result.output
        assert "init" in result.output
        assert "upload" in result.output
        assert "ls" in result.output

    def test_main_with_global_api_key(self, runner):
        """Test that global --api-key option is accepted."""
        result = runner.invoke(main, ["--api-key", "test_key", "--help"])
        assert result.exit_code == 0


class TestInitCommand:
    """Tests for the init command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_init_with_valid_api_key(self, mock_config, mock_client_class, runner):
        """Test init with a valid API key."""
        # Mock the client and its methods
        mock_client = Mock()
        mock_client.get_logged_user.return_value = {
            "user": {"email": "test@example.com"}
        }
        mock_client_class.return_value = mock_client

        # Mock config methods
        mock_config.save_api_key = Mock()
        mock_config.get_config_path.return_value = Path("/mock/config")

        result = runner.invoke(main, ["init"], input="valid_api_key\n")

        assert result.exit_code == 0
        assert "API key is valid" in result.output
        assert "Configuration saved successfully" in result.output
        mock_config.save_api_key.assert_called_once_with("valid_api_key")

    @patch("pydrime.cli.DrimeClient")
    def test_init_with_invalid_api_key_cancel(self, mock_client_class, runner):
        """Test init with invalid API key and user cancels."""
        mock_client = Mock()
        mock_client.get_logged_user.return_value = {"user": None}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["init"], input="invalid_key\nn\n")

        assert result.exit_code == 1
        assert "Invalid API key" in result.output
        assert "Configuration cancelled" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_init_with_invalid_api_key_save_anyway(
        self, mock_config, mock_client_class, runner
    ):
        """Test init with invalid API key but user saves anyway."""
        mock_client = Mock()
        mock_client.get_logged_user.return_value = {"user": None}
        mock_client_class.return_value = mock_client

        mock_config.save_api_key = Mock()
        mock_config.get_config_path.return_value = Path("/mock/config")

        result = runner.invoke(main, ["init"], input="invalid_key\ny\n")

        assert result.exit_code == 0
        assert "Configuration saved successfully" in result.output
        mock_config.save_api_key.assert_called_once()

    @patch("pydrime.cli.DrimeClient")
    def test_init_with_network_error(self, mock_client_class, runner):
        """Test init when network error occurs."""
        mock_client = Mock()
        mock_client.get_logged_user.side_effect = DrimeAPIError("Network error")
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["init"], input="test_key\nn\n")

        assert result.exit_code == 1
        assert "Network error" in result.output


class TestStatusCommand:
    """Tests for the status command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_status_with_valid_api_key(self, mock_config, mock_client_class, runner):
        """Test status command with valid API key."""
        mock_config.is_configured.return_value = True
        mock_config.api_key = "valid_key"

        mock_client = Mock()
        mock_client.get_logged_user.return_value = {
            "user": {"email": "test@example.com", "id": 123}
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["status"])

        assert result.exit_code == 0
        # In text format, it shows just the summary line
        assert "test@example.com" in result.output
        assert "Email:" in result.output or "test@example.com" in result.output

    @patch("pydrime.cli.DrimeClient")
    def test_status_with_invalid_api_key(self, mock_client_class, runner):
        """Test status command with invalid API key."""
        mock_client = Mock()
        mock_client.get_logged_user.return_value = {"user": None}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["--api-key", "invalid", "status"])

        assert result.exit_code == 1
        assert "Invalid API key" in result.output

    @patch("pydrime.cli.config")
    def test_status_without_api_key(self, mock_config, runner):
        """Test status command without API key configured."""
        mock_config.is_configured.return_value = False

        result = runner.invoke(main, ["status"], env={"DRIME_API_KEY": ""})

        assert result.exit_code == 1
        assert "API key not configured" in result.output


class TestUploadCommand:
    """Tests for the upload command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    @patch("pydrime.cli.Path")
    def test_upload_file_success(
        self, mock_path_class, mock_config, mock_client_class, runner, tmp_path
    ):
        """Test successful file upload."""
        # Create a temporary test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        # Mock Path to return our test file
        mock_path = Mock()
        mock_path.is_file.return_value = True
        mock_path.name = "test.txt"
        mock_path.stat.return_value.st_size = 100
        mock_path_class.return_value = mock_path

        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main, ["upload", str(test_file), "--dry-run"], input="n\n"
        )

        assert "Dry run mode" in result.output or "Upload cancelled" in result.output

    @patch("pydrime.cli.config")
    def test_upload_without_api_key(self, mock_config, runner, tmp_path):
        """Test upload without API key configured."""
        mock_config.is_configured.return_value = False

        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        result = runner.invoke(
            main, ["upload", str(test_file)], env={"DRIME_API_KEY": ""}
        )

        assert result.exit_code == 1
        assert "API key not configured" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    @patch("pydrime.cli.Path")
    def test_upload_displays_destination_info(
        self, mock_path_class, mock_config, mock_client_class, runner, tmp_path
    ):
        """Test that upload displays workspace and parent folder information."""
        # Create a temporary test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        # Mock Path to return our test file
        mock_path = Mock()
        mock_path.is_file.return_value = True
        mock_path.name = "test.txt"
        mock_path.stat.return_value.st_size = 100
        mock_path_class.return_value = mock_path

        mock_config.is_configured.return_value = True
        mock_config.get_default_workspace.return_value = 5
        mock_config.get_current_folder.return_value = 123

        mock_client = Mock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [{"id": 5, "name": "Test Workspace"}]
        }
        mock_client.get_folder_info.return_value = {"name": "MyFolder"}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["upload", str(test_file), "--dry-run"])

        assert result.exit_code == 0
        assert "Workspace: Test Workspace (5)" in result.output
        assert "Parent folder: /MyFolder (ID: 123)" in result.output
        assert "Dry run mode" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    @patch("pydrime.cli.Path")
    def test_upload_displays_root_folder(
        self, mock_path_class, mock_config, mock_client_class, runner, tmp_path
    ):
        """Test that upload displays root folder when no current folder set."""
        # Create a temporary test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        # Mock Path to return our test file
        mock_path = Mock()
        mock_path.is_file.return_value = True
        mock_path.name = "test.txt"
        mock_path.stat.return_value.st_size = 100
        mock_path_class.return_value = mock_path

        mock_config.is_configured.return_value = True
        mock_config.get_default_workspace.return_value = None
        mock_config.get_current_folder.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["upload", str(test_file), "--dry-run"])

        assert result.exit_code == 0
        assert "Workspace: Personal (0)" in result.output
        assert "Parent folder: / (Root, ID: 0)" in result.output


class TestLsCommand:
    """Tests for the ls (list files) command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_ls_with_files(self, mock_config, mock_client_class, runner):
        """Test ls command with files present."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "file1.txt",
                    "file_name": "file1",
                    "mime": "text/plain",
                    "type": "file",
                    "file_size": 1024,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 2,
                    "name": "folder1",
                    "file_name": "",
                    "mime": "",
                    "type": "folder",
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2025-11-19T19:00:00.000000Z",
                    "extension": None,
                    "hash": "def456",
                    "url": "api/v1/file-entries/2",
                    "users": [],
                    "tags": [],
                },
            ]
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["ls"])

        assert result.exit_code == 0
        # Default text format shows summary
        assert "folder" in result.output
        assert "file" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_ls_no_files(self, mock_config, mock_client_class, runner):
        """Test ls command with no files."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_file_entries.return_value = {"data": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["ls"])

        assert result.exit_code == 0
        # ls command outputs nothing when directory is empty (like Unix ls)
        assert result.output.strip() == ""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_ls_with_query(self, mock_config, mock_client_class, runner):
        """Test ls command with search query."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_file_entries.return_value = {"data": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["ls", "--query", "test"])

        assert result.exit_code == 0
        mock_client.get_file_entries.assert_called_once()
        call_kwargs = mock_client.get_file_entries.call_args.kwargs
        assert call_kwargs["query"] == "test"

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_ls_with_folder_name(self, mock_config, mock_client_class, runner):
        """Test ls command with folder name instead of ID."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0

        mock_client = Mock()
        mock_client.resolve_folder_identifier.return_value = 123
        mock_client.get_file_entries.return_value = {
            "data": [{"id": 456, "name": "file.txt", "type": "file", "file_size": 100}]
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["ls", "test_folder"])

        assert result.exit_code == 0
        mock_client.resolve_folder_identifier.assert_called_once_with(
            identifier="test_folder", parent_id=None, workspace_id=0
        )
        mock_client.get_file_entries.assert_called_once()
        call_kwargs = mock_client.get_file_entries.call_args.kwargs
        assert call_kwargs["parent_ids"] == [123]

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_ls_with_folder_id(self, mock_config, mock_client_class, runner):
        """Test ls command with numeric folder ID."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0

        mock_client = Mock()
        mock_client.resolve_folder_identifier.return_value = 123
        mock_client.get_file_entries.return_value = {
            "data": [{"id": 456, "name": "file.txt", "type": "file", "file_size": 100}]
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["ls", "123"])

        assert result.exit_code == 0
        # Should still resolve the identifier
        mock_client.resolve_folder_identifier.assert_called_once_with(
            identifier="123", parent_id=None, workspace_id=0
        )


class TestDuCommand:
    """Tests for the du (disk usage) command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_du_with_files(self, mock_config, mock_client_class, runner):
        """Test du command with files."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0

        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "test_folder",
                    "type": "folder",
                    "file_size": 0,
                    "hash": "abc123",
                    "created_at": "2024-01-01T00:00:00Z",
                    "file_name": "test_folder",
                    "mime": "",
                    "parent_id": None,
                    "url": "",
                },
                {
                    "id": 2,
                    "name": "test_file.txt",
                    "type": "file",
                    "file_size": 1024,
                    "hash": "def456",
                    "created_at": "2024-01-01T00:00:00Z",
                    "file_name": "test_file.txt",
                    "mime": "text/plain",
                    "parent_id": None,
                    "url": "",
                },
            ]
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["du"])

        assert result.exit_code == 0
        # Default text format shows summary with folder and file counts
        assert "folder" in result.output
        assert "file" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_du_no_files(self, mock_config, mock_client_class, runner):
        """Test du command with no files."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_file_entries.return_value = {"data": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["du"])

        assert result.exit_code == 0
        # du command outputs a warning when directory is empty
        assert "No files found" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_du_with_query(self, mock_config, mock_client_class, runner):
        """Test du command with search query."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_file_entries.return_value = {"data": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["du", "--query", "test"])

        assert result.exit_code == 0
        mock_client.get_file_entries.assert_called_once()
        call_kwargs = mock_client.get_file_entries.call_args.kwargs
        assert call_kwargs["query"] == "test"

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_du_with_folder_name(self, mock_config, mock_client_class, runner):
        """Test du command with folder name instead of ID."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0

        mock_client = Mock()
        mock_client.resolve_folder_identifier.return_value = 123
        mock_client.get_file_entries.return_value = {
            "data": [{"id": 456, "name": "file.txt", "type": "file", "file_size": 100}]
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["du", "test_folder"])

        assert result.exit_code == 0
        mock_client.resolve_folder_identifier.assert_called_once_with(
            identifier="test_folder", parent_id=None, workspace_id=0
        )
        mock_client.get_file_entries.assert_called_once()
        call_kwargs = mock_client.get_file_entries.call_args.kwargs
        assert call_kwargs["parent_ids"] == [123]


class TestMkdirCommand:
    """Tests for the mkdir command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_mkdir_success(self, mock_config, mock_client_class, runner):
        """Test successful directory creation."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.create_directory.return_value = {
            "folder": {"id": 1, "name": "test_folder"}
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["mkdir", "test_folder"])

        assert result.exit_code == 0
        assert "Directory created" in result.output
        assert "test_folder" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_mkdir_with_parent(self, mock_config, mock_client_class, runner):
        """Test directory creation with parent ID."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.create_directory.return_value = {"folder": {"id": 2}}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["mkdir", "subfolder", "--parent-id", "1"])

        assert result.exit_code == 0
        mock_client.create_directory.assert_called_once_with(
            name="subfolder", parent_id=1
        )


class TestRmCommand:
    """Tests for the rm (delete) command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_rm_to_trash(self, mock_config, mock_client_class, runner):
        """Test moving files to trash."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.resolve_entry_identifier.side_effect = (
            lambda identifier, **kwargs: int(identifier)
        )
        mock_client.delete_file_entries.return_value = {"status": "success"}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["rm", "1", "2"], input="y\n")

        assert result.exit_code == 0
        assert "Moved" in result.output and "trash" in result.output
        mock_client.delete_file_entries.assert_called_once_with(
            [1, 2], delete_forever=False
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_rm_permanent(self, mock_config, mock_client_class, runner):
        """Test permanent file deletion."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.resolve_entry_identifier.side_effect = (
            lambda identifier, **kwargs: int(identifier)
        )
        mock_client.delete_file_entries.return_value = {"status": "success"}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["rm", "1", "--permanent"], input="y\n")

        assert result.exit_code == 0
        assert "Permanently deleted" in result.output
        mock_client.delete_file_entries.assert_called_once_with(
            [1], delete_forever=True
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_rm_cancel(self, mock_config, mock_client_class, runner):
        """Test canceling file deletion."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.resolve_entry_identifier.side_effect = (
            lambda identifier, **kwargs: int(identifier)
        )
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["rm", "1"], input="n\n")

        assert "Deletion cancelled" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_rm_by_name(self, mock_config, mock_client_class, runner):
        """Test deleting file by name."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        # Simulate resolving name to ID
        mock_client.resolve_entry_identifier.return_value = 123
        mock_client.delete_file_entries.return_value = {"status": "success"}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["rm", "test.txt"], input="y\n")

        assert result.exit_code == 0
        assert "Resolved 'test.txt' to entry ID: 123" in result.output
        mock_client.delete_file_entries.assert_called_once_with(
            [123], delete_forever=False
        )


class TestWorkspacesCommand:
    """Tests for the workspaces command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspaces_list(self, mock_config, mock_client_class, runner):
        """Test listing workspaces."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {
                    "id": 1,
                    "name": "My Workspace",
                    "currentUser": {"role_name": "owner"},
                    "owner": {"email": "owner@example.com"},
                }
            ]
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["workspaces"])

        assert result.exit_code == 0
        assert "My Workspace" in result.output
        assert "owner@example.com" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspaces_empty(self, mock_config, mock_client_class, runner):
        """Test listing workspaces when none exist."""
        mock_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["workspaces"])

        assert result.exit_code == 0
        assert "No workspaces found" in result.output


class TestDownloadCommandWithIdSupport:
    """Tests for the download command with ID and hash support."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_by_hash(self, mock_config, mock_client_class, runner):
        """Test downloading file by hash (original functionality)."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.download_file.return_value = Path("/tmp/file.txt")
        # Mock resolve_entry_identifier to raise exception (hash not found as name)
        from pydrime.exceptions import DrimeNotFoundError

        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )
        # Mock get_file_entries to return a file (not a folder)
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "file.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        result = runner.invoke(main, ["download", "NDgwNDI0Nzk2fA"])

        assert result.exit_code == 0
        mock_client.download_file.assert_called_once()
        # Should be called with the hash directly
        call_args = mock_client.download_file.call_args
        assert call_args[0][0] == "NDgwNDI0Nzk2fA"

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_by_id(self, mock_config, mock_client_class, runner):
        """Test downloading file by numeric ID (new functionality)."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.download_file.return_value = Path("/tmp/file.txt")
        # Mock resolve_entry_identifier to raise exception (ID not found as name)
        from pydrime.exceptions import DrimeNotFoundError

        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )
        # Mock get_file_entries to return a file (not a folder)
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "file.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        result = runner.invoke(main, ["download", "480424796"])

        assert result.exit_code == 0
        mock_client.download_file.assert_called_once()
        # Should be called with the converted hash
        call_args = mock_client.download_file.call_args
        assert call_args[0][0] == "NDgwNDI0Nzk2fA"

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_multiple_ids(self, mock_config, mock_client_class, runner):
        """Test downloading multiple files by ID."""
        mock_config.is_configured.return_value = True
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.download_file.return_value = Path("/tmp/file.txt")
        # Mock get_file_entries to return files (not folders)
        mock_client.get_file_entries.side_effect = [
            {
                "data": [
                    {
                        "id": 480424796,
                        "name": "file1.txt",
                        "type": "text",
                        "hash": "NDgwNDI0Nzk2fA",
                    }
                ]
            },
            {
                "data": [
                    {
                        "id": 480424802,
                        "name": "file2.txt",
                        "type": "text",
                        "hash": "NDgwNDI0ODAyfA",
                    }
                ]
            },
        ]

        result = runner.invoke(main, ["download", "480424796", "480424802"])

        assert result.exit_code == 0
        assert mock_client.download_file.call_count == 2

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_mixed_ids_and_hashes(
        self, mock_config, mock_client_class, runner
    ):
        """Test downloading with mixed IDs and hashes."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.download_file.return_value = Path("/tmp/file.txt")
        # Mock resolve_entry_identifier to raise exception (not found as name)
        from pydrime.exceptions import DrimeNotFoundError

        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )
        # Mock get_file_entries to return files (not folders)
        mock_client.get_file_entries.side_effect = [
            {
                "data": [
                    {
                        "id": 480424796,
                        "name": "file1.txt",
                        "type": "text",
                        "hash": "NDgwNDI0Nzk2fA",
                    }
                ]
            },
            {
                "data": [
                    {
                        "id": 480424802,
                        "name": "file2.txt",
                        "type": "text",
                        "hash": "NDgwNDI0ODAyfA",
                    }
                ]
            },
            {
                "data": [
                    {
                        "id": 480432024,
                        "name": "file3.txt",
                        "type": "text",
                        "hash": "NDgwNDMyMDI0fA",
                    }
                ]
            },
        ]

        result = runner.invoke(
            main, ["download", "480424796", "NDgwNDI0ODAyfA", "480432024"]
        )

        assert result.exit_code == 0
        assert mock_client.download_file.call_count == 3

        # Verify all calls were made with hashes
        calls = mock_client.download_file.call_args_list
        assert calls[0][0][0] == "NDgwNDI0Nzk2fA"  # Converted from ID
        assert calls[1][0][0] == "NDgwNDI0ODAyfA"  # Already a hash
        assert calls[2][0][0] == "NDgwNDMyMDI0fA"  # Converted from ID

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_by_id_with_output_option(
        self, mock_config, mock_client_class, runner
    ):
        """Test downloading by ID with custom output path."""
        mock_config.is_configured.return_value = True
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.download_file.return_value = Path("/tmp/custom_file.txt")
        # Mock get_file_entries to return a file (not a folder)
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "file.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        result = runner.invoke(
            main, ["download", "480424796", "--output", "/tmp/custom_file.txt"]
        )

        assert result.exit_code == 0
        mock_client.download_file.assert_called_once()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_shows_conversion_message(
        self, mock_config, mock_client_class, runner
    ):
        """Test that conversion message is shown for IDs."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.download_file.return_value = Path("/tmp/file.txt")
        # Mock resolve_entry_identifier to raise exception (not found as name)
        from pydrime.exceptions import DrimeNotFoundError

        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )
        # Mock get_file_entries to return a file
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "file.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        result = runner.invoke(main, ["download", "480424796"])

        assert result.exit_code == 0
        # Check that conversion message is in output
        assert "Converting ID" in result.output
        assert "480424796" in result.output
        assert "NDgwNDI0Nzk2fA" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_no_conversion_message_for_hash(
        self, mock_config, mock_client_class, runner
    ):
        """Test that no conversion message is shown for hashes."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.download_file.return_value = Path("/tmp/file.txt")
        # Mock resolve_entry_identifier to raise exception (not found as name)
        from pydrime.exceptions import DrimeNotFoundError

        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )
        # Mock get_file_entries to return a file
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "file.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        result = runner.invoke(main, ["download", "NDgwNDI0Nzk2fA"])

        assert result.exit_code == 0
        # Check that no conversion message is shown
        assert "Converting ID" not in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_quiet_mode_no_conversion_message(
        self, mock_config, mock_client_class, runner
    ):
        """Test that conversion message is suppressed in quiet mode."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.download_file.return_value = Path("/tmp/file.txt")
        # Mock resolve_entry_identifier to raise exception (not found as name)
        from pydrime.exceptions import DrimeNotFoundError

        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )
        # Mock get_file_entries to return a file
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "file.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        result = runner.invoke(main, ["--quiet", "download", "480424796"])

        assert result.exit_code == 0
        # Check that conversion message is suppressed in quiet mode
        assert "Converting ID" not in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_help_mentions_ids(self, mock_config, mock_client_class, runner):
        """Test that download help mentions both IDs and hashes."""
        result = runner.invoke(main, ["download", "--help"])

        assert result.exit_code == 0
        assert "ID" in result.output or "id" in result.output.lower()
        assert "hash" in result.output.lower()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_by_name(self, mock_config, mock_client_class, runner):
        """Test downloading file by name."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier to return entry ID
        mock_client.resolve_entry_identifier.return_value = 480424796

        # Mock get_file_entries to return a file
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "test.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        mock_client.download_file.return_value = Path("/tmp/test.txt")

        result = runner.invoke(main, ["download", "test.txt"])

        assert result.exit_code == 0
        assert "Resolved 'test.txt' to entry ID: 480424796" in result.output
        mock_client.resolve_entry_identifier.assert_called_once()
        mock_client.download_file.assert_called_once()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_folder_by_name(self, mock_config, mock_client_class, runner):
        """Test downloading folder by name (automatically recursive)."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier to return folder ID
        mock_client.resolve_entry_identifier.return_value = 480432024

        # Mock get_file_entries to return a folder
        mock_client.get_file_entries.side_effect = [
            {
                "data": [
                    {
                        "id": 480432024,
                        "name": "test_folder",
                        "type": "folder",
                        "hash": "NDgwNDMyMDI0fA",
                    }
                ]
            },
            # Contents of the folder
            {
                "data": [
                    {
                        "id": 480432025,
                        "name": "file1.txt",
                        "type": "text",
                        "hash": "NDgwNDMyMDI1fA",
                    }
                ]
            },
        ]

        mock_client.download_file.return_value = Path("/tmp/test_folder/file1.txt")

        result = runner.invoke(main, ["download", "test_folder"])

        assert result.exit_code == 0
        assert "Resolved 'test_folder' to entry ID: 480432024" in result.output
        assert "Downloading folder: test_folder" in result.output
        mock_client.resolve_entry_identifier.assert_called_once()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_on_duplicate_skip(self, mock_config, mock_client_class, runner):
        """Test download with --on-duplicate skip."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier to return entry ID
        mock_client.resolve_entry_identifier.return_value = 480424796

        # Mock get_file_entries to return a file
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "test.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        mock_client.download_file.return_value = Path("test.txt")

        with runner.isolated_filesystem():
            # Create existing file
            Path("test.txt").write_text("existing")

            result = runner.invoke(
                main, ["download", "test.txt", "--on-duplicate", "skip"]
            )

        assert result.exit_code == 0
        assert "Skipped (already exists)" in result.output
        # download_file should not be called when skipping
        mock_client.download_file.assert_not_called()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_on_duplicate_overwrite(
        self, mock_config, mock_client_class, runner
    ):
        """Test download with --on-duplicate overwrite (default)."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier to return entry ID
        mock_client.resolve_entry_identifier.return_value = 480424796

        # Mock get_file_entries to return a file
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "test.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        mock_client.download_file.return_value = Path("test.txt")

        with runner.isolated_filesystem():
            # Create existing file
            Path("test.txt").write_text("existing")

            result = runner.invoke(
                main,
                [
                    "download",
                    "test.txt",
                    "--on-duplicate",
                    "overwrite",
                    "--no-progress",
                ],
            )

        assert result.exit_code == 0
        assert "Downloaded:" in result.output
        # download_file should be called when overwriting
        mock_client.download_file.assert_called_once()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_on_duplicate_rename(self, mock_config, mock_client_class, runner):
        """Test download with --on-duplicate rename."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier to return entry ID
        mock_client.resolve_entry_identifier.return_value = 480424796

        # Mock get_file_entries to return a file
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "test.txt",
                    "type": "text",
                    "hash": "NDgwNDI0Nzk2fA",
                }
            ]
        }

        def download_side_effect(hash_val, path, **kwargs):
            # Create the file at the specified path
            path.write_text("downloaded")
            return path

        mock_client.download_file.side_effect = download_side_effect

        with runner.isolated_filesystem():
            # Create existing file
            Path("test.txt").write_text("existing")

            result = runner.invoke(
                main, ["download", "test.txt", "--on-duplicate", "rename"]
            )

        assert result.exit_code == 0
        assert "Renaming to avoid duplicate" in result.output
        assert "test (1).txt" in result.output
        # download_file should be called with renamed path
        mock_client.download_file.assert_called_once()
        call_args = mock_client.download_file.call_args
        assert "test (1).txt" in str(call_args[0][1])


class TestInfoCommand:
    """Tests for the info command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_info_by_id(self, mock_config, mock_client_class, runner):
        """Test info command with file ID."""
        mock_config.is_configured.return_value = True
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock API response
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "test.txt",
                    "type": "file",
                    "hash": "NDgwNDI0Nzk2fA",
                    "file_size": 1024,
                    "parent_id": None,
                    "created_at": "2025-01-01T00:00:00.000000Z",
                    "updated_at": "2025-01-01T00:00:00.000000Z",
                    "users": [],
                    "tags": [],
                    "permissions": None,
                    "public": False,
                    "file_name": "test.txt",
                    "mime": "text/plain",
                    "url": "https://dri.me/test",
                }
            ]
        }

        result = runner.invoke(main, ["info", "480424796"])

        assert result.exit_code == 0
        assert "test.txt" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_info_by_hash(self, mock_config, mock_client_class, runner):
        """Test info command with file hash."""
        mock_config.is_configured.return_value = True
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 480424796,
                    "name": "test.txt",
                    "type": "file",
                    "hash": "NDgwNDI0Nzk2fA",
                    "file_size": 1024,
                    "parent_id": None,
                    "created_at": "2025-01-01T00:00:00.000000Z",
                    "updated_at": "2025-01-01T00:00:00.000000Z",
                    "users": [],
                    "tags": [],
                    "permissions": None,
                    "public": False,
                    "file_name": "test.txt",
                    "mime": "text/plain",
                    "url": "https://dri.me/test",
                }
            ]
        }

        result = runner.invoke(main, ["info", "NDgwNDI0Nzk2fA"])

        assert result.exit_code == 0
        assert "test.txt" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_info_not_found(self, mock_config, mock_client_class, runner):
        """Test info command with non-existent ID."""
        mock_config.is_configured.return_value = True
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_file_entries.return_value = {"data": []}

        result = runner.invoke(main, ["info", "999999"])

        assert result.exit_code == 1
        assert "no file found" in result.output.lower()


class TestCdCommand:
    """Tests for the cd command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_cd_to_folder(self, mock_config, mock_client_class, runner):
        """Test changing to a specific folder."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.save_current_folder = Mock()
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        # Mock the resolve_folder_identifier to return the folder ID
        mock_client.resolve_folder_identifier.return_value = 480432024
        mock_client.get_file_entries.return_value = {"data": []}

        result = runner.invoke(main, ["cd", "480432024"])

        assert result.exit_code == 0
        assert "Changed to folder ID: 480432024" in result.output
        mock_config.save_current_folder.assert_called_once_with(480432024)

    @patch("pydrime.cli.config")
    def test_cd_to_root(self, mock_config, runner):
        """Test changing to root directory."""
        mock_config.is_configured.return_value = True
        mock_config.save_current_folder = Mock()

        result = runner.invoke(main, ["cd"])

        assert result.exit_code == 0
        assert "root" in result.output.lower()
        mock_config.save_current_folder.assert_called_once_with(None)

    @patch("pydrime.cli.config")
    def test_cd_to_root_explicit(self, mock_config, runner):
        """Test changing to root directory with explicit 0."""
        mock_config.is_configured.return_value = True
        mock_config.save_current_folder = Mock()

        result = runner.invoke(main, ["cd", "0"])

        assert result.exit_code == 0
        assert "root" in result.output.lower()
        mock_config.save_current_folder.assert_called_once_with(None)

    @patch("pydrime.cli.config")
    def test_cd_to_root_with_slash(self, mock_config, runner):
        """Test changing to root directory with /."""
        mock_config.is_configured.return_value = True
        mock_config.save_current_folder = Mock()

        result = runner.invoke(main, ["cd", "/"])

        assert result.exit_code == 0
        assert "root" in result.output.lower()
        mock_config.save_current_folder.assert_called_once_with(None)


class TestPwdCommand:
    """Tests for the pwd command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_pwd_with_current_folder(self, mock_config, mock_client_class, runner):
        """Test pwd when a current folder is set (text format)."""
        mock_config.get_current_folder.return_value = 480432024
        mock_config.get_default_workspace.return_value = None
        mock_config.is_configured.return_value = True

        # Mock the DrimeClient and get_folder_info to return folder name
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_folder_info.return_value = {
            "name": "Documents",
            "id": 480432024,
        }

        result = runner.invoke(main, ["pwd"])

        assert result.exit_code == 0
        # Should show folder name with ID and workspace
        assert "/Documents (ID: 480432024)" in result.output
        assert "Workspace: 0" in result.output

    @patch("pydrime.cli.config")
    def test_pwd_at_root(self, mock_config, runner):
        """Test pwd when at root directory (text format)."""
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        result = runner.invoke(main, ["pwd"])

        assert result.exit_code == 0
        assert "/ (ID: 0)" in result.output
        assert "Workspace: 0" in result.output

    @patch("pydrime.cli.config")
    def test_pwd_json_format(self, mock_config, runner):
        """Test pwd with JSON format."""
        mock_config.get_current_folder.return_value = 480432024
        mock_config.get_default_workspace.return_value = 5

        result = runner.invoke(main, ["--json", "pwd"])

        assert result.exit_code == 0
        assert "480432024" in result.output
        assert "5" in result.output or '"default_workspace"' in result.output

    @patch("pydrime.cli.config")
    def test_pwd_id_only_with_folder(self, mock_config, runner):
        """Test pwd with --id-only flag when a current folder is set."""
        mock_config.get_current_folder.return_value = 480432024
        mock_config.get_default_workspace.return_value = None

        result = runner.invoke(main, ["pwd", "--id-only"])

        assert result.exit_code == 0
        assert result.output.strip() == "480432024"

    @patch("pydrime.cli.config")
    def test_pwd_id_only_at_root(self, mock_config, runner):
        """Test pwd with --id-only flag when at root directory."""
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        result = runner.invoke(main, ["pwd", "--id-only"])

        assert result.exit_code == 0
        assert result.output.strip() == "0"

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_pwd_with_workspace_name(self, mock_config, mock_client_class, runner):
        """Test pwd displays workspace name when available."""
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 1465
        mock_config.is_configured.return_value = True

        # Mock the DrimeClient and get_workspaces to return workspace info
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 1465, "name": "test"},
                {"id": 5, "name": "Team Workspace"},
            ]
        }

        result = runner.invoke(main, ["pwd"])

        assert result.exit_code == 0
        assert "/ (ID: 0)" in result.output
        assert "Workspace: test (1465)" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_cd_uses_default_workspace(self, mock_config, mock_client_class, runner):
        """Test cd command uses default workspace when resolving folder names."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 1465
        mock_config.save_current_folder = Mock()

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.resolve_folder_identifier.return_value = 480983233
        mock_client.get_file_entries.return_value = {"data": []}

        result = runner.invoke(main, ["cd", "subdir1"])

        assert result.exit_code == 0
        assert "Changed to folder ID: 480983233" in result.output
        # Verify workspace_id was passed to resolve_folder_identifier
        mock_client.resolve_folder_identifier.assert_called_once_with(
            identifier="subdir1", parent_id=None, workspace_id=1465
        )


class TestRecursiveFlag:
    """Tests for recursive operations."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_ls_recursive(self, mock_config, mock_client_class, runner):
        """Test ls with recursive flag."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None  # Mock current folder
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock response - the recursive ls will only scan folders when they have content
        # In our case, one folder with no subfolders
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "test.txt",
                    "type": "file",
                    "hash": "abc123",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-01-01T00:00:00.000000Z",
                    "users": [],
                    "tags": [],
                    "permissions": None,
                    "public": False,
                    "file_name": "test.txt",
                    "mime": "text/plain",
                    "url": "",
                }
            ]
        }

        result = runner.invoke(main, ["ls", "--recursive"])

        assert result.exit_code == 0
        # With no folders, should only call once
        assert mock_client.get_file_entries.call_count >= 1

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_recursive_folder(self, mock_config, mock_client_class, runner):
        """Test downloading a folder (automatically recursive)."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier to raise exception (ID used directly)
        from pydrime.exceptions import DrimeNotFoundError

        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Not found"
        )

        # Mock response for folder info
        mock_client.get_file_entries.side_effect = [
            {
                "data": [
                    {
                        "id": 1,
                        "name": "myfolder",
                        "type": "folder",
                        "hash": "abc123",
                        "file_size": 0,
                        "parent_id": None,
                        "created_at": "2025-01-01T00:00:00.000000Z",
                        "users": [],
                        "tags": [],
                        "permissions": None,
                        "public": False,
                        "file_name": "myfolder",
                        "mime": "folder",
                        "url": "",
                    }
                ]
            },
            # Mock folder contents
            {
                "data": [
                    {
                        "id": 2,
                        "name": "file.txt",
                        "type": "file",
                        "hash": "def456",
                        "file_size": 100,
                        "parent_id": 1,
                        "created_at": "2025-01-01T00:00:00.000000Z",
                        "users": [],
                        "tags": [],
                        "permissions": None,
                        "public": False,
                        "file_name": "file.txt",
                        "mime": "text/plain",
                        "url": "",
                    }
                ]
            },
        ]

        mock_client.download_file.return_value = Path("myfolder/file.txt")

        with runner.isolated_filesystem():
            result = runner.invoke(main, ["download", "1"])

        assert result.exit_code == 0
        assert "Downloading folder: myfolder" in result.output


class TestWorkspaceCommand:
    """Tests for the workspace command."""

    @patch("pydrime.cli.config")
    def test_workspace_show_current_default(self, mock_config, runner):
        """Test showing current default workspace."""
        mock_config.is_configured.return_value = True
        mock_config.get_default_workspace.return_value = None

        result = runner.invoke(main, ["workspace"])

        assert result.exit_code == 0
        assert "Personal (0)" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspace_show_custom_default(
        self, mock_config, mock_client_class, runner
    ):
        """Test showing custom default workspace."""
        mock_config.is_configured.return_value = True
        mock_config.get_default_workspace.return_value = 5
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 5, "name": "Team Workspace"},
                {"id": 10, "name": "Another Workspace"},
            ]
        }

        result = runner.invoke(main, ["workspace"])

        assert result.exit_code == 0
        assert "Team Workspace" in result.output
        assert "5" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspace_set_to_personal(self, mock_config, mock_client_class, runner):
        """Test setting workspace to personal (0)."""
        mock_config.is_configured.return_value = True
        mock_config.save_default_workspace = Mock()
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["workspace", "0"])

        assert result.exit_code == 0
        assert "Personal (0)" in result.output
        mock_config.save_default_workspace.assert_called_once_with(None)

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspace_set_to_custom(self, mock_config, mock_client_class, runner):
        """Test setting workspace to custom ID."""
        mock_config.is_configured.return_value = True
        mock_config.save_default_workspace = Mock()
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 5, "name": "Team Workspace"},
                {"id": 10, "name": "Another Workspace"},
            ]
        }

        result = runner.invoke(main, ["workspace", "5"])

        assert result.exit_code == 0
        assert "Team Workspace" in result.output
        assert "5" in result.output
        mock_config.save_default_workspace.assert_called_once_with(5)

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspace_set_invalid_id(self, mock_config, mock_client_class, runner):
        """Test setting workspace to invalid ID."""
        mock_config.is_configured.return_value = True
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 5, "name": "Team Workspace"},
            ]
        }

        result = runner.invoke(main, ["workspace", "99"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspace_set_by_name(self, mock_config, mock_client_class, runner):
        """Test setting workspace by name."""
        mock_config.is_configured.return_value = True
        mock_config.save_default_workspace = Mock()
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 5, "name": "Team Workspace"},
                {"id": 10, "name": "Another Workspace"},
            ]
        }

        result = runner.invoke(main, ["workspace", "Team Workspace"])

        assert result.exit_code == 0
        assert "Resolved workspace 'Team Workspace' to ID: 5" in result.output
        assert "Team Workspace" in result.output
        assert "5" in result.output
        mock_config.save_default_workspace.assert_called_once_with(5)

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspace_set_by_name_case_insensitive(
        self, mock_config, mock_client_class, runner
    ):
        """Test setting workspace by name with different case."""
        mock_config.is_configured.return_value = True
        mock_config.save_default_workspace = Mock()
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 5, "name": "Team Workspace"},
                {"id": 10, "name": "Another Workspace"},
            ]
        }

        result = runner.invoke(main, ["workspace", "team workspace"])

        assert result.exit_code == 0
        assert "Resolved workspace 'team workspace' to ID: 5" in result.output
        mock_config.save_default_workspace.assert_called_once_with(5)

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_workspace_set_by_invalid_name(
        self, mock_config, mock_client_class, runner
    ):
        """Test setting workspace with non-existent name."""
        mock_config.is_configured.return_value = True
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 5, "name": "Team Workspace"},
            ]
        }

        result = runner.invoke(main, ["workspace", "NonExistent"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()
        assert "NonExistent" in result.output


class TestRenameCommand:
    """Tests for the rename command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_rename_by_id(self, mock_config, mock_client_class, runner):
        """Test renaming file by ID."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.update_file_entry.return_value = {"id": 123, "name": "newfile.txt"}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["rename", "123", "newfile.txt"])

        assert result.exit_code == 0
        assert "renamed to: newfile.txt" in result.output
        mock_client.update_file_entry.assert_called_once_with(
            123, name="newfile.txt", description=None
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_rename_by_name(self, mock_config, mock_client_class, runner):
        """Test renaming file by name."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        # Simulate resolving name to ID
        mock_client.resolve_entry_identifier.return_value = 123
        mock_client.update_file_entry.return_value = {"id": 123, "name": "newfile.txt"}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["rename", "test.txt", "newfile.txt"])

        assert result.exit_code == 0
        assert "renamed to: newfile.txt" in result.output
        mock_client.resolve_entry_identifier.assert_called_once_with(
            "test.txt", None, 0
        )
        mock_client.update_file_entry.assert_called_once_with(
            123, name="newfile.txt", description=None
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_rename_with_description(self, mock_config, mock_client_class, runner):
        """Test renaming file with description."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.update_file_entry.return_value = {
            "id": 123,
            "name": "newfile.txt",
            "description": "New description",
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main, ["rename", "123", "newfile.txt", "-d", "New description"]
        )

        assert result.exit_code == 0
        assert "renamed to: newfile.txt" in result.output
        mock_client.update_file_entry.assert_called_once_with(
            123, name="newfile.txt", description="New description"
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_rename_not_found(self, mock_config, mock_client_class, runner):
        """Test renaming non-existent file."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Entry not found"
        )
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["rename", "nonexistent.txt", "newname.txt"])

        assert result.exit_code == 1
        assert "Entry not found" in result.output


class TestShareCommand:
    """Tests for the share command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_share_by_id(self, mock_config, mock_client_class, runner):
        """Test sharing file by ID."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.create_shareable_link.return_value = {
            "link": {"hash": "abc123def456"}
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["share", "123"])

        assert result.exit_code == 0
        assert "Shareable link created" in result.output
        assert "https://dri.me/abc123def456" in result.output
        mock_client.create_shareable_link.assert_called_once_with(
            entry_id=123,
            password=None,
            expires_at=None,
            allow_edit=False,
            allow_download=True,
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_share_by_name(self, mock_config, mock_client_class, runner):
        """Test sharing file by name."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        # Simulate resolving name to ID
        mock_client.resolve_entry_identifier.return_value = 123
        mock_client.create_shareable_link.return_value = {
            "link": {"hash": "abc123def456"}
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["share", "test.txt"])

        assert result.exit_code == 0
        assert "Shareable link created" in result.output
        assert "https://dri.me/abc123def456" in result.output
        mock_client.resolve_entry_identifier.assert_called_once_with(
            "test.txt", None, 0
        )
        mock_client.create_shareable_link.assert_called_once_with(
            entry_id=123,
            password=None,
            expires_at=None,
            allow_edit=False,
            allow_download=True,
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_share_with_password(self, mock_config, mock_client_class, runner):
        """Test sharing file with password."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.create_shareable_link.return_value = {
            "link": {"hash": "abc123def456"}
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["share", "123", "-p", "mypassword"])

        assert result.exit_code == 0
        assert "Shareable link created" in result.output
        mock_client.create_shareable_link.assert_called_once_with(
            entry_id=123,
            password="mypassword",
            expires_at=None,
            allow_edit=False,
            allow_download=True,
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_share_with_expiration(self, mock_config, mock_client_class, runner):
        """Test sharing file with expiration date."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.create_shareable_link.return_value = {
            "link": {"hash": "abc123def456"}
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main, ["share", "123", "-e", "2025-12-31T23:59:59.000000Z"]
        )

        assert result.exit_code == 0
        assert "Shareable link created" in result.output
        mock_client.create_shareable_link.assert_called_once_with(
            entry_id=123,
            password=None,
            expires_at="2025-12-31T23:59:59.000000Z",
            allow_edit=False,
            allow_download=True,
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_share_with_edit_permission(self, mock_config, mock_client_class, runner):
        """Test sharing file with edit permission."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.create_shareable_link.return_value = {
            "link": {"hash": "abc123def456"}
        }
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["share", "123", "--allow-edit"])

        assert result.exit_code == 0
        assert "Shareable link created" in result.output
        mock_client.create_shareable_link.assert_called_once_with(
            entry_id=123,
            password=None,
            expires_at=None,
            allow_edit=True,
            allow_download=True,
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_share_not_found(self, mock_config, mock_client_class, runner):
        """Test sharing non-existent file."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.resolve_entry_identifier.side_effect = DrimeNotFoundError(
            "Entry not found"
        )
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["share", "nonexistent.txt"])

        assert result.exit_code == 1
        assert "Entry not found" in result.output


class TestValidateCommand:
    """Tests for the validate command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_validate_single_file_success(self, mock_config, mock_client_class, runner):
        """Test validating a single file that exists with correct size."""
        mock_config.is_configured.return_value = True
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        # Mock file entry response
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "test.txt",
                    "type": "text",
                    "file_size": 100,
                    "hash": "abc123",
                }
            ]
        }
        mock_client_class.return_value = mock_client

        with runner.isolated_filesystem():
            # Create a test file
            Path("test.txt").write_text("x" * 100)

            result = runner.invoke(main, ["validate", "test.txt"])

            assert result.exit_code == 0
            assert "Valid: 1 file(s)" in result.output
            assert "validated successfully" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_validate_missing_file(self, mock_config, mock_client_class, runner):
        """Test validating a file that doesn't exist in cloud."""
        mock_config.is_configured.return_value = True
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        # Mock empty response
        mock_client.get_file_entries.return_value = {"data": []}
        mock_client_class.return_value = mock_client

        with runner.isolated_filesystem():
            # Create a test file
            Path("test.txt").write_text("test content")

            result = runner.invoke(main, ["validate", "test.txt"])

            assert result.exit_code == 1
            assert "Missing: 1 file(s)" in result.output
            assert "Not found in cloud" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_validate_size_mismatch(self, mock_config, mock_client_class, runner):
        """Test validating a file with size mismatch."""
        mock_config.is_configured.return_value = True
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        # Mock file entry with different size
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "test.txt",
                    "type": "text",
                    "file_size": 200,  # Different size
                    "hash": "abc123",
                }
            ]
        }
        mock_client_class.return_value = mock_client

        with runner.isolated_filesystem():
            # Create a test file
            Path("test.txt").write_text("x" * 100)

            result = runner.invoke(main, ["validate", "test.txt"])

            assert result.exit_code == 1
            assert "Size mismatch: 1 file(s)" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_validate_json_output(self, mock_config, mock_client_class, runner):
        """Test validate with JSON output format."""
        mock_config.is_configured.return_value = True
        mock_config.get_default_workspace.return_value = None

        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "test.txt",
                    "type": "text",
                    "file_size": 100,
                    "hash": "abc123",
                }
            ]
        }
        mock_client_class.return_value = mock_client

        with runner.isolated_filesystem():
            Path("test.txt").write_text("x" * 100)

            result = runner.invoke(main, ["--json", "validate", "test.txt"])

            assert result.exit_code == 0
            assert '"total": 1' in result.output
            assert '"valid": 1' in result.output
            assert '"missing": 0' in result.output

    @patch("pydrime.cli.config")
    def test_validate_without_api_key(self, mock_config, runner):
        """Test validate without API key configured."""
        mock_config.is_configured.return_value = False

        with runner.isolated_filesystem():
            Path("test.txt").write_text("test")
            # Don't pass api_key to ensure it fails early
            result = runner.invoke(
                main, ["validate", "test.txt"], env={"DRIME_API_KEY": ""}
            )

            assert result.exit_code == 1
            assert "API key not configured" in result.output
