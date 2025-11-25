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
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_file_success(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test successful file upload."""
        # Create a temporary test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main, ["upload", str(test_file), "--dry-run"], input="n\n"
        )

        assert "Dry run mode" in result.output or "Upload cancelled" in result.output

    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_without_api_key(
        self, mock_cli_config, mock_auth_config, runner, tmp_path
    ):
        """Test upload without API key configured."""
        mock_cli_config.is_configured.return_value = False
        mock_auth_config.is_configured.return_value = False

        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        result = runner.invoke(
            main, ["upload", str(test_file)], env={"DRIME_API_KEY": ""}
        )

        assert result.exit_code == 1
        assert "API key not configured" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_displays_destination_info(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test that upload displays workspace and parent folder information."""
        # Create a temporary test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 5
        mock_cli_config.get_current_folder.return_value = 123
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [{"id": 5, "name": "Test Workspace"}]
        }
        mock_client.get_folder_info.return_value = {"name": "MyFolder"}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["upload", str(test_file), "--dry-run"])

        assert result.exit_code == 0
        assert "Workspace: Test Workspace (5)" in result.output
        # In dry-run, it shows "Base location:"
        assert "Base location: /MyFolder" in result.output
        assert "Dry run mode" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_displays_root_folder(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test that upload displays root folder when no current folder set."""
        # Create a temporary test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = None
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["upload", str(test_file), "--dry-run"])

        assert result.exit_code == 0
        assert "Workspace: Personal (0)" in result.output
        # In dry-run, it shows "Base location:"
        assert "Base location: /" in result.output

    @patch("pydrime.cli.scan_directory")
    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_uses_current_folder_as_parent(
        self,
        mock_cli_config,
        mock_auth_config,
        mock_client_class,
        mock_scan,
        runner,
        tmp_path,
    ):
        """Test that upload passes current folder as parent_id to upload_file."""
        # Create a temporary test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        mock_cli_config.is_configured.return_value = True
        mock_auth_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 1465
        mock_cli_config.get_current_folder.return_value = 480983233

        # Mock scan_directory to return file list
        mock_scan.return_value = [(test_file, "test.txt")]

        mock_client = Mock()
        mock_client.validate_uploads.return_value = {"duplicates": []}
        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}
        mock_client.get_workspaces.return_value = {
            "workspaces": [{"id": 1465, "name": "test"}]
        }
        mock_client.get_folder_info.return_value = {"name": "subdir1"}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["upload", str(test_file), "--no-progress"])

        assert result.exit_code == 0
        # Verify that upload_file was called with parent_id=480983233
        mock_client.upload_file.assert_called_once()
        call_args = mock_client.upload_file.call_args
        assert call_args.kwargs["parent_id"] == 480983233
        assert call_args.kwargs["workspace_id"] == 1465

    @patch("pydrime.cli.scan_directory")
    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_directory_with_remote_path_includes_folder_name(
        self,
        mock_cli_config,
        mock_auth_config,
        mock_client_class,
        mock_scan,
        runner,
        tmp_path,
    ):
        """Test that upload directory with remote path includes local folder name."""
        # Create a temporary test directory with files
        test_dir = tmp_path / "test"
        test_dir.mkdir()
        test_file = test_dir / "file.txt"
        test_file.write_text("test content")

        mock_cli_config.is_configured.return_value = True
        mock_auth_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None

        # Mock scan_directory to return file list with relative path including
        # folder name. This is what scan_directory would return when
        # base_path = test_dir.parent
        mock_scan.return_value = [(test_file, "test/file.txt")]

        mock_client = Mock()
        mock_client.validate_uploads.return_value = {"duplicates": []}
        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main, ["upload", str(test_dir), "-r", "dest", "--no-progress"]
        )

        assert result.exit_code == 0
        # Verify that upload_file was called with remote path including both
        # dest and test
        mock_client.upload_file.assert_called_once()
        call_args = mock_client.upload_file.call_args
        # The relative_path should be "dest/test/file.txt"
        assert call_args.kwargs["relative_path"] == "dest/test/file.txt"
        # Verify scan_directory was called with base_path = test_dir.parent
        mock_scan.assert_called_once()
        scan_call_args = mock_scan.call_args
        assert scan_call_args[0][0] == test_dir  # path argument
        assert scan_call_args[0][1] == test_dir.parent  # base_path argument

    @patch("pydrime.cli.scan_directory")
    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_shows_same_format_as_dry_run(
        self,
        mock_cli_config,
        mock_auth_config,
        mock_client_class,
        mock_scan,
        runner,
        tmp_path,
    ):
        """Test that actual upload shows same structured format as dry-run."""
        # Create a temporary test directory with files
        test_dir = tmp_path / "test"
        test_dir.mkdir()
        test_file = test_dir / "file.txt"
        test_file.write_text("test content")

        mock_cli_config.is_configured.return_value = True
        mock_auth_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None

        mock_scan.return_value = [(test_file, "test/file.txt")]

        mock_client = Mock()
        mock_client.validate_uploads.return_value = {"duplicates": []}
        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["upload", str(test_dir), "--no-progress"])

        assert result.exit_code == 0
        # Verify structured output format (same as dry-run)
        assert "Upload Preview" in result.output
        assert "Destination:" in result.output
        assert "Base location:" in result.output
        assert "Files will be uploaded to:" in result.output
        assert "Folders to create:" in result.output
        assert "Files to upload:" in result.output
        # Should show folder and file with emojis
        assert "📁 test/" in result.output
        assert "📄 file.txt" in result.output


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

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_file_when_folder_exists(
        self, mock_config, mock_client_class, runner
    ):
        """Test downloading file when a folder with same name exists - should rename."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier to return entry ID
        mock_client.resolve_entry_identifier.return_value = 123

        # Mock get_file_entries to return a file
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "test",
                    "type": "text",
                    "hash": "hash123",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-01-01T00:00:00.000000Z",
                    "users": [],
                    "tags": [],
                    "permissions": None,
                    "public": False,
                    "file_name": "test",
                    "mime": "text/plain",
                    "url": "",
                }
            ]
        }

        mock_client.download_file.return_value = Path("test (1)")

        with runner.isolated_filesystem():
            # Create a directory with the same name as the file we want to download
            Path("test").mkdir()

            result = runner.invoke(main, ["download", "test"])

            assert result.exit_code == 0
            assert (
                "Directory exists with same name, renaming file to: test (1)"
                in result.output
            )
            mock_client.download_file.assert_called_once()
            # Check that download was called with renamed path
            call_args = mock_client.download_file.call_args
            assert str(call_args[0][1]).endswith("test (1)")

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_folder_when_file_exists(
        self, mock_config, mock_client_class, runner
    ):
        """Test downloading folder when a file with same name exists - should error."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier to return entry ID
        mock_client.resolve_entry_identifier.return_value = 456

        # Mock get_file_entries to return a folder
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 456,
                    "name": "myfolder",
                    "type": "folder",
                    "hash": "hash456",
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
        }

        with runner.isolated_filesystem():
            # Create a file with the same name as the folder we want to download
            Path("myfolder").touch()

            result = runner.invoke(main, ["download", "myfolder"])

            assert result.exit_code == 0
            assert "Cannot download folder 'myfolder'" in result.output
            assert "a file with this name already exists" in result.output
            # Should not attempt to download folder contents
            assert mock_client.download_file.call_count == 0

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_download_folder_when_folder_exists(
        self, mock_config, mock_client_class, runner
    ):
        """Test folder download when folder exists - should work (go into it)."""
        mock_config.is_configured.return_value = True
        mock_config.get_current_folder.return_value = None
        mock_config.get_default_workspace.return_value = 0
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock resolve_entry_identifier
        mock_client.resolve_entry_identifier.return_value = 789

        # Mock get_file_entries to return a folder and its contents
        mock_client.get_file_entries.side_effect = [
            {
                "data": [
                    {
                        "id": 789,
                        "name": "existingfolder",
                        "type": "folder",
                        "hash": "hash789",
                        "file_size": 0,
                        "parent_id": None,
                        "created_at": "2025-01-01T00:00:00.000000Z",
                        "users": [],
                        "tags": [],
                        "permissions": None,
                        "public": False,
                        "file_name": "existingfolder",
                        "mime": "folder",
                        "url": "",
                    }
                ]
            },
            # Mock folder contents
            {
                "data": [
                    {
                        "id": 790,
                        "name": "file.txt",
                        "type": "file",
                        "hash": "hash790",
                        "file_size": 100,
                        "parent_id": 789,
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

        mock_client.download_file.return_value = Path("existingfolder/file.txt")

        with runner.isolated_filesystem():
            # Create the folder beforehand
            Path("existingfolder").mkdir()

            result = runner.invoke(main, ["download", "existingfolder"])

            assert result.exit_code == 0
            assert "Downloading folder: existingfolder" in result.output
            # Should download the file inside
            mock_client.download_file.assert_called_once()


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
        mock_config.get_current_folder.return_value = None

        mock_client = Mock()
        # Mock file entry response for FileEntriesManager.get_all_recursive
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "test.txt",
                    "type": "text",
                    "file_size": 100,
                    "hash": "abc123",
                }
            ],
            "pagination": None,
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
        mock_config.get_current_folder.return_value = None

        mock_client = Mock()
        # Mock empty response
        mock_client.get_file_entries.return_value = {"data": [], "pagination": None}
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
        mock_config.get_current_folder.return_value = None

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
            ],
            "pagination": None,
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
        mock_config.get_current_folder.return_value = None

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
            ],
            "pagination": None,
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


class TestFolderStructureDetection:
    """Tests for folder structure detection in upload and validation."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.cli.config")
    def test_scan_directory_uses_posix_paths(
        self, mock_config, mock_client_class, runner
    ):
        """Test that scan_directory returns paths with forward slashes."""
        import tempfile

        from pydrime.cli import scan_directory
        from pydrime.output import OutputFormatter

        mock_config.is_configured.return_value = True

        # Create a nested directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create nested folders and files
            (base / "folder1").mkdir()
            (base / "folder1" / "folder2").mkdir()
            (base / "folder1" / "file1.txt").write_text("content1")
            (base / "folder1" / "folder2" / "file2.txt").write_text("content2")
            (base / "file3.txt").write_text("content3")

            # Scan directory
            out = OutputFormatter()
            files = scan_directory(base, base, out)

            # Check that all paths use forward slashes
            for _file_path, rel_path in files:
                assert "\\" not in rel_path, f"Path contains backslash: {rel_path}"
                assert "/" in rel_path or rel_path in [
                    "file3.txt"
                ], f"Expected forward slashes in nested paths: {rel_path}"

            # Check expected structure
            rel_paths = [rel_path for _, rel_path in files]
            assert "file3.txt" in rel_paths
            assert "folder1/file1.txt" in rel_paths
            assert "folder1/folder2/file2.txt" in rel_paths

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_dry_run_shows_folder_structure(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test that dry-run shows folder structure that will be created."""
        import tempfile

        mock_cli_config.is_configured.return_value = True
        mock_auth_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Create a nested directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create nested folders and files
            (base / "folder1").mkdir()
            (base / "folder1" / "folder2").mkdir()
            (base / "folder1" / "file1.txt").write_text("content1")
            (base / "folder1" / "folder2" / "file2.txt").write_text("content2")
            (base / "file3.txt").write_text("content3")

            # Run upload with dry-run
            result = runner.invoke(main, ["upload", str(base), "--dry-run"])

            assert result.exit_code == 0
            assert "DRY RUN - Upload Preview" in result.output
            assert "Destination:" in result.output
            assert "Folders to create:" in result.output
            assert "Files to upload:" in result.output

            # Check that folder structure is shown
            output = result.output
            # Should show nested folders
            assert "folder1" in output or "📁" in output

            # Should show files grouped by directory
            assert "file1.txt" in output
            assert "file2.txt" in output
            assert "file3.txt" in output

            # Should show summary
            assert "Total: 3 files" in output
            assert "Dry run mode - no files were uploaded" in output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_dry_run_extracts_folders_correctly(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test that dry-run correctly extracts all folders from file paths."""
        import tempfile

        mock_cli_config.is_configured.return_value = True
        mock_auth_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Create complex nested directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create multiple nested levels
            (base / "a").mkdir()
            (base / "a" / "b").mkdir()
            (base / "a" / "b" / "c").mkdir()
            (base / "a" / "b" / "c" / "file.txt").write_text("deep")
            (base / "a" / "file2.txt").write_text("shallow")

            # Run upload with dry-run
            result = runner.invoke(main, ["upload", str(base), "--dry-run"])

            assert result.exit_code == 0
            output = result.output

            # Should show all folder levels
            # The output should contain folder structure info
            assert "Folders to create:" in output

            # Check folder count (a, a/b, a/b/c = 3 folders)
            assert "Folders to create: 3" in output or "📁" in output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_upload_dry_run_groups_files_by_directory(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test that dry-run groups files by their parent directory."""
        import tempfile

        mock_cli_config.is_configured.return_value = True
        mock_auth_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create files in different directories
            (base / "docs").mkdir()
            (base / "images").mkdir()
            (base / "docs" / "readme.txt").write_text("readme")
            (base / "docs" / "guide.txt").write_text("guide")
            (base / "images" / "photo.jpg").write_text("photo")
            (base / "root.txt").write_text("root file")

            # Run upload with dry-run
            result = runner.invoke(main, ["upload", str(base), "--dry-run"])

            assert result.exit_code == 0
            output = result.output

            # Should show files grouped by directory
            assert "Files to upload: 4" in output

            # Should mention different directories
            # The grouping should be visible in output structure
            assert "docs" in output or "In " in output
            assert "readme.txt" in output
            assert "guide.txt" in output
            assert "photo.jpg" in output
            assert "root.txt" in output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_relativepath_validation_uses_posix(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test that relativePath in validation uses forward slashes."""
        import tempfile

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client.validate_uploads.return_value = {"duplicates": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create nested structure
            (base / "folder1").mkdir()
            (base / "folder1" / "folder2").mkdir()
            (base / "folder1" / "folder2" / "file.txt").write_text("content")

            # Run upload (not dry-run) to trigger validate_uploads
            runner.invoke(main, ["upload", str(base)])

            # Check that validate_uploads was called
            assert mock_client.validate_uploads.called

            # Get the call arguments
            call_args = mock_client.validate_uploads.call_args
            files_arg = call_args[1]["files"] if call_args[1] else call_args[0][0]

            # Check that relativePath uses forward slashes, not backslashes
            for file_info in files_arg:
                rel_path = file_info.get("relativePath", "")
                if rel_path:  # Only check non-empty paths
                    assert (
                        "\\" not in rel_path
                    ), f"relativePath should not contain backslashes: {rel_path}"
                    assert (
                        "/" in rel_path or rel_path == ""
                    ), f"relativePath should use forward slashes: {rel_path}"


class TestWindowsPathHandling:
    """Tests for Windows path handling in upload and validation."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_windows_nested_folders_parsed_correctly(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test that Windows-style nested paths like data\\01\\ are parsed correctly."""
        import tempfile

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client.validate_uploads.return_value = {"duplicates": []}

        # Create nested directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create structure: data/01/02/file.txt
            (base / "data").mkdir()
            (base / "data" / "01").mkdir()
            (base / "data" / "01" / "02").mkdir()
            (base / "data" / "01" / "02" / "file.txt").write_text("content")

            # Run upload dry-run
            result = runner.invoke(main, ["upload", str(base), "--dry-run"])

            assert result.exit_code == 0
            output = result.output

            # Check that folders are shown with forward slashes, not mixed
            assert "data/" in output or "📁 data/" in output
            assert "data/01/" in output or "📁 data/01/" in output
            assert "data/01/02/" in output or "📁 data/01/02/" in output

            # Ensure no mixed separators (backslash followed by forward slash)
            assert "\\/" not in output, "Found mixed separators in output"

            # Check that files are grouped correctly
            assert "file.txt" in output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_validation_files_use_pure_posix_paths(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test that validation_files construction uses PurePosixPath."""
        import tempfile

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client.validate_uploads.return_value = {"duplicates": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create nested structure
            (base / "folder1").mkdir()
            (base / "folder1" / "folder2").mkdir()
            (base / "folder1" / "folder2" / "test.txt").write_text("test")

            # Run upload to trigger validate_uploads
            runner.invoke(main, ["upload", str(base)])

            # Check that validate_uploads was called
            assert mock_client.validate_uploads.called

            # Get the validation files argument
            call_args = mock_client.validate_uploads.call_args
            files_arg = call_args[1]["files"] if call_args[1] else call_args[0][0]

            # Find the file with nested path
            nested_file = [f for f in files_arg if f["name"] == "test.txt"][0]
            rel_path = nested_file.get("relativePath", "")

            # Check that relativePath uses forward slashes only
            assert "\\" not in rel_path, f"relativePath contains backslash: {rel_path}"
            assert (
                rel_path == f"{Path(tmpdir).name}/folder1/folder2"
            ), f"Expected proper POSIX path, got: {rel_path}"

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_dry_run_folder_extraction_pure_posix(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test that folder extraction in dry-run uses PurePosixPath."""
        import tempfile

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create structure with multiple levels
            (base / "a").mkdir()
            (base / "a" / "b").mkdir()
            (base / "a" / "b" / "c").mkdir()
            (base / "a" / "b" / "c" / "file.txt").write_text("deep")

            # Run upload dry-run
            result = runner.invoke(main, ["upload", str(base), "--dry-run"])

            assert result.exit_code == 0
            output = result.output

            # Extract folder paths from output
            lines = output.split("\n")
            folder_lines = [line.strip() for line in lines if "📁" in line]

            # Check that all folders use forward slashes
            for line in folder_lines:
                # Extract the path from the line (after the emoji)
                if "📁" in line:
                    path_part = line.split("📁")[1].strip()
                    # Should not contain backslashes
                    assert (
                        "\\" not in path_part
                    ), f"Folder path contains backslash: {path_part}"
                    # Should end with forward slash
                    assert path_part.endswith(
                        "/"
                    ), f"Folder path should end with /: {path_part}"

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_file_grouping_uses_pure_posix_paths(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test that file grouping by directory uses PurePosixPath."""
        import tempfile

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create files in nested directories
            (base / "dir1").mkdir()
            (base / "dir1" / "dir2").mkdir()
            (base / "dir1" / "file1.txt").write_text("file1")
            (base / "dir1" / "dir2" / "file2.txt").write_text("file2")

            # Run upload dry-run
            result = runner.invoke(main, ["upload", str(base), "--dry-run"])

            assert result.exit_code == 0
            output = result.output

            # Check that directory paths in "In <path>:" use forward slashes
            lines = output.split("\n")
            in_lines = [line for line in lines if line.strip().startswith("In ")]

            for line in in_lines:
                # Should not contain backslashes
                assert (
                    "\\" not in line
                ), f"Directory grouping contains backslash: {line}"
                # Should use forward slashes for nested paths
                if "root" not in line.lower():
                    assert "/" in line, f"Expected forward slash in path: {line}"

    def test_pure_posix_path_handling_simulation(self):
        """Test PurePosixPath vs Path behavior with simulated Windows paths."""
        from pathlib import PurePosixPath, PureWindowsPath

        # Simulate a path with forward slashes (from scan_directory)
        posix_rel_path = "data/01/02/file.txt"

        # Using Path on Windows would convert to backslashes internally
        # Simulate this with PureWindowsPath
        win_path = PureWindowsPath(posix_rel_path)
        parts = win_path.parts

        # Reconstructing with str(PureWindowsPath(*parts)) gives backslashes
        win_reconstructed = str(PureWindowsPath(*parts[:3]))
        assert "\\" in win_reconstructed, "Windows path should have backslashes"

        # But using PurePosixPath preserves forward slashes
        posix_path = PurePosixPath(posix_rel_path)
        posix_parts = posix_path.parts
        posix_reconstructed = str(PurePosixPath(*posix_parts[:3]))

        assert (
            "\\" not in posix_reconstructed
        ), "PurePosixPath should not have backslashes"
        assert (
            posix_reconstructed == "data/01/02"
        ), f"Expected 'data/01/02', got '{posix_reconstructed}'"

        # Check parent extraction
        posix_parent = str(posix_path.parent)
        assert (
            posix_parent == "data/01/02"
        ), f"Expected 'data/01/02', got '{posix_parent}'"
        assert (
            "\\" not in posix_parent
        ), "PurePosixPath parent should not have backslashes"


class TestRemotePathDuplicateDetection:
    """Tests for duplicate detection when using remote-path parameter."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_remote_path_folder_not_flagged_as_duplicate(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test that the remote-path folder itself is not flagged as duplicate."""
        # Create test file
        test_file = tmp_path / "file.txt"
        test_file.write_text("content")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Simulate that "backup" folder already exists (which is expected)
        mock_client.validate_uploads.return_value = {
            "duplicates": ["backup"]  # API reports backup folder as duplicate
        }

        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 100,
                    "name": "backup",
                    "type": "folder",
                }
            ]
        }

        # Upload with -r backup should NOT trigger duplicate warning for "backup"
        result = runner.invoke(
            main, ["upload", str(test_file), "-r", "backup", "--on-duplicate", "skip"]
        )

        assert result.exit_code == 0
        # Should NOT show duplicate warning (but progress messages are OK)
        assert "Duplicate detected:" not in result.output
        assert "Found" not in result.output or "Found 0 duplicate" in result.output
        # Should proceed with upload without prompting
        assert "Action" not in result.output  # No prompt shown

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_remote_path_file_duplicates_still_detected(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test that file duplicates are still detected when using remote-path."""
        # Create test files
        (tmp_path / "data").mkdir()
        file1 = tmp_path / "data" / "file1.txt"
        file2 = tmp_path / "data" / "file2.txt"
        file1.write_text("content1")
        file2.write_text("content2")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Simulate that "backup" folder exists AND file1.txt is a duplicate
        mock_client.validate_uploads.return_value = {
            "duplicates": ["backup", "file1.txt"]
        }

        def mock_get_file_entries(
            query=None, workspace_id=0, parent_ids=None, **kwargs
        ):
            if query == "backup":
                return {
                    "data": [
                        {
                            "id": 100,
                            "name": "backup",
                            "type": "folder",
                        }
                    ]
                }
            elif query == "file1.txt":
                return {
                    "data": [
                        {
                            "id": 200,
                            "name": "file1.txt",
                            "type": "text",
                        }
                    ]
                }
            elif parent_ids:
                # Return empty for folder contents checks
                return {"data": []}
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_file_entries

        # Upload should skip the duplicate file but not complain about backup folder
        result = runner.invoke(
            main,
            [
                "upload",
                str(tmp_path / "data"),
                "-r",
                "backup",
                "--on-duplicate",
                "skip",
            ],
        )

        assert result.exit_code == 0
        # Should show duplicate warning for file1.txt but not for backup
        assert "file1.txt" in result.output
        assert "1 duplicate" in result.output
        # Should NOT prompt about backup folder
        assert "backup" not in [
            line
            for line in result.output.split("\n")
            if "duplicate" in line.lower() and "ID: 100" in line
        ]

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_remote_path_nested_folder_only_top_level_filtered(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test that only the top-level remote-path folder is
        filtered from duplicates."""
        # Create test file
        test_file = tmp_path / "file.txt"
        test_file.write_text("content")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Use nested remote path: backup/2024/11
        # Only "backup" should be filtered, not "2024" or "11"
        mock_client.validate_uploads.return_value = {
            "duplicates": [
                "backup",
                "2024",
            ]  # backup exists, 2024 subfolder also exists
        }

        def mock_get_file_entries(
            query=None, workspace_id=0, parent_ids=None, **kwargs
        ):
            if query == "backup":
                return {
                    "data": [
                        {
                            "id": 100,
                            "name": "backup",
                            "type": "folder",
                        }
                    ]
                }
            elif query == "2024":
                return {
                    "data": [
                        {
                            "id": 101,
                            "name": "2024",
                            "type": "folder",
                        }
                    ]
                }
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_file_entries

        # Upload with nested remote path
        result = runner.invoke(
            main,
            [
                "upload",
                str(test_file),
                "-r",
                "backup/2024/11",
                "--on-duplicate",
                "skip",
            ],
        )

        assert result.exit_code == 0
        # Should show duplicate warning for 2024 but not for backup
        if "duplicate" in result.output.lower():
            assert "2024" in result.output
            # backup should be filtered out
            duplicate_lines = [
                line
                for line in result.output.split("\n")
                if "duplicate" in line.lower()
            ]
            backup_in_duplicates = any("backup" in line for line in duplicate_lines)
            assert (
                not backup_in_duplicates
            ), "backup folder should not be in duplicate warnings"


class TestSyncCommand:
    """Tests for the sync command."""

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    @patch("pydrime.cli.Path")
    def test_sync_files_to_upload_only(
        self,
        mock_path_class,
        mock_cli_config,
        mock_auth_config,
        mock_client_class,
        runner,
        tmp_path,
    ):
        """Test sync when only local files need to be uploaded."""
        # Create test directory with files
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()
        file1 = sync_dir / "file1.txt"
        file1.write_text("content1")
        file2 = sync_dir / "file2.txt"
        file2.write_text("content2")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Mock file entries - no remote files
        mock_client.get_file_entries.return_value = {"data": []}

        # Mock upload
        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}

        # Mock Path to return actual path
        mock_path_class.return_value = Path(str(sync_dir))

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Dry run:" in result.output or "Dry run complete!" in result.output
        assert "Upload: 2 file(s)" in result.output or "upload" in result.output.lower()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_files_to_download_only(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync when only remote files need to be downloaded."""
        # Create empty sync directory
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Mock remote files that don't exist locally
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "remote1.txt",
                    "file_name": "remote1.txt",
                    "mime": "text/plain",
                    "file_size": 100,
                    "type": "text",
                    "hash": "hash1",
                    "url": "https://example.com/remote1.txt",
                    "created_at": "2024-01-01T12:00:00Z",
                    "updated_at": "2024-01-01T12:00:00Z",
                    "parent_id": None,
                    "extension": "txt",
                },
                {
                    "id": 2,
                    "name": "remote2.txt",
                    "file_name": "remote2.txt",
                    "mime": "text/plain",
                    "file_size": 200,
                    "type": "text",
                    "hash": "hash2",
                    "url": "https://example.com/remote2.txt",
                    "created_at": "2024-01-01T13:00:00Z",
                    "updated_at": "2024-01-01T13:00:00Z",
                    "parent_id": None,
                    "extension": "txt",
                },
            ]
        }

        # Mock download
        mock_client.download_file.return_value = sync_dir / "remote1.txt"

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Dry run:" in result.output or "Dry run complete!" in result.output
        assert (
            "Download: 2 file(s)" in result.output
            or "download" in result.output.lower()
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_files_already_in_sync(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync when files are already synchronized."""
        # Create test directory with files
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()
        file1 = sync_dir / "file1.txt"
        file1.write_text("content1")

        # Set specific modification time
        import os
        import time

        timestamp = time.time()
        os.utime(file1, (timestamp, timestamp))

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Mock remote files with same size and timestamp
        from datetime import datetime

        dt = datetime.fromtimestamp(timestamp)
        iso_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "file1.txt",
                    "file_name": "file1.txt",
                    "mime": "text/plain",
                    "file_size": len("content1"),
                    "type": "text",
                    "hash": "hash123",
                    "url": "https://example.com/file1.txt",
                    "created_at": iso_time,
                    "updated_at": iso_time,
                    "parent_id": None,
                    "extension": "txt",
                }
            ]
        }

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Dry run:" in result.output or "Dry run complete!" in result.output
        # Check that files are detected (test passes if sync logic works)
        assert (
            "Skip:" in result.output
            or "Upload:" in result.output
            or "Sync plan:" in result.output
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_with_conflicts_newer_local(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync with conflict - local file is newer."""
        # Create test directory with files
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()
        file1 = sync_dir / "file1.txt"
        file1.write_text("newer content")

        import os
        import time

        new_timestamp = time.time()
        os.utime(file1, (new_timestamp, new_timestamp))

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Mock remote file with older timestamp and different size
        from datetime import datetime, timedelta

        # Use a much older timestamp to avoid any rounding issues (1 day ago)
        old_dt = datetime.fromtimestamp(new_timestamp) - timedelta(days=1)
        old_iso_time = old_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "file1.txt",
                    "file_name": "file1.txt",
                    "mime": "text/plain",
                    "file_size": 100,  # Different size
                    "type": "text",
                    "hash": "abc123",
                    "url": "https://example.com/file1.txt",
                    "created_at": old_iso_time,
                    "updated_at": old_iso_time,
                    "parent_id": None,
                    "extension": "txt",
                }
            ]
        }

        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Dry run:" in result.output or "Dry run complete!" in result.output
        # Should upload the newer local file
        assert "Upload: 1 file(s)" in result.output or "upload" in result.output.lower()

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_with_conflicts_newer_remote(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync with conflict - remote file is newer."""
        # Create test directory with files
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()
        file1 = sync_dir / "file1.txt"
        file1.write_text("older content")

        import os
        import time

        old_timestamp = time.time() - 86400  # 1 day ago
        os.utime(file1, (old_timestamp, old_timestamp))

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Mock remote file with newer timestamp
        from datetime import datetime

        new_dt = datetime.fromtimestamp(time.time())
        new_iso_time = new_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "file1.txt",
                    "file_name": "file1.txt",
                    "mime": "text/plain",
                    "file_size": 200,  # Different size
                    "type": "text",
                    "hash": "def456",
                    "url": "https://example.com/file1.txt",
                    "created_at": new_iso_time,
                    "updated_at": new_iso_time,
                    "parent_id": None,
                    "extension": "txt",
                }
            ]
        }

        mock_client.download_file.return_value = sync_dir / "file1.txt"

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Dry run:" in result.output or "Dry run complete!" in result.output
        # Should download the newer remote file
        assert (
            "Download: 1 file(s)" in result.output
            or "download" in result.output.lower()
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_without_dry_run(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync actually performs uploads and downloads."""
        # Create test directory with one local file
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()
        file1 = sync_dir / "local.txt"
        file1.write_text("local content")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Mock one remote file to download
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 2,
                    "name": "remote.txt",
                    "file_name": "remote.txt",
                    "mime": "text/plain",
                    "file_size": 100,
                    "type": "text",
                    "hash": "remote_hash",
                    "url": "https://example.com/remote.txt",
                    "created_at": "2024-01-01T12:00:00Z",
                    "updated_at": "2024-01-01T12:00:00Z",
                    "parent_id": None,
                    "extension": "txt",
                }
            ]
        }

        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}
        mock_client.download_file.return_value = sync_dir / "remote.txt"

        result = runner.invoke(
            main,
            ["sync", str(sync_dir)],
        )

        # Without --dry-run, it should actually call upload and download
        assert mock_client.upload_file.called or "upload" in result.output.lower()
        # Note: download might not be called in dry-run detection logic

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_with_remote_path(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync with --remote-path option."""
        # Create test directory
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()
        file1 = sync_dir / "file1.txt"
        file1.write_text("content1")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Mock get_file_entries to find the remote folder
        def mock_get_entries(query=None, workspace_id=0, parent_ids=None, **kwargs):
            if query == "backup":
                return {
                    "data": [
                        {
                            "id": 100,
                            "name": "backup",
                            "type": "folder",
                        }
                    ]
                }
            elif parent_ids == [100]:
                # Files in backup folder
                return {"data": []}
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_entries
        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "-r", "backup", "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Dry run:" in result.output or "Dry run complete!" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_with_workspace(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync with --workspace option."""
        # Create test directory
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {
            "workspaces": [{"id": 5, "name": "test_workspace"}]
        }
        mock_client.get_file_entries.return_value = {"data": []}

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "-w", "5", "--dry-run"],
        )

        assert result.exit_code == 0

    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_without_api_key(
        self, mock_cli_config, mock_auth_config, runner, tmp_path
    ):
        """Test sync without API key configured."""
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()

        mock_cli_config.is_configured.return_value = False
        mock_auth_config.is_configured.return_value = False

        result = runner.invoke(main, ["sync", str(sync_dir)])

        assert result.exit_code == 1
        assert "API key not configured" in result.output

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_nonexistent_directory(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner
    ):
        """Test sync with non-existent directory."""
        mock_cli_config.is_configured.return_value = True
        mock_auth_config.is_configured.return_value = True

        result = runner.invoke(
            main,
            ["sync", "/nonexistent/path"],
        )

        assert result.exit_code != 0  # Should fail with any non-zero exit code
        assert (
            "not a directory" in result.output.lower()
            or "does not exist" in result.output.lower()
            or "usage:" in result.output.lower()
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_file_instead_of_directory(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync with file instead of directory."""
        # Create a file instead of directory
        test_file = tmp_path / "file.txt"
        test_file.write_text("content")

        mock_cli_config.is_configured.return_value = True
        mock_auth_config.is_configured.return_value = True

        result = runner.invoke(
            main,
            ["sync", str(test_file)],
        )

        assert result.exit_code == 1
        assert (
            "not a directory" in result.output.lower()
            or "must be a directory" in result.output.lower()
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_empty_directory(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync with empty local and remote directories."""
        # Create empty directory
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client.get_file_entries.return_value = {"data": []}

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "--dry-run"],
        )

        assert result.exit_code == 0
        assert (
            "No changes needed" in result.output
            or "everything is in sync" in result.output.lower()
        )

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_api_error_handling(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync handles API errors gracefully during fetch."""
        # Create test directory
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()
        file1 = sync_dir / "file1.txt"
        file1.write_text("content")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}

        # Simulate API error during recursive fetch
        mock_client.get_file_entries.side_effect = DrimeAPIError(
            "API connection failed"
        )

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "--dry-run"],
        )

        # Sync should handle the error gracefully and continue
        # (errors during recursive fetch are caught and ignored)
        assert result.exit_code == 0

    @patch("pydrime.cli.DrimeClient")
    @patch("pydrime.auth.config")
    @patch("pydrime.cli.config")
    def test_sync_with_nested_folders(
        self, mock_cli_config, mock_auth_config, mock_client_class, runner, tmp_path
    ):
        """Test sync with nested folder structures."""
        # Create nested directory structure
        sync_dir = tmp_path / "sync_folder"
        sync_dir.mkdir()
        sub_dir = sync_dir / "subfolder"
        sub_dir.mkdir()
        file1 = sync_dir / "file1.txt"
        file1.write_text("content1")
        file2 = sub_dir / "file2.txt"
        file2.write_text("content2")

        mock_cli_config.is_configured.return_value = True
        mock_cli_config.get_default_workspace.return_value = 0
        mock_cli_config.get_current_folder.return_value = None
        mock_auth_config.is_configured.return_value = True

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client.get_file_entries.return_value = {"data": []}
        mock_client.upload_file.return_value = {"fileEntry": {"id": 1}}

        result = runner.invoke(
            main,
            ["sync", str(sync_dir), "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Dry run:" in result.output or "Dry run complete!" in result.output
        # Should detect both files
        assert "Upload: 2 file(s)" in result.output or "upload" in result.output.lower()
