"""Tests for upload preview utilities."""

from unittest.mock import MagicMock, patch

from pydrime.output import OutputFormatter
from pydrime.upload_preview import display_upload_preview


class TestDisplayUploadPreview:
    """Tests for display_upload_preview function."""

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_quiet_mode_does_not_output(self, mock_format_ws, tmp_path, capsys):
        """Test quiet mode suppresses all output."""
        out = OutputFormatter(json_output=False, quiet=True)
        mock_client = MagicMock()
        files = [(tmp_path / "test.txt", "test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert captured.out == ""
        mock_format_ws.assert_not_called()

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_dry_run_header(self, mock_format_ws, tmp_path, capsys):
        """Test displays dry run header."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files = [(test_file, "test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=True)

        captured = capsys.readouterr()
        assert "DRY RUN - Upload Preview" in captured.out
        assert "=" * 70 in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_normal_header(self, mock_format_ws, tmp_path, capsys):
        """Test displays normal upload preview header."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files = [(test_file, "test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert "Upload Preview" in captured.out
        assert "DRY RUN" not in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_workspace_info(self, mock_format_ws, tmp_path, capsys):
        """Test displays workspace information."""
        mock_format_ws.return_value = ("Team Workspace (5)", "Team Workspace")
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files = [(test_file, "test.txt")]

        display_upload_preview(out, mock_client, files, 5, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert "Workspace: Team Workspace (5)" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_root_folder_location(self, mock_format_ws, tmp_path, capsys):
        """Test displays root folder as base location."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files = [(test_file, "test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert "Base location: /" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_folder_location_with_name(self, mock_format_ws, tmp_path, capsys):
        """Test displays folder location with name."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files = [(test_file, "test.txt")]

        display_upload_preview(
            out, mock_client, files, 0, 123, "Documents", is_dry_run=False
        )

        captured = capsys.readouterr()
        assert "Base location: /Documents" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_folder_location_without_name(
        self, mock_format_ws, tmp_path, capsys
    ):
        """Test displays folder location without name."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files = [(test_file, "test.txt")]

        display_upload_preview(out, mock_client, files, 0, 123, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert "Base location: /Folder_123" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_files_count(self, mock_format_ws, tmp_path, capsys):
        """Test displays correct file count."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        files = []
        for i in range(3):
            test_file = tmp_path / f"test{i}.txt"
            test_file.write_text("content")
            files.append((test_file, f"test{i}.txt"))

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert "Files to upload: 3" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_folders_to_create(self, mock_format_ws, tmp_path, capsys):
        """Test displays folders that will be created."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files = [(test_file, "folder1/folder2/test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert "Folders to create: 2" in captured.out
        assert "[D] /folder1/" in captured.out
        assert "[D] /folder1/folder2/" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_file_with_size(self, mock_format_ws, tmp_path, capsys):
        """Test displays files with their sizes."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello world")  # 11 bytes
        files = [(test_file, "test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert "[F] test.txt" in captured.out
        # format_size now uses 2 decimals (from syncengine)
        assert "11 B" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_displays_total_size(self, mock_format_ws, tmp_path, capsys):
        """Test displays total size summary."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello world")  # 11 bytes
        files = [(test_file, "test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        # format_size now uses 2 decimals (from syncengine)
        assert "Total: 1 files, 11 B" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_groups_files_by_directory(self, mock_format_ws, tmp_path, capsys):
        """Test groups files by directory for display."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()

        file1 = tmp_path / "file1.txt"
        file1.write_text("content1")
        file2 = tmp_path / "file2.txt"
        file2.write_text("content2")

        files = [
            (file1, "dir1/file1.txt"),
            (file2, "dir1/file2.txt"),
        ]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()
        assert "In dir1/:" in captured.out
        assert "file1.txt" in captured.out
        assert "file2.txt" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_shows_nested_upload_location_with_parent_folder(
        self, mock_format_ws, tmp_path, capsys
    ):
        """Test shows correct upload location for nested files with parent folder."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        # Mock get_folder_info to return folder name
        mock_client.get_folder_info.return_value = {"name": "projects"}

        file1 = tmp_path / "file.txt"
        file1.write_text("content")

        # File in nested folder with parent folder specified
        files = [(file1, "myproject/file.txt")]

        display_upload_preview(
            out,
            mock_client,
            files,
            0,
            current_folder_id=123,
            current_folder_name="projects",
            is_dry_run=False,
        )

        captured = capsys.readouterr()
        # Should show "projects/myproject/..." format
        assert "myproject" in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_folders_with_leading_slash_no_root_folder(
        self, mock_format_ws, tmp_path, capsys
    ):
        """Test that paths with leading slash don't create a root '/' folder entry.

        When uploading with paths like '/folder/file.txt', we should not display
        '/' or '//' as a folder to create - only '/folder/' should be shown.
        """
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        # Path with leading slash (common when using -r /folder/filename)
        files = [(test_file, "/benchmark_folder/test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()

        # Should show exactly 1 folder to create
        assert "Folders to create: 1" in captured.out
        # Should show the correct folder path
        assert "[D] /benchmark_folder/" in captured.out
        # Should NOT contain '//' (which would indicate root '/' was added)
        assert "[D] //" not in captured.out

    @patch("pydrime.upload_preview.format_workspace_display")
    def test_folders_with_leading_slash_nested(self, mock_format_ws, tmp_path, capsys):
        """Test nested paths with leading slash create correct folder structure."""
        mock_format_ws.return_value = ("Personal (0)", None)
        out = OutputFormatter(json_output=False, quiet=False)
        mock_client = MagicMock()
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        # Nested path with leading slash
        files = [(test_file, "/a/b/c/test.txt")]

        display_upload_preview(out, mock_client, files, 0, None, None, is_dry_run=False)

        captured = capsys.readouterr()

        # Should show 3 folders to create
        assert "Folders to create: 3" in captured.out
        assert "[D] /a/" in captured.out
        assert "[D] /a/b/" in captured.out
        assert "[D] /a/b/c/" in captured.out
        # Should NOT contain '//'
        assert "[D] //" not in captured.out
