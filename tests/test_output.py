"""Unit tests for output formatting."""

from unittest.mock import patch

from pydrime.output import OutputFormatter


class TestOutputFormatter:
    """Tests for OutputFormatter class."""

    def test_init_defaults(self):
        """Test OutputFormatter initialization with defaults."""
        out = OutputFormatter()
        assert not out.json_output
        assert not out.quiet
        assert out.console is not None
        assert out.console_err is not None

    def test_init_json_output(self):
        """Test OutputFormatter with JSON output enabled."""
        out = OutputFormatter(json_output=True)
        assert out.json_output

    def test_init_quiet(self):
        """Test OutputFormatter with quiet mode enabled."""
        out = OutputFormatter(quiet=True)
        assert out.quiet

    def test_init_no_color(self):
        """Test OutputFormatter with color disabled."""
        out = OutputFormatter(no_color=True)
        assert out.no_color

    def test_print_not_quiet(self):
        """Test print method when not in quiet mode."""
        out = OutputFormatter(quiet=False)
        with patch.object(out.console, "print") as mock_print:
            out.print("test message")
            mock_print.assert_called_once_with("test message")

    def test_print_quiet(self):
        """Test print method when in quiet mode."""
        out = OutputFormatter(quiet=True)
        with patch.object(out.console, "print") as mock_print:
            out.print("test message")
            mock_print.assert_not_called()

    def test_error(self):
        """Test error message output."""
        out = OutputFormatter()
        with patch.object(out.console_err, "print") as mock_print:
            out.error("error message")
            mock_print.assert_called_once()
            call_args = mock_print.call_args[0][0]
            assert "Error:" in call_args
            assert "error message" in call_args

    def test_warning_not_quiet(self):
        """Test warning message output when not quiet."""
        out = OutputFormatter(quiet=False)
        with patch.object(out.console_err, "print") as mock_print:
            out.warning("warning message")
            mock_print.assert_called_once()
            call_args = mock_print.call_args[0][0]
            assert "Warning:" in call_args
            assert "warning message" in call_args

    def test_warning_quiet(self):
        """Test warning message output when quiet."""
        out = OutputFormatter(quiet=True)
        with patch.object(out.console_err, "print") as mock_print:
            out.warning("warning message")
            mock_print.assert_not_called()

    def test_success_not_quiet(self):
        """Test success message output when not quiet."""
        out = OutputFormatter(quiet=False)
        with patch.object(out.console, "print") as mock_print:
            out.success("success message")
            mock_print.assert_called_once()
            call_args = mock_print.call_args[0][0]
            assert "success message" in call_args

    def test_success_quiet(self):
        """Test success message output when quiet."""
        out = OutputFormatter(quiet=True)
        with patch.object(out.console, "print") as mock_print:
            out.success("success message")
            mock_print.assert_not_called()

    def test_info_not_quiet(self):
        """Test info message output when not quiet."""
        out = OutputFormatter(quiet=False)
        with patch.object(out.console_err, "print") as mock_print:
            out.info("info message")
            mock_print.assert_called_once()
            call_args = mock_print.call_args[0][0]
            assert "info message" in call_args

    def test_info_quiet(self):
        """Test info message output when quiet."""
        out = OutputFormatter(quiet=True)
        with patch.object(out.console_err, "print") as mock_print:
            out.info("info message")
            mock_print.assert_not_called()

    def test_output_json(self):
        """Test JSON output."""
        out = OutputFormatter()
        data = {"key": "value", "number": 123}
        with patch.object(out.console, "print") as mock_print:
            out.output_json(data)
            mock_print.assert_called_once()

    def test_output_table_empty_json(self):
        """Test output_table with empty data in JSON mode."""
        out = OutputFormatter(json_output=True)
        with patch.object(out, "output_json") as mock_json:
            out.output_table([], ["col1"], {"col1": "Column 1"})
            mock_json.assert_called_once_with([])

    def test_output_table_empty_text(self):
        """Test output_table with empty data in text mode."""
        out = OutputFormatter()
        # Should just return without printing anything
        with patch.object(out.console, "print") as mock_print:
            out.output_table([], ["col1"], {"col1": "Column 1"})
            mock_print.assert_not_called()

    def test_output_table_json(self):
        """Test output_table in JSON mode."""
        out = OutputFormatter(json_output=True)
        data = [{"name": "file1", "size": 100}]
        with patch.object(out, "output_json") as mock_json:
            out.output_table(data, ["name", "size"])
            mock_json.assert_called_once_with(data)

    def test_output_table_text_with_name(self):
        """Test output_table in text mode with name field."""
        out = OutputFormatter()
        data = [
            {"name": "file1.txt", "type": "file", "size": 100},
            {"name": "folder1", "type": "folder", "size": 0},
        ]
        with patch.object(out.console, "print") as mock_print:
            out.output_table(data, ["name", "type", "size"])
            # Should print something
            assert mock_print.called

    def test_output_table_text_columns_fallback(self):
        """Test _output_text_columns fallback."""
        out = OutputFormatter()
        data = [
            {"id": 1, "name": "workspace1", "role": "owner", "owner": "test@test.com"}
        ]
        with patch.object(out.console, "print") as mock_print:
            out.output_table(data, ["name", "role", "owner"])
            # Should print header, separator, and data rows
            assert mock_print.call_count >= 3

    def test_format_size_bytes(self):
        """Test format_size with bytes.

        Note: Uses syncengine.constants.format_size which formats with 2 decimals.
        """
        out = OutputFormatter()
        assert out.format_size(100) == "100 B"
        assert out.format_size(1023) == "1023 B"

    def test_format_size_kb(self):
        """Test format_size with kilobytes."""
        out = OutputFormatter()
        result = out.format_size(1024)
        assert "KB" in result
        result = out.format_size(100 * 1024)
        assert "KB" in result

    def test_format_size_mb(self):
        """Test format_size with megabytes."""
        out = OutputFormatter()
        result = out.format_size(1024 * 1024)
        assert "MB" in result
        result = out.format_size(100 * 1024 * 1024)
        assert "MB" in result

    def test_format_size_gb(self):
        """Test format_size with gigabytes."""
        out = OutputFormatter()
        result = out.format_size(1024 * 1024 * 1024)
        assert "GB" in result
        result = out.format_size(5 * 1024 * 1024 * 1024)
        assert "GB" in result

    def test_progress_message_not_quiet(self):
        """Test progress_message when not quiet."""
        out = OutputFormatter(quiet=False)
        with patch.object(out.console_err, "print") as mock_print:
            out.progress_message("Processing...")
            mock_print.assert_called_once()
            call_args = mock_print.call_args[0][0]
            assert "Processing..." in call_args

    def test_progress_message_quiet(self):
        """Test progress_message when quiet."""
        out = OutputFormatter(quiet=True)
        with patch.object(out.console_err, "print") as mock_print:
            out.progress_message("Processing...")
            mock_print.assert_not_called()

    def test_print_summary_not_quiet(self):
        """Test print_summary when not quiet."""
        out = OutputFormatter(quiet=False)
        items = [("Status", "Success"), ("Count", "5")]
        with patch.object(out.console_err, "print") as mock_print:
            out.print_summary("Test Summary", items)
            mock_print.assert_called_once()

    def test_print_summary_quiet(self):
        """Test print_summary when quiet."""
        out = OutputFormatter(quiet=True)
        items = [("Status", "Success"), ("Count", "5")]
        with patch.object(out.console_err, "print") as mock_print:
            out.print_summary("Test Summary", items)
            mock_print.assert_not_called()

    def test_output_text_simple_with_folders(self):
        """Test _output_text_simple with folder coloring."""
        out = OutputFormatter()
        data = [
            {"name": "folder1", "type": "folder", "id": 1},
            {"name": "file1.txt", "type": "file", "id": 2},
        ]
        with patch.object(out.console, "print") as mock_print:
            out._output_text_simple(data)
            assert mock_print.called

    def test_output_text_simple_executable_files(self):
        """Test _output_text_simple with executable file coloring."""
        out = OutputFormatter()
        data = [
            {"name": "script.py", "type": "file", "id": 1},
            {"name": "run.sh", "type": "file", "id": 2},
            {"name": "app.exe", "type": "file", "id": 3},
        ]
        with patch.object(out.console, "print") as mock_print:
            out._output_text_simple(data)
            assert mock_print.called

    def test_output_text_columns_empty(self):
        """Test _output_text_columns with empty data."""
        out = OutputFormatter()
        with patch.object(out.console, "print") as mock_print:
            out._output_text_columns([])
            # Empty data should not print anything
            mock_print.assert_not_called()

    def test_output_text_simple_fallback_no_name_field(self):
        """Test _output_text_simple falls back to columns when data
        has no 'name' field."""
        out = OutputFormatter()
        data = [{"id": 1, "status": "active"}, {"id": 2, "status": "inactive"}]
        with patch.object(out, "_output_text_columns") as mock_columns:
            out._output_text_simple(data)
            mock_columns.assert_called_once_with(data)

    def test_output_text_simple_empty_data(self):
        """Test _output_text_simple with empty data."""
        out = OutputFormatter()
        with patch.object(out, "_output_text_columns") as mock_columns:
            out._output_text_simple([])
            mock_columns.assert_called_once_with([])
