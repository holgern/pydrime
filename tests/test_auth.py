"""Tests for authentication helpers."""

from unittest.mock import MagicMock, patch

from pydrime.auth import require_api_key, require_api_key_simple
from pydrime.output import OutputFormatter


class TestRequireApiKey:
    """Tests for require_api_key function."""

    @patch("pydrime.auth.config")
    def test_with_valid_api_key_from_context(self, mock_config):
        """Test with valid API key from context."""
        mock_config.is_configured.return_value = False
        ctx = MagicMock()
        ctx.obj = {"api_key": "test-api-key"}
        out = OutputFormatter(json_output=False, quiet=False)

        result = require_api_key(ctx, out)

        assert result == "test-api-key"
        ctx.exit.assert_not_called()

    @patch("pydrime.auth.config")
    def test_with_configured_api_key(self, mock_config):
        """Test with API key configured in config."""
        mock_config.is_configured.return_value = True
        ctx = MagicMock()
        ctx.obj = {"api_key": None}
        out = OutputFormatter(json_output=False, quiet=False)

        result = require_api_key(ctx, out)

        assert result is None
        ctx.exit.assert_not_called()

    @patch("pydrime.auth.config")
    def test_without_api_key_exits_with_error(self, mock_config):
        """Test without API key exits with error code 1."""
        mock_config.is_configured.return_value = False
        ctx = MagicMock()
        ctx.obj = {"api_key": None}
        out = OutputFormatter(json_output=False, quiet=False)

        require_api_key(ctx, out)

        ctx.exit.assert_called_once_with(1)

    @patch("pydrime.auth.config")
    def test_with_empty_string_api_key_exits_with_error(self, mock_config):
        """Test with empty string API key exits with error."""
        mock_config.is_configured.return_value = False
        ctx = MagicMock()
        ctx.obj = {"api_key": ""}
        out = OutputFormatter(json_output=False, quiet=False)

        require_api_key(ctx, out)

        ctx.exit.assert_called_once_with(1)

    @patch("pydrime.auth.config")
    def test_error_message_contains_instructions(self, mock_config, capsys):
        """Test error message contains helpful instructions."""
        mock_config.is_configured.return_value = False
        ctx = MagicMock()
        ctx.obj = {"api_key": None}
        out = OutputFormatter(json_output=False, quiet=False)

        require_api_key(ctx, out)

        captured = capsys.readouterr()
        assert "API key not configured" in captured.err  # Error goes to stderr
        assert "Run 'pydrime init'" in captured.err
        assert "DRIME_API_KEY" in captured.err


class TestRequireApiKeySimple:
    """Tests for require_api_key_simple function."""

    @patch("pydrime.auth.config")
    def test_with_valid_api_key(self, mock_config):
        """Test with valid API key."""
        mock_config.is_configured.return_value = False
        ctx = MagicMock()
        ctx.obj = {"api_key": "test-api-key"}
        out = OutputFormatter(json_output=False, quiet=False)

        result = require_api_key_simple(ctx, out)

        assert result == "test-api-key"
        ctx.exit.assert_not_called()

    @patch("pydrime.auth.config")
    def test_without_api_key_exits_with_error(self, mock_config):
        """Test without API key exits with error."""
        mock_config.is_configured.return_value = False
        ctx = MagicMock()
        ctx.obj = {"api_key": None}
        out = OutputFormatter(json_output=False, quiet=False)

        require_api_key_simple(ctx, out)

        ctx.exit.assert_called_once_with(1)

    @patch("pydrime.auth.config")
    def test_with_empty_string_exits_with_error(self, mock_config):
        """Test with empty string exits with error."""
        mock_config.is_configured.return_value = False
        ctx = MagicMock()
        ctx.obj = {"api_key": ""}
        out = OutputFormatter(json_output=False, quiet=False)

        require_api_key_simple(ctx, out)

        ctx.exit.assert_called_once_with(1)

    @patch("pydrime.auth.config")
    def test_simple_error_message(self, mock_config, capsys):
        """Test error message is simpler than require_api_key."""
        mock_config.is_configured.return_value = False
        ctx = MagicMock()
        ctx.obj = {"api_key": None}
        out = OutputFormatter(json_output=False, quiet=False)

        require_api_key_simple(ctx, out)

        captured = capsys.readouterr()
        assert "API key not configured" in captured.err  # Error goes to stderr
        assert "Run 'pydrime init'" in captured.err
        # Should not contain detailed instructions
        assert "DRIME_API_KEY" not in captured.out
