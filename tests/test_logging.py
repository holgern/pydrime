"""Tests for pydrime logging module."""

import logging
import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from pydrime.logging import (
    API_LEVEL,
    LOG_LEVELS,
    get_log_level_from_string,
    log_api_request,
    setup_logging,
    truncate_payload,
    truncate_value,
)


class TestLogLevels:
    """Test log level constants and mapping."""

    def test_api_level_is_below_debug(self):
        """API level should be lower than DEBUG for most verbose output."""
        assert API_LEVEL < logging.DEBUG

    def test_api_level_value(self):
        """API level should be 5."""
        assert API_LEVEL == 5

    def test_log_levels_mapping(self):
        """LOG_LEVELS should contain all standard levels plus API."""
        assert "error" in LOG_LEVELS
        assert "warning" in LOG_LEVELS
        assert "info" in LOG_LEVELS
        assert "debug" in LOG_LEVELS
        assert "api" in LOG_LEVELS

    def test_log_levels_values(self):
        """LOG_LEVELS values should match logging constants."""
        assert LOG_LEVELS["error"] == logging.ERROR
        assert LOG_LEVELS["warning"] == logging.WARNING
        assert LOG_LEVELS["info"] == logging.INFO
        assert LOG_LEVELS["debug"] == logging.DEBUG
        assert LOG_LEVELS["api"] == API_LEVEL


class TestGetLogLevelFromString:
    """Test get_log_level_from_string function."""

    def test_valid_levels(self):
        """Valid level strings should return correct numeric values."""
        assert get_log_level_from_string("error") == logging.ERROR
        assert get_log_level_from_string("warning") == logging.WARNING
        assert get_log_level_from_string("info") == logging.INFO
        assert get_log_level_from_string("debug") == logging.DEBUG
        assert get_log_level_from_string("api") == API_LEVEL

    def test_case_insensitive(self):
        """Level strings should be case-insensitive."""
        assert get_log_level_from_string("ERROR") == logging.ERROR
        assert get_log_level_from_string("Warning") == logging.WARNING
        assert get_log_level_from_string("INFO") == logging.INFO

    def test_invalid_level_raises_error(self):
        """Invalid level strings should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid log level"):
            get_log_level_from_string("invalid")

        with pytest.raises(ValueError, match="Invalid log level"):
            get_log_level_from_string("verbose")


class TestTruncateValue:
    """Test truncate_value function."""

    def test_short_string_unchanged(self):
        """Short strings should not be truncated."""
        assert truncate_value("hello") == "hello"

    def test_long_string_truncated(self):
        """Long strings should be truncated with ellipsis."""
        long_string = "a" * 200
        result = truncate_value(long_string, max_string_length=100)
        assert len(result) == 103  # 100 chars + "..."
        assert result.endswith("...")

    def test_bytes_converted_to_size_string(self):
        """Bytes should be converted to size description."""
        result = truncate_value(b"hello world")
        assert result == "<11 bytes>"

    def test_short_list_unchanged(self):
        """Short lists should be preserved."""
        result = truncate_value([1, 2, 3])
        assert result == [1, 2, 3]

    def test_long_list_summarized(self):
        """Long lists should be summarized."""
        long_list = list(range(20))
        result = truncate_value(long_list)
        assert result == "[<20 items>]"

    def test_nested_list_values_truncated(self):
        """Values inside lists should also be truncated."""
        result = truncate_value(["a" * 200], max_string_length=50)
        assert len(result) == 1
        assert result[0].endswith("...")

    def test_short_dict_preserved(self):
        """Short dicts should be preserved."""
        result = truncate_value({"a": 1, "b": 2})
        assert result == {"a": 1, "b": 2}

    def test_large_dict_summarized(self):
        """Large dicts should be summarized."""
        large_dict = {f"key{i}": i for i in range(20)}
        result = truncate_value(large_dict)
        # Should have 10 keys + "..." key
        assert len(result) == 11
        assert "..." in result

    def test_nested_dict_values_truncated(self):
        """Values inside dicts should also be truncated."""
        result = truncate_value({"key": "a" * 200}, max_string_length=50)
        assert result["key"].endswith("...")

    def test_path_converted_to_string(self):
        """Path objects should be converted to strings."""
        test_path = Path("/home/user/file.txt")
        result = truncate_value(test_path)
        # Compare as Path objects to handle platform differences
        assert result == str(test_path)

    def test_other_types_unchanged(self):
        """Other types should be returned unchanged."""
        assert truncate_value(42) == 42
        assert truncate_value(3.14) == 3.14
        assert truncate_value(None) is None
        assert truncate_value(True) is True


class TestTruncatePayload:
    """Test truncate_payload function."""

    def test_none_payload(self):
        """None payload should return empty JSON object."""
        assert truncate_payload(None) == "{}"

    def test_simple_payload(self):
        """Simple payload should be JSON stringified."""
        result = truncate_payload({"key": "value"})
        assert result == '{"key": "value"}'

    def test_large_list_in_payload(self):
        """Large lists in payload should be summarized."""
        result = truncate_payload({"files": list(range(50))})
        assert "[<50 items>]" in result

    def test_long_string_in_payload(self):
        """Long strings in payload should be truncated."""
        result = truncate_payload({"content": "a" * 200}, max_string_length=50)
        assert "..." in result

    def test_nested_payload(self):
        """Nested structures should be handled."""
        result = truncate_payload({"data": {"nested": [1, 2, 3], "value": "short"}})
        assert "nested" in result
        assert "value" in result


class TestSetupLogging:
    """Test setup_logging function."""

    def setup_method(self):
        """Reset pydrime logger before each test."""
        logger = logging.getLogger("pydrime")
        logger.handlers.clear()
        logger.setLevel(logging.WARNING)

    def teardown_method(self):
        """Clean up after each test."""
        logger = logging.getLogger("pydrime")
        logger.handlers.clear()
        logger.setLevel(logging.WARNING)

    def test_no_log_level_disables_handlers(self):
        """When no log level is specified, no handlers should be added."""
        setup_logging(log_level=None, log_file=None)
        logger = logging.getLogger("pydrime")
        assert len(logger.handlers) == 0
        assert logger.level == logging.WARNING

    def test_console_logging(self):
        """When log_level is set without log_file, should log to console."""
        setup_logging(log_level="info", log_file=None)
        logger = logging.getLogger("pydrime")
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)
        assert logger.level == logging.INFO

    def test_file_logging(self):
        """When log_file is set, should log to file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "test.log"
            setup_logging(log_level="debug", log_file=str(log_path))

            logger = logging.getLogger("pydrime")
            assert len(logger.handlers) == 1
            assert isinstance(logger.handlers[0], logging.FileHandler)
            assert logger.level == logging.DEBUG

            # Test that logging works
            logger.debug("test message")
            logger.handlers[0].flush()

            assert log_path.exists()
            content = log_path.read_text()
            assert "test message" in content

            # Close handler before temp directory cleanup (important for Windows)
            logger.handlers[0].close()
            logger.handlers.clear()

    def test_file_logging_creates_directory(self):
        """Log file directory should be created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "subdir" / "test.log"
            setup_logging(log_level="info", log_file=str(log_path))

            assert log_path.parent.exists()

            # Close handler before temp directory cleanup (important for Windows)
            logger = logging.getLogger("pydrime")
            if logger.handlers:
                logger.handlers[0].close()
                logger.handlers.clear()

    def test_api_level_logging(self):
        """API level should enable most verbose logging."""
        setup_logging(log_level="api", log_file=None)
        logger = logging.getLogger("pydrime")
        assert logger.level == API_LEVEL
        assert logger.isEnabledFor(API_LEVEL)
        assert logger.isEnabledFor(logging.DEBUG)
        assert logger.isEnabledFor(logging.INFO)

    def test_invalid_log_level_raises_error(self):
        """Invalid log level should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid log level"):
            setup_logging(log_level="invalid")

    def test_env_var_log_level(self):
        """PYDRIME_LOG_LEVEL environment variable should be respected."""
        with mock.patch.dict(os.environ, {"PYDRIME_LOG_LEVEL": "debug"}):
            setup_logging(log_level=None, log_file=None)
            logger = logging.getLogger("pydrime")
            assert logger.level == logging.DEBUG

    def test_env_var_log_file(self):
        """PYDRIME_LOG_FILE environment variable should be respected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "env.log"
            with mock.patch.dict(os.environ, {"PYDRIME_LOG_FILE": str(log_path)}):
                setup_logging(log_level="info", log_file=None)
                logger = logging.getLogger("pydrime")
                assert isinstance(logger.handlers[0], logging.FileHandler)

                # Close handler before temp directory cleanup (important for Windows)
                logger.handlers[0].close()
                logger.handlers.clear()

    def test_explicit_params_override_env_vars(self):
        """Explicit parameters should override environment variables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            env_log_path = Path(tmpdir) / "env.log"
            explicit_log_path = Path(tmpdir) / "explicit.log"

            with mock.patch.dict(
                os.environ,
                {
                    "PYDRIME_LOG_LEVEL": "error",
                    "PYDRIME_LOG_FILE": str(env_log_path),
                },
            ):
                setup_logging(log_level="debug", log_file=str(explicit_log_path))
                logger = logging.getLogger("pydrime")
                assert logger.level == logging.DEBUG

                # Close handler before temp directory cleanup (important for Windows)
                logger.handlers[0].close()
                logger.handlers.clear()

    def test_repeated_setup_clears_old_handlers(self):
        """Calling setup_logging multiple times should not add duplicate handlers."""
        setup_logging(log_level="info")
        setup_logging(log_level="debug")
        setup_logging(log_level="error")

        logger = logging.getLogger("pydrime")
        assert len(logger.handlers) == 1


class TestLogApiRequest:
    """Test log_api_request function."""

    def setup_method(self):
        """Set up test logger."""
        self.logger = logging.getLogger("test.api")
        self.logger.handlers.clear()
        self.handler = logging.StreamHandler()
        self.handler.setLevel(API_LEVEL)
        self.logger.addHandler(self.handler)
        self.logger.setLevel(API_LEVEL)

    def teardown_method(self):
        """Clean up test logger."""
        self.logger.handlers.clear()

    def test_logs_method_and_endpoint(self):
        """Should log HTTP method and endpoint."""
        with mock.patch.object(self.logger, "log") as mock_log:
            log_api_request(self.logger, "GET", "/api/v1/files")
            mock_log.assert_called_once()
            args = mock_log.call_args
            assert args[0][0] == API_LEVEL
            assert "GET /api/v1/files" in args[0][1]

    def test_logs_params(self):
        """Should log query parameters."""
        with mock.patch.object(self.logger, "log") as mock_log:
            log_api_request(
                self.logger,
                "GET",
                "/api/v1/files",
                params={"page": 1, "limit": 50},
            )
            args = mock_log.call_args
            message = args[0][1]
            assert "params=" in message
            assert "page" in message

    def test_logs_json_data(self):
        """Should log JSON body data."""
        with mock.patch.object(self.logger, "log") as mock_log:
            log_api_request(
                self.logger,
                "POST",
                "/api/v1/upload",
                json_data={"filename": "test.txt", "size": 1024},
            )
            args = mock_log.call_args
            message = args[0][1]
            assert "json=" in message
            assert "filename" in message

    def test_does_not_log_when_level_disabled(self):
        """Should not log when API level is not enabled."""
        self.logger.setLevel(logging.WARNING)
        with mock.patch.object(self.logger, "log") as mock_log:
            log_api_request(self.logger, "GET", "/api/v1/files")
            mock_log.assert_not_called()

    def test_truncates_large_payloads(self):
        """Large payloads should be truncated in logs."""
        with mock.patch.object(self.logger, "log") as mock_log:
            large_list = list(range(100))
            log_api_request(
                self.logger,
                "POST",
                "/api/v1/validate",
                json_data={"files": large_list},
            )
            args = mock_log.call_args
            message = args[0][1]
            # Should show summary, not all 100 items
            assert "<100 items>" in message
