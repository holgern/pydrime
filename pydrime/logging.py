"""Logging configuration for pydrime.

This module provides centralized logging configuration with support for:
- Multiple log levels including a custom API level for request tracing
- Console or file output
- Payload truncation for readable API logs
- Environment variable configuration (PYDRIME_LOG_LEVEL, PYDRIME_LOG_FILE)
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

# Custom log level for API requests (below DEBUG)
API_LEVEL = 5
logging.addLevelName(API_LEVEL, "API")

# Log level mapping from CLI strings to logging constants
LOG_LEVELS: dict[str, int] = {
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "api": API_LEVEL,
}

# Default paths
DEFAULT_LOG_DIR = Path.home() / ".config" / "pydrime" / "logs"
DEFAULT_LOG_FILE = DEFAULT_LOG_DIR / "pydrime.log"

# Log formats
FILE_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
CONSOLE_LOG_FORMAT = "%(asctime)s %(levelname)-5s %(name)s: %(message)s"
CONSOLE_DATE_FORMAT = "%H:%M:%S"
FILE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Truncation settings
MAX_LIST_ITEMS = 5
MAX_STRING_LENGTH = 100
MAX_DICT_KEYS = 10


def truncate_value(value: Any, max_string_length: int = MAX_STRING_LENGTH) -> Any:
    """Truncate a single value for logging.

    Args:
        value: Value to truncate
        max_string_length: Maximum length for string values

    Returns:
        Truncated value suitable for logging
    """
    if isinstance(value, str):
        if len(value) > max_string_length:
            return value[:max_string_length] + "..."
        return value
    elif isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    elif isinstance(value, list):
        if len(value) > MAX_LIST_ITEMS:
            return f"[<{len(value)} items>]"
        return [truncate_value(item) for item in value]
    elif isinstance(value, dict):
        if len(value) > MAX_DICT_KEYS:
            keys_shown = list(value.keys())[:MAX_DICT_KEYS]
            truncated = {k: truncate_value(value[k]) for k in keys_shown}
            truncated["..."] = f"<{len(value) - MAX_DICT_KEYS} more keys>"
            return truncated
        return {k: truncate_value(v) for k, v in value.items()}
    elif isinstance(value, Path):
        return str(value)
    else:
        return value


def truncate_payload(
    payload: dict[str, Any] | None,
    max_items: int = MAX_LIST_ITEMS,
    max_string_length: int = MAX_STRING_LENGTH,
) -> str:
    """Truncate a payload dictionary for logging.

    Summarizes large lists and truncates long strings to keep log output readable.

    Args:
        payload: Dictionary payload to truncate
        max_items: Maximum number of items to show in lists
        max_string_length: Maximum length for string values

    Returns:
        JSON string representation of truncated payload
    """
    if payload is None:
        return "{}"

    truncated = truncate_value(payload, max_string_length)

    try:
        return json.dumps(truncated, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(truncated)


def setup_logging(
    log_level: str | None = None,
    log_file: str | Path | None = None,
) -> None:
    """Configure logging for pydrime.

    Sets up logging to either console (stderr) or file based on parameters.
    If log_level is None, logging is effectively disabled (set to WARNING
    with no handlers on pydrime logger).

    Args:
        log_level: Log level string (error, warning, info, debug, api).
                  If None, uses PYDRIME_LOG_LEVEL env var or disables logging.
        log_file: Path to log file. If None, logs to console.
                 Can also be set via PYDRIME_LOG_FILE env var.
    """
    # Check environment variables if not provided
    if log_level is None:
        log_level = os.environ.get("PYDRIME_LOG_LEVEL")

    if log_file is None:
        env_log_file = os.environ.get("PYDRIME_LOG_FILE")
        if env_log_file:
            log_file = env_log_file

    # If no log level specified, don't configure any handlers
    if log_level is None:
        # Set root pydrime logger to WARNING to suppress debug/info
        logging.getLogger("pydrime").setLevel(logging.WARNING)
        return

    # Validate log level
    level_str = log_level.lower()
    if level_str not in LOG_LEVELS:
        valid_levels = ", ".join(LOG_LEVELS.keys())
        raise ValueError(
            f"Invalid log level '{log_level}'. Valid levels: {valid_levels}"
        )

    level = LOG_LEVELS[level_str]

    # Get the root pydrime logger
    pydrime_logger = logging.getLogger("pydrime")
    pydrime_logger.setLevel(level)

    # Remove any existing handlers to avoid duplicates
    pydrime_logger.handlers.clear()

    # Create handler based on whether we're logging to file or console
    if log_file:
        log_path = Path(log_file)

        # Create log directory if it doesn't exist
        log_path.parent.mkdir(parents=True, exist_ok=True)

        handler: logging.Handler = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter(FILE_LOG_FORMAT, datefmt=FILE_DATE_FORMAT)
    else:
        # Console handler (stderr)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(CONSOLE_LOG_FORMAT, datefmt=CONSOLE_DATE_FORMAT)

    handler.setLevel(level)
    handler.setFormatter(formatter)
    pydrime_logger.addHandler(handler)

    # Don't propagate to root logger to avoid duplicate output
    pydrime_logger.propagate = False


def log_api_request(
    logger: logging.Logger,
    method: str,
    endpoint: str,
    params: dict[str, Any] | None = None,
    json_data: dict[str, Any] | None = None,
) -> None:
    """Log an API request at API level.

    Logs the HTTP method, endpoint, and truncated request data.
    Only logs if the logger is enabled for API_LEVEL.

    Args:
        logger: Logger instance to use
        method: HTTP method (GET, POST, PUT, DELETE, etc.)
        endpoint: API endpoint path
        params: Query parameters (optional)
        json_data: JSON body data (optional)
    """
    if not logger.isEnabledFor(API_LEVEL):
        return

    parts = [f"{method} {endpoint}"]

    if params:
        parts.append(f"params={truncate_payload(params)}")

    if json_data:
        parts.append(f"json={truncate_payload(json_data)}")

    message = " ".join(parts)
    logger.log(API_LEVEL, message)


def get_log_level_from_string(level_str: str) -> int:
    """Convert a log level string to its numeric value.

    Args:
        level_str: Log level string (error, warning, info, debug, api)

    Returns:
        Numeric log level

    Raises:
        ValueError: If level string is not valid
    """
    level_str = level_str.lower()
    if level_str not in LOG_LEVELS:
        valid_levels = ", ".join(LOG_LEVELS.keys())
        raise ValueError(
            f"Invalid log level '{level_str}'. Valid levels: {valid_levels}"
        )
    return LOG_LEVELS[level_str]
