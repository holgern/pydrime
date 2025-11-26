"""Utility functions for Drime Cloud."""

import base64
from datetime import datetime
from typing import Optional

# =============================================================================
# Constants for file operations
# =============================================================================

# Chunk size for multipart uploads (25 MB)
DEFAULT_CHUNK_SIZE: int = 25 * 1024 * 1024

# Threshold for using multipart upload (30 MB)
DEFAULT_MULTIPART_THRESHOLD: int = 30 * 1024 * 1024

# Retry configuration for transient errors
DEFAULT_MAX_RETRIES: int = 3
DEFAULT_RETRY_DELAY: float = 2.0  # seconds

# Batch size for bulk delete operations
DEFAULT_DELETE_BATCH_SIZE: int = 10


# =============================================================================
# Timestamp parsing utilities
# =============================================================================


def parse_iso_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO format timestamp from Drime API.

    Args:
        timestamp_str: ISO format timestamp string (e.g., "2025-01-15T10:30:00.000000Z")

    Returns:
        datetime object in local timezone or None if parsing fails
    """
    if not timestamp_str:
        return None

    try:
        # Handle various ISO formats
        # The 'Z' suffix indicates UTC time
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        # Try parsing with timezone
        try:
            dt = datetime.fromisoformat(timestamp_str)
            # Convert to local time (naive datetime in local timezone)
            if dt.tzinfo is not None:
                # Convert to timestamp (UTC) then to local naive datetime
                timestamp = dt.timestamp()
                return datetime.fromtimestamp(timestamp)
            return dt
        except ValueError:
            # Try without microseconds
            if "." in timestamp_str:
                timestamp_str = timestamp_str.split(".")[0] + "+00:00"
            dt = datetime.fromisoformat(timestamp_str)
            if dt.tzinfo is not None:
                timestamp = dt.timestamp()
                return datetime.fromtimestamp(timestamp)
            return dt
    except (ValueError, AttributeError):
        return None


# =============================================================================
# Size formatting utilities
# =============================================================================


def format_size(size_bytes: int) -> str:
    """Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted size string (e.g., "1.5 MB", "256 B")
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / 1024 / 1024:.1f} MB"
    else:
        return f"{size_bytes / 1024 / 1024 / 1024:.1f} GB"


# =============================================================================
# Hash calculation utilities
# =============================================================================


def calculate_drime_hash(file_id: int) -> str:
    """Calculate the Drime Cloud hash for a file entry.

    The hash is a base64-encoded string containing the file entry ID
    followed by a pipe character, with padding stripped.

    Args:
        file_id: The numeric ID of the file entry

    Returns:
        Base64-encoded hash string (without padding)

    Examples:
        >>> calculate_drime_hash(480424796)
        'NDgwNDI0Nzk2fA'
        >>> calculate_drime_hash(480424802)
        'NDgwNDI0ODAyfA'
    """
    hash_str = f"{file_id}|"
    return base64.b64encode(hash_str.encode("utf-8")).decode("utf-8").rstrip("=")


def decode_drime_hash(hash_value: str) -> int:
    """Decode a Drime Cloud hash to extract the file entry ID.

    Args:
        hash_value: Base64-encoded hash string

    Returns:
        The numeric file entry ID

    Raises:
        ValueError: If the hash is invalid or cannot be decoded

    Examples:
        >>> decode_drime_hash('NDgwNDI0Nzk2fA')
        480424796
        >>> decode_drime_hash('NDgwNDI0ODAyfA')
        480424802
    """
    # Add padding if needed
    hash_value += "=" * (4 - len(hash_value) % 4)

    try:
        decoded = base64.b64decode(hash_value).decode("utf-8")
        # Remove the trailing pipe character
        file_id_str = decoded.rstrip("|")
        return int(file_id_str)
    except (ValueError, UnicodeDecodeError) as e:
        raise ValueError(f"Invalid Drime hash: {hash_value}") from e


def is_file_id(value: str) -> bool:
    """Check if a value is a numeric file ID (as opposed to a hash).

    Args:
        value: String value to check

    Returns:
        True if the value is a numeric file ID, False if it's a hash

    Examples:
        >>> is_file_id("480424796")
        True
        >>> is_file_id("NDgwNDI0Nzk2fA")
        False
        >>> is_file_id("123")
        True
        >>> is_file_id("abc123")
        False
    """
    return value.isdigit()


def normalize_to_hash(value: str) -> str:
    """Normalize a file identifier (ID or hash) to hash format.

    Accepts either a numeric file ID or an existing hash and returns
    the corresponding hash value.

    Args:
        value: File ID (numeric string) or hash (alphanumeric string)

    Returns:
        Hash string suitable for API calls

    Examples:
        >>> normalize_to_hash("480424796")
        'NDgwNDI0Nzk2fA'
        >>> normalize_to_hash("NDgwNDI0Nzk2fA")
        'NDgwNDI0Nzk2fA'
    """
    if is_file_id(value):
        return calculate_drime_hash(int(value))
    return value
