"""Utility functions for Drime Cloud."""

import base64


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
