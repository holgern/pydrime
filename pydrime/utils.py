"""Utility functions for Drime Cloud."""

from __future__ import annotations

import base64
import fnmatch
import re
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .api import DrimeClient
    from .models import FileEntry

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


def parse_iso_timestamp(timestamp_str: str | None) -> datetime | None:
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
    """Format bytes into human-readable size string.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human-readable size string (e.g., "1.5 MB", "256 KB")

    Examples:
        >>> format_size(1024)
        '1.00 KB'
        >>> format_size(1536000)
        '1.46 MB'
        >>> format_size(1073741824)
        '1.00 GB'
    """
    if size_bytes < 0:
        return "0 B"

    size_float = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(size_float) < 1024.0:
            if unit == "B":
                return f"{int(size_float)} {unit}"
            return f"{size_float:.2f} {unit}"
        size_float /= 1024.0

    return f"{size_float:.2f} EB"


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


# =============================================================================
# Remote file verification utilities
# =============================================================================


class RemoteFileVerificationResult:
    """Result of remote file verification.

    Attributes:
        all_verified: True if all files passed verification
        verified_count: Number of files that passed verification
        total_count: Total number of files checked
        expected_count: Expected number of files (if specified)
        unverified_files: List of file names that failed verification
        errors: List of error messages if verification failed
    """

    def __init__(
        self,
        all_verified: bool,
        verified_count: int,
        total_count: int,
        expected_count: int | None = None,
        unverified_files: list[str] | None = None,
        errors: list[str] | None = None,
    ):
        self.all_verified = all_verified
        self.verified_count = verified_count
        self.total_count = total_count
        self.expected_count = expected_count
        self.unverified_files = unverified_files or []
        self.errors = errors or []

    def __bool__(self) -> bool:
        """Return True if all files were verified."""
        return self.all_verified

    def __repr__(self) -> str:
        return (
            f"RemoteFileVerificationResult("
            f"verified={self.verified_count}/{self.total_count}, "
            f"all_verified={self.all_verified})"
        )


def verify_remote_files_have_users(
    client: DrimeClient,
    remote_folder: str,
    expected_count: int | None = None,
    verbose: bool = True,
    workspace_id: int = 0,
) -> RemoteFileVerificationResult:
    """Verify that remote files have the users field correctly set.

    This checks that uploaded files are properly associated with user ownership,
    which is critical for ensuring uploads are complete and not affected by
    race conditions during parallel uploads.

    The users field being populated indicates that the file entry has been
    fully processed by the server and is properly associated with the user.

    Args:
        client: DrimeClient instance for API calls
        remote_folder: Name of the remote folder to check (without leading /)
        expected_count: Expected number of files (optional)
        verbose: If True, print verification details
        workspace_id: Workspace ID to search in (default: 0 for personal)

    Returns:
        RemoteFileVerificationResult with verification details

    Example:
        >>> from pydrime.api import DrimeClient
        >>> from pydrime.utils import verify_remote_files_have_users
        >>> client = DrimeClient()
        >>> result = verify_remote_files_have_users(
        ...     client, "my_folder", expected_count=5
        ... )
        >>> if result:
        ...     print("All files verified!")
        >>> else:
        ...     print(f"Verification failed: {result.errors}")
    """
    # Import here to avoid circular imports
    from .models import FileEntriesResult

    if verbose:
        print(f"\n[VERIFY] Checking users field for files in /{remote_folder}")

    try:
        # Find the folder first
        result = client.get_file_entries(
            query=remote_folder, entry_type="folder", workspace_id=workspace_id
        )
        folder_hash = None

        if result and result.get("data"):
            entries = FileEntriesResult.from_api_response(result)
            for entry in entries.entries:
                if entry.name == remote_folder:
                    folder_hash = entry.hash
                    break

        if not folder_hash:
            error_msg = f"Folder not found: {remote_folder}"
            if verbose:
                print(f"  ! {error_msg}")
            return RemoteFileVerificationResult(
                all_verified=False,
                verified_count=0,
                total_count=0,
                expected_count=expected_count,
                errors=[error_msg],
            )

        # Get files in the folder using folder hash
        result = client.get_file_entries(
            folder_id=folder_hash,
            per_page=100,
            workspace_id=workspace_id,
        )

        if not result or not result.get("data"):
            error_msg = "No files found in folder"
            if verbose:
                print(f"  ! {error_msg}")
            return RemoteFileVerificationResult(
                all_verified=False,
                verified_count=0,
                total_count=0,
                expected_count=expected_count,
                errors=[error_msg],
            )

        entries = FileEntriesResult.from_api_response(result)
        # Filter to only include files (not folders)
        file_entries = [e for e in entries.entries if e.type != "folder"]
        total_count = len(file_entries)
        verified_count = 0
        unverified_files = []

        for entry in file_entries:
            has_users = len(entry.users) > 0
            has_size = entry.file_size > 0

            if has_users and has_size:
                verified_count += 1
                if verbose:
                    print(
                        f"  + {entry.name}: VERIFIED "
                        f"(size={entry.file_size}, users={len(entry.users)})"
                    )
            else:
                unverified_files.append(entry.name)
                if verbose:
                    print(
                        f"  ! {entry.name}: UNVERIFIED "
                        f"(size={entry.file_size}, users={len(entry.users)})"
                    )

        # Determine if verification passed
        errors = []
        all_verified = verified_count == total_count

        if expected_count is not None:
            if total_count < expected_count:
                all_verified = False
                errors.append(
                    f"Expected {expected_count} files, but only found {total_count}"
                )

        if verbose:
            print(f"\n[VERIFY] Result: {verified_count}/{total_count} files verified")
            for error in errors:
                print(f"  ! WARNING: {error}")

        return RemoteFileVerificationResult(
            all_verified=all_verified,
            verified_count=verified_count,
            total_count=total_count,
            expected_count=expected_count,
            unverified_files=unverified_files,
            errors=errors,
        )

    except Exception as e:
        error_msg = f"Error verifying files: {e}"
        if verbose:
            print(f"  ! {error_msg}")
        return RemoteFileVerificationResult(
            all_verified=False,
            verified_count=0,
            total_count=0,
            expected_count=expected_count,
            errors=[error_msg],
        )


# =============================================================================
# Glob pattern matching utilities
# =============================================================================


def is_glob_pattern(pattern: str) -> bool:
    """Check if a string contains glob pattern characters.

    Args:
        pattern: String to check

    Returns:
        True if the pattern contains glob characters (*, ?, [)

    Examples:
        >>> is_glob_pattern("file.txt")
        False
        >>> is_glob_pattern("*.txt")
        True
        >>> is_glob_pattern("file?.txt")
        True
        >>> is_glob_pattern("file[0-9].txt")
        True
        >>> is_glob_pattern("bench*")
        True
    """
    glob_chars = {"*", "?", "["}
    return any(c in pattern for c in glob_chars)


def glob_match(pattern: str, name: str) -> bool:
    """Match a name against a glob pattern.

    Supports shell-style wildcards:
    - * matches any sequence of characters
    - ? matches any single character
    - [seq] matches any character in seq
    - [!seq] matches any character not in seq

    Args:
        pattern: Glob pattern to match against
        name: Name to match

    Returns:
        True if the name matches the pattern

    Examples:
        >>> glob_match("*.txt", "file.txt")
        True
        >>> glob_match("*.txt", "file.py")
        False
        >>> glob_match("bench*", "benchmark.py")
        True
        >>> glob_match("file?.txt", "file1.txt")
        True
        >>> glob_match("file?.txt", "file12.txt")
        False
        >>> glob_match("[abc]*.py", "api.py")
        True
    """
    # Use fnmatchcase for case-sensitive matching on all platforms
    return fnmatch.fnmatchcase(name, pattern)


def glob_match_entries(
    pattern: str,
    entries: list[FileEntry],
) -> list[FileEntry]:
    """Match file entries against a glob pattern.

    Args:
        pattern: Glob pattern to match against
        entries: List of FileEntry objects to filter

    Returns:
        List of FileEntry objects whose names match the pattern

    Examples:
        >>> # Filter entries to only include .txt files
        >>> txt_files = glob_match_entries("*.txt", entries)
        >>> # Get all entries starting with "test"
        >>> test_entries = glob_match_entries("test*", entries)
    """
    return [e for e in entries if glob_match(pattern, e.name)]


def glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Convert a glob pattern to a compiled regex pattern.

    This is useful when you need to match against many strings
    and want the performance benefit of a compiled regex.

    Args:
        pattern: Glob pattern to convert

    Returns:
        Compiled regex pattern

    Examples:
        >>> regex = glob_to_regex("*.txt")
        >>> bool(regex.match("file.txt"))
        True
        >>> bool(regex.match("file.py"))
        False
    """
    return re.compile(fnmatch.translate(pattern))
