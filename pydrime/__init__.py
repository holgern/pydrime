"""Drime Cloud Uploader - CLI tool for uploading files to Drime Cloud."""

from .api import DrimeClient
from .exceptions import (
    DrimeAPIError,
    DrimeAuthenticationError,
    DrimeConfigError,
    DrimeDownloadError,
    DrimeFileNotFoundError,
    DrimeInvalidResponseError,
    DrimeNetworkError,
    DrimeNotFoundError,
    DrimePermissionError,
    DrimeRateLimitError,
    DrimeUploadError,
)
from .file_entries_manager import FileEntriesManager
from .utils import (
    RemoteFileVerificationResult,
    calculate_drime_hash,
    decode_drime_hash,
    format_size,
    parse_iso_timestamp,
    verify_remote_files_have_users,
)

__all__ = [
    "DrimeClient",
    "DrimeAPIError",
    "DrimeAuthenticationError",
    "DrimeConfigError",
    "DrimeDownloadError",
    "DrimeFileNotFoundError",
    "DrimeInvalidResponseError",
    "DrimeNetworkError",
    "DrimeNotFoundError",
    "DrimePermissionError",
    "DrimeRateLimitError",
    "DrimeUploadError",
    "FileEntriesManager",
    "RemoteFileVerificationResult",
    "calculate_drime_hash",
    "decode_drime_hash",
    "format_size",
    "parse_iso_timestamp",
    "verify_remote_files_have_users",
]
