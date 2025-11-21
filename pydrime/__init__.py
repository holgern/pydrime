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
from .utils import calculate_drime_hash, decode_drime_hash

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
    "calculate_drime_hash",
    "decode_drime_hash",
]
