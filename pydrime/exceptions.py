"""Exceptions for Drime Cloud API client.

Exception Hierarchy
-------------------
DrimeAPIError (base exception)
├── DrimeConfigError - Configuration/setup issues
├── DrimeAuthenticationError - Authentication failures (401)
├── DrimePermissionError - Permission/authorization errors (403)
├── DrimeNotFoundError - Resource not found (404)
├── DrimeRateLimitError - Rate limit exceeded (429)
├── DrimeNetworkError - Network-related errors
├── DrimeUploadError - File upload errors
├── DrimeDownloadError - File download errors
├── DrimeInvalidResponseError - Invalid/unexpected server responses
└── DrimeFileNotFoundError - Local file not found

Usage
-----
All exceptions inherit from DrimeAPIError, so you can catch all API-related
errors with a single except clause:

    try:
        client.upload_file(path)
    except DrimeAPIError as e:
        print(f"API error: {e}")

Or catch specific exceptions for fine-grained error handling:

    try:
        client.upload_file(path)
    except DrimeAuthenticationError:
        print("Invalid API key")
    except DrimeUploadError as e:
        print(f"Upload failed: {e}")
    except DrimeAPIError as e:
        print(f"Other API error: {e}")
"""


class DrimeAPIError(Exception):
    """Base exception for all Drime API errors.

    All other Drime exceptions inherit from this class, allowing
    users to catch all API-related errors with a single handler.
    """

    pass


class DrimeConfigError(DrimeAPIError):
    """Exception raised for configuration-related errors.

    Raised when:
    - API key is not configured
    - Configuration file is invalid
    """

    pass


class DrimeAuthenticationError(DrimeAPIError):
    """Exception raised for authentication failures.

    Raised when:
    - Invalid API key (HTTP 401)
    - Server returns HTML instead of JSON (indicates auth failure)
    """

    pass


class DrimePermissionError(DrimeAPIError):
    """Exception raised for permission/authorization errors.

    Raised when:
    - User lacks permissions for requested operation (HTTP 403)
    """

    pass


class DrimeNotFoundError(DrimeAPIError):
    """Exception raised when a resource is not found.

    Raised when:
    - Requested file/folder doesn't exist (HTTP 404)
    """

    pass


class DrimeRateLimitError(DrimeAPIError):
    """Exception raised when rate limit is exceeded.

    Raised when:
    - Too many requests in a short time period (HTTP 429)
    """

    pass


class DrimeNetworkError(DrimeAPIError):
    """Exception raised for network-related errors.

    Raised when:
    - Connection failures
    - Timeouts
    - DNS resolution errors
    - SSL/TLS errors (certificate issues, connection reset, EOF errors)

    For SSL errors, the error message will include diagnostic hints.
    Common SSL issues include:
    - Server closing connection unexpectedly (UNEXPECTED_EOF_WHILE_READING)
    - Certificate verification failures
    - TLS version mismatches
    """

    pass


class DrimeSSLError(DrimeNetworkError):
    """Exception raised specifically for SSL/TLS errors.

    This is a subclass of DrimeNetworkError for more specific error handling.

    Raised when:
    - SSL handshake fails
    - Certificate verification fails
    - Connection reset during SSL communication
    - Unexpected EOF during SSL read (server dropped connection)

    Common causes:
    - Unstable network connection
    - Server-side rate limiting or load balancing
    - VPN or firewall interference
    - Proxy stripping SSL connections
    """

    pass


class DrimeUploadError(DrimeAPIError):
    """Exception raised for file upload errors.

    Raised when:
    - Multipart upload initialization fails
    - Upload chunk fails
    - Upload completion fails
    """

    pass


class DrimeDownloadError(DrimeAPIError):
    """Exception raised for file download errors.

    Raised when:
    - Download request fails
    - File write fails during download
    """

    pass


class DrimeInvalidResponseError(DrimeAPIError):
    """Exception raised when server returns invalid/unexpected response.

    Raised when:
    - Response is not valid JSON
    - Response has unexpected content type
    - Response structure doesn't match expected format
    """

    pass


class DrimeFileNotFoundError(DrimeAPIError):
    """Exception raised when a local file is not found.

    Raised when:
    - Attempting to upload a file that doesn't exist locally

    Attributes:
        file_path: Path to the file that was not found
    """

    def __init__(self, file_path: str):
        """Initialize exception with file path.

        Args:
            file_path: Path to the file that was not found
        """
        super().__init__(f"File not found: {file_path}")
        self.file_path = file_path
