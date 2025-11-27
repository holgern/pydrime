"""API client for Drime Cloud."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal

import httpx

from .config import config
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

if TYPE_CHECKING:
    from .models import FileEntry

FileEntryType = Literal["folder", "image", "text", "audio", "video", "pdf"]
Permission = Literal["view", "edit", "download"]


class DrimeClient:
    """Client for interacting with Drime Cloud API."""

    def __init__(
        self,
        api_key: str | None = None,
        api_url: str | None = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0,
    ):
        """Initialize Drime API client.

        Args:
            api_key: Optional API key (uses config if not provided)
            api_url: Optional API URL (uses config if not provided)
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Initial delay between retries in seconds (default: 1.0)
            timeout: Request timeout in seconds (default: 30.0)
        """
        self.api_key = api_key or config.api_key
        self.api_url = api_url or config.api_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout

        if not self.api_key:
            raise DrimeConfigError(
                "API key not configured. Please set DRIME_API_KEY environment variable."
            )

        self._client: httpx.Client | None = None

    def _get_client(self) -> httpx.Client:
        """Get or create the httpx client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=httpx.Timeout(self.timeout),
                follow_redirects=True,
            )
        return self._client

    def close(self) -> None:
        """Close the client and release connections.

        This ensures all pending requests are completed and connections are
        properly closed. Call this after batch uploads to ensure files are
        fully available for download.
        """
        if self._client is not None and not self._client.is_closed:
            self._client.close()
            self._client = None

    def _should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if a request should be retried.

        Args:
            exception: The exception that occurred
            attempt: Current attempt number (0-based)

        Returns:
            True if the request should be retried, False otherwise
        """
        # Don't retry if we've exhausted our attempts
        if attempt >= self.max_retries:
            return False

        # Retry on network errors (transient failures)
        if isinstance(exception, DrimeNetworkError):
            return True

        # Retry on rate limit errors
        if isinstance(exception, DrimeRateLimitError):
            return True

        # Retry on server errors (5xx status codes)
        if isinstance(exception, httpx.HTTPStatusError):
            status_code = exception.response.status_code
            # Retry on server errors (500-599)
            if 500 <= status_code < 600:
                return True

        # Don't retry on client errors (authentication, permission, etc.)
        return False

    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate delay before next retry using exponential backoff.

        Args:
            attempt: Current attempt number (0-based)

        Returns:
            Delay in seconds
        """
        # Exponential backoff: retry_delay * (2 ** attempt)
        # With jitter to avoid thundering herd
        import random

        base_delay = self.retry_delay * (2**attempt)
        # Add jitter: +/- 25% of base delay
        jitter = base_delay * 0.25 * (2 * random.random() - 1)
        return base_delay + jitter

    def _handle_http_error(
        self, e: httpx.HTTPStatusError, attempt: int
    ) -> tuple[Exception, bool]:
        """Handle HTTP errors and determine if retry should occur.

        Args:
            e: The HTTP error exception
            attempt: Current attempt number

        Returns:
            Tuple of (exception to raise, should_retry)
        """
        status_code = e.response.status_code

        # Handle common HTTP status codes with user-friendly messages
        if status_code == 401:
            raise DrimeAuthenticationError(
                "Invalid API key or unauthorized access"
            ) from e
        elif status_code == 403:
            raise DrimePermissionError(
                "Access forbidden - check your permissions"
            ) from e
        elif status_code == 404:
            raise DrimeNotFoundError("Resource not found") from e
        elif status_code == 429:
            error = DrimeRateLimitError("Rate limit exceeded - please try again later")
            # Always retry rate limits if we haven't exhausted retries
            should_retry = attempt < self.max_retries
            return (error, should_retry)
        else:
            error_msg = f"API request failed with status {status_code}"

            # Try to extract more details from response body
            try:
                if e.response.content:
                    error_data = e.response.json()
                    if isinstance(error_data, dict):
                        # Try to extract error message from common fields
                        msg = (
                            error_data.get("message")
                            or error_data.get("error")
                            or error_data.get("detail")
                        )
                        if msg:
                            error_msg = f"{error_msg}: {msg}"
            except Exception:
                # If we can't parse the error response, use the
                # status-based message
                pass

            error = DrimeAPIError(error_msg)
            # Retry on 5xx server errors
            should_retry = 500 <= status_code < 600 and attempt < self.max_retries
            return (error, should_retry)

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Any:
        """Make an API request with retry logic.

        Args:
            method: HTTP method
            endpoint: API endpoint path
            **kwargs: Additional arguments passed to httpx

        Returns:
            Response JSON data

        Raises:
            DrimeAPIError: If the request fails after all retries
        """
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        last_exception: Exception | None = None
        client = self._get_client()

        for attempt in range(self.max_retries + 1):
            try:
                response = client.request(method, url, **kwargs)
                response.raise_for_status()

                # Check if response is JSON
                content_type = response.headers.get("Content-Type", "")
                if response.content and "application/json" not in content_type:
                    # Server returned non-JSON response (likely HTML error page)
                    # This often means authentication failed
                    if "text/html" in content_type:
                        raise DrimeAuthenticationError(
                            "Invalid API key - server returned HTML instead of JSON"
                        )
                    raise DrimeInvalidResponseError(
                        f"Unexpected response type: {content_type}"
                    )

                # Try to parse JSON response
                if response.content:
                    try:
                        return response.json()
                    except ValueError as e:
                        # Response is not valid JSON
                        raise DrimeInvalidResponseError(
                            "Invalid JSON response from server - "
                            "check your API key and network connection"
                        ) from e
                return {}

            except httpx.HTTPStatusError as e:
                error, should_retry = self._handle_http_error(e, attempt)
                last_exception = error

                if should_retry:
                    # Special handling for rate limits: use Retry-After header
                    if isinstance(error, DrimeRateLimitError):
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after and retry_after.isdigit():
                            delay = float(retry_after)
                        else:
                            delay = self._calculate_retry_delay(attempt)
                    else:
                        delay = self._calculate_retry_delay(attempt)
                    time.sleep(delay)
                    continue
                raise error from e
            except DrimeAPIError:
                # Re-raise our own exceptions
                # (don't retry authentication/permission errors)
                raise
            except httpx.RequestError as e:
                error = DrimeNetworkError(f"Network error: {e}")
                last_exception = error
                if self._should_retry(error, attempt):
                    delay = self._calculate_retry_delay(attempt)
                    time.sleep(delay)
                    continue
                raise error from e

        # If we get here, we've exhausted all retries
        if last_exception:
            raise last_exception
        raise DrimeAPIError("Request failed after all retry attempts")

    # =========================
    # Upload Operations
    # =========================

    def _detect_mime_type(self, file_path: Path) -> str:
        """Detect MIME type of a file.

        Args:
            file_path: Path to the file

        Returns:
            MIME type string (defaults to 'application/octet-stream' if detection fails)
        """
        import mimetypes

        mime_type = None

        # Try python-magic first for more accurate detection
        try:
            import magic  # type: ignore

            try:
                mime_type = magic.from_file(str(file_path), mime=True)
            except Exception:
                mime_type = None
        except ImportError:
            # python-magic not installed, skip
            pass

        # Fall back to mimetypes module if magic didn't work
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(str(file_path))

        # Default to octet-stream if still no MIME type
        if not mime_type:
            mime_type = "application/octet-stream"

        return mime_type

    def validate_uploads(
        self,
        files: list[dict[str, Any]],
        workspace_id: int = 0,
    ) -> dict[str, Any]:
        """Validate files before upload to check for duplicates.

        Args:
            files: List of file dictionaries with keys:
                   - name: File name
                   - size: File size in bytes
                   - relativePath: Relative path (optional)
            workspace_id: ID of the workspace

        Returns:
            Validation response containing:
            - duplicates: List of duplicate file/folder names

        Raises:
            DrimeAPIError: If validation request fails

        Example:
            >>> files = [
            ...     {
            ...         "name": "example.txt",
            ...         "size": 1024,
            ...         "relativePath": "docs/",
            ...     }
            ... ]
            >>> result = client.validate_uploads(files, workspace_id=0)
            >>> duplicates = result.get("duplicates", [])
        """
        payload = {
            "files": files,
            "workspaceId": workspace_id,
        }
        result: dict[str, Any] = self._request(
            "POST", "/uploads/validate", json=payload
        )
        return result

    def get_available_name(
        self,
        name: str,
        workspace_id: int = 0,
    ) -> str:
        """Get an available name for a file/folder when there's a duplicate.

        Args:
            name: Original name that conflicts
            workspace_id: ID of the workspace

        Returns:
            Available name (e.g., "file (1).txt" if "file.txt" exists)

        Raises:
            DrimeAPIError: If request fails

        Example:
            >>> new_name = client.get_available_name("document.pdf", workspace_id=0)
            >>> print(new_name)  # "document (1).pdf"
        """
        payload = {
            "name": name,
            "workspaceId": workspace_id,
        }
        response: dict[str, Any] = self._request(
            "POST", "/entry/getAvailableName", json=payload
        )
        available_name = response.get("available")
        if not available_name or not isinstance(available_name, str):
            raise DrimeAPIError(f"Could not get available name for '{name}'")
        # After the isinstance check, available_name is confirmed to be str
        result: str = available_name
        return result

    def upload_file_multipart(
        self,
        file_path: Path,
        relative_path: str | None = None,
        workspace_id: int = 0,
        chunk_size: int = 25 * 1024 * 1024,  # 25MB chunks
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> Any:
        """Upload a large file using multipart upload.

        Args:
            file_path: Local path to the file
            relative_path: Relative path where file should be stored
            workspace_id: ID of the workspace
            chunk_size: Size of each chunk in bytes (default: 25MB)
            progress_callback: Optional callback function(bytes_uploaded, total_bytes)

        Returns:
            Upload response data

        Raises:
            DrimeAPIError: If upload fails
        """
        import math

        if not file_path.exists():
            raise DrimeFileNotFoundError(str(file_path))

        file_size = file_path.stat().st_size
        file_name = file_path.name
        extension = file_path.suffix.lstrip(".")
        num_parts = math.ceil(file_size / chunk_size)

        # Detect MIME type
        mime_type = self._detect_mime_type(file_path)

        # Initialize multipart upload
        init_data = {
            "filename": file_name,
            "mime": mime_type,
            "size": file_size,
            "extension": extension,
            "relativePath": relative_path or "",
            "workspaceId": workspace_id,
        }

        init_response = self._request("POST", "/s3/multipart/create", json=init_data)
        upload_id = init_response.get("uploadId")
        key = init_response.get("key")

        if not upload_id or not key:
            raise DrimeUploadError("Failed to initialize multipart upload")

        uploaded_parts = []
        bytes_uploaded = 0

        try:
            with open(file_path, "rb") as f:
                part_number = 1

                while part_number <= num_parts:
                    # Request signed URLs for batch of parts
                    batch_size = min(10, num_parts - part_number + 1)
                    part_numbers = list(range(part_number, part_number + batch_size))

                    sign_response = self._request(
                        "POST",
                        "/s3/multipart/batch-sign-part-urls",
                        json={
                            "key": key,
                            "uploadId": upload_id,
                            "partNumbers": part_numbers,
                        },
                    )

                    urls_list = sign_response.get("urls", [])
                    signed_urls = {u["partNumber"]: u["url"] for u in urls_list}

                    # Upload each part
                    for pn in part_numbers:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break

                        signed_url = signed_urls.get(pn)
                        if not signed_url:
                            raise DrimeUploadError(f"No signed URL for part {pn}")

                        # Upload chunk to S3
                        headers = {
                            "Content-Type": "application/octet-stream",
                            "Content-Length": str(len(chunk)),
                        }

                        response = httpx.put(
                            signed_url, content=chunk, headers=headers, timeout=60
                        )
                        response.raise_for_status()

                        etag = response.headers.get("ETag", "").strip('"')
                        uploaded_parts.append(
                            {
                                "PartNumber": pn,
                                "ETag": etag,
                            }
                        )

                        bytes_uploaded += len(chunk)
                        if progress_callback:
                            progress_callback(bytes_uploaded, file_size)

                    part_number += batch_size

            # Complete multipart upload
            self._request(
                "POST",
                "/s3/multipart/complete",
                json={
                    "key": key,
                    "uploadId": upload_id,
                    "parts": uploaded_parts,
                },
            )

            # Create file entry
            entry_response = self._request(
                "POST",
                "/s3/entries",
                json={
                    "clientMime": mime_type,
                    "clientName": file_name,
                    "filename": key.split("/")[-1],
                    "size": file_size,
                    "clientExtension": extension,
                    "relativePath": relative_path or "",
                    "workspaceId": workspace_id,
                },
            )

            return entry_response

        except Exception as e:
            # Abort upload on error
            try:
                self._request(
                    "POST",
                    "/s3/multipart/abort",
                    json={"key": key, "uploadId": upload_id},
                )
            except Exception:  # noqa: S110
                pass
            raise DrimeUploadError(f"Multipart upload failed: {e}") from e

    def upload_file_presign(
        self,
        file_path: Path,
        relative_path: str | None = None,
        workspace_id: int = 0,
        parent_id: int | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> Any:
        """Upload a file using presigned S3 URL.

        This method uses a 3-step process:
        1. Get a presigned URL from the API
        2. Upload the file directly to S3 using the presigned URL
        3. Create the file entry in the Drime database

        This approach can be faster for some files and avoids server-side
        processing overhead.

        Args:
            file_path: Local path to the file
            relative_path: Relative path where file should be stored
            workspace_id: ID of the workspace (default: 0 for personal)
            parent_id: Optional parent folder ID
            progress_callback: Optional callback function(bytes_uploaded,
                total_bytes) for tracking upload progress

        Returns:
            Upload response data with 'status' and 'fileEntry' keys

        Raises:
            DrimeUploadError: If upload fails
            DrimeFileNotFoundError: If file doesn't exist
        """
        if not file_path.exists():
            raise DrimeFileNotFoundError(str(file_path))

        file_size = file_path.stat().st_size
        mime_type = self._detect_mime_type(file_path)
        extension = file_path.suffix.lstrip(".") if file_path.suffix else ""

        # Step 1: Get presigned URL
        presign_payload: dict[str, Any] = {
            "filename": file_path.name,
            "mime": mime_type,
            "size": file_size,
            "extension": extension,
            "relativePath": relative_path or "",
            "workspaceId": workspace_id,
            "parentId": parent_id,
        }

        presign_response = self._request(
            "POST",
            "/s3/simple/presign",
            json=presign_payload,
            params={"workspaceId": workspace_id},
        )

        presigned_url = presign_response.get("url")
        key = presign_response.get("key")

        if not presigned_url or not key:
            raise DrimeUploadError(f"Invalid presign response: {presign_response}")

        # Step 2: Upload to S3 using presigned URL with progress tracking
        try:
            s3_headers = {
                "Content-Type": mime_type,
                "Content-Length": str(file_size),
                "x-amz-acl": "private",
            }

            # Create a generator for streaming upload with progress tracking
            def file_reader() -> Any:
                bytes_uploaded = 0
                chunk_size = 64 * 1024  # 64KB chunks for progress updates
                with open(file_path, "rb") as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        bytes_uploaded += len(chunk)
                        if progress_callback:
                            progress_callback(bytes_uploaded, file_size)
                        yield chunk

            # Use httpx directly for S3 upload (not going through Drime API)
            s3_response = httpx.put(
                presigned_url,
                content=file_reader(),
                headers=s3_headers,
                timeout=60.0,
            )
            s3_response.raise_for_status()

        except httpx.HTTPStatusError as e:
            raise DrimeUploadError(f"S3 upload failed: {e}") from e
        except httpx.RequestError as e:
            raise DrimeUploadError(f"Network error during S3 upload: {e}") from e

        # Step 3: Create file entry
        entry_payload = {
            "clientMime": mime_type,
            "clientName": file_path.name,
            "filename": key.split("/")[-1],
            "size": file_size,
            "clientExtension": extension,
            "relativePath": relative_path or "",
            "workspaceId": workspace_id,
        }

        entry_response = self._request("POST", "/s3/entries", json=entry_payload)
        return entry_response

    def _verify_upload(
        self,
        entry_id: int,
        expected_size: int,
        workspace_id: int = 0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> tuple[bool, str]:
        """Verify that an uploaded file has valid size and users fields.

        Args:
            entry_id: ID of the uploaded file entry
            expected_size: Expected file size in bytes
            workspace_id: Workspace ID
            max_retries: Maximum number of retries to fetch the entry
            retry_delay: Delay between retries in seconds

        Returns:
            Tuple of (is_valid, error_message)
        """
        import time

        for attempt in range(max_retries):
            try:
                entry = self.get_file_entry(entry_id, workspace_id=workspace_id)

                # Check if we got a valid response
                if not entry:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    return False, "Failed to retrieve file entry after upload"

                # Extract file entry data (may be nested under 'fileEntry' key)
                entry_data = entry.get("fileEntry", entry)

                # Check file_size
                file_size = entry_data.get("file_size", 0)
                if file_size != expected_size:
                    return (
                        False,
                        f"File size mismatch: expected {expected_size}, "
                        f"got {file_size}",
                    )

                # Check users field - it should be a non-empty list
                users = entry_data.get("users", [])
                if not users:
                    if attempt < max_retries - 1:
                        # Users field may be populated asynchronously, retry
                        time.sleep(retry_delay)
                        continue
                    return False, "Users field is empty (incomplete upload)"

                return True, ""

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return False, f"Error verifying upload: {e}"

        return False, "Verification failed after maximum retries"

    def upload_file(
        self,
        file_path: Path,
        parent_id: int | None = None,
        relative_path: str | None = None,
        workspace_id: int = 0,
        use_multipart_threshold: int = 30 * 1024 * 1024,  # 30MB
        chunk_size: int = 25 * 1024 * 1024,  # 25MB chunks
        progress_callback: Callable[[int, int], None] | None = None,
        message_callback: Callable[[str], None] | None = None,
        verify_upload: bool = True,
        max_upload_retries: int = 3,
    ) -> Any:
        """Upload a file to Drime Cloud.

        Automatically chooses between presigned URL upload and multipart upload
        based on file size. After upload, verifies that the file size and users
        fields are correctly set.

        Args:
            file_path: Local path to the file
            parent_id: ID of folder where this file should be uploaded,
                None will upload to root
            relative_path: Folders in the path will be auto-created if they
                don't exist. Should include original filename as well:
                /some/folders/here/file-name.jpg
            workspace_id: ID of the workspace (default: 0 for personal)
            use_multipart_threshold: File size threshold for using multipart
                upload (default: 30MB)
            chunk_size: Size of each chunk for multipart uploads (default: 25MB)
            progress_callback: Optional callback function(bytes_uploaded,
                total_bytes)
            message_callback: Optional callback function(message) for user
                notifications about upload status and retries
            verify_upload: Whether to verify the upload was successful by
                checking size and users fields (default: True)
            max_upload_retries: Maximum number of upload retry attempts if
                verification fails (default: 3)

        Returns:
            Upload response data with 'status' and 'fileEntry' keys
        """
        if not file_path.exists():
            raise DrimeFileNotFoundError(str(file_path))

        file_size = file_path.stat().st_size

        def _do_upload() -> Any:
            """Perform the actual upload."""
            # Use multipart upload for large files
            if file_size > use_multipart_threshold:
                return self.upload_file_multipart(
                    file_path=file_path,
                    relative_path=relative_path,
                    workspace_id=workspace_id,
                    chunk_size=chunk_size,
                    progress_callback=progress_callback,
                )

            # Use presigned URL upload for smaller files (better progress tracking)
            return self.upload_file_presign(
                file_path=file_path,
                relative_path=relative_path,
                workspace_id=workspace_id,
                parent_id=parent_id,
                progress_callback=progress_callback,
            )

        # If verification is disabled, just upload and return
        if not verify_upload:
            return _do_upload()

        # Upload with verification and retry logic
        last_error = ""
        for attempt in range(max_upload_retries):
            result = _do_upload()

            # Extract entry ID from result
            file_entry = result.get("fileEntry", {})
            entry_id = file_entry.get("id")

            if not entry_id:
                last_error = "Upload response missing file entry ID"
                if message_callback:
                    message_callback(
                        f"Upload attempt {attempt + 1}/{max_upload_retries} failed: "
                        f"{last_error}"
                    )
                continue

            # Verify the upload
            is_valid, error_msg = self._verify_upload(
                entry_id=entry_id,
                expected_size=file_size,
                workspace_id=workspace_id,
            )

            if is_valid:
                return result

            last_error = error_msg

            # Delete the incomplete entry before retrying
            if attempt < max_upload_retries - 1:
                if message_callback:
                    message_callback(
                        f"Upload verification failed for '{file_path.name}': "
                        f"{error_msg}. Retrying ({attempt + 2}/{max_upload_retries})..."
                    )
                try:
                    self.delete_file_entries(
                        entry_ids=[entry_id],
                        delete_forever=True,
                        workspace_id=workspace_id,
                    )
                except Exception:
                    # Ignore deletion errors, just proceed with retry
                    pass

        # All retries exhausted
        raise DrimeUploadError(
            f"Upload verification failed after {max_upload_retries} attempts: "
            f"{last_error}"
        )

    # =========================
    # File Entry Operations
    # =========================

    def get_file_entries(
        self,
        per_page: int = 50,
        page: int | None = None,
        deleted_only: bool | None = None,
        starred_only: bool | None = None,
        recent_only: bool | None = None,
        shared_only: bool | None = None,
        query: str | None = None,
        entry_type: FileEntryType | None = None,
        parent_ids: list[int] | None = None,
        workspace_id: int = 0,
        folder_id: str | None = None,
        page_id: str | None = None,
        backup: int = 0,
        order_by: str | None = None,
        order_dir: str | None = None,
    ) -> Any:
        """Get the list of all file entries you have access to.

        Args:
            per_page: How many entries to return per page (default: 50)
            page: Page number to retrieve (1-based, default: None for page 1)
            deleted_only: Whether only trashed entries should be returned
            starred_only: Whether only starred entries should be returned
            recent_only: Whether only recent entries should be returned
            shared_only: Whether only entries shared with you should be returned
            query: Search query to filter entry names
            entry_type: File type to filter on (folder, image, text, audio, video, pdf)
            parent_ids: Only entries that are children of specified folders
            workspace_id: Only return entries in specified workspace (default: 0)
            folder_id: Display files in specified folder hash
            page_id: Display files in specified page hash (alias for folder_id)
            backup: Include backup files (default: 0)
            order_by: Field to order by (e.g., 'updated_at', 'name', 'file_size')
            order_dir: Order direction ('asc' or 'desc')

        Returns:
            List of file entry objects
        """
        endpoint = "/drive/file-entries"
        params: dict[str, Any] = {"perPage": per_page, "workspaceId": workspace_id}

        if page is not None:
            params["page"] = page
        if deleted_only is not None:
            params["deletedOnly"] = deleted_only
        if starred_only is not None:
            params["starredOnly"] = starred_only
        if recent_only is not None:
            params["recentOnly"] = recent_only
        if shared_only is not None:
            params["sharedOnly"] = shared_only
        if query:
            params["query"] = query
        if entry_type:
            params["type"] = entry_type
        if parent_ids:
            params["parentIds"] = ",".join(map(str, parent_ids))
        if folder_id:
            params["folderId"] = folder_id
        if page_id:
            params["pageId"] = page_id
        if backup:
            params["backup"] = backup
        if order_by:
            params["orderBy"] = order_by
        if order_dir:
            params["orderDir"] = order_dir

        return self._request("GET", endpoint, params=params)

    def get_file_entry(self, entry_id: int, workspace_id: int = 0) -> Any:
        """Get a single file entry by ID.

        Args:
            entry_id: ID of the file entry to retrieve
            workspace_id: Workspace ID (default: 0 for personal)

        Returns:
            File entry object with all fields including 'users' and 'file_size'
        """
        endpoint = f"/file-entries/{entry_id}"
        params: dict[str, Any] = {"workspaceId": workspace_id}
        return self._request("GET", endpoint, params=params)

    def update_file_entry(
        self,
        entry_id: int,
        name: str | None = None,
        description: str | None = None,
    ) -> Any:
        """Update an existing file entry.

        Args:
            entry_id: ID of the entry to update
            name: New name for the entry
            description: New description for the entry

        Returns:
            Response with 'status' and 'fileEntry' keys
        """
        endpoint = f"/file-entries/{entry_id}"
        data: dict[str, Any] = {}

        if name:
            data["name"] = name
        if description:
            data["description"] = description

        return self._request("PUT", endpoint, json=data)

    def delete_file_entries(
        self,
        entry_ids: list[int],
        delete_forever: bool = False,
        workspace_id: int = 0,
    ) -> Any:
        """Move entries to trash or delete permanently.

        Args:
            entry_ids: List of entry IDs to delete
            delete_forever: Whether entries should be deleted permanently
            workspace_id: Workspace ID (default: 0 for personal)

        Returns:
            Response with 'status' key
        """
        endpoint = f"/file-entries/delete?workspaceId={workspace_id}"
        data = {
            "entryIds": entry_ids,
            "deleteForever": delete_forever,
        }
        return self._request("POST", endpoint, json=data)

    def move_file_entries(
        self,
        entry_ids: list[int],
        destination_id: int | None = None,
    ) -> Any:
        """Move specified entries to a different folder.

        Args:
            entry_ids: List of entry IDs to move
            destination_id: ID of destination folder (None for root)

        Returns:
            Response with 'status' and 'entries' keys
        """
        endpoint = "/file-entries/move"
        data: dict[str, Any] = {"entryIds": entry_ids}

        if destination_id is not None:
            data["destinationId"] = destination_id

        return self._request("POST", endpoint, json=data)

    def duplicate_file_entries(
        self,
        entry_ids: list[int],
        destination_id: int | None = None,
    ) -> Any:
        """Duplicate specified entries.

        Args:
            entry_ids: List of entry IDs to duplicate
            destination_id: ID of destination folder (None for root)

        Returns:
            Response with 'status' and 'entries' keys
        """
        endpoint = "/file-entries/duplicate"
        data: dict[str, Any] = {"entryIds": entry_ids}

        if destination_id is not None:
            data["destinationId"] = destination_id

        return self._request("POST", endpoint, json=data)

    def restore_file_entries(self, entry_ids: list[int]) -> Any:
        """Restore file entries from trash to original folder.

        Args:
            entry_ids: List of entry IDs to restore

        Returns:
            Response with 'status' key
        """
        endpoint = "/file-entries/restore"
        data = {"entryIds": entry_ids}
        return self._request("POST", endpoint, json=data)

    # =========================
    # Folder Operations
    # =========================

    def get_user_folders(
        self,
        user_id: int,
        workspace_id: int = 0,
    ) -> Any:
        """Get list of folders for a user in a workspace.

        Args:
            user_id: ID of the user
            workspace_id: ID of the workspace (default: 0 for personal)

        Returns:
            Response with 'folders' key containing list of folder objects
        """
        endpoint = f"/users/{user_id}/folders"
        params = {
            "userId": user_id,
            "workspaceId": workspace_id,
        }
        return self._request("GET", endpoint, params=params)

    def get_folder_count(self, folder_id: int) -> int:
        """Get the count of items in a folder.

        Args:
            folder_id: ID of the folder

        Returns:
            Number of items in the folder

        Raises:
            DrimeAPIError: If the request fails

        Example:
            >>> client = DrimeClient(api_key="your_key")
            >>> count = client.get_folder_count(481967773)
            >>> print(count)
            16
        """
        endpoint = f"/folders/{folder_id}/count"
        result = self._request("GET", endpoint)
        return result.get("count", 0)

    def get_folder_path(
        self,
        folder_hash: str,
        vault_id: int | None = None,
    ) -> Any:
        """Get the path hierarchy of a folder.

        Retrieves the folder path from root to the specified folder.
        Works for both regular folders and encrypted vault folders.

        Args:
            folder_hash: Hash of the folder (e.g., "NDgxMDAzNjAzfA")
            vault_id: Optional vault ID for encrypted folders

        Returns:
            Response with 'path' key containing a list of folder objects
            in the path hierarchy (from root to target). Each folder contains:
            - id: Folder ID
            - name: Folder name
            - hash: Folder hash
            - type: Entry type ("folder")
            - file_size: Size of folder contents
            - parent_id: Parent folder ID or null
            - workspace_id: Workspace ID
            - is_encrypted: Whether the folder is encrypted (0 or 1)
            - vault_id: Associated vault ID (if encrypted)
            - owner_id: Owner's user ID
            - permissions: Permission flags (update, create, download, delete)
            - users: List of users with access
            - created_at: Creation timestamp
            - updated_at: Update timestamp

        Raises:
            DrimeAPIError: If the request fails

        Example:
            >>> client = DrimeClient(api_key="your_key")
            >>> # Regular folder
            >>> result = client.get_folder_path("NDgxMDAzNjAzfA")
            >>> for folder in result["path"]:
            ...     print(folder["name"])
            >>> # Vault folder
            >>> result = client.get_folder_path("MzQ0MzB8cGFkZA", vault_id=784)
        """
        endpoint = f"/folders/{folder_hash}/path"
        params: dict[str, Any] = {}
        if vault_id is not None:
            params["vaultId"] = vault_id
        return self._request("GET", endpoint, params=params if params else None)

    def create_folder(
        self,
        name: str,
        parent_id: int | None = None,
    ) -> Any:
        """Create a new folder.

        Args:
            name: Name of the new folder
            parent_id: ID of parent folder (None for root)

        Returns:
            Response with 'status' and 'folder' keys
        """
        endpoint = "/folders"
        data: dict[str, Any] = {"name": name}

        if parent_id is not None:
            data["parentId"] = parent_id

        return self._request("POST", endpoint, json=data)

    # =========================
    # Sharing Operations
    # =========================

    def share_entry(
        self,
        entry_id: int,
        emails: list[str],
        permissions: list[Permission],
    ) -> Any:
        """Share file or folder with specified users.

        Args:
            entry_id: ID of the entry to share
            emails: List of email addresses to share with
            permissions: List of permissions (view, edit, download)

        Returns:
            Response with 'status' and 'users' keys
        """
        endpoint = f"/file-entries/{entry_id}/share"
        data = {
            "emails": emails,
            "permissions": permissions,
        }
        return self._request("POST", endpoint, json=data)

    def change_permissions(
        self,
        entry_id: int,
        user_id: int,
        permissions: list[Permission],
    ) -> Any:
        """Change permissions user has for shared entries.

        Args:
            entry_id: ID of the entry
            user_id: ID of the user
            permissions: List of permissions (view, edit, download)

        Returns:
            Response with 'status' and 'users' keys
        """
        endpoint = f"/file-entries/{entry_id}/change-permissions"
        data = {
            "userId": user_id,
            "permissions": permissions,
        }
        return self._request("PUT", endpoint, json=data)

    def unshare_entry(self, entry_id: int, user_id: int) -> Any:
        """Unshare file or folder with specified user.

        Args:
            entry_id: ID of the entry
            user_id: ID of the user

        Returns:
            Response with 'status' and 'users' keys
        """
        endpoint = f"/file-entries/{entry_id}/unshare"
        data = {"userId": user_id}
        return self._request("DELETE", endpoint, json=data)

    # =========================
    # Starring Operations
    # =========================

    def star_entries(self, entry_ids: list[int]) -> Any:
        """Mark specified entries as starred.

        Args:
            entry_ids: List of entry IDs to star

        Returns:
            Response with 'status' and 'tag' keys
        """
        endpoint = "/file-entries/star"
        data = {"entryIds": entry_ids}
        return self._request("POST", endpoint, json=data)

    def unstar_entries(self, entry_ids: list[int]) -> Any:
        """Unmark specified entries as starred.

        Args:
            entry_ids: List of entry IDs to unstar

        Returns:
            Response with 'status' and 'tag' keys
        """
        endpoint = "/file-entries/unstar"
        data = {"entryIds": entry_ids}
        return self._request("POST", endpoint, json=data)

    # =========================
    # Shareable Link Operations
    # =========================

    def get_shareable_link(self, entry_id: int) -> Any:
        """Retrieve shareable link for specified entry.

        Args:
            entry_id: ID of the entry

        Returns:
            Response with 'status', 'link', and optionally 'folderChildren' keys
        """
        endpoint = f"/file-entries/{entry_id}/shareable-link"
        return self._request("GET", endpoint)

    def create_shareable_link(
        self,
        entry_id: int,
        password: str | None = None,
        expires_at: str | None = None,
        allow_edit: bool = False,
        allow_download: bool = False,
    ) -> Any:
        """Create new shareable link for specified entry.

        Args:
            entry_id: ID of the entry
            password: Optional password for the link
            expires_at: Optional expiration date (format: 2021-03-06T17:34:00.000000Z)
            allow_edit: Whether editing is allowed
            allow_download: Whether downloading is allowed

        Returns:
            Response with 'status' and 'link' keys
        """
        endpoint = f"/file-entries/{entry_id}/shareable-link"
        data: dict[str, Any] = {
            "allow_edit": allow_edit,
            "allow_download": allow_download,
        }

        if password:
            data["password"] = password
        if expires_at:
            data["expires_at"] = expires_at

        return self._request("POST", endpoint, json=data)

    def update_shareable_link(
        self,
        entry_id: int,
        password: str | None = None,
        expires_at: str | None = None,
        allow_edit: bool | None = None,
        allow_download: bool | None = None,
    ) -> Any:
        """Update shareable link details.

        Args:
            entry_id: ID of the entry
            password: Optional password for the link
            expires_at: Optional expiration date (format: 2021-03-06T17:34:00.000000Z)
            allow_edit: Whether editing is allowed
            allow_download: Whether downloading is allowed

        Returns:
            Response with 'status' and 'link' keys
        """
        endpoint = f"/file_entries/{entry_id}/shareable-link"
        data: dict[str, Any] = {}

        if password is not None:
            data["password"] = password
        if expires_at is not None:
            data["expires_at"] = expires_at
        if allow_edit is not None:
            data["allow_edit"] = allow_edit
        if allow_download is not None:
            data["allow_download"] = allow_download

        return self._request("PUT", endpoint, json=data)

    def delete_shareable_link(self, entry_id: int) -> Any:
        """Delete shareable link.

        Args:
            entry_id: ID of the entry

        Returns:
            Response with 'status' key
        """
        endpoint = f"/file_entries/{entry_id}/shareable-link"
        return self._request("DELETE", endpoint)

    # =========================
    # Authentication Operations
    # =========================

    def login(
        self,
        email: str,
        password: str,
        device_name: str = "drime-uploader",
    ) -> Any:
        """Get access token by logging in.

        Args:
            email: User email
            password: User password
            device_name: Name of the device/application

        Returns:
            Response with 'status' and 'user' keys (user contains access_token)
        """
        endpoint = "/auth/login"
        data = {
            "email": email,
            "password": password,
            "device_name": device_name,
        }
        # Don't use authorization header for login
        return self._request("POST", endpoint, json=data)

    def register(
        self,
        email: str,
        password: str,
        device_name: str = "drime-uploader",
    ) -> Any:
        """Register for a new account.

        Args:
            email: User email
            password: User password
            device_name: Name of the device/application

        Returns:
            Response with 'status' and 'user' keys
        """
        endpoint = "/auth/register"
        data = {
            "email": email,
            "password": password,
            "device_name": device_name,
        }
        # Don't use authorization header for registration
        return self._request("POST", endpoint, json=data)

    # =========================
    # Download Operations
    # =========================

    def download_file(
        self,
        hash_value: str,
        output_path: Path | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
        timeout: int = 60,
    ) -> Path:
        """Download a file from Drime Cloud.

        Args:
            hash_value: Hash of the file to download
            output_path: Optional path where to save the file
            progress_callback: Optional callback function(bytes_downloaded, total_bytes)
            timeout: Request timeout in seconds (default: 60)

        Returns:
            Path where the file was saved

        Raises:
            DrimeAPIError: If download fails
        """
        endpoint = f"/file-entries/download/{hash_value}"
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        client = self._get_client()

        try:
            with client.stream("GET", url, timeout=timeout) as response:
                response.raise_for_status()

                # Try to extract filename from Content-Disposition header
                filename = None
                content_disp = response.headers.get("Content-Disposition", "")
                if "filename=" in content_disp:
                    # Try filename* first (RFC 5987)
                    if "filename*=" in content_disp:
                        parts = content_disp.split("filename*=")
                        if len(parts) > 1:
                            encoded = parts[1].split(";")[0].strip()
                            if "''" in encoded:
                                filename = encoded.split("''", 1)[1]
                    # Fall back to regular filename
                    elif "filename=" in content_disp:
                        parts = content_disp.split("filename=")
                        if len(parts) > 1:
                            filename = (
                                parts[1].split(";")[0].strip().strip('"').strip("'")
                            )

                # Use provided output path or generate one
                if output_path:
                    save_path = output_path
                elif filename:
                    save_path = Path(filename)
                else:
                    save_path = Path(f"drime_{hash_value}")

                # Write file content
                total_size = int(response.headers.get("Content-Length", 0))
                bytes_downloaded = 0

                with open(save_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            bytes_downloaded += len(chunk)
                            if progress_callback:
                                progress_callback(bytes_downloaded, total_size)

                return save_path

        except httpx.HTTPStatusError as e:
            raise DrimeDownloadError(f"Download failed: {e}") from e
        except httpx.RequestError as e:
            raise DrimeNetworkError(f"Network error during download: {e}") from e
        except OSError as e:
            raise DrimeDownloadError(f"Failed to write file: {e}") from e

    def get_file_content(
        self,
        hash_value: str,
        max_bytes: int | None = None,
        timeout: int = 60,
    ) -> bytes:
        """Get file content from Drime Cloud without saving to disk.

        Args:
            hash_value: Hash of the file to download
            max_bytes: Maximum number of bytes to read (None for entire file)
            timeout: Request timeout in seconds (default: 60)

        Returns:
            File content as bytes

        Raises:
            DrimeAPIError: If download fails
        """
        endpoint = f"/file-entries/download/{hash_value}"
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        client = self._get_client()

        try:
            with client.stream("GET", url, timeout=timeout) as response:
                response.raise_for_status()

                if max_bytes is None:
                    # Read entire file
                    return response.read()
                else:
                    # Read only up to max_bytes
                    content = b""
                    for chunk in response.iter_bytes(chunk_size=8192):
                        if chunk:
                            content += chunk
                            if len(content) >= max_bytes:
                                return content[:max_bytes]
                    return content

        except httpx.HTTPStatusError as e:
            raise DrimeDownloadError(f"Download failed: {e}") from e
        except httpx.RequestError as e:
            raise DrimeNetworkError(f"Network error during download: {e}") from e

    # =========================
    # Workspace Operations
    # =========================

    def get_workspaces(self) -> Any:
        """Get list of workspaces the user has access to.

        Returns:
            Response with 'workspaces' key containing list of workspace objects
        """
        endpoint = "/me/workspaces"
        return self._request("GET", endpoint)

    def get_logged_user(self) -> Any:
        """Get information about the currently logged in user.

        Returns:
            Response with user information
        """
        endpoint = "/cli/loggedUser"
        return self._request("GET", endpoint)

    def get_space_usage(self) -> Any:
        """Get space usage information for the current user.

        Returns:
            Response with space usage details including used space by type
        """
        endpoint = "/user/space-usage"
        return self._request("GET", endpoint)

    # =========================
    # Notification Operations
    # =========================

    def get_notifications(
        self,
        per_page: int = 10,
        page: int = 1,
    ) -> Any:
        """Get user notifications.

        Args:
            per_page: Number of notifications per page (default: 10)
            page: Page number to retrieve (default: 1)

        Returns:
            Response with 'pagination' key containing:
            - current_page: Current page number
            - data: List of notification objects
            - from: Starting index
            - last_page: Last page number
            - next_page: Next page number or null
            - per_page: Items per page
            - prev_page: Previous page number or null
            - to: Ending index
            - total: Total number of notifications

            Each notification contains:
            - id: Notification UUID
            - type: Notification type
            - data: Notification content with lines and actions
            - read_at: Read timestamp or null
            - created_at: Creation timestamp
            - updated_at: Update timestamp

        Raises:
            DrimeAPIError: If the request fails

        Example:
            >>> client = DrimeClient(api_key="your_key")
            >>> result = client.get_notifications(per_page=10, page=1)
            >>> notifications = result["pagination"]["data"]
            >>> for notif in notifications:
            ...     print(notif["data"]["lines"][0]["content"])
        """
        endpoint = "/notifications"
        params: dict[str, Any] = {
            "perPage": per_page,
            "page": page,
        }
        return self._request("GET", endpoint, params=params)

    # =========================
    # Vault Operations
    # =========================

    def get_vault(self) -> Any:
        """Get user's vault information.

        The vault contains encryption-related data for secure storage.

        Returns:
            Response with 'vault' key containing:
            - id: Vault ID
            - user_id: Owner's user ID
            - salt: Encryption salt (base64 encoded)
            - check: Encryption check value (base64 encoded)
            - iv: Initialization vector
            - created_at: Creation timestamp
            - updated_at: Update timestamp

        Raises:
            DrimeAPIError: If the request fails

        Example:
            >>> client = DrimeClient(api_key="your_key")
            >>> result = client.get_vault()
            >>> vault = result["vault"]
            >>> print(vault["id"])
        """
        endpoint = "/vault"
        return self._request("GET", endpoint)

    def get_vault_file_entries(
        self,
        folder_hash: str = "",
        page: int = 1,
        per_page: int = 50,
        order_by: str = "updated_at",
        order_dir: Literal["asc", "desc"] = "desc",
        backup: int = 0,
    ) -> Any:
        """Get file entries from the vault.

        Retrieves encrypted file entries stored in the user's vault.

        Args:
            folder_hash: Hash of the vault folder (empty string for root)
            page: Page number (default: 1)
            per_page: Number of entries per page (default: 50)
            order_by: Field to order by (default: "updated_at")
            order_dir: Order direction, "asc" or "desc" (default: "desc")
            backup: Include backup files (default: 0)

        Returns:
            Response with file entries data similar to get_file_entries,
            but for encrypted vault files. Contains:
            - pagination.data: List of encrypted file entry objects
            - Each entry includes is_encrypted=1 and vault_id

        Raises:
            DrimeAPIError: If the request fails

        Example:
            >>> client = DrimeClient(api_key="your_key")
            >>> # List vault root
            >>> result = client.get_vault_file_entries()
            >>> # List specific folder by hash
            >>> result = client.get_vault_file_entries(folder_hash="MzQ0MzB8cGFkZA")
            >>> for entry in result.get("pagination", {}).get("data", []):
            ...     print(entry["name"])
        """
        endpoint = "/vault/file-entries"
        params: dict[str, Any] = {
            "page": page,
            "perPage": per_page,
            "orderBy": order_by,
            "orderDir": order_dir,
            "backup": backup,
        }
        # Only include folder params if a folder hash is specified
        if folder_hash:
            params["folderId"] = folder_hash
            params["pageId"] = folder_hash
        return self._request("GET", endpoint, params=params)

    def download_vault_file(
        self,
        hash_value: str,
        output_path: Path | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
        timeout: int = 60,
    ) -> Path:
        """Download an encrypted file from the vault.

        Downloads encrypted files from the vault. The difference from regular
        download is that vault files use workspaceId=null and encrypted=true.

        Args:
            hash_value: Hash of the vault file to download
            output_path: Optional path where to save the file
            progress_callback: Optional callback function(bytes_downloaded, total_bytes)
            timeout: Request timeout in seconds (default: 60)

        Returns:
            Path where the file was saved

        Raises:
            DrimeAPIError: If download fails

        Example:
            >>> client = DrimeClient(api_key="your_key")
            >>> # Download vault file by hash
            >>> path = client.download_vault_file("MzQ0MzF8cGFkZA")
            >>> print(f"Downloaded to: {path}")
        """
        # Vault files use workspaceId=null and encrypted=true
        endpoint = (
            f"/file-entries/download/{hash_value}?workspaceId=null&encrypted=true"
        )
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        client = self._get_client()

        try:
            with client.stream("GET", url, timeout=timeout) as response:
                response.raise_for_status()

                # Try to extract filename from Content-Disposition header
                filename = None
                content_disp = response.headers.get("Content-Disposition", "")
                if "filename=" in content_disp:
                    # Try filename* first (RFC 5987)
                    if "filename*=" in content_disp:
                        parts = content_disp.split("filename*=")
                        if len(parts) > 1:
                            encoded = parts[1].split(";")[0].strip()
                            if "''" in encoded:
                                filename = encoded.split("''", 1)[1]
                    # Fall back to regular filename
                    elif "filename=" in content_disp:
                        parts = content_disp.split("filename=")
                        if len(parts) > 1:
                            filename = (
                                parts[1].split(";")[0].strip().strip('"').strip("'")
                            )

                # Use provided output path or generate one
                if output_path:
                    save_path = output_path
                elif filename:
                    save_path = Path(filename)
                else:
                    save_path = Path(f"vault_{hash_value}")

                # Write file content
                total_size = int(response.headers.get("Content-Length", 0))
                bytes_downloaded = 0

                with open(save_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            bytes_downloaded += len(chunk)
                            if progress_callback:
                                progress_callback(bytes_downloaded, total_size)

                return save_path

        except httpx.HTTPStatusError as e:
            raise DrimeDownloadError(f"Vault download failed: {e}") from e
        except httpx.RequestError as e:
            raise DrimeNetworkError(f"Network error during vault download: {e}") from e
        except OSError as e:
            raise DrimeDownloadError(f"Failed to write vault file: {e}") from e

    def upload_vault_file(
        self,
        file_path: Path,
        encrypted_content: bytes,
        encrypted_name: str,
        name_iv: str,
        content_iv: str,
        vault_id: int,
        parent_id: int | None = None,
    ) -> Any:
        """Upload an encrypted file to the vault using presigned URL.

        The file content and filename must be encrypted client-side before calling
        this method. Uses presigned S3 URL upload to properly include the IV values
        in the file entry.

        Args:
            file_path: Original local file path (for size/extension info and
                original name)
            encrypted_content: Encrypted file content bytes
            encrypted_name: Base64 encoded encrypted filename
            name_iv: Base64 encoded IV used for filename encryption
            content_iv: Base64 encoded IV used for content encryption
            vault_id: ID of the vault
            parent_id: Optional parent folder ID in the vault

        Returns:
            Upload response data with 'status' and 'fileEntry' keys

        Raises:
            DrimeUploadError: If upload fails
            DrimeFileNotFoundError: If file doesn't exist

        Example:
            >>> from pydrime.vault_crypto import unlock_vault, encrypt_filename
            >>> vault_key = unlock_vault(password, salt, check, iv)
            >>> encrypted_name, name_iv = encrypt_filename(vault_key, "secret.txt")
            >>> # Encrypt content and get IV
            >>> client.upload_vault_file(
            ...     file_path=Path("secret.txt"),
            ...     encrypted_content=encrypted_bytes,
            ...     encrypted_name=encrypted_name,
            ...     name_iv=name_iv,
            ...     content_iv=content_iv,
            ...     vault_id=784,
            ... )
        """
        if not file_path.exists():
            raise DrimeFileNotFoundError(str(file_path))

        # Use encrypted content size
        file_size = len(encrypted_content)
        mime_type = "application/octet-stream"  # Encrypted files are binary
        extension = file_path.suffix.lstrip(".") if file_path.suffix else ""

        # Combine IVs in the format expected by the API: ",contentIv"
        # (nameIv is empty in presign, full IVs go in entries)
        vault_ivs = f",{content_iv}"

        # Step 1: Get presigned URL for vault upload
        # Use ORIGINAL filename (not encrypted) for presign
        # Format: {"filename":"original.txt","mime":"application/octet-stream",
        #   "size":...,"extension":"...","relativePath":"","workspaceId":null,
        #   "parentId":null,"isEncrypted":1,"vaultIvs":",contentIv","vaultId":...}
        presign_payload: dict[str, Any] = {
            "filename": file_path.name,  # Original filename, not encrypted
            "mime": mime_type,
            "size": file_size,
            "extension": extension,
            "relativePath": "",
            "workspaceId": None,
            "parentId": parent_id,
            "isEncrypted": 1,
            "vaultIvs": vault_ivs,
            "vaultId": vault_id,
        }

        presign_response = self._request(
            "POST",
            "/s3/simple/presign",
            json=presign_payload,
            params={"vaultId": vault_id},
        )

        presigned_url = presign_response.get("url")
        key = presign_response.get("key")

        if not presigned_url or not key:
            raise DrimeUploadError(f"Invalid presign response: {presign_response}")

        # Step 2: Upload encrypted content to S3 using presigned URL
        try:
            s3_headers = {
                "Content-Type": mime_type,
                "x-amz-acl": "private",
            }

            s3_response = httpx.put(
                presigned_url,
                content=encrypted_content,
                headers=s3_headers,
                timeout=60.0,
            )
            s3_response.raise_for_status()

        except httpx.HTTPStatusError as e:
            raise DrimeUploadError(f"S3 upload failed: {e}") from e
        except httpx.RequestError as e:
            raise DrimeUploadError(f"Network error during S3 upload: {e}") from e

        # Step 3: Create file entry with encryption info
        # Use full IVs format: "nameIv,contentIv" for entries
        entry_vault_ivs = f"{name_iv},{content_iv}"
        entry_payload: dict[str, Any] = {
            "clientMime": mime_type,
            "clientName": file_path.name,  # Original filename, not encrypted
            "filename": key.split("/")[-1],
            "size": file_size,
            "clientExtension": extension,
            "relativePath": "",
            "workspaceId": None,
            "parentId": parent_id,
            "isEncrypted": 1,
            "vaultIvs": entry_vault_ivs,
            "vaultId": vault_id,
        }

        entry_response = self._request("POST", "s3/entries", json=entry_payload)
        return entry_response

    def delete_vault_file_entries(
        self,
        entry_ids: list[int],
        delete_forever: bool = False,
    ) -> Any:
        """Delete file entries from the vault.

        Moves vault entries to trash or deletes them permanently.

        Args:
            entry_ids: List of entry IDs to delete
            delete_forever: Whether to delete permanently (default: False,
                moves to trash)

        Returns:
            Response with 'status' key

        Raises:
            DrimeAPIError: If deletion fails

        Example:
            >>> client = DrimeClient(api_key="your_key")
            >>> # Move vault file to trash
            >>> client.delete_vault_file_entries([123])
            >>> # Delete vault file permanently
            >>> client.delete_vault_file_entries([123], delete_forever=True)
        """
        data = {
            "entryIds": entry_ids,
            "deleteForever": delete_forever,
        }
        return self._request("POST", "vault/delete-entries", json=data)

    def create_vault_folder(
        self,
        name: str,
        vault_id: int,
        parent_id: int | None = None,
    ) -> Any:
        """Create a folder in the vault.

        Args:
            name: Folder name (can be encrypted)
            vault_id: ID of the vault
            parent_id: Optional parent folder ID

        Returns:
            Response with 'status' and 'folder' keys

        Raises:
            DrimeAPIError: If folder creation fails

        Example:
            >>> client = DrimeClient(api_key="your_key")
            >>> result = client.create_vault_folder("MyFolder", vault_id=784)
            >>> folder_id = result["folder"]["id"]
        """
        endpoint = "/folders"
        data: dict[str, Any] = {
            "name": name,
            "vaultId": vault_id,
        }
        if parent_id is not None:
            data["parentId"] = parent_id
        return self._request("POST", endpoint, json=data)

    # =========================
    # Helper/Alias Methods
    # =========================

    def list_files(
        self,
        parent_id: int | None = None,
        query: str | None = None,
    ) -> Any:
        """List files in a directory (convenience method).

        Args:
            parent_id: Parent folder ID (None for root)
            query: Optional search query

        Returns:
            List of file entries
        """
        parent_ids = [parent_id] if parent_id is not None else None
        return self.get_file_entries(parent_ids=parent_ids, query=query)

    def create_directory(self, name: str, parent_id: int | None = None) -> Any:
        """Create a directory (alias for create_folder).

        Args:
            name: Directory name
            parent_id: Parent folder ID (None for root)

        Returns:
            Response with 'status' and 'folder' keys
        """
        return self.create_folder(name=name, parent_id=parent_id)

    def get_folder_by_name(
        self,
        folder_name: str,
        parent_id: int | None = None,
        case_sensitive: bool = True,
        workspace_id: int = 0,
    ) -> dict[str, Any]:
        """Get folder entry by name.

        Searches for a folder with the specified name in the given parent folder.
        If multiple folders match, returns the first one found.

        Args:
            folder_name: Name of the folder to find
            parent_id: Parent folder ID to search in (None for root)
            case_sensitive: Whether to match case-sensitively (default: True)
            workspace_id: Workspace ID (default: 0 for personal)

        Returns:
            Folder entry dictionary containing id, name, hash, and other metadata

        Raises:
            DrimeNotFoundError: If folder not found
            DrimeAPIError: If API call fails

        Examples:
            >>> client = DrimeClient(api_key="your_key")
            >>> folder = client.get_folder_by_name("Documents")
            >>> print(folder['id'])
            480432024
        """
        from .models import FileEntriesResult

        # Search for the folder by name in the specified parent
        parent_ids: list[int] | None = [parent_id] if parent_id is not None else None

        result = self.get_file_entries(
            workspace_id=workspace_id,
            query=folder_name,
            entry_type="folder",
            parent_ids=parent_ids,
            per_page=10,  # Small limit since we only need one match
        )

        if not result or not result.get("data"):
            raise DrimeNotFoundError(
                f"Folder '{folder_name}' not found in "
                f"{'root' if parent_id is None else f'folder {parent_id}'}"
            )

        # Parse the response
        file_entries = FileEntriesResult.from_api_response(result)

        if file_entries.is_empty:
            raise DrimeNotFoundError(
                f"Folder '{folder_name}' not found in "
                f"{'root' if parent_id is None else f'folder {parent_id}'}"
            )

        # Find exact match (case-sensitive or case-insensitive)
        matching_folders = []
        if case_sensitive:
            matching_folders = [
                e for e in file_entries.entries if e.name == folder_name
            ]
        else:
            matching_folders = [
                e for e in file_entries.entries if e.name.lower() == folder_name.lower()
            ]

        if not matching_folders:
            # Try the opposite case sensitivity if no match found
            if case_sensitive:
                matching_folders = [
                    e
                    for e in file_entries.entries
                    if e.name.lower() == folder_name.lower()
                ]

        if not matching_folders:
            raise DrimeNotFoundError(
                f"Folder '{folder_name}' not found in "
                f"{'root' if parent_id is None else f'folder {parent_id}'}. "
                f"Found {len(file_entries.entries)} folder(s) matching the query, "
                "but none with exact name"
            )

        # Return the first match as a dictionary
        folder = matching_folders[0]
        return {
            "id": folder.id,
            "name": folder.name,
            "hash": folder.hash,
            "type": folder.type,
            "file_size": folder.file_size,
            "parent_id": folder.parent_id,
            "created_at": folder.created_at,
            "updated_at": folder.updated_at,
            "owner": folder.owner.email if folder.owner else None,
            "public": folder.public,
            "description": folder.description,
        }

    def resolve_folder_identifier(
        self,
        identifier: str,
        parent_id: int | None = None,
        workspace_id: int = 0,
    ) -> int:
        """Resolve folder identifier (ID or name) to folder ID.

        Accepts either a numeric folder ID or a folder name and returns the
        corresponding folder ID. If a name is provided, searches in the
        specified parent folder.

        Args:
            identifier: Folder ID (numeric string) or folder name
            parent_id: Parent folder to search in if identifier is a name
            workspace_id: Workspace ID (default: 0 for personal)

        Returns:
            Folder ID as integer

        Raises:
            DrimeNotFoundError: If folder not found
            DrimeAPIError: If API call fails

        Examples:
            >>> client = DrimeClient(api_key="your_key")
            >>> # Resolve by ID
            >>> folder_id = client.resolve_folder_identifier("480432024")
            >>> # Resolve by name
            >>> folder_id = client.resolve_folder_identifier("Documents")
        """
        from .utils import is_file_id

        # Check if identifier is a numeric ID
        if is_file_id(identifier):
            return int(identifier)

        # It's a folder name - search for it
        folder = self.get_folder_by_name(
            folder_name=identifier,
            parent_id=parent_id,
            case_sensitive=True,
            workspace_id=workspace_id,
        )

        folder_id: int = folder["id"]
        return folder_id

    def resolve_entry_identifier(
        self,
        identifier: str,
        parent_id: int | None = None,
        workspace_id: int = 0,
    ) -> int:
        """Resolve entry identifier (ID or name) to entry ID.

        Accepts either a numeric entry ID or an entry name (file or folder) and
        returns the corresponding entry ID. If a name is provided, searches in
        the specified parent folder.

        Args:
            identifier: Entry ID (numeric string) or entry name
            parent_id: Parent folder to search in if identifier is a name
            workspace_id: Workspace ID (default: 0 for personal)

        Returns:
            Entry ID as integer

        Raises:
            DrimeNotFoundError: If entry not found
            DrimeAPIError: If API call fails

        Examples:
            >>> client = DrimeClient(api_key="your_key")
            >>> # Resolve by ID
            >>> entry_id = client.resolve_entry_identifier("480432024")
            >>> # Resolve by name
            >>> entry_id = client.resolve_entry_identifier("test1.txt")
        """
        from .models import FileEntriesResult
        from .utils import is_file_id

        # Check if identifier is a numeric ID
        if is_file_id(identifier):
            return int(identifier)

        # It's an entry name - search for it
        parent_ids: list[int] | None = [parent_id] if parent_id is not None else None

        result = self.get_file_entries(
            workspace_id=workspace_id,
            query=identifier,
            parent_ids=parent_ids,
        )

        if not result or not result.get("data"):
            raise DrimeNotFoundError(
                f"Entry '{identifier}' not found in "
                f"{'root' if parent_id is None else f'folder {parent_id}'}"
            )

        # Parse the response
        file_entries = FileEntriesResult.from_api_response(result)

        if file_entries.is_empty:
            raise DrimeNotFoundError(
                f"Entry '{identifier}' not found in "
                f"{'root' if parent_id is None else f'folder {parent_id}'}"
            )

        # Find exact match (case-sensitive)
        matching_entries = [e for e in file_entries.entries if e.name == identifier]

        # Try case-insensitive if no exact match
        if not matching_entries:
            matching_entries = [
                e for e in file_entries.entries if e.name.lower() == identifier.lower()
            ]

        if not matching_entries:
            raise DrimeNotFoundError(
                f"Entry '{identifier}' not found in "
                f"{'root' if parent_id is None else f'folder {parent_id}'}. "
                f"Found {len(file_entries.entries)} entry(ies) matching the query, "
                "but none with exact name"
            )

        # Return the first match
        return matching_entries[0].id

    def resolve_path_to_id(
        self,
        path: str,
        workspace_id: int = 0,
    ) -> int:
        """Resolve a path (e.g., folder/subfolder/file.txt) to entry ID.

        Supports both absolute paths (starting with /) and relative paths.
        For absolute paths, the leading slash is stripped and resolution
        starts from the root.

        Args:
            path: Path to resolve (e.g., "folder/file.txt" or "/folder/file.txt")
            workspace_id: Workspace ID (default: 0 for personal)

        Returns:
            Entry ID as integer

        Raises:
            DrimeNotFoundError: If path not found
            DrimeAPIError: If API call fails

        Examples:
            >>> client = DrimeClient(api_key="your_key")
            >>> # Resolve by path
            >>> entry_id = client.resolve_path_to_id("folder/file.txt")
            >>> entry_id = client.resolve_path_to_id("/folder/file.txt")
        """
        from .models import FileEntriesResult

        # Strip leading slash for absolute paths
        path = path.lstrip("/")

        parts = path.split("/")
        current_parent: int | None = None

        # Navigate through each path component
        for i, part in enumerate(parts):
            if not part:
                continue  # Skip empty parts (e.g., double slashes)

            is_last = i == len(parts) - 1

            # Search for the entry in the current folder
            parent_ids: list[int] | None = (
                [current_parent] if current_parent is not None else None
            )
            result = self.get_file_entries(
                workspace_id=workspace_id,
                parent_ids=parent_ids,
            )

            if not result or not result.get("data"):
                raise DrimeNotFoundError(f"Path not found: {'/'.join(parts[: i + 1])}")

            file_entries = FileEntriesResult.from_api_response(result)

            # Find the matching entry (case-sensitive first)
            matching = [e for e in file_entries.entries if e.name == part]
            if not matching:
                # Try case-insensitive
                matching = [
                    e for e in file_entries.entries if e.name.lower() == part.lower()
                ]

            if not matching:
                raise DrimeNotFoundError(f"Path not found: {'/'.join(parts[: i + 1])}")

            entry = matching[0]

            if is_last:
                # Found the target entry
                return entry.id
            else:
                # This should be a folder, continue navigating
                if not entry.is_folder:
                    raise DrimeNotFoundError(
                        f"'{part}' is not a folder in path: {path}"
                    )
                current_parent = entry.id

        raise DrimeNotFoundError(f"Path not found: {path}")

    def get_folder_info(
        self,
        folder_id: int,
    ) -> dict[str, Any]:
        """Get folder information including name and metadata.

        Args:
            folder_id: ID of the folder

        Returns:
            Folder entry dictionary with name and metadata

        Raises:
            DrimeNotFoundError: If folder not found
            DrimeAPIError: If API call fails

        Examples:
            >>> client = DrimeClient(api_key="your_key")
            >>> folder_info = client.get_folder_info(480432024)
            >>> print(f"Name: {folder_info['name']}")
            Name: Documents
        """
        from .models import FileEntry
        from .utils import calculate_drime_hash

        # Get the folder using its hash with folder_id parameter
        # This returns the folder info in the 'folder' field of the response
        folder_hash = calculate_drime_hash(folder_id)
        result = self.get_file_entries(folder_id=folder_hash)

        if not result:
            raise DrimeNotFoundError(f"Folder with ID {folder_id} not found")

        # Check if the folder field exists in the response
        folder_data = result.get("folder")
        if folder_data and folder_data.get("id") == folder_id:
            # Parse the folder entry
            folder_entry = FileEntry.from_dict(folder_data)
            return {
                "id": folder_entry.id,
                "name": folder_entry.name,
                "hash": folder_entry.hash,
                "type": folder_entry.type,
                "file_size": folder_entry.file_size,
                "parent_id": folder_entry.parent_id,
                "created_at": folder_entry.created_at,
                "updated_at": folder_entry.updated_at,
                "owner": folder_entry.owner.email if folder_entry.owner else None,
                "public": folder_entry.public,
                "description": folder_entry.description,
            }

        raise DrimeNotFoundError(f"Folder with ID {folder_id} not found")

    def resolve_entries_by_pattern(
        self,
        pattern: str,
        parent_id: int | None = None,
        workspace_id: int = 0,
        entry_type: FileEntryType | None = None,
    ) -> list[FileEntry]:
        """Resolve entries matching a glob pattern.

        If the pattern contains glob characters (*, ?, [), returns all entries
        matching the pattern. Otherwise, returns entries with an exact name match.

        Args:
            pattern: Glob pattern or exact name to match
            parent_id: Parent folder to search in (None for root)
            workspace_id: Workspace ID (default: 0 for personal)
            entry_type: Filter by entry type (e.g., 'folder')

        Returns:
            List of FileEntry objects matching the pattern

        Examples:
            >>> client = DrimeClient(api_key="your_key")
            >>> # Get all .txt files
            >>> entries = client.resolve_entries_by_pattern("*.txt")
            >>> # Get entries starting with "bench"
            >>> entries = client.resolve_entries_by_pattern("bench*")
            >>> # Get exact match (no glob)
            >>> entries = client.resolve_entries_by_pattern("file.txt")
        """
        from .models import FileEntriesResult
        from .utils import glob_match, is_glob_pattern

        # Get entries in the folder
        parent_ids: list[int] | None = [parent_id] if parent_id is not None else None

        # For glob patterns, we need to get all entries and filter locally
        # For exact matches, we can use the query parameter
        if is_glob_pattern(pattern):
            # Get all entries and filter locally
            result = self.get_file_entries(
                workspace_id=workspace_id,
                parent_ids=parent_ids,
                entry_type=entry_type,
                per_page=1000,  # Get more entries for pattern matching
            )
        else:
            # Use API query for exact match (more efficient)
            result = self.get_file_entries(
                workspace_id=workspace_id,
                parent_ids=parent_ids,
                query=pattern,
                entry_type=entry_type,
            )

        if not result or not result.get("data"):
            return []

        file_entries = FileEntriesResult.from_api_response(result)

        if is_glob_pattern(pattern):
            # Filter entries by glob pattern
            return [e for e in file_entries.entries if glob_match(pattern, e.name)]
        else:
            # Return exact matches
            matching = [e for e in file_entries.entries if e.name == pattern]
            if not matching:
                # Try case-insensitive
                matching = [
                    e for e in file_entries.entries if e.name.lower() == pattern.lower()
                ]
            return matching
