"""API client for Drime Cloud."""

from pathlib import Path
from typing import Any, Callable, Literal, Optional

import requests

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

FileEntryType = Literal["folder", "image", "text", "audio", "video", "pdf"]
Permission = Literal["view", "edit", "download"]


class DrimeClient:
    """Client for interacting with Drime Cloud API."""

    def __init__(self, api_key: Optional[str] = None, api_url: Optional[str] = None):
        """Initialize Drime API client.

        Args:
            api_key: Optional API key (uses config if not provided)
            api_url: Optional API URL (uses config if not provided)
        """
        self.api_key = api_key or config.api_key
        self.api_url = api_url or config.api_url

        if not self.api_key:
            raise DrimeConfigError(
                "API key not configured. Please set DRIME_API_KEY environment variable."
            )

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
            }
        )

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Any:
        """Make an API request.

        Args:
            method: HTTP method
            endpoint: API endpoint path
            **kwargs: Additional arguments passed to requests

        Returns:
            Response JSON data

        Raises:
            DrimeAPIError: If the request fails
        """
        url = f"{self.api_url}/{endpoint.lstrip('/')}"

        try:
            response = self.session.request(method, url, **kwargs)
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

        except requests.exceptions.HTTPError as e:
            # Try to get error message from response
            # Note: e.response can be a Response object that evaluates to False
            # if the status code indicates an error, so we must check "is not None"
            status_code = e.response.status_code if e.response is not None else None

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
                raise DrimeRateLimitError(
                    "Rate limit exceeded - please try again later"
                ) from e
            else:
                error_msg = f"API request failed with status {status_code}"

                # Try to extract more details from response body
                try:
                    if e.response is not None and e.response.content:
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
                    # If we can't parse the error response, use the status-based message
                    pass

                raise DrimeAPIError(error_msg) from e
        except DrimeAPIError:
            # Re-raise our own exceptions
            raise
        except requests.exceptions.RequestException as e:
            raise DrimeNetworkError(f"Network error: {e}") from e

    # =========================
    # Upload Operations
    # =========================

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
        relative_path: Optional[str] = None,
        workspace_id: int = 0,
        chunk_size: int = 25 * 1024 * 1024,  # 25MB chunks
        progress_callback: Optional[Callable[[int, int], None]] = None,
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
        import mimetypes

        if not file_path.exists():
            raise DrimeFileNotFoundError(str(file_path))

        file_size = file_path.stat().st_size
        file_name = file_path.name
        extension = file_path.suffix.lstrip(".")
        num_parts = math.ceil(file_size / chunk_size)

        # Detect MIME type - try python-magic first, then fall back to mimetypes
        mime_type = None
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

                        response = requests.put(
                            signed_url, data=chunk, headers=headers, timeout=60
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

    def upload_file(
        self,
        file_path: Path,
        parent_id: Optional[int] = None,
        relative_path: Optional[str] = None,
        workspace_id: int = 0,
        use_multipart_threshold: int = 30 * 1024 * 1024,  # 30MB
        chunk_size: int = 25 * 1024 * 1024,  # 25MB chunks
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Any:
        """Upload a file to Drime Cloud.

        Automatically chooses between simple and multipart upload based on file size.

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

        Returns:
            Upload response data with 'status' and 'fileEntry' keys
        """
        if not file_path.exists():
            raise DrimeFileNotFoundError(str(file_path))

        file_size = file_path.stat().st_size

        # Use multipart upload for large files
        if file_size > use_multipart_threshold:
            return self.upload_file_multipart(
                file_path=file_path,
                relative_path=relative_path,
                workspace_id=workspace_id,
                chunk_size=chunk_size,
                progress_callback=progress_callback,
            )

        # Simple upload for smaller files
        endpoint = "/uploads"

        # For small files, progress tracking doesn't work well with requests library
        # as it reads the entire file at once. Progress is only tracked for large files
        # using multipart upload.
        with open(file_path, "rb") as f:
            files = {"file": (file_path.name, f)}
            data: dict[str, Any] = {}

            if parent_id is not None:
                data["parentId"] = parent_id

            if relative_path:
                data["relativePath"] = relative_path

            if workspace_id:
                data["workspaceId"] = workspace_id

            # Remove Content-Type header for multipart/form-data
            headers = dict(self.session.headers)
            headers.pop("Content-Type", None)

            result = self._request(
                "POST", endpoint, files=files, data=data, headers=headers
            )

            # Call progress callback with 100% to update overall progress
            if progress_callback:
                progress_callback(file_size, file_size)

            return result

    # =========================
    # File Entry Operations
    # =========================

    def get_file_entries(
        self,
        per_page: int = 50,
        deleted_only: Optional[bool] = None,
        starred_only: Optional[bool] = None,
        recent_only: Optional[bool] = None,
        shared_only: Optional[bool] = None,
        query: Optional[str] = None,
        entry_type: Optional[FileEntryType] = None,
        parent_ids: Optional[list[int]] = None,
        workspace_id: int = 0,
        folder_id: Optional[str] = None,
        page_id: Optional[str] = None,
        backup: int = 0,
    ) -> Any:
        """Get the list of all file entries you have access to.

        Args:
            per_page: How many entries to return per page (default: 50)
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

        Returns:
            List of file entry objects
        """
        endpoint = "/drive/file-entries"
        params: dict[str, Any] = {"perPage": per_page, "workspaceId": workspace_id}

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

        return self._request("GET", endpoint, params=params)

    def update_file_entry(
        self,
        entry_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
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
    ) -> Any:
        """Move entries to trash or delete permanently.

        Args:
            entry_ids: List of entry IDs to delete
            delete_forever: Whether entries should be deleted permanently

        Returns:
            Response with 'status' key
        """
        endpoint = "/file-entries/delete"
        data = {
            "entryIds": entry_ids,
            "deleteForever": delete_forever,
        }
        return self._request("POST", endpoint, json=data)

    def move_file_entries(
        self,
        entry_ids: list[int],
        destination_id: Optional[int] = None,
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
        destination_id: Optional[int] = None,
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

    def create_folder(
        self,
        name: str,
        parent_id: Optional[int] = None,
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
        password: Optional[str] = None,
        expires_at: Optional[str] = None,
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
        password: Optional[str] = None,
        expires_at: Optional[str] = None,
        allow_edit: Optional[bool] = None,
        allow_download: Optional[bool] = None,
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
        output_path: Optional[Path] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """Download a file from Drime Cloud.

        Args:
            hash_value: Hash of the file to download
            output_path: Optional path where to save the file
            progress_callback: Optional callback function(bytes_downloaded, total_bytes)

        Returns:
            Path where the file was saved

        Raises:
            DrimeAPIError: If download fails
        """
        endpoint = f"/file-entries/download/{hash_value}"
        url = f"{self.api_url}/{endpoint.lstrip('/')}"

        try:
            response = self.session.get(url, stream=True)
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
                        filename = parts[1].split(";")[0].strip().strip('"').strip("'")

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
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        if progress_callback:
                            progress_callback(bytes_downloaded, total_size)

            return save_path

        except requests.exceptions.HTTPError as e:
            raise DrimeDownloadError(f"Download failed: {e}") from e
        except requests.exceptions.RequestException as e:
            raise DrimeNetworkError(f"Network error during download: {e}") from e
        except OSError as e:
            raise DrimeDownloadError(f"Failed to write file: {e}") from e

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
    # Helper/Alias Methods
    # =========================

    def list_files(
        self,
        parent_id: Optional[int] = None,
        query: Optional[str] = None,
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

    def create_directory(self, name: str, parent_id: Optional[int] = None) -> Any:
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
        parent_id: Optional[int] = None,
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
        parent_ids: Optional[list[int]] = [parent_id] if parent_id is not None else None

        result = self.get_file_entries(
            workspace_id=workspace_id,
            query=folder_name,
            entry_type="folder",
            parent_ids=parent_ids,
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
        parent_id: Optional[int] = None,
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
        parent_id: Optional[int] = None,
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
        parent_ids: Optional[list[int]] = [parent_id] if parent_id is not None else None

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
