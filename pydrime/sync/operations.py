"""Sync operations wrapper for unified upload/download interface."""

from pathlib import Path
from typing import Any, Callable, Optional

from ..api import DrimeClient
from .scanner import LocalFile, RemoteFile


class SyncOperations:
    """Unified operations for upload/download with common interface."""

    def __init__(self, client: DrimeClient):
        """Initialize sync operations.

        Args:
            client: Drime API client
        """
        self.client = client

    def upload_file(
        self,
        local_file: LocalFile,
        remote_path: str,
        workspace_id: int = 0,
        chunk_size: int = 25 * 1024 * 1024,
        multipart_threshold: int = 30 * 1024 * 1024,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Any:
        """Upload a local file to remote storage.

        Args:
            local_file: Local file to upload
            remote_path: Remote path (relative path for the file)
            workspace_id: Workspace ID
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for using multipart upload
            progress_callback: Optional progress callback
                function(bytes_uploaded, total_bytes)

        Returns:
            Upload response from API
        """
        return self.client.upload_file(
            file_path=local_file.path,
            relative_path=remote_path,
            workspace_id=workspace_id,
            chunk_size=chunk_size,
            use_multipart_threshold=multipart_threshold,
            progress_callback=progress_callback,
        )

    def download_file(
        self,
        remote_file: RemoteFile,
        local_path: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """Download a remote file to local storage.

        Args:
            remote_file: Remote file to download
            local_path: Local path where file should be saved
            progress_callback: Optional progress callback
                function(bytes_downloaded, total_bytes)

        Returns:
            Path where file was saved
        """
        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        return self.client.download_file(
            hash_value=remote_file.hash,
            output_path=local_path,
            progress_callback=progress_callback,
        )

    def delete_remote(
        self,
        remote_file: RemoteFile,
        permanent: bool = False,
    ) -> Any:
        """Delete a remote file.

        Args:
            remote_file: Remote file to delete
            permanent: If True, delete permanently; if False, move to trash

        Returns:
            Delete response from API
        """
        return self.client.delete_file_entries(
            entry_ids=[remote_file.id],
            delete_forever=permanent,
        )

    def delete_local(
        self,
        local_file: LocalFile,
        use_trash: bool = True,
    ) -> None:
        """Delete a local file.

        Args:
            local_file: Local file to delete
            use_trash: If True and available, move to trash;
                otherwise delete permanently
        """
        if use_trash:
            # Try to use system trash if available
            try:
                import send2trash

                send2trash.send2trash(str(local_file.path))
                return
            except ImportError:
                # send2trash not available, fall back to permanent delete
                pass

        # Permanent delete
        local_file.path.unlink()
