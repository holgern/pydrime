"""Sync operations wrapper for unified upload/download interface."""

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from ..api import DrimeClient
from ..utils import DEFAULT_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD
from .scanner import LocalFile, RemoteFile

# Local trash directory name
LOCAL_TRASH_DIR_NAME = ".pydrime.trash.local"


def get_local_trash_path(sync_root: Path) -> Path:
    """Get the path to the local trash directory.

    Args:
        sync_root: Root directory of the sync operation

    Returns:
        Path to the .pydrime.trash.local directory
    """
    return sync_root / LOCAL_TRASH_DIR_NAME


def move_to_local_trash(
    file_path: Path,
    sync_root: Path,
) -> Path:
    """Move a file to the local trash directory.

    The file will be moved to .pydrime.trash.local at the sync root,
    preserving its relative path structure. A timestamp is added to
    avoid name collisions when the same file is deleted multiple times.

    Args:
        file_path: Path to the file to move to trash
        sync_root: Root directory of the sync operation

    Returns:
        Path where the file was moved to

    Raises:
        FileNotFoundError: If the file does not exist
        OSError: If the file cannot be moved
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get the trash directory
    trash_dir = get_local_trash_path(sync_root)

    # Get the relative path from sync root
    try:
        relative_path = file_path.relative_to(sync_root)
    except ValueError:
        # File is not under sync_root, use just the filename
        relative_path = Path(file_path.name)

    # Create a timestamped trash path to avoid collisions
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    trash_path = trash_dir / f"{timestamp}" / relative_path

    # Ensure parent directory exists
    trash_path.parent.mkdir(parents=True, exist_ok=True)

    # Move the file
    shutil.move(str(file_path), str(trash_path))

    return trash_path


def rename_local_file(
    old_path: Path,
    new_path: Path,
) -> Path:
    """Rename/move a local file.

    Creates parent directories as needed and handles the rename atomically
    where possible.

    Args:
        old_path: Current path of the file
        new_path: New path for the file

    Returns:
        The new path

    Raises:
        FileNotFoundError: If the source file does not exist
        FileExistsError: If the target path already exists
        OSError: If the rename fails
    """
    if not old_path.exists():
        raise FileNotFoundError(f"Source file not found: {old_path}")

    if new_path.exists():
        raise FileExistsError(f"Target path already exists: {new_path}")

    # Ensure parent directory exists
    new_path.parent.mkdir(parents=True, exist_ok=True)

    # Rename the file
    old_path.rename(new_path)

    return new_path


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
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD,
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
        sync_root: Optional[Path] = None,
    ) -> None:
        """Delete a local file.

        When use_trash is True and sync_root is provided, the file is moved to
        the .pydrime.trash.local directory at the sync root. This allows easy
        recovery of accidentally deleted files while keeping them out of sync.

        Args:
            local_file: Local file to delete
            use_trash: If True, move to local trash directory;
                if False, delete permanently
            sync_root: Root directory of the sync operation (required for trash)
        """
        if use_trash and sync_root is not None:
            # Move to local trash directory
            move_to_local_trash(local_file.path, sync_root)
            return

        # Permanent delete
        local_file.path.unlink()

    def rename_local(
        self,
        local_file: LocalFile,
        new_relative_path: str,
        sync_root: Path,
    ) -> Path:
        """Rename/move a local file.

        This is used when a remote rename is detected and needs to be
        propagated to the local filesystem.

        Args:
            local_file: Local file to rename
            new_relative_path: New relative path for the file
            sync_root: Root directory of the sync operation

        Returns:
            The new absolute path of the file

        Raises:
            FileNotFoundError: If the source file does not exist
            FileExistsError: If the target path already exists
            OSError: If the rename fails
        """
        # Convert forward slashes to OS-native path separators
        new_path = sync_root / Path(new_relative_path)
        return rename_local_file(local_file.path, new_path)

    def rename_remote(
        self,
        remote_file: RemoteFile,
        new_name: str,
        new_parent_id: Optional[int] = None,
    ) -> Any:
        """Rename/move a remote file.

        This is used when a local rename is detected and needs to be
        propagated to the remote storage.

        For simple renames (same folder, different name), uses update_file_entry.
        For moves (different folder), uses move_file_entries followed by rename.

        Args:
            remote_file: Remote file to rename
            new_name: New name for the file (just the filename, not full path)
            new_parent_id: New parent folder ID (for moves), or None to keep same parent

        Returns:
            Response from API
        """
        # If moving to a different folder, do the move first
        if new_parent_id is not None:
            self.client.move_file_entries(
                entry_ids=[remote_file.id],
                destination_id=new_parent_id,
            )

        # Rename the file
        return self.client.update_file_entry(
            entry_id=remote_file.id,
            name=new_name,
        )
