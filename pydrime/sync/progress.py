"""Progress tracking types and callbacks for sync operations.

This module defines the progress callback interfaces used by the sync engine
to report progress. The actual display (Rich progress bars, etc.) is handled
by the CLI layer, keeping the sync engine UI-agnostic.
"""

import threading
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional


class SyncProgressEvent(Enum):
    """Events emitted during sync operations."""

    # Scanning events
    SCAN_DIR_START = "scan_dir_start"
    SCAN_DIR_COMPLETE = "scan_dir_complete"

    # Upload events
    UPLOAD_BATCH_START = "upload_batch_start"
    UPLOAD_FILE_START = "upload_file_start"
    UPLOAD_FILE_PROGRESS = "upload_file_progress"
    UPLOAD_FILE_COMPLETE = "upload_file_complete"
    UPLOAD_FILE_ERROR = "upload_file_error"
    UPLOAD_BATCH_COMPLETE = "upload_batch_complete"

    # Overall sync events
    SYNC_START = "sync_start"
    SYNC_COMPLETE = "sync_complete"


@dataclass
class SyncProgressInfo:
    """Information about sync progress.

    Attributes:
        event: The type of progress event
        directory: Current directory being processed (relative path)
        file_path: Current file being processed (relative path)
        files_in_batch: Number of files in current batch
        files_uploaded: Total files uploaded so far
        files_skipped: Total files skipped so far
        bytes_uploaded: Total bytes uploaded so far
        bytes_total: Total bytes to upload (if known)
        current_file_bytes: Bytes uploaded for current file
        current_file_total: Total bytes for current file
        error_message: Error message if event is an error
        folder_files_uploaded: Files uploaded in current folder
        folder_files_total: Total files to upload in current folder
        folder_bytes_uploaded: Bytes uploaded in current folder
        folder_bytes_total: Total bytes to upload in current folder
    """

    event: SyncProgressEvent
    directory: str = ""
    file_path: str = ""
    files_in_batch: int = 0
    files_uploaded: int = 0
    files_skipped: int = 0
    bytes_uploaded: int = 0
    bytes_total: Optional[int] = None
    current_file_bytes: int = 0
    current_file_total: int = 0
    error_message: str = ""
    # Per-folder statistics
    folder_files_uploaded: int = 0
    folder_files_total: int = 0
    folder_bytes_uploaded: int = 0
    folder_bytes_total: int = 0


# Type alias for progress callback
SyncProgressCallback = Callable[[SyncProgressInfo], None]


class SyncProgressTracker:
    """Thread-safe progress tracker for sync operations.

    This class aggregates progress from multiple parallel uploads and
    calls a single callback with consolidated progress information.
    """

    def __init__(
        self,
        callback: Optional[SyncProgressCallback] = None,
    ) -> None:
        """Initialize progress tracker.

        Args:
            callback: Optional callback to receive progress updates
        """
        self.callback = callback
        self.lock = threading.Lock()

        # Accumulated stats
        self.total_bytes_uploaded = 0
        self.total_bytes_to_upload = 0
        self.total_files_uploaded = 0
        self.total_files_skipped = 0

        # Per-file tracking for parallel uploads
        self._file_bytes: dict[str, int] = {}
        self._file_totals: dict[str, int] = {}

        # Per-folder tracking
        self._current_folder = ""
        self._folder_files_total = 0
        self._folder_files_uploaded = 0
        self._folder_bytes_total = 0
        self._folder_bytes_uploaded = 0

    def reset(self) -> None:
        """Reset all counters."""
        with self.lock:
            self.total_bytes_uploaded = 0
            self.total_bytes_to_upload = 0
            self.total_files_uploaded = 0
            self.total_files_skipped = 0
            self._file_bytes.clear()
            self._file_totals.clear()
            self._current_folder = ""
            self._folder_files_total = 0
            self._folder_files_uploaded = 0
            self._folder_bytes_total = 0
            self._folder_bytes_uploaded = 0

    def _reset_folder_stats(self) -> None:
        """Reset per-folder counters for a new folder."""
        self._folder_files_total = 0
        self._folder_files_uploaded = 0
        self._folder_bytes_total = 0
        self._folder_bytes_uploaded = 0

    def add_bytes_to_upload(self, total_bytes: int) -> None:
        """Add bytes to the total bytes to upload.

        Args:
            total_bytes: Bytes to add to total
        """
        with self.lock:
            self.total_bytes_to_upload += total_bytes

    def on_scan_dir_start(self, directory: str) -> None:
        """Called when starting to scan a directory.

        Args:
            directory: Relative path of directory being scanned
        """
        if self.callback:
            self.callback(
                SyncProgressInfo(
                    event=SyncProgressEvent.SCAN_DIR_START,
                    directory=directory,
                )
            )

    def on_scan_dir_complete(
        self, directory: str, files_found: int, subdirs_found: int
    ) -> None:
        """Called when directory scan is complete.

        Args:
            directory: Relative path of directory scanned
            files_found: Number of files found
            subdirs_found: Number of subdirectories found
        """
        if self.callback:
            self.callback(
                SyncProgressInfo(
                    event=SyncProgressEvent.SCAN_DIR_COMPLETE,
                    directory=directory,
                    files_in_batch=files_found,
                )
            )

    def on_upload_batch_start(
        self, directory: str, num_files: int, total_bytes: int
    ) -> None:
        """Called when starting to upload a batch of files.

        Args:
            directory: Relative path of directory
            num_files: Number of files to upload
            total_bytes: Total bytes to upload in this batch
        """
        with self.lock:
            self.total_bytes_to_upload += total_bytes
            # Reset folder stats for new folder
            self._current_folder = directory
            self._reset_folder_stats()
            self._folder_files_total = num_files
            self._folder_bytes_total = total_bytes

        if self.callback:
            self.callback(
                SyncProgressInfo(
                    event=SyncProgressEvent.UPLOAD_BATCH_START,
                    directory=directory,
                    files_in_batch=num_files,
                    files_uploaded=self.total_files_uploaded,
                    bytes_uploaded=self.total_bytes_uploaded,
                    bytes_total=self.total_bytes_to_upload,
                    folder_files_uploaded=0,
                    folder_files_total=num_files,
                    folder_bytes_uploaded=0,
                    folder_bytes_total=total_bytes,
                )
            )

    def on_upload_file_start(self, file_path: str, file_size: int) -> None:
        """Called when starting to upload a file.

        Args:
            file_path: Relative path of file
            file_size: Size of file in bytes
        """
        with self.lock:
            self._file_bytes[file_path] = 0
            self._file_totals[file_path] = file_size

        if self.callback:
            self.callback(
                SyncProgressInfo(
                    event=SyncProgressEvent.UPLOAD_FILE_START,
                    file_path=file_path,
                    current_file_total=file_size,
                    files_uploaded=self.total_files_uploaded,
                    bytes_uploaded=self.total_bytes_uploaded,
                    bytes_total=self.total_bytes_to_upload,
                )
            )

    def on_upload_file_progress(
        self, file_path: str, bytes_uploaded: int, total_bytes: int
    ) -> None:
        """Called during file upload with byte progress.

        Args:
            file_path: Relative path of file
            bytes_uploaded: Bytes uploaded so far for this file
            total_bytes: Total bytes for this file
        """
        with self.lock:
            # Calculate increment
            prev_bytes = self._file_bytes.get(file_path, 0)
            increment = bytes_uploaded - prev_bytes
            self._file_bytes[file_path] = bytes_uploaded
            self.total_bytes_uploaded += increment
            self._folder_bytes_uploaded += increment

            current_total = self.total_bytes_uploaded
            overall_total = self.total_bytes_to_upload
            folder_bytes = self._folder_bytes_uploaded
            folder_bytes_total = self._folder_bytes_total
            folder_files = self._folder_files_uploaded
            folder_files_total = self._folder_files_total
            directory = self._current_folder

        if self.callback:
            self.callback(
                SyncProgressInfo(
                    event=SyncProgressEvent.UPLOAD_FILE_PROGRESS,
                    directory=directory,
                    file_path=file_path,
                    current_file_bytes=bytes_uploaded,
                    current_file_total=total_bytes,
                    files_uploaded=self.total_files_uploaded,
                    bytes_uploaded=current_total,
                    bytes_total=overall_total,
                    folder_files_uploaded=folder_files,
                    folder_files_total=folder_files_total,
                    folder_bytes_uploaded=folder_bytes,
                    folder_bytes_total=folder_bytes_total,
                )
            )

    def on_upload_file_complete(self, file_path: str) -> None:
        """Called when file upload is complete.

        Args:
            file_path: Relative path of file
        """
        with self.lock:
            self.total_files_uploaded += 1
            self._folder_files_uploaded += 1
            # Clean up per-file tracking
            self._file_bytes.pop(file_path, None)
            self._file_totals.pop(file_path, None)

            files_uploaded = self.total_files_uploaded
            bytes_uploaded = self.total_bytes_uploaded
            bytes_total = self.total_bytes_to_upload
            folder_files = self._folder_files_uploaded
            folder_files_total = self._folder_files_total
            folder_bytes = self._folder_bytes_uploaded
            folder_bytes_total = self._folder_bytes_total
            directory = self._current_folder

        if self.callback:
            self.callback(
                SyncProgressInfo(
                    event=SyncProgressEvent.UPLOAD_FILE_COMPLETE,
                    directory=directory,
                    file_path=file_path,
                    files_uploaded=files_uploaded,
                    bytes_uploaded=bytes_uploaded,
                    bytes_total=bytes_total,
                    folder_files_uploaded=folder_files,
                    folder_files_total=folder_files_total,
                    folder_bytes_uploaded=folder_bytes,
                    folder_bytes_total=folder_bytes_total,
                )
            )

    def on_upload_file_error(self, file_path: str, error: str) -> None:
        """Called when file upload fails.

        Args:
            file_path: Relative path of file
            error: Error message
        """
        with self.lock:
            # Rollback bytes for this file
            file_bytes = self._file_bytes.pop(file_path, 0)
            self.total_bytes_uploaded -= file_bytes
            self._file_totals.pop(file_path, None)

        if self.callback:
            self.callback(
                SyncProgressInfo(
                    event=SyncProgressEvent.UPLOAD_FILE_ERROR,
                    file_path=file_path,
                    error_message=error,
                    files_uploaded=self.total_files_uploaded,
                    bytes_uploaded=self.total_bytes_uploaded,
                    bytes_total=self.total_bytes_to_upload,
                )
            )

    def on_files_skipped(self, count: int) -> None:
        """Called when files are skipped (already exist remotely).

        Args:
            count: Number of files skipped
        """
        with self.lock:
            self.total_files_skipped += count

    def on_upload_batch_complete(self, directory: str, num_uploaded: int) -> None:
        """Called when batch upload is complete.

        Args:
            directory: Relative path of directory
            num_uploaded: Number of files successfully uploaded
        """
        with self.lock:
            folder_files = self._folder_files_uploaded
            folder_files_total = self._folder_files_total
            folder_bytes = self._folder_bytes_uploaded
            folder_bytes_total = self._folder_bytes_total

        if self.callback:
            self.callback(
                SyncProgressInfo(
                    event=SyncProgressEvent.UPLOAD_BATCH_COMPLETE,
                    directory=directory,
                    files_in_batch=num_uploaded,
                    files_uploaded=self.total_files_uploaded,
                    files_skipped=self.total_files_skipped,
                    bytes_uploaded=self.total_bytes_uploaded,
                    bytes_total=self.total_bytes_to_upload,
                    folder_files_uploaded=folder_files,
                    folder_files_total=folder_files_total,
                    folder_bytes_uploaded=folder_bytes,
                    folder_bytes_total=folder_bytes_total,
                )
            )

    def create_file_progress_callback(
        self, file_path: str
    ) -> Callable[[int, int], None]:
        """Create a byte-level progress callback for a specific file.

        This returns a callback compatible with the existing upload API
        (bytes_uploaded, total_bytes) that internally updates the tracker.

        Args:
            file_path: Relative path of file being uploaded

        Returns:
            Progress callback function
        """

        def callback(bytes_uploaded: int, total_bytes: int) -> None:
            self.on_upload_file_progress(file_path, bytes_uploaded, total_bytes)

        return callback

    def get_stats(self) -> dict:
        """Get current progress statistics.

        Returns:
            Dictionary with current stats
        """
        with self.lock:
            return {
                "files_uploaded": self.total_files_uploaded,
                "files_skipped": self.total_files_skipped,
                "bytes_uploaded": self.total_bytes_uploaded,
                "bytes_total": self.total_bytes_to_upload,
            }
