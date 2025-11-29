"""CLI progress display for sync operations.

This module provides Rich-based progress displays that work with
the SyncProgressTracker from the sync engine.
"""

from typing import Optional

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)

from .sync.progress import SyncProgressEvent, SyncProgressInfo, SyncProgressTracker


def _format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human-readable size string (e.g., "1.5 MB")
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


class SyncProgressDisplay:
    """Rich-based progress display for sync operations.

    This class creates a Rich Progress instance and handles
    SyncProgressInfo events to update the display.

    The progress display shows folder-level statistics:
    - Files: uploaded files / total files in current folder
    - Size: uploaded size / total size in current folder
    """

    def __init__(self) -> None:
        """Initialize the progress display."""
        self._progress: Optional[Progress] = None
        self._upload_task: Optional[TaskID] = None
        self._total_files = 0
        self._current_dir = ""

    def create_tracker(self) -> SyncProgressTracker:
        """Create a SyncProgressTracker that updates this display.

        Returns:
            A configured SyncProgressTracker
        """
        return SyncProgressTracker(callback=self._handle_event)

    def _format_folder_progress(self, info: SyncProgressInfo) -> str:
        """Format folder progress information.

        Args:
            info: Progress information

        Returns:
            Formatted string like "2/5 files, 1.5/10.0 MB"
        """
        files_str = f"{info.folder_files_uploaded}/{info.folder_files_total} files"
        size_uploaded = _format_size(info.folder_bytes_uploaded)
        size_total = _format_size(info.folder_bytes_total)
        size_str = f"{size_uploaded}/{size_total}"
        return f"{files_str}, {size_str}"

    def _handle_event(self, info: SyncProgressInfo) -> None:
        """Handle a progress event from the tracker.

        Args:
            info: Progress information
        """
        if self._progress is None:
            return

        if info.event == SyncProgressEvent.UPLOAD_BATCH_START:
            self._current_dir = info.directory
            # Update task description and total for folder
            if self._upload_task is not None:
                folder_name = info.directory if info.directory else "root"
                self._progress.update(
                    self._upload_task,
                    description=f"Uploading: {folder_name}",
                    total=info.folder_bytes_total,
                    completed=0,
                    folder_info=self._format_folder_progress(info),
                )

        elif info.event == SyncProgressEvent.UPLOAD_FILE_PROGRESS:
            # Update bytes progress for current folder
            if self._upload_task is not None:
                self._progress.update(
                    self._upload_task,
                    completed=info.folder_bytes_uploaded,
                    folder_info=self._format_folder_progress(info),
                )

        elif info.event == SyncProgressEvent.UPLOAD_FILE_COMPLETE:
            # Update file count for folder
            self._total_files = info.files_uploaded
            if self._upload_task is not None:
                self._progress.update(
                    self._upload_task,
                    completed=info.folder_bytes_uploaded,
                    folder_info=self._format_folder_progress(info),
                )

        elif info.event == SyncProgressEvent.UPLOAD_BATCH_COMPLETE:
            # Update final stats for this batch
            if self._upload_task is not None:
                self._progress.update(
                    self._upload_task,
                    completed=info.folder_bytes_uploaded,
                    folder_info=self._format_folder_progress(info),
                )

    def __enter__(self) -> "SyncProgressDisplay":
        """Enter context manager - start progress display."""
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[cyan]{task.fields[folder_info]}"),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            refresh_per_second=4,
        )
        self._progress.__enter__()

        # Create the upload task
        self._upload_task = self._progress.add_task(
            "Preparing upload...",
            total=None,
            folder_info="0/0 files, 0 B/0 B",
        )

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager - stop progress display."""
        if self._progress is not None:
            # Update final description
            if self._upload_task is not None:
                self._progress.update(
                    self._upload_task,
                    description="Upload complete",
                )
            self._progress.__exit__(exc_type, exc_val, exc_tb)
            self._progress = None
            self._upload_task = None


def run_sync_with_progress(
    engine,
    pair,
    dry_run: bool,
    chunk_size: int,
    multipart_threshold: int,
    batch_size: int,
    use_streaming: bool,
    max_workers: int,
    start_delay: float,
) -> dict:
    """Run sync with a Rich progress display.

    This function creates a progress display context and runs the sync
    operation with progress tracking.

    Args:
        engine: SyncEngine instance
        pair: SyncPair to sync
        dry_run: If True, only show what would be done
        chunk_size: Chunk size for multipart uploads (bytes)
        multipart_threshold: Threshold for multipart upload (bytes)
        batch_size: Number of files per batch
        use_streaming: If True, use streaming mode
        max_workers: Number of parallel workers
        start_delay: Delay between parallel operations

    Returns:
        Dictionary with sync statistics
    """
    # For dry-run, don't show progress bar (just text output)
    if dry_run:
        return engine.sync_pair(
            pair,
            dry_run=dry_run,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            batch_size=batch_size,
            use_streaming=use_streaming,
            max_workers=max_workers,
            start_delay=start_delay,
        )

    # For actual sync, use progress display
    with SyncProgressDisplay() as display:
        tracker = display.create_tracker()

        return engine.sync_pair(
            pair,
            dry_run=dry_run,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            batch_size=batch_size,
            use_streaming=use_streaming,
            max_workers=max_workers,
            start_delay=start_delay,
            sync_progress_tracker=tracker,
        )
