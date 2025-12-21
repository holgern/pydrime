"""CLI progress display for sync operations.

This module provides Rich-based progress displays that work with
the SyncProgressTracker from the sync engine.
"""

import sys
from typing import Any, Optional

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
from syncengine.engine import SyncEngine  # type: ignore[import-not-found]
from syncengine.modes import SyncMode  # type: ignore[import-not-found]
from syncengine.pair import SyncPair  # type: ignore[import-not-found]
from syncengine.progress import (  # type: ignore[import-not-found]
    SyncProgressEvent,
    SyncProgressInfo,
    SyncProgressTracker,
)

from .file_entries_manager import FileEntriesManager
from .output import OutputFormatter


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
        self._is_download = False  # Track if we're downloading vs uploading

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

        # Handle upload events
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

        elif info.event == SyncProgressEvent.UPLOAD_FILE_START:
            # Update description when starting a file
            if self._upload_task is not None:
                folder_name = self._current_dir if self._current_dir else "root"
                self._progress.update(
                    self._upload_task,
                    description=f"Uploading: {folder_name}",
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

        # Handle download events (mirror upload logic)
        elif info.event == SyncProgressEvent.DOWNLOAD_BATCH_START:
            self._is_download = True
            self._current_dir = info.directory
            # Update task description and total for folder
            if self._upload_task is not None:
                folder_name = info.directory if info.directory else "root"
                self._progress.update(
                    self._upload_task,
                    description=f"Downloading: {folder_name}",
                    total=info.folder_bytes_total,
                    completed=0,
                    folder_info=self._format_folder_progress(info),
                )

        elif info.event == SyncProgressEvent.DOWNLOAD_FILE_START:
            # Update description when starting a file
            if self._upload_task is not None:
                folder_name = self._current_dir if self._current_dir else "root"
                self._progress.update(
                    self._upload_task,
                    description=f"Downloading: {folder_name}",
                )

        elif info.event == SyncProgressEvent.DOWNLOAD_FILE_PROGRESS:
            # Update bytes progress for current folder
            if self._upload_task is not None:
                self._progress.update(
                    self._upload_task,
                    completed=info.folder_bytes_uploaded,
                    folder_info=self._format_folder_progress(info),
                )

        elif info.event == SyncProgressEvent.DOWNLOAD_FILE_COMPLETE:
            # Update file count for folder
            self._total_files = info.files_uploaded
            if self._upload_task is not None:
                self._progress.update(
                    self._upload_task,
                    completed=info.folder_bytes_uploaded,
                    folder_info=self._format_folder_progress(info),
                )

        elif info.event == SyncProgressEvent.DOWNLOAD_BATCH_COMPLETE:
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

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Exit context manager - stop progress display."""
        if self._progress is not None:
            # Update final description based on operation type
            if self._upload_task is not None:
                completion_msg = (
                    "Download complete" if self._is_download else "Upload complete"
                )
                self._progress.update(
                    self._upload_task,
                    description=completion_msg,
                )
            self._progress.__exit__(exc_type, exc_val, exc_tb)
            self._progress = None
            self._upload_task = None


class SimpleTextProgressDisplay:
    """Simple text-based progress display without spinners or animations.

    This class is suitable for CI/CD environments, piped output, or when
    Rich's interactive display is not desired. It outputs one line per
    significant event (file completion, folder completion).
    """

    def __init__(self) -> None:
        """Initialize the simple text progress display."""
        self._total_files = 0
        self._uploaded_files = 0
        self._total_bytes = 0
        self._uploaded_bytes = 0
        self._current_folder = ""

    def create_tracker(self) -> SyncProgressTracker:
        """Create a SyncProgressTracker that updates this display.

        Returns:
            A configured SyncProgressTracker
        """
        return SyncProgressTracker(callback=self._handle_event)

    def _handle_event(self, info: SyncProgressInfo) -> None:
        """Handle a progress event from the tracker.

        Args:
            info: Progress information
        """
        # Handle upload events
        if info.event == SyncProgressEvent.UPLOAD_BATCH_START:
            self._current_folder = info.directory
            self._total_files = info.folder_files_total
            self._total_bytes = info.folder_bytes_total
            folder_name = info.directory if info.directory else "root"
            size_str = _format_size(info.folder_bytes_total)
            print(
                f"Starting upload: {folder_name} "
                f"({info.folder_files_total} files, {size_str})",
                file=sys.stderr,
            )

        elif info.event == SyncProgressEvent.UPLOAD_FILE_START:
            # Print when starting a new file
            print(f"  Uploading: {info.file_path}", file=sys.stderr)

        elif info.event == SyncProgressEvent.UPLOAD_FILE_COMPLETE:
            # Print completion with running totals
            self._uploaded_files = info.folder_files_uploaded
            self._uploaded_bytes = info.folder_bytes_uploaded
            print(
                f"  ✓ Completed: {info.file_path} "
                f"({info.folder_files_uploaded}/{info.folder_files_total} files, "
                f"{_format_size(info.folder_bytes_uploaded)}/{_format_size(info.folder_bytes_total)})",
                file=sys.stderr,
            )

        elif info.event == SyncProgressEvent.UPLOAD_FILE_ERROR:
            # Print error
            print(
                f"  ✗ Failed: {info.file_path} - {info.error_message}", file=sys.stderr
            )

        elif info.event == SyncProgressEvent.UPLOAD_BATCH_COMPLETE:
            # Print folder completion summary
            folder_name = info.directory if info.directory else "root"
            size_str = _format_size(info.folder_bytes_uploaded)
            print(
                f"Completed: {folder_name} "
                f"({info.folder_files_uploaded} files, {size_str})",
                file=sys.stderr,
            )

        # Handle download events (mirror upload logic)
        elif info.event == SyncProgressEvent.DOWNLOAD_BATCH_START:
            self._current_folder = info.directory
            self._total_files = info.folder_files_total
            self._total_bytes = info.folder_bytes_total
            folder_name = info.directory if info.directory else "root"
            size_str = _format_size(info.folder_bytes_total)
            print(
                f"Starting download: {folder_name} "
                f"({info.folder_files_total} files, {size_str})",
                file=sys.stderr,
            )

        elif info.event == SyncProgressEvent.DOWNLOAD_FILE_START:
            # Print when starting a new file
            print(f"  Downloading: {info.file_path}", file=sys.stderr)

        elif info.event == SyncProgressEvent.DOWNLOAD_FILE_COMPLETE:
            # Print completion with running totals
            self._uploaded_files = info.folder_files_uploaded
            self._uploaded_bytes = info.folder_bytes_uploaded
            print(
                f"  ✓ Completed: {info.file_path} "
                f"({info.folder_files_uploaded}/{info.folder_files_total} files, "
                f"{_format_size(info.folder_bytes_uploaded)}/{_format_size(info.folder_bytes_total)})",
                file=sys.stderr,
            )

        elif info.event == SyncProgressEvent.DOWNLOAD_FILE_ERROR:
            # Print error
            print(
                f"  ✗ Failed: {info.file_path} - {info.error_message}", file=sys.stderr
            )

        elif info.event == SyncProgressEvent.DOWNLOAD_BATCH_COMPLETE:
            # Print folder completion summary
            folder_name = info.directory if info.directory else "root"
            size_str = _format_size(info.folder_bytes_uploaded)
            print(
                f"Completed: {folder_name} "
                f"({info.folder_files_uploaded} files, {size_str})",
                file=sys.stderr,
            )

    def __enter__(self) -> "SimpleTextProgressDisplay":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Exit context manager."""
        pass


def run_sync_with_progress(
    engine: SyncEngine,
    pair: SyncPair,
    dry_run: bool,
    chunk_size: int,
    multipart_threshold: int,
    batch_size: int,
    use_streaming: bool,
    max_workers: int,
    start_delay: float,
    out: Optional[OutputFormatter] = None,
    initial_sync_preference: Optional[Any] = None,
) -> dict[str, Any]:
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
        out: Optional OutputFormatter for status messages (uses engine.output if None)
        initial_sync_preference: Optional preference for initial sync behavior

    Returns:
        Dictionary with sync statistics
    """
    # For dry-run, don't show progress bar (just text output)
    if dry_run:
        return engine.sync_pair(  # type: ignore[no-any-return]
            pair,
            dry_run=dry_run,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            batch_size=batch_size,
            use_streaming=use_streaming,
            max_workers=max_workers,
            start_delay=start_delay,
            initial_sync_preference=initial_sync_preference,
        )

    # Show remote folder status before starting progress bar
    # This is important context that shouldn't be hidden by the progress display
    if pair.sync_mode in (SyncMode.SOURCE_TO_DESTINATION, SyncMode.SOURCE_BACKUP):
        _show_remote_status_before_sync(engine, pair, out=out)

    # For actual sync, use progress display
    with SyncProgressDisplay() as display:
        tracker = display.create_tracker()

        return engine.sync_pair(  # type: ignore[no-any-return]
            pair,
            dry_run=dry_run,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            batch_size=batch_size,
            use_streaming=use_streaming,
            max_workers=max_workers,
            start_delay=start_delay,
            sync_progress_tracker=tracker,
            initial_sync_preference=initial_sync_preference,
        )


def _show_remote_status_before_sync(
    engine: SyncEngine,
    pair: SyncPair,
    out: Optional[OutputFormatter] = None,
) -> None:
    """Show remote folder status before starting the sync progress bar.

    This displays important context about what exists on the remote
    before the progress bar takes over the display.

    Args:
        engine: SyncEngine instance
        pair: SyncPair being synced
        out: Optional OutputFormatter to use (defaults to engine.output)
    """
    # Use provided output formatter or fall back to engine's
    if out is None:
        out = engine.output

    if out.quiet:
        return

    # Determine if syncing to root
    syncing_to_root = not pair.destination or pair.destination == "/"

    # Try to find remote folder and count files
    try:
        manager = FileEntriesManager(engine.client, pair.storage_id)

        # Find the remote folder (or use root)
        if syncing_to_root:
            # Syncing directly to root - use folder_id=0
            remote_folder_id = 0
            out.success("Syncing directly to cloud root")
        else:
            remote_folder_id = None
            effective_remote_name = pair.destination
            if "/" in effective_remote_name:
                # Nested path
                try:
                    remote_folder_id = engine.client.resolve_path_to_id(
                        effective_remote_name, workspace_id=pair.storage_id
                    )
                except Exception:
                    pass
            else:
                # Simple folder name
                folder_entry = manager.find_folder_by_name(
                    effective_remote_name, parent_id=0
                )
                if folder_entry:
                    remote_folder_id = folder_entry.id

            if remote_folder_id is not None:
                out.success(
                    f"Remote folder '{effective_remote_name}' exists "
                    f"(id: {remote_folder_id})"
                )
            else:
                out.info(f"Remote folder '{effective_remote_name}' will be created")
                out.print("")  # Empty line before progress bar
                return

        # Count existing files
        entries_with_paths = manager.get_all_recursive(
            folder_id=remote_folder_id,
            path_prefix="",
        )
        file_count = 0
        total_size = 0
        for entry, _ in entries_with_paths:
            if entry.type != "folder":
                file_count += 1
                if entry.file_size:
                    total_size += entry.file_size

        if file_count > 0:
            out.info(
                f"Found {file_count} file(s) already on remote "
                f"({_format_size(total_size)})"
            )
        else:
            out.info("No files found on remote yet")

        out.print("")  # Empty line before progress bar

    except Exception as e:
        # Don't fail the sync if we can't show status
        out.warning(f"Could not check remote status: {e}")
