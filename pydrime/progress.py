"""Progress tracking utilities for uploads and downloads."""

import threading
from pathlib import Path
from typing import Any, Callable, Optional

from rich.progress import TaskID


class UploadProgressTracker:
    """Thread-safe progress tracker for parallel uploads."""

    def __init__(self) -> None:
        """Initialize progress tracker."""
        self.lock = threading.Lock()
        self.bytes_uploaded = 0
        self.overall_task_id: Optional[TaskID] = None
        self.file_progress: dict[str, int] = {}

    def set_overall_task(self, task_id: TaskID) -> None:
        """Set the overall progress task ID.

        Args:
            task_id: Rich progress task ID
        """
        self.overall_task_id = task_id

    def create_file_callback(
        self, file_path: Path, progress_display: Any
    ) -> Callable[[int, int], None]:
        """Create a progress callback for a specific file.

        Args:
            file_path: Path to the file being uploaded
            progress_display: Rich Progress instance

        Returns:
            Progress callback function
        """
        file_key = str(file_path)
        last_bytes = {"value": 0}

        def progress_callback(bytes_uploaded: int, total_bytes: int) -> None:
            """Progress callback for file upload.

            Args:
                bytes_uploaded: Bytes uploaded so far
                total_bytes: Total bytes to upload
            """
            # Calculate increment
            increment = bytes_uploaded - last_bytes["value"]

            # Update overall progress (thread-safe)
            with self.lock:
                self.bytes_uploaded += increment
                self.file_progress[file_key] = bytes_uploaded

                if self.overall_task_id is not None:
                    progress_display.update(
                        self.overall_task_id,
                        completed=self.bytes_uploaded,
                    )

            last_bytes["value"] = bytes_uploaded

        return progress_callback

    def rollback_file_progress(self, file_path: Path, progress_display: Any) -> None:
        """Rollback progress for a failed file upload.

        Args:
            file_path: Path to the file that failed
            progress_display: Rich Progress instance
        """
        file_key = str(file_path)

        if file_key not in self.file_progress:
            return

        bytes_to_rollback = self.file_progress[file_key]

        with self.lock:
            self.bytes_uploaded -= bytes_to_rollback

            if self.overall_task_id is not None:
                progress_display.update(
                    self.overall_task_id,
                    completed=self.bytes_uploaded,
                )

            del self.file_progress[file_key]
