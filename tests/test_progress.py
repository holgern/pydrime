"""Tests for progress tracking utilities."""

from pathlib import Path
from unittest.mock import MagicMock

from pydrime.progress import UploadProgressTracker


class TestUploadProgressTracker:
    """Tests for UploadProgressTracker class."""

    def test_initialization(self):
        """Test tracker initialization."""
        tracker = UploadProgressTracker()

        assert tracker.bytes_uploaded == 0
        assert tracker.overall_task_id is None
        assert tracker.file_progress == {}
        assert tracker.lock is not None

    def test_set_overall_task(self):
        """Test setting overall task ID."""
        tracker = UploadProgressTracker()
        task_id = "test-task-id"

        tracker.set_overall_task(task_id)

        assert tracker.overall_task_id == task_id

    def test_create_file_callback_basic(self):
        """Test creating a basic file callback."""
        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        file_path = Path("/test/file.txt")
        task_id = "task-123"

        tracker.set_overall_task(task_id)
        callback = tracker.create_file_callback(file_path, progress_display)

        # Verify callback is callable
        assert callable(callback)

        # Simulate progress
        callback(100, 1000)

        # Check bytes_uploaded updated
        assert tracker.bytes_uploaded == 100
        assert tracker.file_progress[str(file_path)] == 100

        # Verify progress display was updated
        progress_display.update.assert_called_once_with(task_id, completed=100)

    def test_create_file_callback_incremental_updates(self):
        """Test callback handles incremental updates correctly."""
        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        file_path = Path("/test/file.txt")
        task_id = "task-123"

        tracker.set_overall_task(task_id)
        callback = tracker.create_file_callback(file_path, progress_display)

        # Simulate incremental progress
        callback(100, 1000)
        assert tracker.bytes_uploaded == 100

        callback(250, 1000)
        assert tracker.bytes_uploaded == 250

        callback(500, 1000)
        assert tracker.bytes_uploaded == 500

        callback(1000, 1000)
        assert tracker.bytes_uploaded == 1000

        # Should have called update 4 times
        assert progress_display.update.call_count == 4

    def test_create_file_callback_multiple_files(self):
        """Test tracking multiple files simultaneously."""
        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        task_id = "task-123"

        tracker.set_overall_task(task_id)

        # Create callbacks for two files
        file1 = Path("/test/file1.txt")
        file2 = Path("/test/file2.txt")

        callback1 = tracker.create_file_callback(file1, progress_display)
        callback2 = tracker.create_file_callback(file2, progress_display)

        # Upload both files in parallel
        callback1(500, 1000)
        callback2(300, 1000)

        # Total bytes should be sum of both
        assert tracker.bytes_uploaded == 800
        assert tracker.file_progress[str(file1)] == 500
        assert tracker.file_progress[str(file2)] == 300

        # Continue uploading
        callback1(1000, 1000)
        callback2(600, 1000)

        assert tracker.bytes_uploaded == 1600
        assert tracker.file_progress[str(file1)] == 1000
        assert tracker.file_progress[str(file2)] == 600

    def test_create_file_callback_no_task_id(self):
        """Test callback works when no overall task ID is set."""
        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        file_path = Path("/test/file.txt")

        # Don't set task ID
        callback = tracker.create_file_callback(file_path, progress_display)

        # Simulate progress
        callback(100, 1000)

        # Bytes should still be tracked
        assert tracker.bytes_uploaded == 100
        assert tracker.file_progress[str(file_path)] == 100

        # But progress display should not be updated
        progress_display.update.assert_not_called()

    def test_rollback_file_progress(self):
        """Test rolling back file progress on failure."""
        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        file_path = Path("/test/file.txt")
        task_id = "task-123"

        tracker.set_overall_task(task_id)
        callback = tracker.create_file_callback(file_path, progress_display)

        # Upload some bytes
        callback(500, 1000)
        assert tracker.bytes_uploaded == 500

        # Reset mock to check rollback call
        progress_display.reset_mock()

        # Rollback due to failure
        tracker.rollback_file_progress(file_path, progress_display)

        # Bytes should be rolled back
        assert tracker.bytes_uploaded == 0
        assert str(file_path) not in tracker.file_progress

        # Progress display should be updated with rolled back value
        progress_display.update.assert_called_once_with(task_id, completed=0)

    def test_rollback_file_progress_multiple_files(self):
        """Test rollback doesn't affect other files."""
        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        task_id = "task-123"

        tracker.set_overall_task(task_id)

        # Track two files
        file1 = Path("/test/file1.txt")
        file2 = Path("/test/file2.txt")

        callback1 = tracker.create_file_callback(file1, progress_display)
        callback2 = tracker.create_file_callback(file2, progress_display)

        callback1(500, 1000)
        callback2(300, 1000)

        assert tracker.bytes_uploaded == 800

        # Rollback only file1
        progress_display.reset_mock()
        tracker.rollback_file_progress(file1, progress_display)

        # Only file1's bytes should be rolled back
        assert tracker.bytes_uploaded == 300
        assert str(file1) not in tracker.file_progress
        assert tracker.file_progress[str(file2)] == 300

        # Progress display should be updated
        progress_display.update.assert_called_once_with(task_id, completed=300)

    def test_rollback_file_progress_nonexistent_file(self):
        """Test rollback handles nonexistent file gracefully."""
        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        file_path = Path("/test/file.txt")
        task_id = "task-123"

        tracker.set_overall_task(task_id)

        # Try to rollback a file that was never tracked
        tracker.rollback_file_progress(file_path, progress_display)

        # Should not crash and should not update progress
        assert tracker.bytes_uploaded == 0
        progress_display.update.assert_not_called()

    def test_rollback_file_progress_no_task_id(self):
        """Test rollback works when no overall task ID is set."""
        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        file_path = Path("/test/file.txt")

        # Don't set task ID
        callback = tracker.create_file_callback(file_path, progress_display)

        # Upload some bytes
        callback(500, 1000)
        assert tracker.bytes_uploaded == 500

        # Rollback
        tracker.rollback_file_progress(file_path, progress_display)

        # Bytes should be rolled back
        assert tracker.bytes_uploaded == 0
        assert str(file_path) not in tracker.file_progress

        # But progress display should not be updated
        progress_display.update.assert_not_called()

    def test_thread_safety(self):
        """Test that operations are thread-safe (basic check)."""
        import threading

        tracker = UploadProgressTracker()
        progress_display = MagicMock()
        task_id = "task-123"

        tracker.set_overall_task(task_id)

        # Create callbacks for multiple files
        files = [Path(f"/test/file{i}.txt") for i in range(10)]
        callbacks = [tracker.create_file_callback(f, progress_display) for f in files]

        # Simulate concurrent uploads
        threads = []
        for callback in callbacks:

            def upload(cb=callback):
                for j in range(10):
                    cb(j * 100, 1000)

            thread = threading.Thread(target=upload)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Final total should be consistent (each file uploaded 900 bytes)
        assert tracker.bytes_uploaded == 10 * 900
        assert len(tracker.file_progress) == 10
        for file_path in files:
            assert tracker.file_progress[str(file_path)] == 900
