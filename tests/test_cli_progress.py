"""Tests for CLI progress tracking features."""

import io
from unittest.mock import patch

from pydrime.cli_progress import SimpleTextProgressDisplay


class TestSimpleTextProgressDisplay:
    """Tests for SimpleTextProgressDisplay class."""

    def test_initialization(self):
        """Test display initialization."""
        display = SimpleTextProgressDisplay()

        assert display._total_files == 0
        assert display._uploaded_files == 0
        assert display._total_bytes == 0
        assert display._uploaded_bytes == 0
        assert display._current_folder == ""

    def test_create_tracker(self):
        """Test creating a SyncProgressTracker."""
        display = SimpleTextProgressDisplay()

        tracker = display.create_tracker()

        # Verify tracker was created with callback
        assert tracker is not None
        assert hasattr(tracker, "callback")

    @patch("sys.stderr", new_callable=io.StringIO)
    def test_upload_batch_start(self, mock_stderr):
        """Test handling UPLOAD_BATCH_START event."""
        from syncengine.progress import SyncProgressEvent, SyncProgressInfo

        display = SimpleTextProgressDisplay()

        # Simulate batch start event
        info = SyncProgressInfo(
            event=SyncProgressEvent.UPLOAD_BATCH_START,
            directory="test_folder",
            folder_files_total=10,
            folder_bytes_total=1000000,
            folder_files_uploaded=0,
            folder_bytes_uploaded=0,
        )

        display._handle_event(info)

        output = mock_stderr.getvalue()
        assert "Starting upload: test_folder" in output
        assert "10 files" in output
        assert "1.0 MB" in output or "976.6 KB" in output  # Format may vary
        assert display._current_folder == "test_folder"
        assert display._total_files == 10
        assert display._total_bytes == 1000000

    @patch("sys.stderr", new_callable=io.StringIO)
    def test_upload_batch_start_root(self, mock_stderr):
        """Test handling batch start for root directory."""
        from syncengine.progress import SyncProgressEvent, SyncProgressInfo

        display = SimpleTextProgressDisplay()

        info = SyncProgressInfo(
            event=SyncProgressEvent.UPLOAD_BATCH_START,
            directory="",
            folder_files_total=5,
            folder_bytes_total=500000,
            folder_files_uploaded=0,
            folder_bytes_uploaded=0,
        )

        display._handle_event(info)

        output = mock_stderr.getvalue()
        assert "Starting upload: root" in output
        assert "5 files" in output

    @patch("sys.stderr", new_callable=io.StringIO)
    def test_upload_file_start(self, mock_stderr):
        """Test handling UPLOAD_FILE_START event."""
        from syncengine.progress import SyncProgressEvent, SyncProgressInfo

        display = SimpleTextProgressDisplay()

        info = SyncProgressInfo(
            event=SyncProgressEvent.UPLOAD_FILE_START,
            file_path="test_folder/file1.txt",
            folder_files_total=10,
            folder_bytes_total=1000000,
            folder_files_uploaded=0,
            folder_bytes_uploaded=0,
        )

        display._handle_event(info)

        output = mock_stderr.getvalue()
        assert "Uploading: test_folder/file1.txt" in output

    @patch("sys.stderr", new_callable=io.StringIO)
    def test_upload_file_complete(self, mock_stderr):
        """Test handling UPLOAD_FILE_COMPLETE event."""
        from syncengine.progress import SyncProgressEvent, SyncProgressInfo

        display = SimpleTextProgressDisplay()

        info = SyncProgressInfo(
            event=SyncProgressEvent.UPLOAD_FILE_COMPLETE,
            file_path="test_folder/file1.txt",
            folder_files_total=10,
            folder_bytes_total=1000000,
            folder_files_uploaded=1,
            folder_bytes_uploaded=100000,
        )

        display._handle_event(info)

        output = mock_stderr.getvalue()
        assert "✓ Completed: test_folder/file1.txt" in output
        assert "1/10 files" in output
        # Format may vary (97.7 KB vs 100.0 KB, 976.6 KB vs 1.0 MB)
        assert "KB" in output
        assert display._uploaded_files == 1
        assert display._uploaded_bytes == 100000

    @patch("sys.stderr", new_callable=io.StringIO)
    def test_upload_file_error(self, mock_stderr):
        """Test handling UPLOAD_FILE_ERROR event."""
        from syncengine.progress import SyncProgressEvent, SyncProgressInfo

        display = SimpleTextProgressDisplay()

        info = SyncProgressInfo(
            event=SyncProgressEvent.UPLOAD_FILE_ERROR,
            file_path="test_folder/file1.txt",
            error_message="Network error",
            folder_files_total=10,
            folder_bytes_total=1000000,
            folder_files_uploaded=0,
            folder_bytes_uploaded=0,
        )

        display._handle_event(info)

        output = mock_stderr.getvalue()
        assert "✗ Failed: test_folder/file1.txt" in output
        assert "Network error" in output

    @patch("sys.stderr", new_callable=io.StringIO)
    def test_upload_batch_complete(self, mock_stderr):
        """Test handling UPLOAD_BATCH_COMPLETE event."""
        from syncengine.progress import SyncProgressEvent, SyncProgressInfo

        display = SimpleTextProgressDisplay()

        info = SyncProgressInfo(
            event=SyncProgressEvent.UPLOAD_BATCH_COMPLETE,
            directory="test_folder",
            folder_files_total=10,
            folder_bytes_total=1000000,
            folder_files_uploaded=10,
            folder_bytes_uploaded=1000000,
        )

        display._handle_event(info)

        output = mock_stderr.getvalue()
        assert "Completed: test_folder" in output
        assert "10 files" in output
        assert "1.0 MB" in output or "976.6 KB" in output  # Format may vary

    @patch("sys.stderr", new_callable=io.StringIO)
    def test_complete_upload_flow(self, mock_stderr):
        """Test complete upload flow with multiple files."""
        from syncengine.progress import SyncProgressEvent, SyncProgressInfo

        display = SimpleTextProgressDisplay()

        # Start batch
        display._handle_event(
            SyncProgressInfo(
                event=SyncProgressEvent.UPLOAD_BATCH_START,
                directory="data",
                folder_files_total=3,
                folder_bytes_total=3000000,
                folder_files_uploaded=0,
                folder_bytes_uploaded=0,
            )
        )

        # Upload first file
        display._handle_event(
            SyncProgressInfo(
                event=SyncProgressEvent.UPLOAD_FILE_START,
                file_path="data/file1.txt",
                folder_files_total=3,
                folder_bytes_total=3000000,
                folder_files_uploaded=0,
                folder_bytes_uploaded=0,
            )
        )
        display._handle_event(
            SyncProgressInfo(
                event=SyncProgressEvent.UPLOAD_FILE_COMPLETE,
                file_path="data/file1.txt",
                folder_files_total=3,
                folder_bytes_total=3000000,
                folder_files_uploaded=1,
                folder_bytes_uploaded=1000000,
            )
        )

        # Upload second file
        display._handle_event(
            SyncProgressInfo(
                event=SyncProgressEvent.UPLOAD_FILE_START,
                file_path="data/file2.txt",
                folder_files_total=3,
                folder_bytes_total=3000000,
                folder_files_uploaded=1,
                folder_bytes_uploaded=1000000,
            )
        )
        display._handle_event(
            SyncProgressInfo(
                event=SyncProgressEvent.UPLOAD_FILE_COMPLETE,
                file_path="data/file2.txt",
                folder_files_total=3,
                folder_bytes_total=3000000,
                folder_files_uploaded=2,
                folder_bytes_uploaded=2000000,
            )
        )

        # Upload third file with error
        display._handle_event(
            SyncProgressInfo(
                event=SyncProgressEvent.UPLOAD_FILE_START,
                file_path="data/file3.txt",
                folder_files_total=3,
                folder_bytes_total=3000000,
                folder_files_uploaded=2,
                folder_bytes_uploaded=2000000,
            )
        )
        display._handle_event(
            SyncProgressInfo(
                event=SyncProgressEvent.UPLOAD_FILE_ERROR,
                file_path="data/file3.txt",
                error_message="Connection timeout",
                folder_files_total=3,
                folder_bytes_total=3000000,
                folder_files_uploaded=2,
                folder_bytes_uploaded=2000000,
            )
        )

        # Complete batch
        display._handle_event(
            SyncProgressInfo(
                event=SyncProgressEvent.UPLOAD_BATCH_COMPLETE,
                directory="data",
                folder_files_total=3,
                folder_bytes_total=3000000,
                folder_files_uploaded=2,
                folder_bytes_uploaded=2000000,
            )
        )

        output = mock_stderr.getvalue()

        # Verify all events were logged
        assert "Starting upload: data" in output
        assert "Uploading: data/file1.txt" in output
        assert "✓ Completed: data/file1.txt" in output
        assert "1/3 files" in output
        assert "Uploading: data/file2.txt" in output
        assert "✓ Completed: data/file2.txt" in output
        assert "2/3 files" in output
        assert "Uploading: data/file3.txt" in output
        assert "✗ Failed: data/file3.txt" in output
        assert "Connection timeout" in output
        assert "Completed: data" in output

    def test_context_manager(self):
        """Test display works as context manager."""
        display = SimpleTextProgressDisplay()

        with display as ctx:
            assert ctx is display

        # Should not raise any exceptions

    def test_context_manager_with_exception(self):
        """Test context manager handles exceptions gracefully."""
        display = SimpleTextProgressDisplay()

        try:
            with display:
                raise ValueError("Test error")
        except ValueError:
            pass  # Expected

        # Should not raise any additional exceptions
