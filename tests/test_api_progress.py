"""Tests for API progress tracking features."""

import io
from unittest.mock import MagicMock

from pydrime.api import _ProgressFileWrapper


class TestProgressFileWrapper:
    """Tests for _ProgressFileWrapper class."""

    def test_initialization(self):
        """Test wrapper initialization."""
        file_obj = io.BytesIO(b"test content")
        total_size = 12
        callback = MagicMock()

        wrapper = _ProgressFileWrapper(file_obj, total_size, callback)

        assert wrapper.file_obj == file_obj
        assert wrapper.total_size == total_size
        assert wrapper.progress_callback == callback
        assert wrapper.bytes_read == 0
        assert wrapper._last_reported_position == 0

    def test_read_basic(self):
        """Test basic read operation."""
        file_obj = io.BytesIO(b"test content")
        wrapper = _ProgressFileWrapper(file_obj, 12)

        data = wrapper.read(4)

        assert data == b"test"
        assert wrapper.bytes_read == 4

    def test_read_with_callback(self):
        """Test read with progress callback."""
        file_obj = io.BytesIO(b"test content")
        callback = MagicMock()
        wrapper = _ProgressFileWrapper(file_obj, 12, callback)

        data = wrapper.read(4)

        assert data == b"test"
        assert wrapper.bytes_read == 4
        # Callback should be called with current position (4) and total (12)
        callback.assert_called_once_with(4, 12)

    def test_read_incremental_with_callback(self):
        """Test incremental reads report progress correctly."""
        file_obj = io.BytesIO(b"test content")
        callback = MagicMock()
        wrapper = _ProgressFileWrapper(file_obj, 12, callback)

        # Read in chunks
        wrapper.read(4)  # "test"
        wrapper.read(1)  # " "
        wrapper.read(7)  # "content"

        # Should be called 3 times, once per read
        assert callback.call_count == 3
        # Check the calls - they should report cumulative position
        assert callback.call_args_list[0][0] == (4, 12)
        assert callback.call_args_list[1][0] == (5, 12)
        assert callback.call_args_list[2][0] == (12, 12)

    def test_read_all_at_once(self):
        """Test reading entire file at once."""
        file_obj = io.BytesIO(b"test content")
        callback = MagicMock()
        wrapper = _ProgressFileWrapper(file_obj, 12, callback)

        data = wrapper.read()  # Read all

        assert data == b"test content"
        assert wrapper.bytes_read == 12
        callback.assert_called_once_with(12, 12)

    def test_read_beyond_eof(self):
        """Test reading beyond end of file."""
        file_obj = io.BytesIO(b"test")
        callback = MagicMock()
        wrapper = _ProgressFileWrapper(file_obj, 4, callback)

        # Read entire file
        wrapper.read()
        callback.reset_mock()

        # Try to read more
        data = wrapper.read(10)

        assert data == b""
        # Should not call callback for empty read
        callback.assert_not_called()

    def test_read_without_callback(self):
        """Test read works without callback."""
        file_obj = io.BytesIO(b"test content")
        wrapper = _ProgressFileWrapper(file_obj, 12, None)

        data = wrapper.read(4)

        assert data == b"test"
        assert wrapper.bytes_read == 4
        # Should not crash without callback

    def test_seek_to_start(self):
        """Test seeking back to start resets tracking."""
        file_obj = io.BytesIO(b"test content")
        callback = MagicMock()
        wrapper = _ProgressFileWrapper(file_obj, 12, callback)

        # Read some data
        wrapper.read(4)
        assert wrapper._last_reported_position == 4

        # Seek back to start
        result = wrapper.seek(0, 0)

        assert result == 0
        assert wrapper._last_reported_position == 0

    def test_seek_forward(self):
        """Test seeking forward."""
        file_obj = io.BytesIO(b"test content")
        wrapper = _ProgressFileWrapper(file_obj, 12)

        result = wrapper.seek(5)

        assert result == 5
        assert file_obj.tell() == 5

    def test_seek_relative(self):
        """Test relative seeking."""
        file_obj = io.BytesIO(b"test content")
        wrapper = _ProgressFileWrapper(file_obj, 12)

        # Read some data
        wrapper.read(4)

        # Seek relative to current position
        result = wrapper.seek(2, 1)

        assert result == 6
        assert file_obj.tell() == 6

    def test_tell(self):
        """Test tell returns current position."""
        file_obj = io.BytesIO(b"test content")
        wrapper = _ProgressFileWrapper(file_obj, 12)

        wrapper.read(4)

        position = wrapper.tell()

        assert position == 4

    def test_reread_after_seek(self):
        """Test re-reading file after seek (simulating httpx behavior)."""
        file_obj = io.BytesIO(b"test content")
        callback = MagicMock()
        wrapper = _ProgressFileWrapper(file_obj, 12, callback)

        # First read (httpx calculating content-length)
        wrapper.read()
        assert callback.call_count == 1
        assert callback.call_args_list[0][0] == (12, 12)

        # Seek back to start (httpx preparing for actual upload)
        wrapper.seek(0, 0)
        callback.reset_mock()

        # Second read (actual upload)
        wrapper.read(4)
        wrapper.read(8)

        # Should report progress again
        assert callback.call_count == 2
        assert callback.call_args_list[0][0] == (4, 12)
        assert callback.call_args_list[1][0] == (12, 12)

    def test_only_reports_forward_progress(self):
        """Test wrapper only reports progress when moving forward."""
        file_obj = io.BytesIO(b"test content")
        callback = MagicMock()
        wrapper = _ProgressFileWrapper(file_obj, 12, callback)

        # Read forward
        wrapper.read(8)
        assert callback.call_count == 1

        # Seek backward
        wrapper.seek(4)
        callback.reset_mock()

        # Read forward again, but not past previous max position
        wrapper.read(2)  # Now at position 6, which is < 8

        # Should not report since we haven't exceeded previous max
        callback.assert_not_called()

        # Now read past previous max
        wrapper.read(3)  # Now at position 9, which is > 8

        # Should report progress now
        callback.assert_called_once()
        assert callback.call_args[0] == (9, 12)

    def test_attribute_forwarding(self):
        """Test that unknown attributes are forwarded to wrapped file."""
        file_obj = io.BytesIO(b"test content")
        wrapper = _ProgressFileWrapper(file_obj, 12)

        # Access attributes that exist on BytesIO but not on wrapper
        assert hasattr(wrapper, "getvalue")
        assert wrapper.getvalue() == b"test content"

        assert hasattr(wrapper, "readable")
        assert wrapper.readable() is True

        assert hasattr(wrapper, "writable")
        assert wrapper.writable() is True

    def test_large_file_simulation(self):
        """Test with larger file to simulate real upload scenario."""
        # Create a 1MB file
        content = b"x" * (1024 * 1024)
        file_obj = io.BytesIO(content)
        callback = MagicMock()
        wrapper = _ProgressFileWrapper(file_obj, len(content), callback)

        # Read in 64KB chunks (typical buffer size)
        chunk_size = 64 * 1024
        chunks_read = 0
        while True:
            chunk = wrapper.read(chunk_size)
            if not chunk:
                break
            chunks_read += 1

        # Should have read ~16 chunks (1MB / 64KB)
        assert chunks_read == 16
        assert callback.call_count == 16

        # Last call should report 100% progress
        last_call = callback.call_args_list[-1][0]
        assert last_call[0] == len(content)
        assert last_call[1] == len(content)


class TestProgressFileWrapperIntegration:
    """Integration tests for _ProgressFileWrapper with real upload scenarios."""

    def test_with_tempfile(self, tmp_path):
        """Test wrapper with actual temporary file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!")

        callback = MagicMock()

        with open(test_file, "rb") as f:
            wrapper = _ProgressFileWrapper(f, 13, callback)
            data = wrapper.read()

        assert data == b"Hello, World!"
        callback.assert_called_with(13, 13)

    def test_concurrent_reads(self, tmp_path):
        """Test that wrapper handles multiple wrappers on same file correctly."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        callback1 = MagicMock()
        callback2 = MagicMock()

        with open(test_file, "rb") as f1, open(test_file, "rb") as f2:
            wrapper1 = _ProgressFileWrapper(f1, 12, callback1)
            wrapper2 = _ProgressFileWrapper(f2, 12, callback2)

            # Read from both wrappers
            wrapper1.read(6)
            wrapper2.read(4)

        # Each wrapper should track independently
        assert callback1.call_count == 1
        assert callback1.call_args[0] == (6, 12)

        assert callback2.call_count == 1
        assert callback2.call_args[0] == (4, 12)
