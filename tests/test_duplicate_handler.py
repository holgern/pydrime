"""Tests for duplicate file handler."""

from typing import Optional
from unittest.mock import MagicMock

from pydrime.duplicate_handler import DuplicateHandler
from pydrime.exceptions import DrimeAPIError
from pydrime.models import FileEntry
from pydrime.output import OutputFormatter


class TestDuplicateHandlerInit:
    """Tests for DuplicateHandler initialization."""

    def test_initialization_with_ask_mode(self):
        """Test initialization with ask mode."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)

        handler = DuplicateHandler(mock_client, out, 0, "ask")

        assert handler.client == mock_client
        assert handler.out == out
        assert handler.workspace_id == 0
        assert handler.on_duplicate == "ask"
        assert (
            handler.chosen_action is None
        )  # Should be None, will be set when user chooses
        assert handler.apply_to_all is False
        assert len(handler.files_to_skip) == 0
        assert len(handler.rename_map) == 0

    def test_initialization_with_skip_mode(self):
        """Test initialization with skip mode."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)

        handler = DuplicateHandler(mock_client, out, 0, "skip")

        assert handler.chosen_action == "skip"
        assert handler.apply_to_all is True

    def test_initialization_with_replace_mode(self):
        """Test initialization with replace mode."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)

        handler = DuplicateHandler(mock_client, out, 5, "replace")

        assert handler.chosen_action == "replace"
        assert handler.apply_to_all is True
        assert handler.workspace_id == 5


class TestValidateAndHandleDuplicates:
    """Tests for validate_and_handle_duplicates method."""

    def test_no_duplicates_does_nothing(self, tmp_path):
        """Test when there are no duplicates."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": []}
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        assert len(handler.files_to_skip) == 0
        assert len(handler.rename_map) == 0

    def test_filters_folder_duplicates(self, tmp_path):
        """Test filters out folder duplicates."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {
            "duplicates": ["folder1", "file1.txt"]
        }
        # Mock folder detection
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "folder1",
                    "type": "folder",
                    "hash": "hash1",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                }
            ]
        }

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "skip")

        test_file = tmp_path / "file1.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "folder1/file1.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # folder1 should be filtered out, only file1.txt should be processed
        assert "folder1/file1.txt" in handler.files_to_skip

    def test_handles_api_error_gracefully(self, tmp_path):
        """Test handles API error during validation gracefully."""
        mock_client = MagicMock()
        mock_client.validate_uploads.side_effect = DrimeAPIError("API Error")
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        # Should not raise an exception
        handler.validate_and_handle_duplicates(files_to_upload)


class TestHandleSkip:
    """Tests for skip action."""

    def test_skip_marks_files_for_skipping(self, tmp_path):
        """Test skip action marks files for skipping."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": ["test.txt"]}
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "skip")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        assert "test.txt" in handler.files_to_skip


class TestHandleRename:
    """Tests for rename action."""

    def test_rename_gets_available_name(self, tmp_path):
        """Test rename action gets available name from API."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": ["test.txt"]}
        mock_client.get_available_name.return_value = "test (1).txt"
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "rename")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        assert handler.rename_map["test.txt"] == "test (1).txt"
        mock_client.get_available_name.assert_called_once_with(
            "test.txt", workspace_id=0
        )

    def test_rename_falls_back_to_skip_on_error(self, tmp_path):
        """Test rename falls back to skip on API error."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": ["test.txt"]}
        mock_client.get_available_name.side_effect = DrimeAPIError("API Error")
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "rename")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # Should fall back to skip
        assert "test.txt" in handler.files_to_skip
        assert "test.txt" not in handler.rename_map


class TestHandleReplace:
    """Tests for replace action."""

    def test_replace_does_not_mark_for_skipping(self, tmp_path):
        """Test replace action does NOT mark files for skipping.

        The API handles replacement automatically when uploading a file
        with the same name - no need to delete first.
        """
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": ["test.txt"]}
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "test.txt",
                    "type": "image",
                    "hash": "hash123",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                }
            ]
        }
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "replace")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # File should NOT be skipped - it will be uploaded and API handles replacement
        assert "test.txt" not in handler.files_to_skip


class TestApplyRenames:
    """Tests for apply_renames method."""

    def test_apply_renames_to_filename(self):
        """Test apply renames to filename."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "rename")
        handler.rename_map = {"test.txt": "test (1).txt"}

        result = handler.apply_renames("test.txt")

        assert result == "test (1).txt"

    def test_apply_renames_to_file_in_folder(self):
        """Test apply renames to file in folder."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "rename")
        handler.rename_map = {"test.txt": "test (1).txt"}

        result = handler.apply_renames("folder/test.txt")

        assert result == "folder/test (1).txt"

    def test_apply_renames_to_folder(self):
        """Test apply renames to folder name in path."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "rename")
        handler.rename_map = {"folder": "folder (1)"}

        result = handler.apply_renames("folder/test.txt")

        assert result == "folder (1)/test.txt"

    def test_apply_no_renames(self):
        """Test when no renames are needed."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "rename")
        handler.rename_map = {}

        result = handler.apply_renames("test.txt")

        assert result == "test.txt"

    def test_apply_multiple_renames_in_path(self):
        """Test apply multiple renames in path."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "rename")
        handler.rename_map = {"folder": "folder (1)", "test.txt": "test (1).txt"}

        result = handler.apply_renames("folder/subfolder/test.txt")

        assert result == "folder (1)/subfolder/test (1).txt"


class TestParentFolderContext:
    """Tests for parent folder context in duplicate detection."""

    def test_initialization_with_parent_id(self):
        """Test initialization with parent_id parameter."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)

        handler = DuplicateHandler(mock_client, out, 0, "ask", parent_id=123)

        assert handler.parent_id == 123

    def test_initialization_without_parent_id(self):
        """Test initialization without parent_id (defaults to None)."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)

        handler = DuplicateHandler(mock_client, out, 0, "ask")

        assert handler.parent_id is None

    def test_resolve_parent_folder_id_simple_path(self):
        """Test resolving a simple folder path to ID."""
        mock_client = MagicMock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 100,
                    "name": "backup",
                    "type": "folder",
                    "hash": "hash1",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                }
            ]
        }

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        folder_id = handler._resolve_parent_folder_id("backup")

        assert folder_id == 100

    def test_resolve_parent_folder_id_nested_path(self):
        """Test resolving a nested folder path to ID with proper parent context."""
        mock_client = MagicMock()

        # Create mock FileEntry objects for folder resolution
        backup_folder = FileEntry(
            id=100,
            name="backup",
            file_name="backup",
            type="folder",
            hash="hash1",
            mime="",
            file_size=0,
            parent_id=0,
            created_at="2023-01-01",
            updated_at="2023-01-01",
            extension=None,
            url="",
        )
        data_folder = FileEntry(
            id=200,
            name="data",
            file_name="data",
            type="folder",
            hash="hash2",
            mime="",
            file_size=0,
            parent_id=100,
            created_at="2023-01-01",
            updated_at="2023-01-01",
            extension=None,
            url="",
        )

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        # Mock the entries_manager.find_folder_by_name method
        def mock_find_folder(
            folder_name: str,
            parent_id: Optional[int] = None,
            search_in_root: bool = True,
        ):
            if folder_name == "backup" and parent_id is None:
                return backup_folder
            if folder_name == "data" and parent_id == 100:
                return data_folder
            return None

        handler.entries_manager.find_folder_by_name = mock_find_folder

        folder_id = handler._resolve_parent_folder_id("backup/data")

        assert folder_id == 200

    def test_resolve_parent_folder_id_not_found(self):
        """Test resolving folder path when folder doesn't exist."""
        mock_client = MagicMock()
        mock_client.get_file_entries.return_value = {"data": []}

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        folder_id = handler._resolve_parent_folder_id("nonexistent")

        assert folder_id is None

    def test_resolve_parent_folder_id_api_error(self):
        """Test resolving folder path when API error occurs."""
        mock_client = MagicMock()
        mock_client.get_file_entries.side_effect = DrimeAPIError("API Error")

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        folder_id = handler._resolve_parent_folder_id("backup")

        assert folder_id is None

    def test_lookup_duplicate_ids_with_parent_context(self, tmp_path):
        """Test that duplicate IDs are looked up in correct parent folder."""
        mock_client = MagicMock()

        # Mock get_file_entries to return different results based on parent_ids
        def mock_get_entries(query=None, parent_ids=None, workspace_id=0, **kwargs):
            if parent_ids == [100]:
                # Return file in specific folder
                return {
                    "data": [
                        {
                            "id": 999,
                            "name": "test.txt",
                            "type": "text",
                            "hash": "hash1",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 100,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                            "path": "/backup/test.txt",
                        }
                    ]
                }
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_entries

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask", parent_id=100)

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        result = handler._lookup_duplicate_ids(["test.txt"], files_to_upload)

        assert "test.txt" in result
        assert len(result["test.txt"]) == 1
        assert result["test.txt"][0][0] == 999
        assert result["test.txt"][0][1] == "/backup/test.txt"

    def test_lookup_duplicate_ids_in_subfolder(self, tmp_path):
        """Test duplicate IDs lookup for file in subfolder."""
        mock_client = MagicMock()

        def mock_get_entries(query=None, parent_ids=None, workspace_id=0, **kwargs):
            if query == "backup":
                return {
                    "data": [
                        {
                            "id": 100,
                            "name": "backup",
                            "type": "folder",
                            "hash": "hash1",
                            "mime": None,
                            "file_size": 0,
                            "parent_id": 0,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                        }
                    ]
                }
            elif parent_ids == [100]:
                # Return file in backup folder
                return {
                    "data": [
                        {
                            "id": 555,
                            "name": "test.txt",
                            "type": "text",
                            "hash": "hash2",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 100,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                            "path": "/backup/test.txt",
                        }
                    ]
                }
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_entries

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask", parent_id=None)

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "backup/test.txt")]

        result = handler._lookup_duplicate_ids(["test.txt"], files_to_upload)

        assert "test.txt" in result
        assert len(result["test.txt"]) == 1
        assert result["test.txt"][0][0] == 555

    def test_lookup_duplicate_ids_multiple_in_same_folder(self, tmp_path):
        """Test that only IDs from target folder are returned, not all."""
        mock_client = MagicMock()

        def mock_get_entries(query=None, parent_ids=None, workspace_id=0, **kwargs):
            if parent_ids == [200]:
                # Return only files in the specific target folder
                return {
                    "data": [
                        {
                            "id": 333,
                            "name": "test.txt",
                            "type": "text",
                            "hash": "hash1",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 200,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                            "path": "/backup/test.txt",
                        }
                    ]
                }
            elif query == "test.txt":
                # Global search would return many results
                return {
                    "data": [
                        {
                            "id": 111,
                            "name": "test.txt",
                            "type": "text",
                            "hash": "hash1",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 10,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                            "path": "/folder1/test.txt",
                        },
                        {
                            "id": 222,
                            "name": "test.txt",
                            "type": "text",
                            "hash": "hash2",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 20,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                            "path": "/folder2/test.txt",
                        },
                        {
                            "id": 333,
                            "name": "test.txt",
                            "type": "text",
                            "hash": "hash3",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 200,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                            "path": "/backup/test.txt",
                        },
                    ]
                }
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_entries

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask", parent_id=200)

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        result = handler._lookup_duplicate_ids(["test.txt"], files_to_upload)

        # Should only return ID 333 from parent_id=200, not all IDs
        assert "test.txt" in result
        assert len(result["test.txt"]) == 1
        assert result["test.txt"][0][0] == 333
        # Should NOT include IDs 111 and 222 from other folders

    def test_lookup_duplicate_ids_fallback_to_global_search(self, tmp_path):
        """Test fallback to global search when parent resolution fails."""
        mock_client = MagicMock()

        def mock_get_entries(query=None, parent_ids=None, workspace_id=0, **kwargs):
            if query == "nonexistent":
                # Folder doesn't exist, will return None from resolve
                return {"data": []}
            elif query == "test.txt":
                # Fallback to global search
                return {
                    "data": [
                        {
                            "id": 777,
                            "name": "test.txt",
                            "type": "text",
                            "hash": "hash1",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 0,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                        }
                    ]
                }
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_entries

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask", parent_id=None)

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "nonexistent/test.txt")]

        result = handler._lookup_duplicate_ids(["test.txt"], files_to_upload)

        # Should fallback to global search and find the file
        assert "test.txt" in result
        assert len(result["test.txt"]) == 1
        assert result["test.txt"][0][0] == 777


class TestDisplayDuplicates:
    """Tests for _display_duplicate_summary method."""

    def test_display_duplicates_with_multiple_ids(self, tmp_path):
        """Test displaying duplicates with multiple IDs."""

        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        file_duplicates = ["test.txt"]
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]] = {
            "test.txt": [(123, "/path1/test.txt"), (456, "/path2/test.txt")]
        }

        # This should exercise lines 165-166 (multiple IDs display)
        handler._display_duplicate_summary(
            file_duplicates, duplicate_info, files_to_upload
        )

    def test_display_duplicates_with_single_id(self, tmp_path):
        """Test displaying duplicates with single ID."""

        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        file_duplicates = ["test.txt"]
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]] = {
            "test.txt": [(789, "/path/test.txt")]
        }

        # This should exercise line 163 (single ID display)
        handler._display_duplicate_summary(
            file_duplicates, duplicate_info, files_to_upload
        )

    def test_display_duplicates_without_ids(self, tmp_path):
        """Test displaying duplicates without ID info."""

        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        file_duplicates = ["test.txt"]
        duplicate_info: dict[str, list[tuple[int, Optional[str]]]] = {}

        # This should exercise line 168 (no ID display)
        handler._display_duplicate_summary(
            file_duplicates, duplicate_info, files_to_upload
        )


class TestFolderResolution:
    """Tests for folder resolution caching and edge cases."""

    def test_resolve_parent_folder_id_with_cache_hit(self):
        """Test folder resolution uses cache when available."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        # Pre-populate cache
        handler._folder_id_cache["backup"] = 100

        folder_id = handler._resolve_parent_folder_id("backup")

        assert folder_id == 100
        # Should not call API if cache hit
        mock_client.get_file_entries.assert_not_called()

    def test_resolve_parent_folder_id_cached_none(self):
        """Test folder resolution returns None from cache."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        # Cache shows folder doesn't exist
        handler._folder_id_cache["nonexistent"] = None

        folder_id = handler._resolve_parent_folder_id("nonexistent")

        assert folder_id is None
        mock_client.get_file_entries.assert_not_called()

    def test_resolve_parent_folder_id_partial_cache_miss(self):
        """Test nested folder resolution with partial cache."""
        mock_client = MagicMock()

        # Create mock FileEntry for subfolder
        subfolder = FileEntry(
            id=200,
            name="subfolder",
            file_name="subfolder",
            type="folder",
            hash="hash2",
            mime="",
            file_size=0,
            parent_id=100,
            created_at="2023-01-01",
            updated_at="2023-01-01",
            extension=None,
            url="",
        )

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        # Mock the entries_manager.find_folder_by_name method
        def mock_find_folder(
            folder_name: str,
            parent_id: Optional[int] = None,
            search_in_root: bool = True,
        ):
            if folder_name == "subfolder" and parent_id == 100:
                return subfolder
            return None

        handler.entries_manager.find_folder_by_name = mock_find_folder

        # Pre-cache the "backup" folder (lines 440-445 tested)
        handler._folder_id_cache["backup"] = 100

        folder_id = handler._resolve_parent_folder_id("backup/subfolder")

        assert folder_id == 200
        # Should use cached "backup" and only query for "subfolder"
        assert handler._folder_id_cache["backup/subfolder"] == 200

    def test_resolve_parent_folder_id_partial_cache_none(self):
        """Test nested path resolution when intermediate folder is cached as None."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        # Cache shows "backup" doesn't exist (lines 442-444)
        handler._folder_id_cache["backup"] = None

        folder_id = handler._resolve_parent_folder_id("backup/subfolder")

        # Should return None without further API calls
        assert folder_id is None
        assert handler._folder_id_cache["backup/subfolder"] is None
        mock_client.get_file_entries.assert_not_called()

    def test_resolve_parent_folder_id_api_error_during_resolution(self):
        """Test folder resolution handles API error gracefully (lines 460-463)."""
        mock_client = MagicMock()
        mock_client.get_file_entries.side_effect = DrimeAPIError("API Error")

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        folder_id = handler._resolve_parent_folder_id("backup")

        # Should cache None and return None on API error
        assert folder_id is None
        assert handler._folder_id_cache["backup"] is None


class TestBatchCheckFolders:
    """Tests for _batch_check_folders method (performance optimization)."""

    def test_batch_check_folders_with_all_cached(self):
        """Test batch check uses cache when all names are cached."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

        # Pre-populate cache
        handler._folder_id_cache["is_folder:folder1"] = 100
        handler._folder_id_cache["is_folder:folder2"] = 200
        handler._folder_id_cache["is_folder:file.txt"] = None

        result = handler._batch_check_folders(["folder1", "folder2", "file.txt"])

        assert result == {"folder1", "folder2"}
        # Should not call entries_manager if all cached
        assert not hasattr(handler.entries_manager, "get_all_in_folder_called")

    def test_batch_check_folders_optimized_single_api_call(self):
        """Test batch check uses individual lookups for specific names."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask", parent_id=0)

        # Mock find_folder_by_name to return folders
        def mock_find_folder(name, parent_id=None):
            if name == "folder1":
                return FileEntry(
                    id=100,
                    name="folder1",
                    file_name="folder1",
                    mime="",
                    file_size=0,
                    parent_id=0,
                    created_at="2023-01-01",
                    type="folder",
                    extension=None,
                    hash="hash1",
                    url="",
                )
            elif name == "folder2":
                return FileEntry(
                    id=200,
                    name="folder2",
                    file_name="folder2",
                    mime="",
                    file_size=0,
                    parent_id=0,
                    created_at="2023-01-01",
                    type="folder",
                    extension=None,
                    hash="hash2",
                    url="",
                )
            return None

        handler.entries_manager.find_folder_by_name = MagicMock(
            side_effect=mock_find_folder
        )

        result = handler._batch_check_folders(["folder1", "folder2", "file.txt"])

        assert result == {"folder1", "folder2"}
        # Should make 3 individual lookups (one for each name)
        assert handler.entries_manager.find_folder_by_name.call_count == 3
        # Should cache results
        assert handler._folder_id_cache["is_folder:folder1"] == 100
        assert handler._folder_id_cache["is_folder:folder2"] == 200
        assert handler._folder_id_cache["is_folder:file.txt"] is None

    def test_batch_check_folders_fallback_on_api_error(self):
        """Test batch check falls back to individual searches on API error."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask", parent_id=0)

        # Mock get_all_in_folder to raise API error
        handler.entries_manager.get_all_in_folder = MagicMock(
            side_effect=DrimeAPIError("API Error")
        )

        # Mock find_folder_by_name for fallback
        def mock_find_folder_by_name(name, parent_id=None):
            if name == "folder1":
                return FileEntry(
                    id=100,
                    name="folder1",
                    file_name="folder1",
                    mime="",
                    file_size=0,
                    parent_id=0,
                    created_at="2023-01-01",
                    type="folder",
                    extension=None,
                    hash="hash1",
                    url="",
                )
            return None

        handler.entries_manager.find_folder_by_name = MagicMock(
            side_effect=mock_find_folder_by_name
        )

        result = handler._batch_check_folders(["folder1", "file.txt"])

        assert result == {"folder1"}
        # Should fall back to individual searches
        assert handler.entries_manager.find_folder_by_name.call_count == 2
        # Should cache results
        assert handler._folder_id_cache["is_folder:folder1"] == 100
        assert handler._folder_id_cache["is_folder:file.txt"] is None


class TestUnicodeFolderDuplicates:
    """Tests for Unicode folder name handling in duplicate detection.

    This tests the bug where files in folders with different Unicode names
    (e.g., 'u' vs 'ü') are incorrectly treated as duplicates.
    """

    def test_different_unicode_folders_not_marked_as_duplicates(self, tmp_path):
        """Test that files in 'u/' and 'ü/' are not both marked as duplicates.

        When the server reports 'file.txt' as a duplicate (because it exists
        in folder 'u'), the handler should NOT mark 'ü/file.txt' as a duplicate
        since it's in a different folder.
        """
        mock_client = MagicMock()

        # Server reports "file.txt" as duplicate (exists in 'u' folder)
        mock_client.validate_uploads.return_value = {"duplicates": ["file.txt"]}

        # Mock folder lookups - 'u' folder exists, 'ü' folder doesn't
        def mock_get_entries(query=None, parent_ids=None, workspace_id=0, **kwargs):
            if query == "u":
                return {
                    "data": [
                        {
                            "id": 100,
                            "name": "u",
                            "type": "folder",
                            "hash": "hash1",
                            "mime": None,
                            "file_size": 0,
                            "parent_id": 0,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                        }
                    ]
                }
            elif query == "ü":
                # ü folder doesn't exist yet
                return {"data": []}
            elif parent_ids == [100]:
                # Return existing file in 'u' folder
                return {
                    "data": [
                        {
                            "id": 999,
                            "name": "file.txt",
                            "type": "text",
                            "hash": "hash2",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 100,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                            "path": "/u/file.txt",
                        }
                    ]
                }
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_entries

        out = OutputFormatter(json_output=False, quiet=True)
        handler = DuplicateHandler(mock_client, out, 0, "skip", parent_id=None)

        # Create test files for both folders
        test_file1 = tmp_path / "file1.txt"
        test_file1.write_text("content for u folder")
        test_file2 = tmp_path / "file2.txt"
        test_file2.write_text("content for ü folder")

        files_to_upload = [
            (test_file1, "u/file.txt"),
            (test_file2, "ü/file.txt"),
        ]

        handler.validate_and_handle_duplicates(files_to_upload)

        # Only u/file.txt should be marked for skipping (it's the actual duplicate)
        # ü/file.txt should NOT be skipped (different folder, even if same filename)
        assert "u/file.txt" in handler.files_to_skip
        assert "ü/file.txt" not in handler.files_to_skip

    def test_same_name_files_in_different_folders_handled_correctly(self, tmp_path):
        """Test files with same name in different folders are handled individually.

        When uploading:
        - folder1/data.txt (exists on server - duplicate)
        - folder2/data.txt (doesn't exist - NOT a duplicate)

        Only folder1/data.txt should be marked as duplicate.
        """
        mock_client = MagicMock()

        # Server reports "data.txt" as duplicate
        mock_client.validate_uploads.return_value = {"duplicates": ["data.txt"]}

        # Mock entries manager behavior
        folder1_entry = FileEntry(
            id=100,
            name="folder1",
            file_name="folder1",
            type="folder",
            hash="hash1",
            mime="",
            file_size=0,
            parent_id=0,
            created_at="2023-01-01",
            updated_at="2023-01-01",
            extension=None,
            url="",
        )

        existing_file = FileEntry(
            id=999,
            name="data.txt",
            file_name="data.txt",
            type="text",
            hash="hash2",
            mime="text/plain",
            file_size=100,
            parent_id=100,
            created_at="2023-01-01",
            updated_at="2023-01-01",
            extension=None,
            url="",
            path="/folder1/data.txt",
        )

        out = OutputFormatter(json_output=False, quiet=True)
        handler = DuplicateHandler(mock_client, out, 0, "skip", parent_id=None)

        # Mock folder resolution
        def mock_find_folder(folder_name, parent_id=None, search_in_root=True):
            if folder_name == "folder1":
                return folder1_entry
            return None  # folder2 doesn't exist

        handler.entries_manager.find_folder_by_name = mock_find_folder

        # Mock getting entries in folder
        def mock_get_all_in_folder(folder_id=None, use_cache=True, per_page=100):
            if folder_id == 100:
                return [existing_file]
            return []

        handler.entries_manager.get_all_in_folder = mock_get_all_in_folder

        # Create test files
        test_file1 = tmp_path / "file1.txt"
        test_file1.write_text("content1")
        test_file2 = tmp_path / "file2.txt"
        test_file2.write_text("content2")

        files_to_upload = [
            (test_file1, "folder1/data.txt"),
            (test_file2, "folder2/data.txt"),
        ]

        handler.validate_and_handle_duplicates(files_to_upload)

        # Only folder1/data.txt should be skipped
        assert "folder1/data.txt" in handler.files_to_skip
        assert "folder2/data.txt" not in handler.files_to_skip

    def test_duplicate_rel_paths_mapping_populated_correctly(self, tmp_path):
        """Test that _duplicate_rel_paths only contains actual duplicates."""
        mock_client = MagicMock()

        mock_client.validate_uploads.return_value = {"duplicates": ["report.txt"]}

        folder_entry = FileEntry(
            id=200,
            name="existing",
            file_name="existing",
            type="folder",
            hash="hash1",
            mime="",
            file_size=0,
            parent_id=0,
            created_at="2023-01-01",
            updated_at="2023-01-01",
            extension=None,
            url="",
        )

        existing_file = FileEntry(
            id=555,
            name="report.txt",
            file_name="report.txt",
            type="text",
            hash="hash2",
            mime="text/plain",
            file_size=100,
            parent_id=200,
            created_at="2023-01-01",
            updated_at="2023-01-01",
            extension=None,
            url="",
            path="/existing/report.txt",
        )

        out = OutputFormatter(json_output=False, quiet=True)
        handler = DuplicateHandler(mock_client, out, 0, "skip", parent_id=None)

        def mock_find_folder(folder_name, parent_id=None, search_in_root=True):
            if folder_name == "existing":
                return folder_entry
            return None

        handler.entries_manager.find_folder_by_name = mock_find_folder

        def mock_get_all_in_folder(folder_id=None, use_cache=True, per_page=100):
            if folder_id == 200:
                return [existing_file]
            return []

        handler.entries_manager.get_all_in_folder = mock_get_all_in_folder

        # Create test files
        test_file1 = tmp_path / "report1.txt"
        test_file1.write_text("duplicate")
        test_file2 = tmp_path / "report2.txt"
        test_file2.write_text("new file")
        test_file3 = tmp_path / "other.txt"
        test_file3.write_text("other file")

        files_to_upload = [
            (test_file1, "existing/report.txt"),  # Actual duplicate
            (test_file2, "new_folder/report.txt"),  # Same name, different folder
            (test_file3, "existing/other.txt"),  # Different name
        ]

        handler.validate_and_handle_duplicates(files_to_upload)

        # Only existing/report.txt should be in _duplicate_rel_paths
        assert "existing/report.txt" in handler._duplicate_rel_paths
        assert "new_folder/report.txt" not in handler._duplicate_rel_paths
        assert "existing/other.txt" not in handler._duplicate_rel_paths

        # Only existing/report.txt should be skipped
        assert "existing/report.txt" in handler.files_to_skip
        assert "new_folder/report.txt" not in handler.files_to_skip


class TestEntriesToDeleteReplace:
    """Tests for entries_to_delete list population with replace action."""

    def test_replace_populates_entries_to_delete(self, tmp_path):
        """Test replace action populates entries_to_delete list."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": ["test.txt"]}
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "test.txt",
                    "type": "text",
                    "hash": "hash123",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                }
            ]
        }
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "replace")

        test_file = tmp_path / "test.txt"
        test_file.write_text("new content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # entries_to_delete should be populated with the duplicate ID
        assert len(handler.entries_to_delete) == 1
        assert 123 in handler.entries_to_delete

    def test_replace_multiple_duplicates(self, tmp_path):
        """Test replace action handles multiple duplicates."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {
            "duplicates": ["file1.txt", "file2.txt"]
        }

        def mock_get_entries(query=None, parent_ids=None, workspace_id=0, **kwargs):
            if query == "file1.txt":
                return {
                    "data": [
                        {
                            "id": 111,
                            "name": "file1.txt",
                            "type": "text",
                            "hash": "hash1",
                            "mime": "text/plain",
                            "file_size": 100,
                            "parent_id": 0,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                        }
                    ]
                }
            elif query == "file2.txt":
                return {
                    "data": [
                        {
                            "id": 222,
                            "name": "file2.txt",
                            "type": "text",
                            "hash": "hash2",
                            "mime": "text/plain",
                            "file_size": 200,
                            "parent_id": 0,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                        }
                    ]
                }
            return {"data": []}

        mock_client.get_file_entries.side_effect = mock_get_entries

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "replace")

        test_file1 = tmp_path / "file1.txt"
        test_file1.write_text("content1")
        test_file2 = tmp_path / "file2.txt"
        test_file2.write_text("content2")

        files_to_upload = [(test_file1, "file1.txt"), (test_file2, "file2.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # Both duplicate IDs should be in entries_to_delete
        assert len(handler.entries_to_delete) == 2
        assert 111 in handler.entries_to_delete
        assert 222 in handler.entries_to_delete

    def test_skip_does_not_populate_entries_to_delete(self, tmp_path):
        """Test skip action does NOT populate entries_to_delete."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": ["test.txt"]}
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "skip")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # entries_to_delete should be empty for skip action
        assert len(handler.entries_to_delete) == 0

    def test_rename_does_not_populate_entries_to_delete(self, tmp_path):
        """Test rename action does NOT populate entries_to_delete."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": ["test.txt"]}
        mock_client.get_available_name.return_value = "test (1).txt"
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "rename")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # entries_to_delete should be empty for rename action
        assert len(handler.entries_to_delete) == 0

    def test_replace_with_multiple_ids_per_file(self, tmp_path):
        """Test replace when a file has multiple duplicates (different locations)."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": ["doc.txt"]}

        # Return multiple entries with same name but different IDs
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 333,
                    "name": "doc.txt",
                    "type": "text",
                    "hash": "hash1",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": 10,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                    "path": "/folder1/doc.txt",
                },
                {
                    "id": 444,
                    "name": "doc.txt",
                    "type": "text",
                    "hash": "hash2",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": 20,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                    "path": "/folder2/doc.txt",
                },
            ]
        }

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "replace")

        test_file = tmp_path / "doc.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "doc.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # Both duplicate IDs should be in entries_to_delete
        assert len(handler.entries_to_delete) == 2
        assert 333 in handler.entries_to_delete
        assert 444 in handler.entries_to_delete

    def test_replace_empty_when_no_duplicates(self, tmp_path):
        """Test entries_to_delete remains empty when no duplicates."""
        mock_client = MagicMock()
        mock_client.validate_uploads.return_value = {"duplicates": []}
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "replace")

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        files_to_upload = [(test_file, "test.txt")]

        handler.validate_and_handle_duplicates(files_to_upload)

        # No duplicates, so entries_to_delete should be empty
        assert len(handler.entries_to_delete) == 0
