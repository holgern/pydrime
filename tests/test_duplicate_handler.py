"""Tests for duplicate file handler."""

from unittest.mock import MagicMock

from pydrime.duplicate_handler import DuplicateHandler
from pydrime.exceptions import DrimeAPIError
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
        assert len(handler.entries_to_delete) == 0

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
        assert len(handler.entries_to_delete) == 0

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

    def test_replace_marks_entries_for_deletion(self, tmp_path):
        """Test replace action marks entries for deletion."""
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

        assert 123 in handler.entries_to_delete


class TestDeleteMarkedEntries:
    """Tests for delete_marked_entries method."""

    def test_delete_marked_entries_success(self):
        """Test successful deletion of marked entries."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "replace")
        handler.entries_to_delete = [1, 2, 3]

        result = handler.delete_marked_entries()

        assert result is True
        mock_client.delete_file_entries.assert_called_once_with(
            [1, 2, 3], delete_forever=False
        )

    def test_delete_no_entries_returns_true(self):
        """Test with no entries to delete returns True."""
        mock_client = MagicMock()
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "replace")
        handler.entries_to_delete = []

        result = handler.delete_marked_entries()

        assert result is True
        mock_client.delete_file_entries.assert_not_called()

    def test_delete_api_error_returns_false(self):
        """Test API error during deletion returns False."""
        mock_client = MagicMock()
        mock_client.delete_file_entries.side_effect = DrimeAPIError("API Error")
        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "replace")
        handler.entries_to_delete = [1, 2]

        result = handler.delete_marked_entries()

        assert result is False


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

        # Mock get_file_entries to return data for searching within specific parents
        def mock_get_entries(
            query=None,
            workspace_id=0,
            parent_ids=None,
            per_page=100,
            page=None,
            **kwargs,
        ):
            # When parent_ids=None or [None], return "backup" folder at root
            if parent_ids is None or (
                isinstance(parent_ids, list)
                and (len(parent_ids) == 0 or None in parent_ids)
            ):
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
                        ],
                        "pagination": {"current_page": 1, "last_page": 1},
                    }
            # When parent_ids=[100], getting all entries in folder 100
            elif parent_ids == [100]:
                return {
                    "data": [
                        {
                            "id": 200,
                            "name": "data",
                            "type": "folder",
                            "hash": "hash2",
                            "mime": None,
                            "file_size": 0,
                            "parent_id": 100,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                        }
                    ],
                    "pagination": {"current_page": 1, "last_page": 1},
                }
            return {"data": [], "pagination": {"current_page": 1, "last_page": 1}}

        mock_client.get_file_entries.side_effect = mock_get_entries

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

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
        from typing import Optional

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
        from typing import Optional

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
        from typing import Optional

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

        def mock_get_entries(
            query=None,
            workspace_id=0,
            parent_ids=None,
            per_page=100,
            page=None,
            **kwargs,
        ):
            # Return "subfolder" when searching in parent 100
            if parent_ids == [100]:
                return {
                    "data": [
                        {
                            "id": 200,
                            "name": "subfolder",
                            "type": "folder",
                            "hash": "hash2",
                            "mime": None,
                            "file_size": 0,
                            "parent_id": 100,
                            "created_at": "2023-01-01",
                            "updated_at": "2023-01-01",
                            "owner": {"email": "test@example.com"},
                        }
                    ],
                    "pagination": {"current_page": 1, "last_page": 1},
                }
            return {"data": [], "pagination": {"current_page": 1, "last_page": 1}}

        mock_client.get_file_entries.side_effect = mock_get_entries

        out = OutputFormatter(json_output=False, quiet=False)
        handler = DuplicateHandler(mock_client, out, 0, "ask")

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
