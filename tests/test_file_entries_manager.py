"""Unit tests for FileEntriesManager."""

from unittest.mock import Mock

import pytest

from pydrime.file_entries_manager import FileEntriesManager


class TestFileEntriesManagerInit:
    """Tests for FileEntriesManager initialization."""

    def test_initialization(self):
        """Test basic initialization."""
        mock_client = Mock()
        manager = FileEntriesManager(mock_client, workspace_id=5)

        assert manager.client == mock_client
        assert manager.workspace_id == 5
        assert manager._cache == {}


class TestGetAllInFolder:
    """Tests for get_all_in_folder method."""

    def test_get_all_single_page(self):
        """Test fetching all entries in a single page."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "file1.txt",
                    "type": "text",
                    "hash": "hash1",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                },
                {
                    "id": 2,
                    "name": "file2.txt",
                    "type": "text",
                    "hash": "hash2",
                    "mime": "text/plain",
                    "file_size": 200,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                },
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 2,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        entries = manager.get_all_in_folder(folder_id=None)

        assert len(entries) == 2
        assert entries[0].name == "file1.txt"
        assert entries[1].name == "file2.txt"
        mock_client.get_file_entries.assert_called_once()

    def test_get_all_multiple_pages(self):
        """Test fetching all entries across multiple pages."""
        mock_client = Mock()

        # First page
        page1_response = {
            "data": [
                {
                    "id": 1,
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
            ],
            "current_page": 1,
            "last_page": 2,
            "per_page": 1,
            "total": 2,
        }

        # Second page
        page2_response = {
            "data": [
                {
                    "id": 2,
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
            ],
            "current_page": 2,
            "last_page": 2,
            "per_page": 1,
            "total": 2,
        }

        mock_client.get_file_entries.side_effect = [page1_response, page2_response]

        manager = FileEntriesManager(mock_client, workspace_id=0)
        entries = manager.get_all_in_folder(folder_id=None, per_page=1)

        assert len(entries) == 2
        assert entries[0].name == "file1.txt"
        assert entries[1].name == "file2.txt"
        assert mock_client.get_file_entries.call_count == 2

    def test_get_all_with_cache(self):
        """Test caching of results."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
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
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 1,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)

        # First call - should hit API
        entries1 = manager.get_all_in_folder(folder_id=10, use_cache=True)
        assert len(entries1) == 1

        # Second call - should use cache
        entries2 = manager.get_all_in_folder(folder_id=10, use_cache=True)
        assert len(entries2) == 1
        assert entries1 == entries2

        # Should only call API once
        assert mock_client.get_file_entries.call_count == 1


class TestSearchByName:
    """Tests for search_by_name method."""

    def test_search_exact_match(self):
        """Test searching with exact match."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "test_folder",
                    "type": "folder",
                    "hash": "hash1",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                },
                {
                    "id": 2,
                    "name": "test_folder_2",
                    "type": "folder",
                    "hash": "hash2",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                },
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 2,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        results = manager.search_by_name("test_folder", exact_match=True)

        assert len(results) == 1
        assert results[0].name == "test_folder"

    def test_search_fuzzy_match(self):
        """Test searching without exact match."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "test_folder",
                    "type": "folder",
                    "hash": "hash1",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                },
                {
                    "id": 2,
                    "name": "test_folder_2",
                    "type": "folder",
                    "hash": "hash2",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                },
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 2,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        results = manager.search_by_name("test", exact_match=False)

        assert len(results) == 2


class TestFindFolderByName:
    """Tests for find_folder_by_name method."""

    def test_find_folder_in_parent(self):
        """Test finding folder within a specific parent."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "target_folder",
                    "type": "folder",
                    "hash": "hash1",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": 100,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                }
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 1,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        folder = manager.find_folder_by_name("target_folder", parent_id=100)

        assert folder is not None
        assert folder.name == "target_folder"
        assert folder.is_folder is True

    def test_find_folder_globally(self):
        """Test finding folder with global search."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "target_folder",
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
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 1,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        folder = manager.find_folder_by_name("target_folder")

        assert folder is not None
        assert folder.name == "target_folder"


class TestGetAllRecursive:
    """Tests for get_all_recursive method."""

    def test_get_all_recursive_single_level(self):
        """Test recursive fetch with single level."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
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
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 1,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        entries = manager.get_all_recursive(folder_id=None)

        assert len(entries) == 1
        assert entries[0][0].name == "file1.txt"
        assert entries[0][1] == "file1.txt"

    def test_get_all_recursive_with_subfolders(self):
        """Test recursive fetch with nested folders."""
        mock_client = Mock()

        # Root folder contents
        root_response = {
            "data": [
                {
                    "id": 10,
                    "name": "subfolder",
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
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 1,
        }

        # Subfolder contents
        subfolder_response = {
            "data": [
                {
                    "id": 1,
                    "name": "file1.txt",
                    "type": "text",
                    "hash": "hash2",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": 10,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                }
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 1,
        }

        mock_client.get_file_entries.side_effect = [root_response, subfolder_response]

        manager = FileEntriesManager(mock_client, workspace_id=0)
        entries = manager.get_all_recursive(folder_id=None)

        assert len(entries) == 1
        assert entries[0][0].name == "file1.txt"
        assert entries[0][1] == "subfolder/file1.txt"
