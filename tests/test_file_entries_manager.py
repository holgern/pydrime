"""Unit tests for FileEntriesManager."""

from unittest.mock import Mock

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

    def test_get_all_recursive_cycle_detection(self):
        """Test that recursive fetch prevents infinite loops from cycles."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
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

        manager = FileEntriesManager(mock_client, workspace_id=0)

        # Manually add folder ID to visited set to test cycle detection
        entries = manager.get_all_recursive(folder_id=10, visited={10})

        # Should return empty because folder 10 was already visited
        assert len(entries) == 0


class TestIterAllRecursive:
    """Tests for iter_all_recursive method."""

    def test_iter_all_recursive_single_batch(self):
        """Test iterating with single batch."""
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
        batches = list(manager.iter_all_recursive(folder_id=None, batch_size=50))

        assert len(batches) == 1
        assert len(batches[0]) == 2

    def test_iter_all_recursive_multiple_batches(self):
        """Test iterating with multiple batches."""
        mock_client = Mock()

        # Create 5 files
        files = []
        for i in range(5):
            files.append(
                {
                    "id": i,
                    "name": f"file{i}.txt",
                    "type": "text",
                    "hash": f"hash{i}",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                }
            )

        mock_client.get_file_entries.return_value = {
            "data": files,
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 5,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        batches = list(manager.iter_all_recursive(folder_id=None, batch_size=2))

        # Should be 3 batches: [2 files], [2 files], [1 file]
        assert len(batches) == 3
        assert len(batches[0]) == 2
        assert len(batches[1]) == 2
        assert len(batches[2]) == 1

    def test_iter_all_recursive_with_subfolders(self):
        """Test iterating with nested folders yields batches correctly."""
        mock_client = Mock()

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
                },
                {
                    "id": 1,
                    "name": "root_file.txt",
                    "type": "text",
                    "hash": "hash2",
                    "mime": "text/plain",
                    "file_size": 100,
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

        subfolder_response = {
            "data": [
                {
                    "id": 2,
                    "name": "nested_file.txt",
                    "type": "text",
                    "hash": "hash3",
                    "mime": "text/plain",
                    "file_size": 200,
                    "parent_id": 10,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                },
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 1,
        }

        mock_client.get_file_entries.side_effect = [root_response, subfolder_response]

        manager = FileEntriesManager(mock_client, workspace_id=0)
        batches = list(manager.iter_all_recursive(folder_id=None, batch_size=50))

        # Collect all files
        all_files = []
        for batch in batches:
            all_files.extend(batch)

        assert len(all_files) == 2
        file_names = [f[1] for f in all_files]
        assert "root_file.txt" in file_names
        assert "subfolder/nested_file.txt" in file_names

    def test_iter_all_recursive_cycle_detection(self):
        """Test that iter_all_recursive prevents cycles."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 0,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        batches = list(manager.iter_all_recursive(folder_id=10, visited={10}))

        # Should return empty because folder 10 was already visited
        assert len(batches) == 0


class TestGetUserFolders:
    """Tests for get_user_folders method."""

    def test_get_user_folders_success(self):
        """Test successful retrieval of user folders."""
        mock_client = Mock()
        mock_client.get_user_folders.return_value = {
            "folders": [
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
                },
                {
                    "id": 2,
                    "name": "folder2",
                    "type": "folder",
                    "hash": "hash2",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": 0,
                    "created_at": "2023-01-01",
                    "updated_at": "2023-01-01",
                    "owner": {"email": "test@example.com"},
                },
            ]
        }

        manager = FileEntriesManager(mock_client, workspace_id=5)
        folders = manager.get_user_folders(user_id=123)

        assert len(folders) == 2
        assert folders[0].name == "folder1"
        assert folders[1].name == "folder2"
        mock_client.get_user_folders.assert_called_once_with(
            user_id=123, workspace_id=5
        )

    def test_get_user_folders_with_cache(self):
        """Test caching of user folders."""
        mock_client = Mock()
        mock_client.get_user_folders.return_value = {
            "folders": [
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

        manager = FileEntriesManager(mock_client, workspace_id=0)

        # First call
        folders1 = manager.get_user_folders(user_id=123, use_cache=True)
        # Second call - should use cache
        folders2 = manager.get_user_folders(user_id=123, use_cache=True)

        assert len(folders1) == 1
        assert folders1 == folders2
        assert mock_client.get_user_folders.call_count == 1

    def test_get_user_folders_api_error(self):
        """Test handling API error when getting user folders."""
        from pydrime.exceptions import DrimeAPIError

        mock_client = Mock()
        mock_client.get_user_folders.side_effect = DrimeAPIError("API error")

        manager = FileEntriesManager(mock_client, workspace_id=0)
        folders = manager.get_user_folders(user_id=123)

        assert folders == []


class TestClearCache:
    """Tests for clear_cache method."""

    def test_clear_cache(self):
        """Test clearing the cache."""
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

        # Populate cache
        manager.get_all_in_folder(folder_id=10, use_cache=True)
        assert len(manager._cache) > 0

        # Clear cache
        manager.clear_cache()
        assert len(manager._cache) == 0


class TestSearchByNamePagination:
    """Tests for search_by_name pagination."""

    def test_search_pagination(self):
        """Test search with pagination."""
        mock_client = Mock()

        page1_response = {
            "data": [
                {
                    "id": 1,
                    "name": "test",
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
            "last_page": 2,
            "per_page": 1,
            "total": 2,
        }

        page2_response = {
            "data": [
                {
                    "id": 2,
                    "name": "test",
                    "type": "folder",
                    "hash": "hash2",
                    "mime": None,
                    "file_size": 0,
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

        # When exact_match is True and we find a match on first page, we stop
        mock_client.get_file_entries.side_effect = [page1_response, page2_response]

        manager = FileEntriesManager(mock_client, workspace_id=0)
        results = manager.search_by_name("test", exact_match=True, per_page=1)

        # Should stop after finding exact match on first page
        assert len(results) == 1

    def test_search_fuzzy_pagination(self):
        """Test fuzzy search with pagination."""
        mock_client = Mock()

        page1_response = {
            "data": [
                {
                    "id": 1,
                    "name": "test_file",
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

        page2_response = {
            "data": [
                {
                    "id": 2,
                    "name": "another_test",
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
        results = manager.search_by_name("test", exact_match=False, per_page=1)

        # Should fetch all pages for fuzzy match
        assert len(results) == 2

    def test_search_with_type_filter(self):
        """Test search with entry type filter."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "test",
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
                    "name": "test",
                    "type": "text",
                    "hash": "hash2",
                    "mime": "text/plain",
                    "file_size": 100,
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
        results = manager.search_by_name("test", exact_match=True, entry_type="folder")

        assert len(results) == 1
        assert results[0].is_folder is True

    def test_search_api_error(self):
        """Test search handles API error gracefully."""
        from pydrime.exceptions import DrimeAPIError

        mock_client = Mock()
        mock_client.get_file_entries.side_effect = DrimeAPIError("API error")

        manager = FileEntriesManager(mock_client, workspace_id=0)
        results = manager.search_by_name("test")

        assert results == []


class TestGetAllInFolderApiError:
    """Tests for get_all_in_folder API error handling."""

    def test_get_all_api_error(self):
        """Test get_all_in_folder handles API error gracefully."""
        from pydrime.exceptions import DrimeAPIError

        mock_client = Mock()
        mock_client.get_file_entries.side_effect = DrimeAPIError("API error")

        manager = FileEntriesManager(mock_client, workspace_id=0)
        entries = manager.get_all_in_folder(folder_id=None)

        # Should return empty list on API error
        assert entries == []
