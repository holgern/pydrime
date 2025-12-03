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

    def test_find_folder_fallback_to_listing(self):
        """Test fallback to listing when search API returns no results."""
        mock_client = Mock()

        # First call (search API) returns empty results
        # Second call (listing root folder) returns the folder
        mock_client.get_file_entries.side_effect = [
            # Search API returns empty
            {
                "data": [],
                "current_page": 1,
                "last_page": 1,
                "per_page": 50,
                "total": 0,
            },
            # Listing root returns the folder
            {
                "data": [
                    {
                        "id": 42,
                        "name": "sync_folder",
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
                        "id": 43,
                        "name": "other_folder",
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
            },
        ]

        manager = FileEntriesManager(mock_client, workspace_id=0)
        folder = manager.find_folder_by_name("sync_folder", parent_id=0)

        assert folder is not None
        assert folder.name == "sync_folder"
        assert folder.id == 42

        # Should have called API twice: once for search, once for listing
        assert mock_client.get_file_entries.call_count == 2

    def test_find_folder_not_found_after_fallback(self):
        """Test that None is returned if folder not found via search or listing."""
        mock_client = Mock()

        # Both search and listing return no matching folder
        mock_client.get_file_entries.side_effect = [
            # Search API returns empty
            {
                "data": [],
                "current_page": 1,
                "last_page": 1,
                "per_page": 50,
                "total": 0,
            },
            # Listing root returns other folders, not the one we're looking for
            {
                "data": [
                    {
                        "id": 43,
                        "name": "other_folder",
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
                "total": 1,
            },
        ]

        manager = FileEntriesManager(mock_client, workspace_id=0)
        folder = manager.find_folder_by_name("nonexistent_folder", parent_id=0)

        assert folder is None

        # Should have called API twice: once for search, once for listing
        assert mock_client.get_file_entries.call_count == 2


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


class TestEnsureFolderPath:
    """Tests for ensure_folder_path method and folder path caching."""

    def test_ensure_folder_path_empty_path(self):
        """Test that empty path returns base_parent_id."""
        mock_client = Mock()
        manager = FileEntriesManager(mock_client, workspace_id=0)

        result = manager.ensure_folder_path("", base_parent_id=None)
        assert result is None

        result = manager.ensure_folder_path(".", base_parent_id=123)
        assert result == 123

        result = manager.ensure_folder_path("/", base_parent_id=456)
        assert result == 456

    def test_ensure_folder_path_single_existing_folder(self):
        """Test finding an existing single folder."""
        mock_client = Mock()

        # Mock search_by_name to return existing folder
        mock_client.get_file_entries.return_value = {
            "data": [
                {
                    "id": 100,
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
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 1,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        result = manager.ensure_folder_path("folder1")

        assert result == 100

    def test_ensure_folder_path_creates_missing_folder(self):
        """Test creating a folder that doesn't exist."""
        mock_client = Mock()

        # Mock search returns empty (folder not found)
        mock_client.get_file_entries.return_value = {
            "data": [],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 0,
        }

        # Mock create_folder
        mock_client.create_folder.return_value = {
            "status": "success",
            "folder": {"id": 200, "name": "newfolder"},
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        result = manager.ensure_folder_path("newfolder")

        assert result == 200
        mock_client.create_folder.assert_called_once_with(
            name="newfolder", parent_id=None, workspace_id=0
        )

    def test_ensure_folder_path_nested_with_caching(self):
        """Test nested folder creation with caching."""
        mock_client = Mock()

        # All folders need to be created
        mock_client.get_file_entries.return_value = {
            "data": [],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 0,
        }

        # Mock create_folder to return incrementing IDs
        folder_id_counter = [100]

        def mock_create_folder(name, parent_id, workspace_id):
            folder_id = folder_id_counter[0]
            folder_id_counter[0] += 1
            return {"status": "success", "folder": {"id": folder_id, "name": name}}

        mock_client.create_folder.side_effect = mock_create_folder

        manager = FileEntriesManager(mock_client, workspace_id=0)

        # First call creates all folders
        result = manager.ensure_folder_path("folder1/folder2/folder3")
        assert result == 102  # Third folder created

        # Verify cache was populated
        assert manager.get_cached_folder_id("folder1") == 100
        assert manager.get_cached_folder_id("folder1/folder2") == 101
        assert manager.get_cached_folder_id("folder1/folder2/folder3") == 102

        # Second call should use cache - no new API calls
        create_call_count = mock_client.create_folder.call_count
        result2 = manager.ensure_folder_path("folder1/folder2/folder3")
        assert result2 == 102
        assert mock_client.create_folder.call_count == create_call_count

    def test_ensure_folder_path_partial_cache(self):
        """Test that partial paths use cache."""
        mock_client = Mock()
        manager = FileEntriesManager(mock_client, workspace_id=0)

        # Pre-populate cache
        manager.cache_folder_path("folder1", 100)
        manager.cache_folder_path("folder1/folder2", 200)

        # Mock for the new folder3 only
        mock_client.get_file_entries.return_value = {
            "data": [],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 0,
        }
        mock_client.create_folder.return_value = {
            "status": "success",
            "folder": {"id": 300, "name": "folder3"},
        }

        result = manager.ensure_folder_path("folder1/folder2/folder3")
        assert result == 300

        # Only folder3 should be created
        mock_client.create_folder.assert_called_once_with(
            name="folder3", parent_id=200, workspace_id=0
        )

    def test_ensure_folder_path_create_if_missing_false(self):
        """Test that create_if_missing=False returns None for missing folders."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 0,
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        result = manager.ensure_folder_path("nonexistent", create_if_missing=False)

        assert result is None
        mock_client.create_folder.assert_not_called()

    def test_ensure_folder_path_with_base_parent_id(self):
        """Test folder creation relative to a base parent."""
        mock_client = Mock()
        mock_client.get_file_entries.return_value = {
            "data": [],
            "current_page": 1,
            "last_page": 1,
            "per_page": 100,
            "total": 0,
        }
        mock_client.create_folder.return_value = {
            "status": "success",
            "folder": {"id": 500, "name": "subfolder"},
        }

        manager = FileEntriesManager(mock_client, workspace_id=0)
        result = manager.ensure_folder_path("subfolder", base_parent_id=999)

        assert result == 500
        mock_client.create_folder.assert_called_once_with(
            name="subfolder", parent_id=999, workspace_id=0
        )


class TestFolderPathCacheInvalidation:
    """Tests for folder path cache invalidation."""

    def test_invalidate_folder_by_id(self):
        """Test invalidating cache entries by folder ID."""
        mock_client = Mock()
        manager = FileEntriesManager(mock_client, workspace_id=0)

        # Populate cache
        manager.cache_folder_path("folder1", 100)
        manager.cache_folder_path("folder1/folder2", 200)
        manager.cache_folder_path("other", 300)

        # Invalidate folder1 (ID 100)
        manager.invalidate_folder_by_id(100)

        # folder1 should be removed
        assert manager.get_cached_folder_id("folder1") is None
        # folder2 and other should still exist
        assert manager.get_cached_folder_id("folder1/folder2") == 200
        assert manager.get_cached_folder_id("other") == 300

    def test_invalidate_folder_path(self):
        """Test invalidating cache entries by path."""
        mock_client = Mock()
        manager = FileEntriesManager(mock_client, workspace_id=0)

        # Populate cache
        manager.cache_folder_path("folder1", 100)
        manager.cache_folder_path("folder1/folder2", 200)
        manager.cache_folder_path("folder1/folder2/folder3", 300)
        manager.cache_folder_path("other", 400)

        # Invalidate folder1 - should remove folder1 and all children
        manager.invalidate_folder_path("folder1")

        # All folder1 paths should be removed
        assert manager.get_cached_folder_id("folder1") is None
        assert manager.get_cached_folder_id("folder1/folder2") is None
        assert manager.get_cached_folder_id("folder1/folder2/folder3") is None
        # Other should remain
        assert manager.get_cached_folder_id("other") == 400

    def test_clear_cache_clears_folder_path_cache(self):
        """Test that clear_cache also clears folder path cache."""
        mock_client = Mock()
        manager = FileEntriesManager(mock_client, workspace_id=0)

        # Populate folder path cache
        manager.cache_folder_path("folder1", 100)
        manager.cache_folder_path("folder2", 200)

        assert len(manager._folder_path_cache) == 2

        manager.clear_cache()

        assert len(manager._folder_path_cache) == 0
        assert len(manager._folder_id_to_paths) == 0


class TestFolderPathCacheWithDifferentBaseParents:
    """Tests for folder path cache with different base parent IDs."""

    def test_same_path_different_base_parents(self):
        """Test that same path with different base parents are cached separately."""
        mock_client = Mock()
        manager = FileEntriesManager(mock_client, workspace_id=0)

        # Cache same path "subfolder" with different base parents
        manager.cache_folder_path("subfolder", 100, base_parent_id=None)
        manager.cache_folder_path("subfolder", 200, base_parent_id=10)
        manager.cache_folder_path("subfolder", 300, base_parent_id=20)

        assert manager.get_cached_folder_id("subfolder", base_parent_id=None) == 100
        assert manager.get_cached_folder_id("subfolder", base_parent_id=10) == 200
        assert manager.get_cached_folder_id("subfolder", base_parent_id=20) == 300

    def test_invalidate_only_affects_matching_base_parent(self):
        """Test that invalidation by path respects base_parent_id."""
        mock_client = Mock()
        manager = FileEntriesManager(mock_client, workspace_id=0)

        manager.cache_folder_path("subfolder", 100, base_parent_id=None)
        manager.cache_folder_path("subfolder", 200, base_parent_id=10)

        # Invalidate only the one with base_parent_id=10
        manager.invalidate_folder_path("subfolder", base_parent_id=10)

        assert manager.get_cached_folder_id("subfolder", base_parent_id=None) == 100
        assert manager.get_cached_folder_id("subfolder", base_parent_id=10) is None
