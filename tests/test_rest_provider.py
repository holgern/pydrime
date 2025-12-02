"""Tests for the REST API provider module."""

# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

from unittest.mock import MagicMock


class TestResticStorageProvider:
    """Tests for the ResticStorageProvider class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()

    def _create_provider(self, readonly: bool = False):
        """Create a ResticStorageProvider with mocked client."""
        from pydrime.rest.provider import ResticStorageProvider

        return ResticStorageProvider(
            client=self.mock_client,
            workspace_id=0,
            readonly=readonly,
        )

    def _mock_file_entries(self, entries: list[dict]):
        """Create mock FileEntriesResult from entries list.

        Uses snake_case keys to match what FileEntry.from_dict() expects.
        """
        return {
            "data": [
                {
                    "id": e.get("id", 1),
                    "name": e.get("name", "test"),
                    "file_name": e.get("file_name", e.get("name", "test")),
                    "mime": e.get("mime", ""),
                    "file_size": e.get("file_size", 0),
                    "parent_id": e.get("parent_id"),
                    "created_at": e.get("created_at", "2024-01-01T00:00:00Z"),
                    "type": e.get("type", "file"),
                    "extension": e.get("extension"),
                    "hash": e.get("hash", "abc123"),
                    "url": e.get("url", ""),
                    "workspace_id": e.get("workspace_id", 0),
                }
                for e in entries
            ],
            "total": len(entries),
        }


class TestNormalizePath(TestResticStorageProvider):
    """Tests for path normalization."""

    def test_normalize_empty_path(self):
        """Test normalizing empty path."""
        provider = self._create_provider()
        assert provider._normalize_path("") == ""
        assert provider._normalize_path("/") == ""

    def test_normalize_simple_path(self):
        """Test normalizing simple path."""
        provider = self._create_provider()
        assert provider._normalize_path("/myrepo") == "myrepo"
        assert provider._normalize_path("myrepo/") == "myrepo"
        assert provider._normalize_path("/myrepo/") == "myrepo"

    def test_normalize_nested_path(self):
        """Test normalizing nested path."""
        provider = self._create_provider()
        assert provider._normalize_path("/backups/myrepo") == "backups/myrepo"


class TestReadonlyMode(TestResticStorageProvider):
    """Tests for readonly mode."""

    def test_is_readonly_false(self):
        """Test is_readonly returns False when not readonly."""
        provider = self._create_provider(readonly=False)
        assert provider.is_readonly() is False

    def test_is_readonly_true(self):
        """Test is_readonly returns True when readonly."""
        provider = self._create_provider(readonly=True)
        assert provider.is_readonly() is True


class TestRepositoryOperations(TestResticStorageProvider):
    """Tests for repository operations."""

    def test_repository_exists_true(self):
        """Test repository_exists returns True when config exists."""
        provider = self._create_provider()
        # Mock folder lookup
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [
                {"id": 1, "name": "myrepo", "type": "folder"},
            ]
        )
        # Second call for config file
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries(
                [{"id": 2, "name": "config", "type": "file", "file_size": 256}]
            ),
        ]

        assert provider.repository_exists("myrepo") is True

    def test_repository_exists_false_no_config(self):
        """Test repository_exists returns False when config doesn't exist."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([]),  # No config file
        ]

        assert provider.repository_exists("myrepo") is False

    def test_create_repository_success(self):
        """Test successful repository creation."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries([])
        self.mock_client.create_folder.return_value = {"folder": {"id": 1}}

        result = provider.create_repository("myrepo")
        assert result is True
        # Should create main folder + subdirectories
        assert self.mock_client.create_folder.call_count >= 1

    def test_create_repository_readonly_fails(self):
        """Test that repository creation fails in readonly mode."""
        provider = self._create_provider(readonly=True)
        result = provider.create_repository("myrepo")
        assert result is False
        self.mock_client.create_folder.assert_not_called()

    def test_delete_repository_success(self):
        """Test successful repository deletion."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [{"id": 1, "name": "myrepo", "type": "folder"}]
        )
        self.mock_client.delete_file_entries.return_value = {}

        result = provider.delete_repository("myrepo")
        assert result is True
        self.mock_client.delete_file_entries.assert_called_once()

    def test_delete_repository_not_found(self):
        """Test deleting non-existent repository."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries([])

        result = provider.delete_repository("myrepo")
        assert result is False

    def test_delete_repository_readonly_fails(self):
        """Test that repository deletion fails in readonly mode."""
        provider = self._create_provider(readonly=True)
        result = provider.delete_repository("myrepo")
        assert result is False


class TestConfigOperations(TestResticStorageProvider):
    """Tests for config operations."""

    def test_config_exists_true(self):
        """Test config_exists returns True and size when config exists."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries(
                [{"id": 2, "name": "config", "type": "file", "file_size": 256}]
            ),
        ]

        exists, size = provider.config_exists("myrepo")
        assert exists is True
        assert size == 256

    def test_config_exists_false(self):
        """Test config_exists returns False when config doesn't exist."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([]),
        ]

        exists, size = provider.config_exists("myrepo")
        assert exists is False
        assert size == 0

    def test_get_config_success(self):
        """Test getting config content."""
        provider = self._create_provider()
        config_data = b"config content"
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries(
                [{"id": 2, "name": "config", "type": "file", "hash": "abc123"}]
            ),
        ]
        self.mock_client.get_file_content.return_value = config_data

        result = provider.get_config("myrepo")
        assert result == config_data

    def test_get_config_not_found(self):
        """Test getting config when it doesn't exist."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([]),
        ]

        result = provider.get_config("myrepo")
        assert result is None

    def test_save_config_success(self):
        """Test saving config content."""
        provider = self._create_provider()
        config_data = b"new config"
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [{"id": 1, "name": "myrepo", "type": "folder"}]
        )
        self.mock_client.upload_file.return_value = {}

        result = provider.save_config("myrepo", config_data)
        assert result is True
        self.mock_client.upload_file.assert_called_once()

    def test_save_config_empty_data_fails(self):
        """Test that saving empty config fails."""
        provider = self._create_provider()
        result = provider.save_config("myrepo", b"")
        assert result is False

    def test_save_config_readonly_fails(self):
        """Test that saving config fails in readonly mode."""
        provider = self._create_provider(readonly=True)
        result = provider.save_config("myrepo", b"config")
        assert result is False


class TestBlobListOperations(TestResticStorageProvider):
    """Tests for blob listing operations."""

    def test_list_blobs_success(self):
        """Test listing blobs."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            # First call: find repo folder
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            # Second call: find keys folder
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
            # Third call: list files in keys folder
            self._mock_file_entries(
                [
                    {"id": 3, "name": "key1", "type": "file", "file_size": 100},
                    {"id": 4, "name": "key2", "type": "file", "file_size": 200},
                ]
            ),
        ]

        result = provider.list_blobs("myrepo", "keys")
        assert result is not None
        assert len(result) == 2
        assert result[0]["name"] == "key1"
        assert result[0]["size"] == 100
        assert result[1]["name"] == "key2"
        assert result[1]["size"] == 200

    def test_list_blobs_empty(self):
        """Test listing blobs when folder is empty."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
            self._mock_file_entries([]),
        ]

        result = provider.list_blobs("myrepo", "keys")
        assert result == []

    def test_list_blobs_invalid_type(self):
        """Test listing blobs with invalid type."""
        provider = self._create_provider()
        result = provider.list_blobs("myrepo", "invalid")
        assert result is None

    def test_list_blobs_data_with_subdirs(self):
        """Test listing data blobs (which have subdirectories)."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            # Find repo folder
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            # Find data folder
            self._mock_file_entries([{"id": 2, "name": "data", "type": "folder"}]),
            # List subdirectories (00, 01, etc.)
            self._mock_file_entries([{"id": 3, "name": "00", "type": "folder"}]),
            # List files in 00 subdirectory
            self._mock_file_entries(
                [{"id": 4, "name": "00abc123", "type": "file", "file_size": 1024}]
            ),
        ]

        result = provider.list_blobs("myrepo", "data")
        assert result is not None
        assert len(result) == 1
        assert result[0]["name"] == "00abc123"


class TestBlobOperations(TestResticStorageProvider):
    """Tests for individual blob operations."""

    def test_blob_exists_true(self):
        """Test blob_exists returns True when blob exists."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
            self._mock_file_entries(
                [{"id": 3, "name": "abc123", "type": "file", "file_size": 512}]
            ),
        ]

        exists, size = provider.blob_exists("myrepo", "keys", "abc123")
        assert exists is True
        assert size == 512

    def test_blob_exists_false(self):
        """Test blob_exists returns False when blob doesn't exist."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
            self._mock_file_entries([]),
        ]

        exists, size = provider.blob_exists("myrepo", "keys", "abc123")
        assert exists is False
        assert size == 0

    def test_blob_exists_data_with_subdir(self):
        """Test blob_exists for data blobs (with subdirectory)."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "data", "type": "folder"}]),
            self._mock_file_entries([{"id": 3, "name": "ab", "type": "folder"}]),
            self._mock_file_entries(
                [{"id": 4, "name": "abc123def", "type": "file", "file_size": 1024}]
            ),
        ]

        # Data blobs use first 2 chars as subdirectory
        exists, size = provider.blob_exists("myrepo", "data", "abc123def")
        assert exists is True
        assert size == 1024

    def test_get_blob_success(self):
        """Test getting blob content."""
        provider = self._create_provider()
        blob_data = b"blob content"
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
            self._mock_file_entries(
                [{"id": 3, "name": "abc123", "type": "file", "hash": "hash123"}]
            ),
        ]
        self.mock_client.get_file_content.return_value = blob_data

        result = provider.get_blob("myrepo", "keys", "abc123")
        assert result == blob_data

    def test_get_blob_not_found(self):
        """Test getting non-existent blob."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
            self._mock_file_entries([]),
        ]

        result = provider.get_blob("myrepo", "keys", "abc123")
        assert result is None

    def test_save_blob_success(self):
        """Test saving blob content."""
        provider = self._create_provider()
        blob_data = b"new blob"
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
        ]
        self.mock_client.upload_file.return_value = {}

        result = provider.save_blob("myrepo", "keys", "abc123", blob_data)
        assert result is True
        self.mock_client.upload_file.assert_called_once()

    def test_save_blob_readonly_fails(self):
        """Test that saving blob fails in readonly mode."""
        provider = self._create_provider(readonly=True)
        result = provider.save_blob("myrepo", "keys", "abc123", b"data")
        assert result is False

    def test_save_blob_invalid_type_fails(self):
        """Test that saving blob with invalid type fails."""
        provider = self._create_provider()
        result = provider.save_blob("myrepo", "invalid", "abc123", b"data")
        assert result is False

    def test_delete_blob_success(self):
        """Test deleting blob."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
            self._mock_file_entries([{"id": 3, "name": "abc123", "type": "file"}]),
        ]
        self.mock_client.delete_file_entries.return_value = {}

        result = provider.delete_blob("myrepo", "keys", "abc123")
        assert result is True
        self.mock_client.delete_file_entries.assert_called_once()

    def test_delete_blob_not_found(self):
        """Test deleting non-existent blob."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([{"id": 1, "name": "myrepo", "type": "folder"}]),
            self._mock_file_entries([{"id": 2, "name": "keys", "type": "folder"}]),
            self._mock_file_entries([]),
        ]

        result = provider.delete_blob("myrepo", "keys", "abc123")
        assert result is False

    def test_delete_blob_readonly_fails(self):
        """Test that deleting blob fails in readonly mode."""
        provider = self._create_provider(readonly=True)
        result = provider.delete_blob("myrepo", "keys", "abc123")
        assert result is False


class TestFolderCache(TestResticStorageProvider):
    """Tests for folder ID caching."""

    def test_folder_cache_hit(self):
        """Test that folder cache is used on subsequent lookups."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [{"id": 1, "name": "myrepo", "type": "folder"}]
        )

        # First lookup - should call API
        folder_id = provider._get_folder_id_by_path("myrepo")
        assert folder_id == 1
        assert self.mock_client.get_file_entries.call_count == 1

        # Second lookup - should use cache
        folder_id = provider._get_folder_id_by_path("myrepo")
        assert folder_id == 1
        # Call count should still be 1 (cache hit)
        assert self.mock_client.get_file_entries.call_count == 1

    def test_folder_cache_cleared_on_delete(self):
        """Test that folder cache is cleared when repository is deleted."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [{"id": 1, "name": "myrepo", "type": "folder"}]
        )
        self.mock_client.delete_file_entries.return_value = {}

        # Populate cache
        provider._get_folder_id_by_path("myrepo")
        assert "myrepo" in provider._folder_cache

        # Delete repository
        provider.delete_repository("myrepo")

        # Cache should be cleared
        assert "myrepo" not in provider._folder_cache


class TestRootPath(TestResticStorageProvider):
    """Tests for root path handling."""

    def test_config_in_root(self):
        """Test config file at root level (empty repo path)."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [{"id": 1, "name": "config", "type": "file", "file_size": 128}]
        )

        exists, size = provider.config_exists("")
        assert exists is True
        assert size == 128

    def test_list_blobs_at_root(self):
        """Test listing blobs at root level."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.side_effect = [
            # Find keys folder at root
            self._mock_file_entries([{"id": 1, "name": "keys", "type": "folder"}]),
            # List files in keys folder
            self._mock_file_entries(
                [{"id": 2, "name": "key1", "type": "file", "file_size": 100}]
            ),
        ]

        result = provider.list_blobs("", "keys")
        assert result is not None
        assert len(result) == 1
