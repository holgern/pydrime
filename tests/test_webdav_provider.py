"""Tests for the WebDAV provider module."""

# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

import time
from io import BytesIO
from unittest.mock import MagicMock

import pytest

# Import wsgidav in correct order to avoid circular import
# WsgiDAVApp must be imported BEFORE dav_error
from wsgidav.wsgidav_app import WsgiDAVApp  # noqa: F401
from wsgidav.dav_error import HTTP_FORBIDDEN, HTTP_NOT_FOUND, DAVError  # noqa: F401


class TestDrimeDAVProvider:
    """Tests for the DrimeDAVProvider class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.environ = {"wsgidav.provider": None}

    def _create_provider(self, readonly: bool = False, cache_ttl: float = 30.0):
        """Create a DrimeDAVProvider with mocked client."""
        from pydrime.webdav.provider import DrimeDAVProvider

        provider = DrimeDAVProvider(
            client=self.mock_client,
            workspace_id=0,
            readonly=readonly,
            cache_ttl=cache_ttl,
            max_file_size=500 * 1024 * 1024,
        )
        return provider

    def _create_file_entry(
        self,
        id: int = 1,
        name: str = "test.txt",
        is_folder: bool = False,
        file_size: int = 100,
        hash: str = "abc123",
        parent_id: int | None = None,
    ):
        """Create a mock FileEntry."""
        from pydrime.models import FileEntry

        return FileEntry(
            id=id,
            name=name,
            file_name=name,
            mime="text/plain" if not is_folder else "",
            file_size=file_size if not is_folder else 0,
            parent_id=parent_id,
            created_at="2024-01-01T00:00:00Z",
            type="folder" if is_folder else "file",
            extension=name.split(".")[-1] if "." in name and not is_folder else None,
            hash=hash if not is_folder else "",
            url="",
            workspace_id=0,
        )

    def _mock_file_entries(self, entries: list):
        """Create mock API response for file entries."""
        return {
            "data": [
                {
                    "id": e.id,
                    "name": e.name,
                    "fileName": e.file_name,
                    "mime": e.mime,
                    "fileSize": e.file_size,
                    "parentId": e.parent_id,
                    "createdAt": e.created_at,
                    "type": e.type,
                    "extension": e.extension,
                    "hash": e.hash,
                    "url": e.url,
                    "workspaceId": e.workspace_id,
                }
                for e in entries
            ],
            "total": len(entries),
        }


class TestProviderReadonly(TestDrimeDAVProvider):
    """Tests for readonly mode."""

    def test_is_readonly_false(self):
        """Test is_readonly returns False when not readonly."""
        provider = self._create_provider(readonly=False)
        assert provider.is_readonly() is False

    def test_is_readonly_true(self):
        """Test is_readonly returns True when readonly."""
        provider = self._create_provider(readonly=True)
        assert provider.is_readonly() is True


class TestRecentCreatesCache(TestDrimeDAVProvider):
    """Tests for recent creates cache (eventual consistency handling)."""

    def test_register_create(self):
        """Test registering a recently created path."""
        provider = self._create_provider()
        entry = self._create_file_entry()

        provider._register_create("/test.txt", entry)

        created, cached_entry = provider._is_recently_created("/test.txt")
        assert created is True
        assert cached_entry == entry

    def test_register_create_removes_from_deletes(self):
        """Test that registering a create removes the path from deletes."""
        provider = self._create_provider()

        provider._register_delete("/test.txt")
        assert provider._is_recently_deleted("/test.txt") is True

        provider._register_create("/test.txt")
        assert provider._is_recently_deleted("/test.txt") is False

    def test_is_recently_created_expires(self):
        """Test that recently created entries expire after cache TTL."""
        provider = self._create_provider(cache_ttl=0.1)  # 100ms TTL
        entry = self._create_file_entry()

        provider._register_create("/test.txt", entry)

        # Should be cached initially
        created, _ = provider._is_recently_created("/test.txt")
        assert created is True

        # Wait for expiry
        time.sleep(0.15)

        # Should be expired now
        created, _ = provider._is_recently_created("/test.txt")
        assert created is False


class TestRecentDeletesCache(TestDrimeDAVProvider):
    """Tests for recent deletes cache."""

    def test_register_delete(self):
        """Test registering a recently deleted path."""
        provider = self._create_provider()

        provider._register_delete("/test.txt")

        assert provider._is_recently_deleted("/test.txt") is True

    def test_register_delete_removes_from_creates(self):
        """Test that registering a delete removes the path from creates."""
        provider = self._create_provider()
        entry = self._create_file_entry()

        provider._register_create("/test.txt", entry)
        created, _ = provider._is_recently_created("/test.txt")
        assert created is True

        provider._register_delete("/test.txt")
        created, _ = provider._is_recently_created("/test.txt")
        assert created is False

    def test_is_recently_deleted_expires(self):
        """Test that recently deleted entries expire after cache TTL."""
        provider = self._create_provider(cache_ttl=0.1)

        provider._register_delete("/test.txt")
        assert provider._is_recently_deleted("/test.txt") is True

        time.sleep(0.15)

        assert provider._is_recently_deleted("/test.txt") is False


class TestGetResourceInst(TestDrimeDAVProvider):
    """Tests for get_resource_inst method."""

    def test_get_resource_inst_root(self):
        """Test getting root collection."""
        from pydrime.webdav.provider import DrimeCollection

        provider = self._create_provider()

        result = provider.get_resource_inst("/", self.environ)
        assert isinstance(result, DrimeCollection)
        assert result.path == "/"

    def test_get_resource_inst_file(self):
        """Test getting a file resource."""
        from pydrime.webdav.provider import DrimeResource

        provider = self._create_provider()
        file_entry = self._create_file_entry(id=1, name="test.txt")
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [file_entry]
        )

        result = provider.get_resource_inst("/test.txt", self.environ)
        assert isinstance(result, DrimeResource)
        assert result.path == "/test.txt"

    def test_get_resource_inst_folder(self):
        """Test getting a folder collection."""
        from pydrime.webdav.provider import DrimeCollection

        provider = self._create_provider()
        folder_entry = self._create_file_entry(id=1, name="docs", is_folder=True)
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [folder_entry]
        )

        result = provider.get_resource_inst("/docs", self.environ)
        assert isinstance(result, DrimeCollection)
        assert result.path == "/docs"

    def test_get_resource_inst_not_found(self):
        """Test getting non-existent resource."""
        provider = self._create_provider()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries([])

        result = provider.get_resource_inst("/nonexistent.txt", self.environ)
        assert result is None

    def test_get_resource_inst_recently_deleted(self):
        """Test that recently deleted paths return None."""
        provider = self._create_provider()
        provider._register_delete("/test.txt")

        result = provider.get_resource_inst("/test.txt", self.environ)
        assert result is None

    def test_get_resource_inst_recently_created(self):
        """Test that recently created paths are found."""
        from pydrime.webdav.provider import DrimeResource

        provider = self._create_provider()
        file_entry = self._create_file_entry(id=1, name="new.txt")
        provider._register_create("/new.txt", file_entry)

        # Mock empty API response (file not yet visible in API)
        self.mock_client.get_file_entries.return_value = self._mock_file_entries([])

        result = provider.get_resource_inst("/new.txt", self.environ)
        assert isinstance(result, DrimeResource)

    def test_get_resource_inst_nested_path(self):
        """Test getting resource in nested path."""
        from pydrime.webdav.provider import DrimeResource

        provider = self._create_provider()
        folder_entry = self._create_file_entry(id=1, name="docs", is_folder=True)
        file_entry = self._create_file_entry(id=2, name="readme.txt", parent_id=1)

        self.mock_client.get_file_entries.side_effect = [
            self._mock_file_entries([folder_entry]),  # Root level
            self._mock_file_entries([file_entry]),  # Inside docs folder
        ]

        result = provider.get_resource_inst("/docs/readme.txt", self.environ)
        assert isinstance(result, DrimeResource)
        assert result.path == "/docs/readme.txt"


class TestDrimeResource:
    """Tests for the DrimeResource class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.environ = {"wsgidav.provider": MagicMock()}

    def _create_file_entry(
        self,
        id: int = 1,
        name: str = "test.txt",
        file_size: int = 100,
        hash: str = "abc123",
        mime: str = "text/plain",
    ):
        """Create a mock FileEntry."""
        from pydrime.models import FileEntry

        return FileEntry(
            id=id,
            name=name,
            file_name=name,
            mime=mime,
            file_size=file_size,
            parent_id=None,
            created_at="2024-01-01T00:00:00Z",
            type="file",
            extension=name.split(".")[-1] if "." in name else None,
            hash=hash,
            url="",
            workspace_id=0,
        )

    def _create_resource(
        self,
        file_entry=None,
        path: str = "/test.txt",
        readonly: bool = False,
        max_file_size: int = 500 * 1024 * 1024,
    ):
        """Create a DrimeResource."""
        from pydrime.webdav.provider import DrimeResource

        if file_entry is None:
            file_entry = self._create_file_entry()

        return DrimeResource(
            path=path,
            environ=self.environ,
            file_entry=file_entry,
            client=self.mock_client,
            workspace_id=0,
            readonly=readonly,
            max_file_size=max_file_size,
        )

    def test_get_content_length(self):
        """Test get_content_length returns file size."""
        entry = self._create_file_entry(file_size=1024)
        resource = self._create_resource(file_entry=entry)
        assert resource.get_content_length() == 1024

    def test_get_content_type(self):
        """Test get_content_type returns MIME type."""
        entry = self._create_file_entry(mime="application/json")
        resource = self._create_resource(file_entry=entry)
        assert resource.get_content_type() == "application/json"

    def test_get_content_type_default(self):
        """Test get_content_type returns default for empty MIME."""
        entry = self._create_file_entry(mime="")
        resource = self._create_resource(file_entry=entry)
        assert resource.get_content_type() == "application/octet-stream"

    def test_get_display_name(self):
        """Test get_display_name returns file name."""
        entry = self._create_file_entry(name="document.pdf")
        resource = self._create_resource(file_entry=entry)
        assert resource.get_display_name() == "document.pdf"

    def test_get_etag(self):
        """Test get_etag returns file hash."""
        entry = self._create_file_entry(hash="xyz789")
        resource = self._create_resource(file_entry=entry)
        assert resource.get_etag() == "xyz789"

    def test_get_etag_strips_quotes(self):
        """Test get_etag strips quotes from hash."""
        entry = self._create_file_entry(hash='"quoted_hash"')
        resource = self._create_resource(file_entry=entry)
        assert resource.get_etag() == "quoted_hash"

    def test_support_etag(self):
        """Test that ETags are supported."""
        resource = self._create_resource()
        assert resource.support_etag() is True

    def test_support_ranges(self):
        """Test that range requests are not supported."""
        resource = self._create_resource()
        assert resource.support_ranges() is False

    def test_get_content_success(self):
        """Test getting file content."""
        content = b"file content"
        entry = self._create_file_entry(hash="abc123")
        resource = self._create_resource(file_entry=entry)
        self.mock_client.get_file_content.return_value = content

        result = resource.get_content()
        assert isinstance(result, BytesIO)
        assert result.read() == content

    def test_get_content_no_hash(self):
        """Test getting content fails when no hash."""
        entry = self._create_file_entry(hash="")
        resource = self._create_resource(file_entry=entry)

        with pytest.raises(DAVError):
            resource.get_content()

    def test_get_content_too_large(self):
        """Test getting content fails when file too large."""
        entry = self._create_file_entry(file_size=1000 * 1024 * 1024)  # 1GB
        resource = self._create_resource(
            file_entry=entry, max_file_size=100 * 1024 * 1024
        )

        with pytest.raises(DAVError):
            resource.get_content()

    def test_begin_write_readonly(self):
        """Test that begin_write fails in readonly mode."""
        resource = self._create_resource(readonly=True)

        with pytest.raises(DAVError):
            resource.begin_write()

    def test_delete_readonly(self):
        """Test that delete fails in readonly mode."""
        resource = self._create_resource(readonly=True)

        with pytest.raises(DAVError):
            resource.delete()


class TestDrimeCollection:
    """Tests for the DrimeCollection class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.environ = {"wsgidav.provider": MagicMock()}

    def _create_folder_entry(
        self,
        id: int = 1,
        name: str = "docs",
        parent_id: int | None = None,
    ):
        """Create a mock folder FileEntry."""
        from pydrime.models import FileEntry

        return FileEntry(
            id=id,
            name=name,
            file_name=name,
            mime="",
            file_size=0,
            parent_id=parent_id,
            created_at="2024-01-01T00:00:00Z",
            type="folder",
            extension=None,
            hash="",
            url="",
            workspace_id=0,
        )

    def _create_file_entry(
        self,
        id: int = 1,
        name: str = "test.txt",
        file_size: int = 100,
        parent_id: int | None = None,
    ):
        """Create a mock file FileEntry."""
        from pydrime.models import FileEntry

        return FileEntry(
            id=id,
            name=name,
            file_name=name,
            mime="text/plain",
            file_size=file_size,
            parent_id=parent_id,
            created_at="2024-01-01T00:00:00Z",
            type="file",
            extension=name.split(".")[-1] if "." in name else None,
            hash="abc123",
            url="",
            workspace_id=0,
        )

    def _create_collection(
        self,
        folder_entry=None,
        path: str = "/docs",
        readonly: bool = False,
    ):
        """Create a DrimeCollection."""
        from pydrime.webdav.provider import DrimeCollection

        return DrimeCollection(
            path=path,
            environ=self.environ,
            folder_entry=folder_entry,
            client=self.mock_client,
            workspace_id=0,
            readonly=readonly,
            cache_ttl=30.0,
            max_file_size=500 * 1024 * 1024,
        )

    def _mock_file_entries(self, entries: list):
        """Create mock API response for file entries."""
        return {
            "data": [
                {
                    "id": e.id,
                    "name": e.name,
                    "fileName": e.file_name,
                    "mime": e.mime,
                    "fileSize": e.file_size,
                    "parentId": e.parent_id,
                    "createdAt": e.created_at,
                    "type": e.type,
                    "extension": e.extension,
                    "hash": e.hash,
                    "url": e.url,
                    "workspaceId": e.workspace_id,
                }
                for e in entries
            ],
            "total": len(entries),
        }

    def test_get_display_name_root(self):
        """Test get_display_name for root collection."""
        collection = self._create_collection(folder_entry=None, path="/")
        assert collection.get_display_name() == "/"

    def test_get_display_name_folder(self):
        """Test get_display_name for folder."""
        entry = self._create_folder_entry(name="documents")
        collection = self._create_collection(folder_entry=entry)
        assert collection.get_display_name() == "documents"

    def test_get_member_names(self):
        """Test getting member names."""
        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)

        file1 = self._create_file_entry(id=2, name="file1.txt", parent_id=1)
        file2 = self._create_file_entry(id=3, name="file2.txt", parent_id=1)
        subfolder = self._create_folder_entry(id=4, name="subfolder", parent_id=1)

        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [file1, file2, subfolder]
        )

        names = collection.get_member_names()
        assert "file1.txt" in names
        assert "file2.txt" in names
        assert "subfolder" in names

    def test_get_member_file(self):
        """Test getting a file member."""
        from pydrime.webdav.provider import DrimeResource

        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)

        file_entry = self._create_file_entry(id=2, name="readme.txt", parent_id=1)
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [file_entry]
        )

        member = collection.get_member("readme.txt")
        assert isinstance(member, DrimeResource)

    def test_get_member_folder(self):
        """Test getting a folder member."""
        from pydrime.webdav.provider import DrimeCollection

        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)

        subfolder = self._create_folder_entry(id=2, name="images", parent_id=1)
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [subfolder]
        )

        member = collection.get_member("images")
        assert isinstance(member, DrimeCollection)

    def test_get_member_not_found(self):
        """Test getting non-existent member."""
        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)

        self.mock_client.get_file_entries.return_value = self._mock_file_entries([])

        member = collection.get_member("nonexistent.txt")
        assert member is None

    def test_create_collection_success(self):
        """Test creating a subcollection."""
        from pydrime.webdav.provider import DrimeCollection

        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)

        self.mock_client.create_folder.return_value = {"folder": {"id": 2}}

        new_collection = collection.create_collection("new_folder")
        assert isinstance(new_collection, DrimeCollection)
        self.mock_client.create_folder.assert_called_once()

    def test_create_collection_readonly(self):
        """Test that creating collection fails in readonly mode."""
        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry, readonly=True)

        with pytest.raises(DAVError):
            collection.create_collection("new_folder")

    def test_create_empty_resource(self):
        """Test creating an empty resource (for file upload)."""
        from pydrime.webdav.provider import DrimeResource

        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)

        resource = collection.create_empty_resource("new_file.txt")
        assert isinstance(resource, DrimeResource)
        assert resource.file_entry.name == "new_file.txt"

    def test_create_empty_resource_readonly(self):
        """Test that creating resource fails in readonly mode."""
        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry, readonly=True)

        with pytest.raises(DAVError):
            collection.create_empty_resource("new_file.txt")

    def test_delete_readonly(self):
        """Test that delete fails in readonly mode."""
        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry, readonly=True)

        with pytest.raises(DAVError):
            collection.delete()

    def test_delete_root_fails(self):
        """Test that deleting root folder fails."""
        collection = self._create_collection(folder_entry=None, path="/")

        with pytest.raises(DAVError):
            collection.delete()

    def test_support_recursive_move(self):
        """Test that recursive move is supported."""
        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)
        assert collection.support_recursive_move("/new_location") is True

    def test_support_recursive_delete(self):
        """Test that recursive delete is supported."""
        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)
        assert collection.support_recursive_delete() is True

    def test_support_etag(self):
        """Test that ETags are supported."""
        folder_entry = self._create_folder_entry(id=1, name="docs")
        collection = self._create_collection(folder_entry=folder_entry)
        assert collection.support_etag() is True


class TestDrimeWriteBuffer:
    """Tests for the _DrimeWriteBuffer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()

    def _create_buffer(
        self,
        file_name: str = "test.txt",
        parent_id: int | None = None,
        max_file_size: int = 500 * 1024 * 1024,
    ):
        """Create a _DrimeWriteBuffer."""
        from pydrime.webdav.provider import _DrimeWriteBuffer

        return _DrimeWriteBuffer(
            client=self.mock_client,
            file_name=file_name,
            parent_id=parent_id,
            workspace_id=0,
            content_type="text/plain",
            max_file_size=max_file_size,
        )

    def test_write_and_upload(self):
        """Test writing content and uploading on close."""
        buffer = self._create_buffer()
        self.mock_client.upload_file.return_value = {}

        buffer.write(b"Hello, World!")
        buffer.close()

        self.mock_client.upload_file.assert_called_once()

    def test_empty_content_not_uploaded(self):
        """Test that empty content is not uploaded."""
        buffer = self._create_buffer()
        buffer.close()

        self.mock_client.upload_file.assert_not_called()

    def test_file_too_large(self):
        """Test that files exceeding max size raise error."""
        buffer = self._create_buffer(max_file_size=10)  # 10 bytes max

        buffer.write(b"This is more than 10 bytes of content")

        with pytest.raises(DAVError):
            buffer.close()

    def test_close_idempotent(self):
        """Test that close can be called multiple times."""
        buffer = self._create_buffer()
        self.mock_client.upload_file.return_value = {}

        buffer.write(b"content")
        buffer.close()
        buffer.close()  # Second close should be no-op

        # Should only upload once
        assert self.mock_client.upload_file.call_count == 1


class TestEntriesCache:
    """Tests for the entries cache in DrimeCollection."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.environ = {"wsgidav.provider": MagicMock()}

    def _create_folder_entry(self, id: int = 1, name: str = "docs"):
        """Create a mock folder FileEntry."""
        from pydrime.models import FileEntry

        return FileEntry(
            id=id,
            name=name,
            file_name=name,
            mime="",
            file_size=0,
            parent_id=None,
            created_at="2024-01-01T00:00:00Z",
            type="folder",
            extension=None,
            hash="",
            url="",
            workspace_id=0,
        )

    def _create_file_entry(self, id: int = 1, name: str = "test.txt"):
        """Create a mock file FileEntry."""
        from pydrime.models import FileEntry

        return FileEntry(
            id=id,
            name=name,
            file_name=name,
            mime="text/plain",
            file_size=100,
            parent_id=1,
            created_at="2024-01-01T00:00:00Z",
            type="file",
            extension=name.split(".")[-1] if "." in name else None,
            hash="abc123",
            url="",
            workspace_id=0,
        )

    def _mock_file_entries(self, entries: list):
        """Create mock API response."""
        return {
            "data": [
                {
                    "id": e.id,
                    "name": e.name,
                    "fileName": e.file_name,
                    "mime": e.mime,
                    "fileSize": e.file_size,
                    "parentId": e.parent_id,
                    "createdAt": e.created_at,
                    "type": e.type,
                    "extension": e.extension,
                    "hash": e.hash,
                    "url": e.url,
                    "workspaceId": e.workspace_id,
                }
                for e in entries
            ],
            "total": len(entries),
        }

    def test_cache_used_on_repeated_calls(self):
        """Test that cache is used for repeated member name requests."""
        from pydrime.webdav.provider import DrimeCollection

        folder_entry = self._create_folder_entry()
        collection = DrimeCollection(
            path="/docs",
            environ=self.environ,
            folder_entry=folder_entry,
            client=self.mock_client,
            workspace_id=0,
            readonly=False,
            cache_ttl=30.0,
            max_file_size=500 * 1024 * 1024,
        )

        file_entry = self._create_file_entry()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [file_entry]
        )

        # First call - should hit API
        collection.get_member_names()
        assert self.mock_client.get_file_entries.call_count == 1

        # Second call - should use cache
        collection.get_member_names()
        assert self.mock_client.get_file_entries.call_count == 1

    def test_cache_expires(self):
        """Test that cache expires after TTL."""
        from pydrime.webdav.provider import DrimeCollection

        folder_entry = self._create_folder_entry()
        collection = DrimeCollection(
            path="/docs",
            environ=self.environ,
            folder_entry=folder_entry,
            client=self.mock_client,
            workspace_id=0,
            readonly=False,
            cache_ttl=0.1,  # 100ms TTL
            max_file_size=500 * 1024 * 1024,
        )

        file_entry = self._create_file_entry()
        self.mock_client.get_file_entries.return_value = self._mock_file_entries(
            [file_entry]
        )

        # First call
        collection.get_member_names()
        assert self.mock_client.get_file_entries.call_count == 1

        # Wait for cache expiry
        time.sleep(0.15)

        # Second call - should hit API again
        collection.get_member_names()
        assert self.mock_client.get_file_entries.call_count == 2
