"""Tests for CLI adapter classes."""

from unittest.mock import MagicMock

from pydrime.api import DrimeClient


class TestDrimeClientAdapter:
    """Tests for _DrimeClientAdapter class."""

    def test_initialization(self):
        """Test adapter initialization."""
        # Import inside test to avoid syncengine dependency at module level
        from pydrime.cli.adapters import _DrimeClientAdapter

        mock_client = MagicMock(spec=DrimeClient)
        adapter = _DrimeClientAdapter(mock_client)

        assert adapter._client == mock_client

    def test_upload_file_adapts_storage_id(self, tmp_path):
        """Test upload_file converts storage_id to workspace_id."""
        from pydrime.cli.adapters import _DrimeClientAdapter

        mock_client = MagicMock(spec=DrimeClient)
        mock_client.upload_file.return_value = {"fileEntry": {"id": 123}}
        adapter = _DrimeClientAdapter(mock_client)

        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        # Call with storage_id (syncengine convention)
        result = adapter.upload_file(
            file_path=test_file,
            relative_path="test.txt",
            storage_id=5,  # syncengine uses storage_id
            chunk_size=1024,
            use_multipart_threshold=2048,
        )

        # Verify client was called with workspace_id (pydrime convention)
        mock_client.upload_file.assert_called_once_with(
            file_path=test_file,
            relative_path="test.txt",
            workspace_id=5,  # Should convert to workspace_id
            chunk_size=1024,
            use_multipart_threshold=2048,
            progress_callback=None,
        )

        assert result == {"fileEntry": {"id": 123}}

    def test_upload_file_with_progress_callback(self, tmp_path):
        """Test upload_file forwards progress_callback."""
        from pydrime.cli.adapters import _DrimeClientAdapter

        mock_client = MagicMock(spec=DrimeClient)
        mock_client.upload_file.return_value = {"fileEntry": {"id": 456}}
        adapter = _DrimeClientAdapter(mock_client)

        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        callback = MagicMock()

        adapter.upload_file(
            file_path=test_file,
            relative_path="test.txt",
            storage_id=0,
            progress_callback=callback,
        )

        # Verify callback was passed through
        call_kwargs = mock_client.upload_file.call_args.kwargs
        assert call_kwargs["progress_callback"] == callback

    def test_create_folder_adapts_storage_id(self):
        """Test create_folder converts storage_id to workspace_id."""
        from pydrime.cli.adapters import _DrimeClientAdapter

        mock_client = MagicMock(spec=DrimeClient)
        mock_client.create_folder.return_value = {"folder": {"id": 789}}
        adapter = _DrimeClientAdapter(mock_client)

        # Call with storage_id (syncengine convention)
        result = adapter.create_folder(name="test_folder", parent_id=100, storage_id=5)

        # Verify client was called with workspace_id (pydrime convention)
        mock_client.create_folder.assert_called_once_with(
            name="test_folder",
            parent_id=100,
            workspace_id=5,  # Should convert to workspace_id
        )

        assert result == {"folder": {"id": 789}}

    def test_create_folder_without_parent(self):
        """Test create_folder works with no parent_id."""
        from pydrime.cli.adapters import _DrimeClientAdapter

        mock_client = MagicMock(spec=DrimeClient)
        mock_client.create_folder.return_value = {"folder": {"id": 999}}
        adapter = _DrimeClientAdapter(mock_client)

        result = adapter.create_folder(name="root_folder", storage_id=0)

        # Verify call
        mock_client.create_folder.assert_called_once_with(
            name="root_folder", parent_id=None, workspace_id=0
        )

        assert result == {"folder": {"id": 999}}

    def test_attribute_forwarding(self):
        """Test that unknown attributes are forwarded to wrapped client."""
        from pydrime.cli.adapters import _DrimeClientAdapter

        mock_client = MagicMock(spec=DrimeClient)
        mock_client.get_workspaces.return_value = {"workspaces": []}
        mock_client.get_file_entries.return_value = {"data": []}

        adapter = _DrimeClientAdapter(mock_client)

        # Access methods that aren't overridden in adapter
        workspaces = adapter.get_workspaces()
        assert workspaces == {"workspaces": []}

        entries = adapter.get_file_entries()
        assert entries == {"data": []}

    def test_adapter_with_syncengine(self, tmp_path):
        """Integration test: verify adapter works with SyncEngine."""
        from pydrime.cli.adapters import _DrimeClientAdapter

        mock_client = MagicMock(spec=DrimeClient)
        mock_client.upload_file.return_value = {"fileEntry": {"id": 111}}
        mock_client.create_folder.return_value = {"folder": {"id": 222}}

        adapter = _DrimeClientAdapter(mock_client)

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        # Simulate syncengine calling the adapter methods
        # syncengine always uses storage_id parameter
        folder_result = adapter.create_folder(
            name="uploads", parent_id=None, storage_id=10
        )

        upload_result = adapter.upload_file(
            file_path=test_file, relative_path="uploads/test.txt", storage_id=10
        )

        # Verify both calls worked
        assert folder_result["folder"]["id"] == 222
        assert upload_result["fileEntry"]["id"] == 111

        # Verify client was called with correct parameter names
        assert mock_client.create_folder.call_args.kwargs["workspace_id"] == 10
        assert mock_client.upload_file.call_args.kwargs["workspace_id"] == 10


class TestEntriesManagerFactory:
    """Tests for _create_entries_manager_factory."""

    def test_factory_creates_adapted_manager(self):
        """Test factory creates FileEntriesManager with proper adaptation."""
        from pydrime.cli.adapters import create_entries_manager_factory

        factory = create_entries_manager_factory()

        mock_client = MagicMock()

        # Call factory with storage_id (syncengine convention)
        manager = factory(mock_client, storage_id=5)

        # Verify manager was created
        assert manager is not None
        # Manager should have the wrapped manager
        assert hasattr(manager, "_manager")
        assert manager._manager.workspace_id == 5
