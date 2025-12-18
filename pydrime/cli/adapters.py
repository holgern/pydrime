"""Adapter classes for syncengine compatibility."""

from pathlib import Path
from typing import Callable, Optional

from ..file_entries_manager import FileEntriesManager
from ..models import FileEntry


class _FileEntriesManagerAdapter:
    """Adapter to make FileEntriesManager compatible with syncengine's
    FileEntriesManagerProtocol.

    This adapter wraps pydrime's FileEntriesManager and adapts its method signatures
    to match the protocol expected by syncengine.
    """

    def __init__(self, manager: FileEntriesManager):
        self._manager = manager

    def find_folder_by_name(self, name: str, parent_id: int = 0) -> Optional[FileEntry]:
        """Find folder by name (adapted signature for syncengine protocol)."""
        # Convert parent_id: 0 → None (syncengine uses 0 for root, pydrime uses None)
        actual_parent_id = None if parent_id == 0 else parent_id
        return self._manager.find_folder_by_name(name, parent_id=actual_parent_id)

    def get_all_recursive(
        self, folder_id: Optional[int], path_prefix: str
    ) -> list[tuple[FileEntry, str]]:
        """Get all entries recursively (adapted signature for syncengine protocol)."""
        return self._manager.get_all_recursive(
            folder_id=folder_id, path_prefix=path_prefix
        )

    def iter_all_recursive(
        self, folder_id: Optional[int], path_prefix: str, batch_size: int
    ):
        """Iterate all entries recursively in batches (adapted signature for
        syncengine protocol)."""
        return self._manager.iter_all_recursive(
            folder_id=folder_id, path_prefix=path_prefix, batch_size=batch_size
        )


class _DrimeClientAdapter:
    """Adapter to make DrimeClient compatible with syncengine's StorageClientProtocol.

    This adapter wraps pydrime's DrimeClient and adapts parameter names
    to match the protocol expected by syncengine (storage_id → workspace_id).
    """

    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        """Forward all other attributes to the wrapped client."""
        return getattr(self._client, name)

    def upload_file(
        self,
        file_path: Path,
        relative_path: str,
        storage_id: int = 0,
        chunk_size: int = 25 * 1024 * 1024,
        use_multipart_threshold: int = 100 * 1024 * 1024,
        progress_callback: Optional[Callable] = None,
    ):
        """Upload file (adapted signature for syncengine protocol).

        Converts storage_id → workspace_id for DrimeClient.
        """
        return self._client.upload_file(
            file_path=file_path,
            relative_path=relative_path,
            workspace_id=storage_id,  # Convert storage_id to workspace_id
            chunk_size=chunk_size,
            use_multipart_threshold=use_multipart_threshold,
            progress_callback=progress_callback,
        )

    def create_folder(
        self,
        name: str,
        parent_id: Optional[int] = None,
        storage_id: int = 0,
    ):
        """Create folder (adapted signature for syncengine protocol).

        Converts storage_id → workspace_id for DrimeClient.
        """
        return self._client.create_folder(
            name=name,
            parent_id=parent_id,
            workspace_id=storage_id,
        )


def create_entries_manager_factory():
    """Create a factory function for FileEntriesManager.

    This factory is required by SyncEngine to create FileEntriesManager instances
    that implement the FileEntriesManagerProtocol from syncengine.

    Returns:
        A callable that takes (client, storage_id) and returns an adapted
        FileEntriesManager
    """

    def factory(client, storage_id: int):
        manager = FileEntriesManager(client, workspace_id=storage_id)
        return _FileEntriesManagerAdapter(manager)

    return factory
