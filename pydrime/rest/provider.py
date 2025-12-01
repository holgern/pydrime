"""Storage provider for restic REST API backend.

This module provides storage operations for the restic REST API server,
interfacing with Drime Cloud storage through the DrimeClient.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..api import DrimeClient
    from ..models import FileEntry

logger = logging.getLogger(__name__)

# Valid restic blob types
VALID_TYPES = {"data", "keys", "locks", "snapshots", "index", "config"}


class ResticStorageProvider:
    """Storage provider for restic repository data in Drime Cloud.

    Repository structure in Drime Cloud:
        /{repo_path}/config         - Repository configuration file
        /{repo_path}/data/          - Data blobs (subdirectories 00-ff)
        /{repo_path}/keys/          - Key files
        /{repo_path}/locks/         - Lock files
        /{repo_path}/snapshots/     - Snapshot files
        /{repo_path}/index/         - Index files
    """

    def __init__(
        self,
        client: DrimeClient,
        workspace_id: int = 0,
        readonly: bool = False,
    ) -> None:
        """Initialize the storage provider.

        Args:
            client: The Drime API client
            workspace_id: The workspace ID to use (0 for personal)
            readonly: Whether to allow write operations
        """
        self.client = client
        self.workspace_id = workspace_id
        self._readonly = readonly
        # Cache for folder IDs to reduce API calls
        self._folder_cache: dict[str, int | None] = {}

    def _normalize_path(self, path: str) -> str:
        """Normalize a path by removing leading/trailing slashes."""
        return path.strip("/")

    def _get_folder_id_by_path(
        self, folder_path: str, create: bool = False
    ) -> int | None:
        """Get the folder ID for a given path, optionally creating it.

        Args:
            folder_path: Path like "repo/data" (without leading slash)
            create: If True, create missing folders

        Returns:
            Folder ID or None for root
        """
        if not folder_path:
            return None

        # Check cache first
        if folder_path in self._folder_cache:
            return self._folder_cache[folder_path]

        from ..models import FileEntriesResult

        parts = folder_path.split("/")
        current_folder_id: int | None = None

        for i, part in enumerate(parts):
            # Check cache for partial path
            partial_path = "/".join(parts[: i + 1])
            if partial_path in self._folder_cache:
                current_folder_id = self._folder_cache[partial_path]
                continue

            # Get entries in current folder
            params: dict[str, Any] = {
                "workspace_id": self.workspace_id,
                "per_page": 1000,
            }
            if current_folder_id is not None:
                params["parent_ids"] = [current_folder_id]

            result = self.client.get_file_entries(**params)
            file_entries = FileEntriesResult.from_api_response(result)

            # Filter for root if no parent
            entries = file_entries.entries
            if current_folder_id is None:
                entries = [
                    e for e in entries if e.parent_id is None or e.parent_id == 0
                ]

            # Find the folder
            found = None
            for entry in entries:
                if entry.name == part and entry.is_folder:
                    found = entry
                    break

            if found is None:
                if create and not self._readonly:
                    # Create the folder
                    result = self.client.create_folder(
                        name=part,
                        parent_id=current_folder_id,
                        workspace_id=self.workspace_id,
                    )
                    # Extract folder ID from response
                    folder_data: dict[str, Any] = {}
                    if isinstance(result, dict):
                        if "folder" in result:
                            folder_data = result["folder"]
                        elif "fileEntry" in result:
                            folder_data = result["fileEntry"]
                        elif "id" in result:
                            folder_data = result
                    current_folder_id = folder_data.get("id")
                    logger.debug(f"Created folder '{part}' with ID {current_folder_id}")
                else:
                    return None
            else:
                current_folder_id = found.id

            # Cache the result
            self._folder_cache[partial_path] = current_folder_id

        return current_folder_id

    def _get_file_entry(self, folder_id: int | None, filename: str) -> FileEntry | None:
        """Get a file entry by name in a folder.

        Args:
            folder_id: Parent folder ID (None for root)
            filename: Name of the file

        Returns:
            FileEntry or None if not found
        """
        from ..models import FileEntriesResult

        params: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "per_page": 1000,
        }
        if folder_id is not None:
            params["parent_ids"] = [folder_id]

        result = self.client.get_file_entries(**params)
        file_entries = FileEntriesResult.from_api_response(result)

        # Filter for root if no parent
        entries = file_entries.entries
        if folder_id is None:
            entries = [e for e in entries if e.parent_id is None or e.parent_id == 0]

        for entry in entries:
            if entry.name == filename and not entry.is_folder:
                return entry

        return None

    def repository_exists(self, repo_path: str) -> bool:
        """Check if a repository exists (has a config file).

        Args:
            repo_path: Path to the repository root

        Returns:
            True if config file exists
        """
        exists, _ = self.config_exists(repo_path)
        return exists

    def config_exists(self, repo_path: str) -> tuple[bool, int]:
        """Check if a repository config exists and get its size.

        Args:
            repo_path: Path to the repository root

        Returns:
            Tuple of (exists, size)
        """
        path = self._normalize_path(repo_path)
        folder_id = self._get_folder_id_by_path(path)
        if folder_id is None and path:
            return False, 0

        config_entry = self._get_file_entry(folder_id, "config")
        if config_entry is None:
            return False, 0

        return True, config_entry.file_size or 0

    def create_repository(self, repo_path: str) -> bool:
        """Create repository folder structure.

        Args:
            repo_path: Path to the repository root

        Returns:
            True if created successfully
        """
        if self._readonly:
            return False

        path = self._normalize_path(repo_path)

        # Create main repository folder and subdirectories
        folders_to_create = ["data", "keys", "locks", "snapshots", "index"]

        try:
            # Create repo root
            self._get_folder_id_by_path(path, create=True)

            # Create subdirectories
            for folder in folders_to_create:
                folder_path = f"{path}/{folder}" if path else folder
                self._get_folder_id_by_path(folder_path, create=True)

            return True
        except Exception as e:
            logger.error(f"Error creating repository: {e}")
            return False

    def delete_repository(self, repo_path: str) -> bool:
        """Delete a repository and all its contents.

        Args:
            repo_path: Path to the repository root

        Returns:
            True if deleted successfully
        """
        if self._readonly:
            return False

        path = self._normalize_path(repo_path)
        folder_id = self._get_folder_id_by_path(path)

        if folder_id is None:
            return False

        try:
            self.client.delete_file_entries([folder_id], workspace_id=self.workspace_id)
            # Clear cache entries for this repo
            to_remove = [k for k in self._folder_cache if k.startswith(path)]
            for k in to_remove:
                del self._folder_cache[k]
            return True
        except Exception as e:
            logger.error(f"Error deleting repository: {e}")
            return False

    def get_config(self, repo_path: str) -> bytes | None:
        """Get repository config file content.

        Args:
            repo_path: Path to the repository root

        Returns:
            Config file content or None if not found
        """
        path = self._normalize_path(repo_path)
        folder_id = self._get_folder_id_by_path(path)
        if folder_id is None and path:
            return None

        config_entry = self._get_file_entry(folder_id, "config")
        if config_entry is None or not config_entry.hash:
            return None

        try:
            return self.client.get_file_content(config_entry.hash)
        except Exception as e:
            logger.error(f"Error getting config: {e}")
            return None

    def save_config(self, repo_path: str, data: bytes) -> bool:
        """Save repository config file.

        Args:
            repo_path: Path to the repository root
            data: Config file content

        Returns:
            True if saved successfully
        """
        if self._readonly:
            return False

        if len(data) == 0:
            logger.error("Refusing to save empty config file")
            return False

        path = self._normalize_path(repo_path)
        folder_id = self._get_folder_id_by_path(path, create=True)

        try:
            # Create temp file and upload
            tmp_dir = Path(tempfile.gettempdir())
            tmp_path = tmp_dir / "config"
            tmp_path.write_bytes(data)

            logger.debug(
                f"Uploading config file ({len(data)} bytes) to folder {folder_id}"
            )

            try:
                self.client.upload_file(
                    tmp_path,
                    parent_id=folder_id,
                    workspace_id=self.workspace_id,
                    relative_path="config",
                )
                return True
            finally:
                tmp_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            return False

    def list_blobs(self, repo_path: str, blob_type: str) -> list[dict[str, Any]] | None:
        """List all blobs of a given type.

        Args:
            repo_path: Path to the repository root
            blob_type: Type of blobs (data, keys, locks, snapshots, index)

        Returns:
            List of {name, size} dicts, or None if type folder doesn't exist
        """
        if blob_type not in VALID_TYPES:
            return None

        path = self._normalize_path(repo_path)
        type_path = f"{path}/{blob_type}" if path else blob_type
        folder_id = self._get_folder_id_by_path(type_path)

        if folder_id is None:
            return []

        from ..models import FileEntriesResult

        blobs: list[dict[str, Any]] = []

        # For data type, we need to list subdirectories (00-ff)
        if blob_type == "data":
            # Get all subdirectories
            params: dict[str, Any] = {
                "workspace_id": self.workspace_id,
                "parent_ids": [folder_id],
                "per_page": 1000,
            }
            result = self.client.get_file_entries(**params)
            file_entries = FileEntriesResult.from_api_response(result)

            for entry in file_entries.entries:
                if entry.is_folder:
                    # List files in this subdirectory
                    sub_params: dict[str, Any] = {
                        "workspace_id": self.workspace_id,
                        "parent_ids": [entry.id],
                        "per_page": 1000,
                    }
                    sub_result = self.client.get_file_entries(**sub_params)
                    sub_entries = FileEntriesResult.from_api_response(sub_result)
                    for sub_entry in sub_entries.entries:
                        if not sub_entry.is_folder:
                            blobs.append(
                                {
                                    "name": sub_entry.name,
                                    "size": sub_entry.file_size or 0,
                                }
                            )
        else:
            # List files directly in the type folder
            params = {
                "workspace_id": self.workspace_id,
                "parent_ids": [folder_id],
                "per_page": 1000,
            }
            result = self.client.get_file_entries(**params)
            file_entries = FileEntriesResult.from_api_response(result)

            for entry in file_entries.entries:
                if not entry.is_folder:
                    blobs.append(
                        {
                            "name": entry.name,
                            "size": entry.file_size or 0,
                        }
                    )

        return blobs

    def blob_exists(
        self, repo_path: str, blob_type: str, name: str
    ) -> tuple[bool, int]:
        """Check if a blob exists and get its size.

        Args:
            repo_path: Path to the repository root
            blob_type: Type of blob
            name: Blob name

        Returns:
            Tuple of (exists, size)
        """
        if blob_type not in VALID_TYPES:
            return False, 0

        path = self._normalize_path(repo_path)

        # For data blobs, they're stored in subdirectories based on first 2 chars
        if blob_type == "data" and len(name) >= 2:
            subdir = name[:2]
            type_path = (
                f"{path}/{blob_type}/{subdir}" if path else f"{blob_type}/{subdir}"
            )
        else:
            type_path = f"{path}/{blob_type}" if path else blob_type

        folder_id = self._get_folder_id_by_path(type_path)
        if folder_id is None:
            return False, 0

        entry = self._get_file_entry(folder_id, name)
        if entry is None:
            return False, 0

        return True, entry.file_size or 0

    def get_blob(self, repo_path: str, blob_type: str, name: str) -> bytes | None:
        """Get blob content.

        Args:
            repo_path: Path to the repository root
            blob_type: Type of blob
            name: Blob name

        Returns:
            Blob content or None if not found
        """
        if blob_type not in VALID_TYPES:
            return None

        path = self._normalize_path(repo_path)

        # For data blobs, they're stored in subdirectories based on first 2 chars
        if blob_type == "data" and len(name) >= 2:
            subdir = name[:2]
            type_path = (
                f"{path}/{blob_type}/{subdir}" if path else f"{blob_type}/{subdir}"
            )
        else:
            type_path = f"{path}/{blob_type}" if path else blob_type

        folder_id = self._get_folder_id_by_path(type_path)
        if folder_id is None:
            return None

        entry = self._get_file_entry(folder_id, name)
        if entry is None or not entry.hash:
            return None

        try:
            return self.client.get_file_content(entry.hash)
        except Exception as e:
            logger.error(f"Error getting blob {blob_type}/{name}: {e}")
            return None

    def save_blob(self, repo_path: str, blob_type: str, name: str, data: bytes) -> bool:
        """Save blob content.

        Args:
            repo_path: Path to the repository root
            blob_type: Type of blob
            name: Blob name
            data: Blob content

        Returns:
            True if saved successfully
        """
        if self._readonly:
            return False

        if blob_type not in VALID_TYPES:
            return False

        path = self._normalize_path(repo_path)

        # For data blobs, they're stored in subdirectories based on first 2 chars
        if blob_type == "data" and len(name) >= 2:
            subdir = name[:2]
            type_path = (
                f"{path}/{blob_type}/{subdir}" if path else f"{blob_type}/{subdir}"
            )
        else:
            type_path = f"{path}/{blob_type}" if path else blob_type

        folder_id = self._get_folder_id_by_path(type_path, create=True)

        try:
            # Create temp file and upload
            tmp_dir = Path(tempfile.gettempdir())
            tmp_path = tmp_dir / name
            tmp_path.write_bytes(data)

            try:
                self.client.upload_file(
                    tmp_path,
                    parent_id=folder_id,
                    workspace_id=self.workspace_id,
                    relative_path=name,
                )
                return True
            finally:
                tmp_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"Error saving blob {blob_type}/{name}: {e}")
            return False

    def delete_blob(self, repo_path: str, blob_type: str, name: str) -> bool:
        """Delete a blob.

        Args:
            repo_path: Path to the repository root
            blob_type: Type of blob
            name: Blob name

        Returns:
            True if deleted successfully
        """
        if self._readonly:
            return False

        if blob_type not in VALID_TYPES:
            return False

        path = self._normalize_path(repo_path)

        # For data blobs, they're stored in subdirectories based on first 2 chars
        if blob_type == "data" and len(name) >= 2:
            subdir = name[:2]
            type_path = (
                f"{path}/{blob_type}/{subdir}" if path else f"{blob_type}/{subdir}"
            )
        else:
            type_path = f"{path}/{blob_type}" if path else blob_type

        folder_id = self._get_folder_id_by_path(type_path)
        if folder_id is None:
            return False

        entry = self._get_file_entry(folder_id, name)
        if entry is None:
            return False

        try:
            self.client.delete_file_entries([entry.id], workspace_id=self.workspace_id)
            return True
        except Exception as e:
            logger.error(f"Error deleting blob {blob_type}/{name}: {e}")
            return False

    def is_readonly(self) -> bool:
        """Check if the provider is in read-only mode."""
        return self._readonly
