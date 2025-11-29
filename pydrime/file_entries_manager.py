"""Manager for fetching and caching file entries with automatic pagination."""

import logging
from collections.abc import Generator
from typing import Optional

from .api import DrimeClient
from .exceptions import DrimeAPIError
from .models import FileEntriesResult, FileEntry

logger = logging.getLogger(__name__)


class FileEntriesManager:
    """Manages file entry fetching with automatic pagination and caching."""

    def __init__(self, client: DrimeClient, workspace_id: int = 0):
        """Initialize the file entries manager.

        Args:
            client: Drime API client
            workspace_id: Workspace ID to query (default: 0 for personal)
        """
        self.client = client
        self.workspace_id = workspace_id
        self._cache: dict[str, list[FileEntry]] = {}

    def get_all_in_folder(
        self,
        folder_id: Optional[int] = None,
        use_cache: bool = True,
        per_page: int = 100,
    ) -> list[FileEntry]:
        """Get all file entries in a folder with automatic pagination.

        Args:
            folder_id: Folder ID to query (None for root)
            use_cache: Whether to use cached results
            per_page: Number of entries per page (default: 100)

        Returns:
            List of all file entries in the folder
        """
        cache_key = f"folder:{folder_id}:{self.workspace_id}"

        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]

        all_entries = []
        current_page = 1

        try:
            while True:
                # Note: parent_ids should be None to list root directory
                parent_ids_param = [folder_id] if folder_id is not None else None
                result = self.client.get_file_entries(
                    parent_ids=parent_ids_param,
                    workspace_id=self.workspace_id,
                    per_page=per_page,
                    page=current_page,
                )
                entries = FileEntriesResult.from_api_response(result)
                all_entries.extend(entries.entries)

                # Check if there are more pages
                if entries.pagination:
                    current = entries.pagination.get("current_page")
                    last = entries.pagination.get("last_page")
                    if current is not None and last is not None and current < last:
                        current_page += 1
                        continue
                break

        except DrimeAPIError as e:
            # Log the error but return partial results
            logger.warning(
                f"API error while fetching folder {folder_id}, "
                f"returning {len(all_entries)} partial results: {e}"
            )

        if use_cache:
            self._cache[cache_key] = all_entries

        return all_entries

    def get_all_recursive(
        self,
        folder_id: Optional[int] = None,
        path_prefix: str = "",
        visited: Optional[set[int]] = None,
        per_page: int = 100,
    ) -> list[tuple[FileEntry, str]]:
        """Recursively get all file entries in a folder and subfolders.

        Args:
            folder_id: Folder ID to start from (None for root)
            path_prefix: Path prefix for nested folders
            visited: Set of visited folder IDs (for cycle detection)
            per_page: Number of entries per page

        Returns:
            List of (FileEntry, relative_path) tuples
        """
        if visited is None:
            visited = set()

        # Prevent infinite recursion
        if folder_id is not None and folder_id in visited:
            return []
        if folder_id is not None:
            visited.add(folder_id)

        result_entries = []

        # Get all entries in this folder
        entries = self.get_all_in_folder(
            folder_id=folder_id, use_cache=False, per_page=per_page
        )

        for entry in entries:
            entry_path = f"{path_prefix}/{entry.name}" if path_prefix else entry.name

            if entry.is_folder:
                # Recursively get entries in subfolder
                subfolder_entries = self.get_all_recursive(
                    folder_id=entry.id,
                    path_prefix=entry_path,
                    visited=visited,
                    per_page=per_page,
                )
                result_entries.extend(subfolder_entries)
            else:
                result_entries.append((entry, entry_path))

        return result_entries

    def search_by_name(
        self,
        query: str,
        exact_match: bool = True,
        entry_type: Optional[str] = None,
        per_page: int = 100,
    ) -> list[FileEntry]:
        """Search for file entries by name.

        Args:
            query: Search query
            exact_match: Whether to filter for exact name matches
            entry_type: Filter by entry type (e.g., 'folder')
            per_page: Number of entries per page

        Returns:
            List of matching file entries
        """
        all_entries = []
        current_page = 1

        try:
            while True:
                result = self.client.get_file_entries(
                    query=query,
                    workspace_id=self.workspace_id,
                    per_page=per_page,
                    page=current_page,
                )
                entries = FileEntriesResult.from_api_response(result)

                # Filter current page entries
                page_entries = entries.entries

                if exact_match:
                    # Filter for exact matches on current page
                    page_entries = [e for e in page_entries if e.name == query]

                # Filter by type if specified on current page
                if entry_type:
                    page_entries = [
                        e
                        for e in page_entries
                        if (entry_type == "folder" and e.is_folder)
                        or (entry_type != "folder" and e.type == entry_type)
                    ]

                all_entries.extend(page_entries)

                # If we found exact match, no need to check more pages
                if exact_match and all_entries:
                    break

                # Check if there are more pages
                if entries.pagination:
                    current = entries.pagination.get("current_page")
                    last = entries.pagination.get("last_page")
                    if current is not None and last is not None and current < last:
                        current_page += 1
                        continue
                break

        except DrimeAPIError as e:
            # Log the error but return partial results
            logger.warning(
                f"API error while searching for '{query}', "
                f"returning {len(all_entries)} partial results: {e}"
            )

        return all_entries

    def find_folder_by_name(
        self,
        folder_name: str,
        parent_id: Optional[int] = None,
        search_in_root: bool = True,
    ) -> Optional[FileEntry]:
        """Find a folder by exact name match.

        Args:
            folder_name: Folder name to search for
            parent_id: Parent folder ID to search within
            search_in_root: If True and parent_id is None, search in root (parent_id=0)

        Returns:
            FileEntry if found, None otherwise
        """
        logger.debug(
            f"find_folder_by_name: searching for '{folder_name}' "
            f"in workspace_id={self.workspace_id}, parent_id={parent_id}"
        )
        # Try search API first for faster lookups
        folders = self.search_by_name(
            query=folder_name, exact_match=True, entry_type="folder", per_page=50
        )
        logger.debug(
            f"find_folder_by_name: search_by_name returned {len(folders)} folders"
        )

        # If parent_id specified, filter by parent
        if parent_id is not None:
            if parent_id == 0:
                # Root folder: parent_id can be 0 or None depending on API
                folders = [
                    f for f in folders if f.parent_id == 0 or f.parent_id is None
                ]
            else:
                folders = [f for f in folders if f.parent_id == parent_id]
        elif search_in_root:
            # Search in root means parent_id=0 or None
            folders = [f for f in folders if f.parent_id == 0 or f.parent_id is None]

        # Return first match if found via search API
        if folders:
            return folders[0]

        # Fallback: If search API didn't find the folder, try listing the parent
        # folder directly. This handles cases where:
        # 1. Search index hasn't been updated yet (newly created folder)
        # 2. Search API has issues or is rate-limited
        logger.debug(
            f"Search API did not find folder '{folder_name}', "
            f"falling back to listing parent folder"
        )

        try:
            # Determine which folder to list
            list_folder_id = None
            if parent_id is not None and parent_id != 0:
                list_folder_id = parent_id
            elif search_in_root or parent_id == 0:
                list_folder_id = None  # List root

            entries = self.get_all_in_folder(
                folder_id=list_folder_id, use_cache=False, per_page=100
            )

            # Find folder with exact name match
            for entry in entries:
                if entry.is_folder and entry.name == folder_name:
                    logger.debug(
                        f"Found folder '{folder_name}' via listing (id={entry.id})"
                    )
                    return entry

        except DrimeAPIError as e:
            logger.debug(f"Fallback listing also failed: {e}")

        return None

    def iter_all_recursive(
        self,
        folder_id: Optional[int] = None,
        path_prefix: str = "",
        visited: Optional[set[int]] = None,
        per_page: int = 100,
        batch_size: int = 50,
    ) -> "Generator[list[tuple[FileEntry, str]], None, None]":
        """Recursively iterate all file entries in batches (generator).

        This is a streaming version of get_all_recursive that yields batches
        of files as they're discovered, allowing for immediate processing
        without waiting for all files to be fetched.

        Args:
            folder_id: Folder ID to start from (None for root)
            path_prefix: Path prefix for nested folders
            visited: Set of visited folder IDs (for cycle detection)
            per_page: Number of entries per page when fetching from API
            batch_size: Number of entries to yield per batch

        Yields:
            Batches of (FileEntry, relative_path) tuples
        """

        if visited is None:
            visited = set()

        # Prevent infinite recursion
        if folder_id is not None and folder_id in visited:
            return
        if folder_id is not None:
            visited.add(folder_id)

        current_batch = []
        folders_to_process = []

        # Get all entries in this folder
        entries = self.get_all_in_folder(
            folder_id=folder_id, use_cache=False, per_page=per_page
        )

        for entry in entries:
            entry_path = f"{path_prefix}/{entry.name}" if path_prefix else entry.name

            if entry.is_folder:
                # Store folders for later processing
                folders_to_process.append((entry.id, entry_path))
            else:
                # Add file to current batch
                current_batch.append((entry, entry_path))

                # Yield batch when it reaches batch_size
                if len(current_batch) >= batch_size:
                    yield current_batch
                    current_batch = []

        # Yield remaining files from this folder
        if current_batch:
            yield current_batch

        # Recursively process subfolders
        for subfolder_id, subfolder_path in folders_to_process:
            yield from self.iter_all_recursive(
                folder_id=subfolder_id,
                path_prefix=subfolder_path,
                visited=visited,
                per_page=per_page,
                batch_size=batch_size,
            )

    def get_user_folders(
        self,
        user_id: int,
        use_cache: bool = True,
    ) -> list[FileEntry]:
        """Get all folders for a user in the workspace.

        Args:
            user_id: ID of the user
            use_cache: Whether to use cached results

        Returns:
            List of folder entries
        """
        cache_key = f"user_folders:{user_id}:{self.workspace_id}"

        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]

        try:
            result = self.client.get_user_folders(
                user_id=user_id,
                workspace_id=self.workspace_id,
            )

            # Parse folders from response
            folders_data = result.get("folders", [])
            folders = [FileEntry.from_dict(f) for f in folders_data]

            if use_cache:
                self._cache[cache_key] = folders

            return folders

        except DrimeAPIError as e:
            logger.warning(f"API error while fetching user folders: {e}")
            return []

    def clear_cache(self) -> None:
        """Clear the internal cache."""
        self._cache.clear()
