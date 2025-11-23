"""Manager for fetching and caching file entries with automatic pagination."""

from typing import Optional

from .api import DrimeClient
from .exceptions import DrimeAPIError
from .models import FileEntriesResult, FileEntry


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
                # Note: parent_ids=[None] should list root, parent_ids=None lists all
                result = self.client.get_file_entries(
                    parent_ids=[folder_id] if folder_id is not None else [None],
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

        except DrimeAPIError:
            # Return what we have so far
            pass

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

                if exact_match:
                    # Filter for exact matches
                    matching = [e for e in entries.entries if e.name == query]
                    all_entries.extend(matching)
                else:
                    all_entries.extend(entries.entries)

                # Filter by type if specified
                if entry_type:
                    all_entries = [
                        e
                        for e in all_entries
                        if (entry_type == "folder" and e.is_folder)
                        or (entry_type != "folder" and e.type == entry_type)
                    ]

                # Check if there are more pages
                if entries.pagination:
                    current = entries.pagination.get("current_page")
                    last = entries.pagination.get("last_page")
                    if current is not None and last is not None and current < last:
                        current_page += 1
                        continue
                break

        except DrimeAPIError:
            pass

        return all_entries

    def find_folder_by_name(
        self, folder_name: str, parent_id: Optional[int] = None
    ) -> Optional[FileEntry]:
        """Find a folder by exact name match.

        Args:
            folder_name: Folder name to search for
            parent_id: Parent folder ID to search within

        Returns:
            FileEntry if found, None otherwise
        """
        if parent_id is not None:
            # Get all entries in the parent folder and search for exact name match
            # This is more reliable than using the query API which may have issues
            entries = self.get_all_in_folder(folder_id=parent_id, use_cache=False)
            for entry in entries:
                if entry.name == folder_name and entry.is_folder:
                    return entry
        else:
            # Global search
            folders = self.search_by_name(
                query=folder_name, exact_match=True, entry_type="folder"
            )
            if folders:
                return folders[0]

        return None

    def clear_cache(self) -> None:
        """Clear the internal cache."""
        self._cache.clear()
