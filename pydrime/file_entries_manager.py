"""Manager for fetching and caching file entries with automatic pagination."""

import logging
import threading
from collections.abc import Generator
from typing import Optional

from .api import DrimeClient
from .exceptions import DrimeAPIError
from .models import FileEntriesResult, FileEntry

logger = logging.getLogger(__name__)


class FileEntriesManager:
    """Manages file entry fetching with automatic pagination and caching.

    This class provides efficient folder path resolution with caching to avoid
    redundant API calls when uploading many files to the same folder structure.
    The folder path cache maps (base_parent_id, folder_path) -> folder_id.
    """

    def __init__(self, client: DrimeClient, workspace_id: int = 0):
        """Initialize the file entries manager.

        Args:
            client: Drime API client
            workspace_id: Workspace ID to query (default: 0 for personal)
        """
        self.client = client
        self.workspace_id = workspace_id
        self._cache: dict[str, list[FileEntry]] = {}
        # Cache for resolved folder paths: (base_parent_id, path) -> folder_id
        self._folder_path_cache: dict[tuple[Optional[int], str], int] = {}
        # Reverse mapping: folder_id -> list of (base_parent_id, path) cache keys
        # Used for efficient cache invalidation when folders are deleted/renamed
        self._folder_id_to_paths: dict[int, list[tuple[Optional[int], str]]] = {}
        # Lock for thread-safe folder path operations
        self._folder_lock = threading.Lock()

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
                # To list root directory entries, we need parent_ids=[0]
                # parent_ids=None returns all entries (no parent filter)
                if folder_id is not None:
                    parent_ids_param = [folder_id]
                else:
                    # Root folder: use parent_id=0 to get only root-level entries
                    parent_ids_param = [0]
                logger.debug(
                    f"get_all_in_folder: workspace_id={self.workspace_id}, "
                    f"parent_ids={parent_ids_param}, page={current_page}"
                )
                result = self.client.get_file_entries(
                    parent_ids=parent_ids_param,
                    workspace_id=self.workspace_id,
                    per_page=per_page,
                    page=current_page,
                )
                entries = FileEntriesResult.from_api_response(result)
                logger.debug(
                    "get_all_in_folder: got %d entries on page %d",
                    len(entries.entries),
                    current_page,
                )
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

    def ensure_folder_path(
        self,
        folder_path: str,
        base_parent_id: Optional[int] = None,
        create_if_missing: bool = True,
    ) -> Optional[int]:
        """Ensure all folders in the path exist, return the deepest folder's ID.

        This method walks through each component of the folder path, finding or
        creating folders as needed. Results are cached to avoid redundant API
        calls when uploading many files to the same folder structure.

        This method is thread-safe: concurrent calls for the same folder path
        will serialize to prevent race conditions where multiple threads try
        to create the same folder simultaneously.

        Args:
            folder_path: Path like "folder1/folder2/folder3"
            base_parent_id: Starting parent folder ID (None for root)
            create_if_missing: If True, create missing folders; if False,
                return None when a folder doesn't exist

        Returns:
            The ID of the deepest folder, or None if path is empty/root
            or if create_if_missing=False and a folder doesn't exist

        Example:
            >>> manager = FileEntriesManager(client, workspace_id=0)
            >>> folder_id = manager.ensure_folder_path("photos/2024/vacation")
            >>> # Creates photos, photos/2024, photos/2024/vacation if needed
            >>> # Returns ID of "vacation" folder
        """
        if not folder_path or folder_path in (".", "/", ""):
            return base_parent_id

        # Normalize path
        normalized = folder_path.strip("/")
        if not normalized:
            return base_parent_id

        # Check cache first for the full path (without lock for fast path)
        cache_key = (base_parent_id, normalized)
        if cache_key in self._folder_path_cache:
            logger.debug(
                f"ensure_folder_path: cache hit for '{normalized}' "
                f"(base_parent_id={base_parent_id})"
            )
            return self._folder_path_cache[cache_key]

        # Acquire lock for thread-safe folder creation
        # This prevents multiple threads from creating the same folder
        with self._folder_lock:
            # Double-check cache after acquiring lock
            # (another thread may have populated it)
            if cache_key in self._folder_path_cache:
                logger.debug(
                    f"ensure_folder_path: cache hit after lock for '{normalized}' "
                    f"(base_parent_id={base_parent_id})"
                )
                return self._folder_path_cache[cache_key]

            # Split into components
            parts = [p for p in normalized.split("/") if p]
            if not parts:
                return base_parent_id

            current_parent_id = base_parent_id
            current_path_parts: list[str] = []

            for folder_name in parts:
                current_path_parts.append(folder_name)
                partial_path = "/".join(current_path_parts)
                partial_cache_key = (base_parent_id, partial_path)

                # Check if this partial path is already cached
                if partial_cache_key in self._folder_path_cache:
                    current_parent_id = self._folder_path_cache[partial_cache_key]
                    continue

                # Try to find existing folder
                # For root level (current_parent_id is None), use parent_id=0
                search_parent_id = 0 if current_parent_id is None else current_parent_id
                existing = self.find_folder_by_name(
                    folder_name,
                    parent_id=search_parent_id,
                    search_in_root=(current_parent_id is None),
                )

                if existing:
                    current_parent_id = existing.id
                    logger.debug(
                        f"ensure_folder_path: found existing folder '{folder_name}' "
                        f"(id={current_parent_id})"
                    )
                elif create_if_missing:
                    # Create the folder
                    logger.debug(
                        f"ensure_folder_path: creating folder '{folder_name}' "
                        f"(parent_id={current_parent_id})"
                    )
                    result = self.client.create_folder(
                        name=folder_name,
                        parent_id=current_parent_id,
                        workspace_id=self.workspace_id,
                    )
                    if result.get("status") == "success":
                        folder_data = result.get("folder", {})
                        current_parent_id = folder_data.get("id")
                        if current_parent_id is None:
                            raise DrimeAPIError(
                                f"Failed to get folder ID after creating "
                                f"'{folder_name}'"
                            )
                        logger.debug(
                            f"ensure_folder_path: created folder '{folder_name}' "
                            f"(id={current_parent_id})"
                        )
                    else:
                        raise DrimeAPIError(
                            f"Failed to create folder '{folder_name}': {result}"
                        )
                else:
                    # Folder doesn't exist and we're not creating
                    logger.debug(
                        f"ensure_folder_path: folder '{folder_name}' not found "
                        f"and create_if_missing=False"
                    )
                    return None

                # Cache this partial path
                self._cache_folder_path(partial_cache_key, current_parent_id)

            return current_parent_id

    def _cache_folder_path(
        self,
        cache_key: tuple[Optional[int], str],
        folder_id: int,
    ) -> None:
        """Add a folder path to the cache with reverse mapping for invalidation.

        Args:
            cache_key: Tuple of (base_parent_id, path)
            folder_id: The folder ID to cache
        """
        self._folder_path_cache[cache_key] = folder_id

        # Add to reverse mapping for invalidation
        if folder_id not in self._folder_id_to_paths:
            self._folder_id_to_paths[folder_id] = []
        if cache_key not in self._folder_id_to_paths[folder_id]:
            self._folder_id_to_paths[folder_id].append(cache_key)

    def get_cached_folder_id(
        self,
        folder_path: str,
        base_parent_id: Optional[int] = None,
    ) -> Optional[int]:
        """Get folder ID from cache without making API calls.

        Returns None if not in cache (does NOT look up or create).

        Args:
            folder_path: Path like "folder1/folder2"
            base_parent_id: Starting parent folder ID

        Returns:
            Folder ID if cached, None otherwise
        """
        normalized = folder_path.strip("/")
        cache_key = (base_parent_id, normalized)
        return self._folder_path_cache.get(cache_key)

    def cache_folder_path(
        self,
        folder_path: str,
        folder_id: int,
        base_parent_id: Optional[int] = None,
    ) -> None:
        """Manually add a folder path to the cache.

        Useful when folder IDs are known from other sources (e.g., API responses).
        This method is thread-safe.

        Args:
            folder_path: Path like "folder1/folder2"
            folder_id: The folder ID
            base_parent_id: Starting parent folder ID
        """
        normalized = folder_path.strip("/")
        cache_key = (base_parent_id, normalized)
        with self._folder_lock:
            self._cache_folder_path(cache_key, folder_id)

    def invalidate_folder_by_id(self, folder_id: int) -> None:
        """Invalidate all cached paths that reference a folder ID.

        Call this when a folder is deleted or renamed.
        This method is thread-safe.

        Args:
            folder_id: The folder ID to invalidate
        """
        with self._folder_lock:
            if folder_id not in self._folder_id_to_paths:
                return

            # Get all cache keys that reference this folder
            keys_to_remove = self._folder_id_to_paths.pop(folder_id, [])

            for cache_key in keys_to_remove:
                if cache_key in self._folder_path_cache:
                    del self._folder_path_cache[cache_key]
                    logger.debug(
                        f"invalidate_folder_by_id: removed cache entry for "
                        f"path='{cache_key[1]}' (folder_id={folder_id})"
                    )

    def invalidate_folder_path(
        self,
        folder_path: str,
        base_parent_id: Optional[int] = None,
    ) -> None:
        """Invalidate cache for a folder path and all its children.

        Call this when a folder is deleted or renamed and you know the path.
        This method is thread-safe.

        Args:
            folder_path: Path like "folder1/folder2"
            base_parent_id: Starting parent folder ID
        """
        normalized = folder_path.strip("/")
        if not normalized:
            return

        with self._folder_lock:
            # Remove exact match and any paths that start with this path
            keys_to_remove = []
            for cache_key in self._folder_path_cache:
                if cache_key[0] == base_parent_id and (
                    cache_key[1] == normalized
                    or cache_key[1].startswith(normalized + "/")
                ):
                    keys_to_remove.append(cache_key)

            for cache_key in keys_to_remove:
                folder_id = self._folder_path_cache.pop(cache_key, None)
                if folder_id is not None and folder_id in self._folder_id_to_paths:
                    # Remove this cache_key from reverse mapping
                    try:
                        self._folder_id_to_paths[folder_id].remove(cache_key)
                        if not self._folder_id_to_paths[folder_id]:
                            del self._folder_id_to_paths[folder_id]
                    except ValueError:
                        pass
                logger.debug(
                    f"invalidate_folder_path: removed cache entry for "
                    f"path='{cache_key[1]}' (base_parent_id={base_parent_id})"
                )

    def clear_cache(self) -> None:
        """Clear all internal caches. This method is thread-safe."""
        with self._folder_lock:
            self._cache.clear()
            self._folder_path_cache.clear()
            self._folder_id_to_paths.clear()
