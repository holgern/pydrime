"""Drime Cloud WebDAV provider implementation using WsgiDAV.

This module provides a WebDAV server that exposes Drime Cloud storage,
allowing clients to mount cloud storage as a local drive.
"""

from __future__ import annotations

import logging
import tempfile
import time
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

from wsgidav.dav_error import (  # type: ignore[import-untyped]
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
    DAVError,
)
from wsgidav.dav_provider import (  # type: ignore[import-untyped]
    DAVCollection,
    DAVNonCollection,
    DAVProvider,
)

if TYPE_CHECKING:
    from ..api import DrimeClient
    from ..models import FileEntry

logger = logging.getLogger(__name__)

# Default file size limits
DEFAULT_MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB
DEFAULT_CACHE_TTL = 30.0  # 30 seconds


class DrimeResource(DAVNonCollection):
    """Represents a Drime Cloud file as a WebDAV resource."""

    def __init__(
        self,
        path: str,
        environ: dict[str, Any],
        file_entry: FileEntry,
        client: DrimeClient,
        workspace_id: int,
        readonly: bool = False,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
        parent_collection: DrimeCollection | None = None,
        provider: DrimeDAVProvider | None = None,
    ) -> None:
        """Initialize the resource.

        Args:
            path: The WebDAV path
            environ: WSGI environment
            file_entry: The Drime Cloud file entry
            client: The Drime API client
            workspace_id: Workspace ID
            readonly: Whether the resource is read-only
            max_file_size: Maximum file size for uploads/downloads
            parent_collection: Reference to parent collection for cache invalidation
            provider: Reference to the DAV provider for cache management
        """
        super().__init__(path, environ)
        # Debug: check if lock_manager is set
        env_provider = environ.get("wsgidav.provider")
        logger.debug(
            f"DrimeResource.__init__: path={path}, "
            f"environ provider id={id(env_provider) if env_provider else 'None'}, "
            f"self.provider id={id(self.provider)}, "
            f"lock_manager={getattr(self.provider, 'lock_manager', 'NOT_SET')}"
        )
        self.file_entry = file_entry
        self.client = client
        self.workspace_id = workspace_id
        self._readonly = readonly
        self._max_file_size = max_file_size
        self._parent_collection = parent_collection
        self._provider = provider

    def get_content_length(self) -> int:
        """Return the content length."""
        return self.file_entry.file_size or 0

    def get_content_type(self) -> str:
        """Return the content type (MIME type)."""
        return self.file_entry.mime or "application/octet-stream"

    def get_creation_date(self) -> float:
        """Return the creation date as timestamp."""
        if self.file_entry.created_at:
            try:
                from ..utils import parse_iso_timestamp

                dt = parse_iso_timestamp(self.file_entry.created_at)
                if dt:
                    return dt.timestamp()
            except Exception:
                pass
        return time.time()

    def get_last_modified(self) -> float:
        """Return the last modified date as timestamp."""
        if self.file_entry.updated_at:
            try:
                from ..utils import parse_iso_timestamp

                dt = parse_iso_timestamp(self.file_entry.updated_at)
                if dt:
                    return dt.timestamp()
            except Exception:
                pass
        return self.get_creation_date()

    def get_etag(self) -> str:
        """Return the ETag for this resource.

        Note: WsgiDAV adds quotes automatically, so we return unquoted value.
        """
        if self.file_entry.hash:
            # Strip any existing quotes from the hash
            return self.file_entry.hash.strip('"')
        return f"{self.file_entry.id}-{self.get_last_modified()}"

    def support_etag(self) -> bool:
        """Return True if ETags are supported."""
        return True

    def support_ranges(self) -> bool:
        """Return True if range requests are supported."""
        return False  # Drime API doesn't support range requests easily

    def get_property_value(self, name: str) -> Any:
        """Return the value of a property with debug logging for locks."""
        if name == "{DAV:}lockdiscovery":
            lm = self.provider.lock_manager
            ref_url = self.get_ref_url()
            logger.debug(
                f"DrimeResource.get_property_value: name={name}, "
                f"ref_url={ref_url!r}, provider id={id(self.provider)}, "
                f"lock_manager={lm}"
            )
            if lm:
                lock_list = lm.get_url_lock_list(ref_url)
                logger.debug(
                    f"DrimeResource.get_property_value: found {len(lock_list)} locks"
                )
                for i, lock in enumerate(lock_list):
                    logger.debug(f"  Lock {i}: {lock}")
            else:
                logger.warning(
                    "DrimeResource.get_property_value: lock_manager is None!"
                )
        return super().get_property_value(name)

    def get_content(self) -> BytesIO:
        """Return the file content as a file-like object."""
        if not self.file_entry.hash:
            raise DAVError(HTTP_NOT_FOUND, "File hash not available")

        # Check file size limit
        if (
            self.file_entry.file_size
            and self.file_entry.file_size > self._max_file_size
        ):
            raise DAVError(
                HTTP_FORBIDDEN,
                f"File too large ({self.file_entry.file_size} bytes). "
                f"Maximum size is {self._max_file_size} bytes.",
            )

        try:
            content = self.client.get_file_content(self.file_entry.hash)
            return BytesIO(content)
        except Exception as e:
            logger.error(f"Error reading file content: {e}")
            raise DAVError(HTTP_NOT_FOUND, f"Error reading file: {e}") from None

    def begin_write(self, content_type: str | None = None) -> BytesIO:
        """Return a file-like object for writing content.

        Args:
            content_type: The content type of the data being written

        Returns:
            A BytesIO buffer that will upload to Drime when closed
        """
        if self._readonly:
            raise DAVError(HTTP_FORBIDDEN, "Resource is read-only")

        return _DrimeWriteBuffer(
            client=self.client,
            file_name=self.file_entry.name,
            parent_id=self.file_entry.parent_id,
            workspace_id=self.workspace_id,
            content_type=content_type,
            max_file_size=self._max_file_size,
            parent_collection=self._parent_collection,
            provider=self._provider,
            file_path=self.path,
        )

    def delete(self) -> None:
        """Delete this resource."""
        if self._readonly:
            raise DAVError(HTTP_FORBIDDEN, "Resource is read-only")

        try:
            # If this is a placeholder entry (id=0), we need to find the real entry
            entry_id = self.file_entry.id
            if entry_id == 0:
                # Try to find the actual entry from the API
                from ..models import FileEntriesResult

                params: dict[str, Any] = {
                    "workspace_id": self.workspace_id,
                    "per_page": 1000,
                }
                if self.file_entry.parent_id is not None:
                    params["parent_ids"] = [self.file_entry.parent_id]

                result = self.client.get_file_entries(**params)
                file_entries = FileEntriesResult.from_api_response(result)

                # Filter for root if no parent
                entries = file_entries.entries
                if self.file_entry.parent_id is None:
                    entries = [
                        e for e in entries if e.parent_id is None or e.parent_id == 0
                    ]

                # Find the entry by name
                for entry in entries:
                    if entry.name == self.file_entry.name:
                        entry_id = entry.id
                        break

                if entry_id == 0:
                    # Entry not found in API yet - just register as deleted
                    if self._provider is not None:
                        self._provider._register_delete(self.path)
                    return

            self.client.delete_file_entries([entry_id], workspace_id=self.workspace_id)

            # Register the deletion
            if self._provider is not None:
                self._provider._register_delete(self.path)

        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            raise DAVError(HTTP_FORBIDDEN, f"Error deleting file: {e}") from None

    def _get_real_entry_id(self) -> int:
        """Get the real entry ID, looking it up from API if needed.

        Returns:
            The entry ID

        Raises:
            DAVError: If the entry cannot be found
        """
        logger.debug(
            f"DrimeResource._get_real_entry_id: name={self.file_entry.name}, "
            f"id={self.file_entry.id}, parent_id={self.file_entry.parent_id}"
        )
        if self.file_entry.id != 0:
            return self.file_entry.id

        from ..models import FileEntriesResult

        params: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "per_page": 1000,
        }
        if self.file_entry.parent_id is not None:
            params["parent_ids"] = [self.file_entry.parent_id]

        result = self.client.get_file_entries(**params)
        file_entries = FileEntriesResult.from_api_response(result)

        entries = file_entries.entries
        if self.file_entry.parent_id is None:
            entries = [e for e in entries if e.parent_id is None or e.parent_id == 0]

        logger.debug(
            f"DrimeResource._get_real_entry_id: Found {len(entries)} entries, "
            f"looking for '{self.file_entry.name}'"
        )
        for e in entries:
            logger.debug(f"  - {e.name} (id={e.id}, is_folder={e.is_folder})")

        for entry in entries:
            if entry.name == self.file_entry.name:
                logger.debug(f"DrimeResource._get_real_entry_id: Found id={entry.id}")
                return entry.id

        logger.warning(
            f"DrimeResource._get_real_entry_id: Could not find '{self.file_entry.name}'"
        )
        raise DAVError(HTTP_FORBIDDEN, "Resource not found")

    def copy_move_single(self, dest_path: str, is_move: bool) -> bool:
        """Copy or move this resource to a new location.

        Args:
            dest_path: Destination path
            is_move: True for move, False for copy

        Returns:
            True if a resource was overwritten
        """
        logger.debug(
            f"DrimeResource.copy_move_single: source={self.path}, dest={dest_path}, "
            f"is_move={is_move}, file_entry.name={self.file_entry.name}, "
            f"file_entry.id={self.file_entry.id}"
        )
        if self._readonly:
            raise DAVError(HTTP_FORBIDDEN, "Resource is read-only")

        # Get the real entry ID (may need API lookup for placeholders)
        entry_id = self._get_real_entry_id()

        # Parse destination path to get parent folder and new name
        # Handle trailing slash - if dest ends with /, the file should be moved
        # INTO that folder with its original name
        dest_ends_with_slash = dest_path.endswith("/")
        dest_path = dest_path.rstrip("/")
        dest_parts = dest_path.strip("/").split("/")

        if dest_ends_with_slash:
            # Destination is a folder - move file into it, keeping original name
            new_name = self.file_entry.name
            dest_parent_path = "/" + "/".join(dest_parts) if dest_parts else "/"
        else:
            # Destination is a file path - move and potentially rename
            new_name = dest_parts[-1] if dest_parts else self.file_entry.name
            dest_parent_path = (
                "/" + "/".join(dest_parts[:-1]) if len(dest_parts) > 1 else "/"
            )

        logger.debug(
            f"DrimeResource.copy_move_single: entry_id={entry_id}, "
            f"new_name={new_name}, dest_parent_path={dest_parent_path}, "
            f"dest_ends_with_slash={dest_ends_with_slash}"
        )

        try:
            # Find the destination parent folder
            dest_parent_id: int | None = None
            if dest_parent_path != "/":
                # Traverse to find destination parent
                from ..models import FileEntriesResult

                current_folder_id: int | None = None
                for part in dest_parent_path.strip("/").split("/"):
                    # First check the recent creates cache
                    current_path = "/" + "/".join(
                        dest_parent_path.strip("/").split("/")[
                            : dest_parent_path.strip("/").split("/").index(part) + 1
                        ]
                    )
                    logger.debug(
                        f"DrimeResource.copy_move_single: looking for part={part}, "
                        f"current_path={current_path}"
                    )
                    if self._provider is not None:
                        created, cached = self._provider._is_recently_created(
                            current_path
                        )
                        if created and cached is not None and cached.is_folder:
                            logger.debug(
                                f"DrimeResource.copy_move_single: found in cache, "
                                f"id={cached.id}"
                            )
                            current_folder_id = cached.id
                            continue

                    params: dict[str, Any] = {
                        "workspace_id": self.workspace_id,
                        "per_page": 1000,
                    }
                    if current_folder_id is not None:
                        params["parent_ids"] = [current_folder_id]

                    result = self.client.get_file_entries(**params)
                    file_entries = FileEntriesResult.from_api_response(result)

                    entries = file_entries.entries
                    if current_folder_id is None:
                        entries = [
                            e
                            for e in entries
                            if e.parent_id is None or e.parent_id == 0
                        ]

                    found = None
                    for entry in entries:
                        if entry.name == part and entry.is_folder:
                            found = entry
                            break

                    if found is None:
                        logger.warning(
                            f"DrimeResource.copy_move_single: parent not found: {part} "
                            f"(available: {[e.name for e in entries]})"
                        )
                        raise DAVError(
                            HTTP_FORBIDDEN, f"Destination parent not found: {part}"
                        )
                    current_folder_id = found.id

                dest_parent_id = current_folder_id

            logger.debug(
                f"DrimeResource.copy_move_single: dest_parent_id={dest_parent_id}, "
                f"calling move_file_entries"
            )

            if is_move:
                # Move the file
                logger.debug(
                    f"DrimeResource.copy_move_single: calling move_file_entries "
                    f"entry_id={entry_id}, dest_parent_id={dest_parent_id}"
                )
                self.client.move_file_entries(
                    [entry_id],
                    destination_id=dest_parent_id,
                    workspace_id=self.workspace_id,
                )
                # Rename if needed
                if new_name != self.file_entry.name:
                    logger.debug(
                        f"DrimeResource.copy_move_single: calling rename_file_entry "
                        f"entry_id={entry_id}, new_name={new_name}, "
                        f"initial_name={self.file_entry.name}"
                    )
                    self.client.rename_file_entry(
                        entry_id,
                        new_name=new_name,
                        initial_name=self.file_entry.name,
                        workspace_id=self.workspace_id,
                    )
                logger.debug("DrimeResource.copy_move_single: move completed")
            else:
                # Copy (duplicate) the file
                self.client.duplicate_file_entries(
                    [entry_id],
                    destination_id=dest_parent_id,
                    workspace_id=self.workspace_id,
                )
                # Note: For copy with rename, we'd need to find the new entry
                # and rename it. This is a limitation - the duplicate API
                # doesn't support custom names

            # Invalidate caches
            if self._parent_collection is not None:
                self._parent_collection._entries_cache = None

            # Register the newly created path for eventual consistency handling
            # Create a new entry with the correct ID and new name/parent
            if self._provider is not None:
                from ..models import FileEntry

                moved_entry = FileEntry(
                    id=entry_id,  # Keep the same ID for moves
                    name=new_name,
                    file_name=new_name,
                    mime=self.file_entry.mime or "application/octet-stream",
                    file_size=self.file_entry.file_size or 0,
                    parent_id=dest_parent_id,
                    created_at=self.file_entry.created_at or "",
                    type=self.file_entry.type or "file",
                    extension=self.file_entry.extension,
                    hash=self.file_entry.hash or "",
                    url=self.file_entry.url or "",
                    workspace_id=self.workspace_id,
                )
                self._provider._register_create(dest_path, moved_entry)

                # Copy/move dead properties
                prop_manager = self._provider.prop_manager
                if prop_manager:
                    dest_res = self._provider.get_resource_inst(dest_path, self.environ)
                    if dest_res is not None:
                        if is_move:
                            prop_manager.move_properties(
                                self.get_ref_url(),
                                dest_res.get_ref_url(),
                                with_children=False,
                                environ=self.environ,
                            )
                        else:
                            prop_manager.copy_properties(
                                self.get_ref_url(),
                                dest_res.get_ref_url(),
                                self.environ,
                            )

            return False  # We don't track if something was overwritten

        except DAVError:
            raise
        except Exception as e:
            logger.error(f"Error {'moving' if is_move else 'copying'} file: {e}")
            raise DAVError(
                HTTP_FORBIDDEN, f"Error {'moving' if is_move else 'copying'} file: {e}"
            ) from None

    def support_recursive_move(self, dest_path: str) -> bool:
        """Return True if move_recursive() is available.

        Files don't support recursive move - only collections do.
        """
        return False

    def handle_copy(
        self, dest_path: str, *, depth_infinity: bool
    ) -> bool | list[tuple[str, DAVError]]:
        """Handle COPY request natively for files.

        This handles the case where a file is copied to a path that currently
        contains a collection (folder). We need to delete the collection first.

        Args:
            dest_path: Destination path
            depth_infinity: True if Depth: infinity header was sent (ignored for files)

        Returns:
            True if handled successfully, False to let WsgiDAV handle it,
            or list of errors
        """
        logger.info(
            f"DrimeResource.handle_copy: source={self.path}, dest={dest_path}, "
            f"depth_infinity={depth_infinity}"
        )

        if self._readonly:
            return [(self.path, DAVError(HTTP_FORBIDDEN, "Resource is read-only"))]

        # Check if the destination exists and is a collection
        # In that case we need to delete it first (overwrite handling)
        dest_was_collection = False
        if self._provider is not None:
            dest_path_clean = dest_path.rstrip("/")
            dest_res = self._provider.get_resource_inst(dest_path_clean, self.environ)
            logger.info(
                f"DrimeResource.handle_copy: dest_res={dest_res}, "
                f"type={type(dest_res).__name__ if dest_res else None}"
            )
            if dest_res is not None and isinstance(dest_res, DrimeCollection):
                logger.info(
                    f"DrimeResource.handle_copy: Destination {dest_path} "
                    "is a collection, deleting before copy"
                )
                try:
                    dest_res.delete()
                    dest_was_collection = True
                    logger.info(
                        f"DrimeResource.handle_copy: Deleted collection {dest_path}"
                    )
                except DAVError as e:
                    logger.error(
                        f"DrimeResource.handle_copy: Error deleting collection: {e}"
                    )
                    return [(dest_path, e)]
                except Exception as e:
                    logger.error(f"DrimeResource.handle_copy: Exception deleting: {e}")
                    return [(dest_path, DAVError(HTTP_FORBIDDEN, str(e)))]

        # Now do the actual copy
        # If we deleted a collection, we want to create a FILE at that location,
        # not put the file inside it (which would fail since it no longer exists).
        # So we strip the trailing slash if the destination was a collection.
        actual_dest_path = dest_path
        if dest_was_collection and dest_path.endswith("/"):
            actual_dest_path = dest_path.rstrip("/")
            logger.info(
                f"DrimeResource.handle_copy: Adjusted dest from {dest_path} "
                f"to {actual_dest_path} (deleted collection)"
            )

        try:
            logger.info(
                f"DrimeResource.handle_copy: Calling copy_move_single "
                f"with dest={actual_dest_path}"
            )
            overwritten = self.copy_move_single(actual_dest_path, is_move=False)
            logger.info(
                f"DrimeResource.handle_copy: Success, overwritten={overwritten}"
            )
            return True
        except DAVError as e:
            logger.error(f"DrimeResource.handle_copy: DAVError: {e}")
            return [(self.path, e)]
        except Exception as e:
            logger.error(f"DrimeResource.handle_copy: Exception: {e}")
            return [(self.path, DAVError(HTTP_FORBIDDEN, str(e)))]

    def handle_move(self, dest_path: str) -> bool | list[tuple[str, DAVError]]:
        """Handle MOVE request natively for files.

        This handles the special case where a file is moved to a collection
        (folder) destination. In this case, the file should be moved INTO
        the collection, not replace it.

        Args:
            dest_path: Destination path

        Returns:
            True if handled successfully, False to let WsgiDAV handle it,
            or list of errors
        """
        logger.info(f"DrimeResource.handle_move: source={self.path}, dest={dest_path}")

        if self._readonly:
            return [(self.path, DAVError(HTTP_FORBIDDEN, "Resource is read-only"))]

        # Check if destination is an existing resource (file or collection)
        # When overwriting, we need to delete it first
        dest_was_collection = False
        if self._provider is not None:
            dest_path_clean = dest_path.rstrip("/")
            dest_res = self._provider.get_resource_inst(dest_path_clean, self.environ)
            logger.info(
                f"DrimeResource.handle_move: dest_res={dest_res}, "
                f"type={type(dest_res).__name__ if dest_res else None}"
            )
            if dest_res is not None:
                if isinstance(dest_res, DrimeCollection):
                    dest_was_collection = True
                logger.info(
                    f"DrimeResource.handle_move: Destination {dest_path} exists "
                    f"({'collection' if dest_was_collection else 'file'}), "
                    "deleting before move"
                )
                try:
                    dest_res.delete()
                    logger.info(f"DrimeResource.handle_move: Deleted {dest_path}")
                except DAVError as e:
                    logger.error(f"DrimeResource.handle_move: Error deleting: {e}")
                    return [(dest_path, e)]
                except Exception as e:
                    logger.error(f"DrimeResource.handle_move: Exception deleting: {e}")
                    return [(dest_path, DAVError(HTTP_FORBIDDEN, str(e)))]

        # If we deleted a collection and dest ends with /, strip it
        # (we want to create a file at that location, not inside it)
        actual_dest_path = dest_path
        if dest_was_collection and dest_path.endswith("/"):
            actual_dest_path = dest_path.rstrip("/")
            logger.info(
                f"DrimeResource.handle_move: Adjusted dest from {dest_path} "
                f"to {actual_dest_path}"
            )

        # Check if destination ends with / (indicates moving into a collection)
        if actual_dest_path.endswith("/"):
            logger.info(
                f"DrimeResource.handle_move: Moving file into collection, "
                f"source={self.path}, dest={actual_dest_path}"
            )
            # Destination is a collection - check if it exists
            if self._provider is not None:
                dest_collection = self._provider.get_resource_inst(
                    actual_dest_path.rstrip("/"), self.environ
                )
                if dest_collection is not None and isinstance(
                    dest_collection, DrimeCollection
                ):
                    # Collection exists - move INTO it with original filename
                    # The actual destination is /dest_path/filename
                    final_dest = (
                        f"{actual_dest_path.rstrip('/')}/{self.file_entry.name}"
                    )
                    logger.info(
                        f"DrimeResource.handle_move: Final destination={final_dest}"
                    )

                    # Check if there's already a file at the final destination
                    existing = self._provider.get_resource_inst(
                        final_dest, self.environ
                    )
                    if existing is not None:
                        # Delete the existing file (overwrite behavior)
                        logger.info(
                            f"DrimeResource.handle_move: Deleting existing {final_dest}"
                        )
                        existing.delete()

                    # Now do the actual move
                    try:
                        self.copy_move_single(final_dest, is_move=True)
                        return True
                    except DAVError as e:
                        return [(self.path, e)]
                    except Exception as e:
                        return [(self.path, DAVError(HTTP_FORBIDDEN, str(e)))]

        # For other cases, handle them ourselves
        try:
            logger.info(
                f"DrimeResource.handle_move: Calling copy_move_single "
                f"with dest={actual_dest_path}"
            )
            self.copy_move_single(actual_dest_path, is_move=True)
            logger.info("DrimeResource.handle_move: Success")
            return True
        except DAVError as e:
            logger.error(f"DrimeResource.handle_move: DAVError: {e}")
            return [(self.path, e)]
        except Exception as e:
            logger.error(f"DrimeResource.handle_move: Exception: {e}")
            return [(self.path, DAVError(HTTP_FORBIDDEN, str(e)))]

    def get_display_name(self) -> str:
        """Return the display name."""
        return self.file_entry.name


class _DrimeWriteBuffer(BytesIO):
    """A buffer that uploads to Drime Cloud when closed."""

    def __init__(
        self,
        client: DrimeClient,
        file_name: str,
        parent_id: int | None,
        workspace_id: int,
        content_type: str | None = None,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
        parent_collection: DrimeCollection | None = None,
        provider: DrimeDAVProvider | None = None,
        file_path: str = "",
    ) -> None:
        super().__init__()
        self.client = client
        self.file_name = file_name
        self.parent_id = parent_id
        self.workspace_id = workspace_id
        self.content_type = content_type
        self._max_file_size = max_file_size
        self._closed = False
        self._parent_collection = parent_collection
        self._provider = provider
        self._file_path = file_path

    def close(self) -> None:
        """Upload the content when buffer is closed."""
        if self._closed:
            return

        self._closed = True
        content = self.getvalue()

        # Check file size limit
        if len(content) > self._max_file_size:
            raise DAVError(
                HTTP_FORBIDDEN,
                f"File too large ({len(content)} bytes). "
                f"Maximum size is {self._max_file_size} bytes.",
            )

        if content:
            try:
                logger.debug(
                    f"Uploading file '{self.file_name}' ({len(content)} bytes)"
                )
                # Create a temporary file for upload with the CORRECT filename
                # This is important because the API uses the temp file's name
                tmp_dir = Path(tempfile.gettempdir())
                tmp_path = tmp_dir / self.file_name

                # Write content to temp file
                tmp_path.write_bytes(content)

                try:
                    # Upload the file - since tmp_path.name matches self.file_name,
                    # and we pass relative_path=self.file_name, the API will use
                    # the correct filename
                    result = self.client.upload_file(
                        tmp_path,
                        parent_id=self.parent_id,
                        workspace_id=self.workspace_id,
                        relative_path=self.file_name,
                    )
                    logger.debug(f"Upload result: {result}")
                finally:
                    # Clean up temp file
                    tmp_path.unlink(missing_ok=True)

                # Invalidate parent collection's cache so the file appears
                if self._parent_collection is not None:
                    self._parent_collection._entries_cache = None
                    logger.debug("Invalidated parent collection cache")

                # Register the newly created file for eventual consistency handling
                if self._provider is not None and self._file_path:
                    self._provider._register_create(self._file_path)

            except DAVError:
                raise
            except Exception as e:
                logger.error(f"Error uploading file: {e}")
                raise DAVError(HTTP_FORBIDDEN, f"Error uploading file: {e}") from None

        super().close()


class DrimeCollection(DAVCollection):
    """Represents a Drime Cloud folder as a WebDAV collection."""

    def __init__(
        self,
        path: str,
        environ: dict[str, Any],
        folder_entry: FileEntry | None,
        client: DrimeClient,
        workspace_id: int,
        readonly: bool = False,
        cache_ttl: float = DEFAULT_CACHE_TTL,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
        provider: DrimeDAVProvider | None = None,
    ) -> None:
        """Initialize the collection.

        Args:
            path: The WebDAV path
            environ: WSGI environment
            folder_entry: The Drime Cloud folder entry (None for root)
            client: The Drime API client
            workspace_id: Workspace ID
            readonly: Whether the collection is read-only
            cache_ttl: Cache time-to-live in seconds
            max_file_size: Maximum file size for uploads/downloads
            provider: Reference to the DAV provider for cache management
        """
        super().__init__(path, environ)
        # Debug: check if lock_manager is set
        logger.debug(
            f"DrimeCollection.__init__: path={path}, "
            f"provider id={id(self.provider)}, "
            f"lock_manager={getattr(self.provider, 'lock_manager', 'NOT_SET')}"
        )
        self.folder_entry = folder_entry
        self.client = client
        self.workspace_id = workspace_id
        self._readonly = readonly
        self._cache_ttl = cache_ttl
        self._max_file_size = max_file_size
        self._entries_cache: list[FileEntry] | None = None
        self._cache_time: float = 0
        self._provider = provider

    def _get_folder_id(self) -> int | None:
        """Get the folder ID, or None for root."""
        if self.folder_entry is None:
            return None
        return self.folder_entry.id

    def _get_entries(self) -> list[FileEntry]:
        """Get the folder entries with caching."""
        now = time.time()
        if (
            self._entries_cache is not None
            and (now - self._cache_time) < self._cache_ttl
        ):
            return self._entries_cache

        try:
            from ..models import FileEntriesResult

            folder_id = self._get_folder_id()
            params: dict[str, Any] = {
                "workspace_id": self.workspace_id,
                "per_page": 1000,  # Get all entries
            }
            if folder_id is not None:
                params["parent_ids"] = [folder_id]

            result = self.client.get_file_entries(**params)
            file_entries = FileEntriesResult.from_api_response(result)

            # For root folder, filter to only entries with no parent
            if folder_id is None:
                self._entries_cache = [
                    entry
                    for entry in file_entries.entries
                    if entry.parent_id is None or entry.parent_id == 0
                ]
            else:
                self._entries_cache = file_entries.entries

            self._cache_time = now
            return self._entries_cache
        except Exception as e:
            logger.error(f"Error listing folder: {e}")
            return []

    def get_display_info(self) -> dict[str, Any]:
        """Return display information for directory browser."""
        return {"type": "Directory"}

    def get_member_names(self) -> list[str]:
        """Return the names of all members (files and subfolders)."""
        entries = self._get_entries()
        return [entry.name for entry in entries]

    def get_member(self, name: str) -> DrimeResource | DrimeCollection | None:
        """Return a member resource by name.

        Args:
            name: The member name

        Returns:
            The member resource or None if not found
        """
        entries = self._get_entries()

        for entry in entries:
            if entry.name == name:
                member_path = f"{self.path.rstrip('/')}/{name}"
                if entry.is_folder:
                    return DrimeCollection(
                        path=member_path,
                        environ=self.environ,
                        folder_entry=entry,
                        client=self.client,
                        workspace_id=self.workspace_id,
                        readonly=self._readonly,
                        cache_ttl=self._cache_ttl,
                        max_file_size=self._max_file_size,
                        provider=self._provider,
                    )
                else:
                    return DrimeResource(
                        path=member_path,
                        environ=self.environ,
                        file_entry=entry,
                        client=self.client,
                        workspace_id=self.workspace_id,
                        readonly=self._readonly,
                        max_file_size=self._max_file_size,
                        provider=self._provider,
                    )

        return None

    def get_member_list(self) -> list[DrimeResource | DrimeCollection]:
        """Return all member resources as a list."""
        entries = self._get_entries()
        members: list[DrimeResource | DrimeCollection] = []

        for entry in entries:
            member_path = f"{self.path.rstrip('/')}/{entry.name}"
            if entry.is_folder:
                members.append(
                    DrimeCollection(
                        path=member_path,
                        environ=self.environ,
                        folder_entry=entry,
                        client=self.client,
                        workspace_id=self.workspace_id,
                        readonly=self._readonly,
                        cache_ttl=self._cache_ttl,
                        max_file_size=self._max_file_size,
                        provider=self._provider,
                    )
                )
            else:
                members.append(
                    DrimeResource(
                        path=member_path,
                        environ=self.environ,
                        file_entry=entry,
                        client=self.client,
                        workspace_id=self.workspace_id,
                        readonly=self._readonly,
                        max_file_size=self._max_file_size,
                        provider=self._provider,
                    )
                )

        return members

    def create_empty_resource(self, name: str) -> DrimeResource:
        """Create a new empty file resource.

        Args:
            name: The name of the new file

        Returns:
            The new resource (placeholder for write)
        """
        if self._readonly:
            raise DAVError(HTTP_FORBIDDEN, "Collection is read-only")

        # Create a placeholder FileEntry for the new file
        from ..models import FileEntry

        placeholder_entry = FileEntry(
            id=0,  # Will be assigned after upload
            name=name,
            file_name=name,
            mime="application/octet-stream",
            file_size=0,
            parent_id=self._get_folder_id(),
            created_at="",
            type="file",
            extension=Path(name).suffix.lstrip(".") if "." in name else None,
            hash="",
            url="",
            workspace_id=self.workspace_id,
        )

        member_path = f"{self.path.rstrip('/')}/{name}"

        # Register the placeholder in the cache so get_resource_inst can find it
        # This is needed for LOCK on unmapped URLs
        if self._provider is not None:
            self._provider._register_create(member_path, placeholder_entry)

        resource = DrimeResource(
            path=member_path,
            environ=self.environ,
            file_entry=placeholder_entry,
            client=self.client,
            workspace_id=self.workspace_id,
            readonly=self._readonly,
            max_file_size=self._max_file_size,
            parent_collection=self,  # Pass self for cache invalidation
            provider=self._provider,
        )

        return resource

    def create_collection(self, name: str) -> DrimeCollection:
        """Create a new subfolder.

        Args:
            name: The name of the new folder

        Returns:
            The new collection
        """
        if self._readonly:
            raise DAVError(HTTP_FORBIDDEN, "Collection is read-only")

        try:
            result = self.client.create_folder(
                name=name,
                parent_id=self._get_folder_id(),
                workspace_id=self.workspace_id,
            )

            # Clear the cache
            self._entries_cache = None

            # Create a placeholder entry for the new folder
            from ..models import FileEntry

            # Extract folder ID from response - handle different response structures
            folder_id = 0
            folder_data: dict[str, Any] = {}
            if isinstance(result, dict):
                if "folder" in result:
                    folder_data = result["folder"]
                elif "fileEntry" in result:
                    folder_data = result["fileEntry"]
                elif "id" in result:
                    folder_data = result
                folder_id = folder_data.get("id", 0)

            result_info = (
                list(result.keys()) if isinstance(result, dict) else type(result)
            )
            logger.debug(
                f"create_collection: Created folder '{name}' with ID {folder_id}, "
                f"result keys: {result_info}"
            )

            folder_entry = FileEntry(
                id=folder_id,
                name=name,
                file_name=name,
                mime="",
                file_size=0,
                parent_id=self._get_folder_id(),
                created_at="",
                type="folder",
                extension=None,
                hash=folder_data.get("hash", ""),
                url="",
                workspace_id=self.workspace_id,
            )

            member_path = f"{self.path.rstrip('/')}/{name}"

            # Register the newly created folder for eventual consistency handling
            if self._provider is not None:
                self._provider._register_create(member_path, folder_entry)

            return DrimeCollection(
                path=member_path,
                environ=self.environ,
                folder_entry=folder_entry,
                client=self.client,
                workspace_id=self.workspace_id,
                readonly=self._readonly,
                cache_ttl=self._cache_ttl,
                max_file_size=self._max_file_size,
                provider=self._provider,
            )
        except Exception as e:
            logger.error(f"Error creating folder: {e}")
            raise DAVError(HTTP_FORBIDDEN, f"Error creating folder: {e}") from None

    def delete(self) -> None:
        """Delete this collection and all its contents."""
        if self._readonly:
            raise DAVError(HTTP_FORBIDDEN, "Collection is read-only")

        if self.folder_entry is None:
            raise DAVError(HTTP_FORBIDDEN, "Cannot delete root folder")

        try:
            # If this is a placeholder entry (id=0), we need to find the real entry
            entry_id = self.folder_entry.id
            if entry_id == 0:
                # Try to find the actual entry from the API
                from ..models import FileEntriesResult

                params: dict[str, Any] = {
                    "workspace_id": self.workspace_id,
                    "per_page": 1000,
                }
                if self.folder_entry.parent_id is not None:
                    params["parent_ids"] = [self.folder_entry.parent_id]

                result = self.client.get_file_entries(**params)
                file_entries = FileEntriesResult.from_api_response(result)

                # Filter for root if no parent
                entries = file_entries.entries
                if self.folder_entry.parent_id is None:
                    entries = [
                        e for e in entries if e.parent_id is None or e.parent_id == 0
                    ]

                # Find the entry by name
                for entry in entries:
                    if entry.name == self.folder_entry.name and entry.is_folder:
                        entry_id = entry.id
                        break

                if entry_id == 0:
                    # Entry not found in API yet - just register as deleted
                    if self._provider is not None:
                        self._provider._register_delete(self.path)
                    return

            self.client.delete_file_entries([entry_id], workspace_id=self.workspace_id)

            # Register the deletion
            if self._provider is not None:
                self._provider._register_delete(self.path)

        except Exception as e:
            logger.error(f"Error deleting folder: {e}")
            raise DAVError(HTTP_FORBIDDEN, f"Error deleting folder: {e}") from None

    def copy_move_single(self, dest_path: str, is_move: bool) -> bool:
        """Copy or move this collection to a new location (non-recursive).

        For collections, this method:
        - MOVE: moves the folder structure (including contents via API)
        - COPY: creates an EMPTY folder at destination IF IT DOESN'T EXIST
          (WsgiDAV handles recursive content copying by calling copy_move_single
          on each member)

        Args:
            dest_path: Destination path
            is_move: True for move, False for copy

        Returns:
            True if a resource was overwritten
        """
        if self._readonly:
            raise DAVError(HTTP_FORBIDDEN, "Collection is read-only")

        if self.folder_entry is None:
            raise DAVError(HTTP_FORBIDDEN, "Cannot copy/move root folder")

        # Parse destination path to get parent folder and new name
        dest_path_clean = dest_path.rstrip("/")
        dest_parts = dest_path_clean.strip("/").split("/")
        new_name = dest_parts[-1] if dest_parts else self.folder_entry.name
        dest_parent_path = (
            "/" + "/".join(dest_parts[:-1]) if len(dest_parts) > 1 else "/"
        )

        try:
            # Find the destination parent folder
            dest_parent_id: int | None = None
            if dest_parent_path != "/":
                dest_parent_id = self._find_folder_by_path(dest_parent_path)

            if is_move:
                # Get the real entry ID (may need API lookup for placeholders)
                entry_id = self._get_real_entry_id()

                # Move the folder (this moves contents too via the API)
                self.client.move_file_entries(
                    [entry_id],
                    destination_id=dest_parent_id,
                    workspace_id=self.workspace_id,
                )
                # Rename if needed
                if new_name != self.folder_entry.name:
                    self.client.rename_file_entry(
                        entry_id,
                        new_name=new_name,
                        initial_name=self.folder_entry.name,
                        workspace_id=self.workspace_id,
                    )
            else:
                # COPY: Create an EMPTY folder at the destination only if it
                # doesn't exist.
                # WsgiDAV will handle copying contents by calling copy_move_single
                # on each child resource.

                # Check if destination already exists
                dest_exists = False
                if self._provider is not None:
                    dest_res = self._provider.get_resource_inst(
                        dest_path_clean, self.environ
                    )
                    if dest_res is not None:
                        dest_exists = True

                if not dest_exists:
                    result = self.client.create_folder(
                        name=new_name,
                        parent_id=dest_parent_id,
                        workspace_id=self.workspace_id,
                    )

                    # Extract folder info for cache
                    folder_id = 0
                    folder_data: dict[str, Any] = {}
                    if isinstance(result, dict):
                        if "folder" in result:
                            folder_data = result["folder"]
                        elif "fileEntry" in result:
                            folder_data = result["fileEntry"]
                        elif "id" in result:
                            folder_data = result
                        folder_id = folder_data.get("id", 0)

                    # Create entry for cache
                    from ..models import FileEntry

                    new_folder_entry = FileEntry(
                        id=folder_id,
                        name=new_name,
                        file_name=new_name,
                        mime="",
                        file_size=0,
                        parent_id=dest_parent_id,
                        created_at="",
                        type="folder",
                        extension=None,
                        hash=folder_data.get("hash", ""),
                        url="",
                        workspace_id=self.workspace_id,
                    )

                    # Register the newly created folder
                    if self._provider is not None:
                        self._provider._register_create(
                            dest_path_clean, new_folder_entry
                        )

            # Invalidate cache
            self._entries_cache = None

            # Register the newly created path for eventual consistency handling
            if self._provider is not None and is_move:
                self._provider._register_create(dest_path_clean)

            # Copy/move dead properties
            if self._provider is not None:
                prop_manager = self._provider.prop_manager
                if prop_manager:
                    dest_res = self._provider.get_resource_inst(
                        dest_path_clean, self.environ
                    )
                    if dest_res is not None:
                        if is_move:
                            # Move children props too for collections
                            prop_manager.move_properties(
                                self.get_ref_url(),
                                dest_res.get_ref_url(),
                                with_children=True,
                                environ=self.environ,
                            )
                        else:
                            prop_manager.copy_properties(
                                self.get_ref_url(),
                                dest_res.get_ref_url(),
                                self.environ,
                            )

            return False

        except DAVError:
            raise
        except Exception as e:
            logger.error(f"Error {'moving' if is_move else 'copying'} folder: {e}")
            raise DAVError(
                HTTP_FORBIDDEN,
                f"Error {'moving' if is_move else 'copying'} folder: {e}",
            ) from None

    def _get_real_entry_id(self) -> int:
        """Get the real entry ID, looking it up from API if needed.

        Returns:
            The entry ID

        Raises:
            DAVError: If the entry cannot be found
        """
        if self.folder_entry is None:
            raise DAVError(HTTP_FORBIDDEN, "Cannot get ID for root folder")

        if self.folder_entry.id != 0:
            logger.debug(
                f"DrimeCollection._get_real_entry_id: Using cached ID "
                f"{self.folder_entry.id} for {self.folder_entry.name} "
                f"(is_folder={self.folder_entry.is_folder})"
            )
            return self.folder_entry.id

        logger.debug(
            f"DrimeCollection._get_real_entry_id: Looking up ID for "
            f"placeholder entry {self.folder_entry.name} "
            f"(is_folder={self.folder_entry.is_folder})"
        )

        from ..models import FileEntriesResult

        params: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "per_page": 1000,
        }
        if self.folder_entry.parent_id is not None:
            params["parent_ids"] = [self.folder_entry.parent_id]

        result = self.client.get_file_entries(**params)
        file_entries = FileEntriesResult.from_api_response(result)

        entries = file_entries.entries
        if self.folder_entry.parent_id is None:
            entries = [e for e in entries if e.parent_id is None or e.parent_id == 0]

        logger.debug(
            f"DrimeCollection._get_real_entry_id: Found {len(entries)} entries, "
            f"looking for {self.folder_entry.name} "
            f"(is_folder={self.folder_entry.is_folder})"
        )
        for e in entries:
            logger.debug(f"  - Entry: {e.name}, is_folder={e.is_folder}, id={e.id}")

        # First try to find by name AND type
        for entry in entries:
            if entry.name == self.folder_entry.name and entry.is_folder:
                logger.debug(f"DrimeCollection._get_real_entry_id: Found ID {entry.id}")
                return entry.id

        # If not found and we were looking for a folder, this might be a bug where
        # WsgiDAV called DrimeCollection methods on what is actually a file
        logger.warning(
            f"DrimeCollection._get_real_entry_id: Could not find folder "
            f"{self.folder_entry.name} in {len(entries)} entries. "
            "This might indicate a type mismatch."
        )
        raise DAVError(HTTP_FORBIDDEN, "Resource not found")

    def _find_folder_by_path(self, folder_path: str) -> int | None:
        """Find a folder by its path and return its ID.

        Args:
            folder_path: Path like "/foo/bar"

        Returns:
            The folder ID, or None for root

        Raises:
            DAVError: If folder not found
        """
        if folder_path == "/":
            return None

        from ..models import FileEntriesResult

        current_folder_id: int | None = None
        parts = folder_path.strip("/").split("/")

        for i, part in enumerate(parts):
            # Check recent creates cache first
            current_path = "/" + "/".join(parts[: i + 1])
            if self._provider is not None:
                created, cached = self._provider._is_recently_created(current_path)
                if created and cached is not None and cached.is_folder:
                    current_folder_id = cached.id
                    continue

            params: dict[str, Any] = {
                "workspace_id": self.workspace_id,
                "per_page": 1000,
            }
            if current_folder_id is not None:
                params["parent_ids"] = [current_folder_id]

            result = self.client.get_file_entries(**params)
            file_entries = FileEntriesResult.from_api_response(result)

            entries = file_entries.entries
            if current_folder_id is None:
                entries = [
                    e for e in entries if e.parent_id is None or e.parent_id == 0
                ]

            found = None
            for entry in entries:
                if entry.name == part and entry.is_folder:
                    found = entry
                    break

            if found is None:
                raise DAVError(HTTP_FORBIDDEN, f"Destination parent not found: {part}")
            current_folder_id = found.id

        return current_folder_id

    def support_recursive_move(self, dest_path: str) -> bool:
        """Return True if move_recursive() is available.

        We return True because our move API handles the entire folder recursively.
        """
        return True

    def support_recursive_delete(self) -> bool:
        """Return True if delete() may be called on non-empty collections.

        We return True because our delete API handles the entire folder recursively.
        """
        return True

    def move_recursive(self, dest_path: str) -> list[tuple[str, DAVError]]:
        """Move this collection recursively to dest_path.

        This is called when support_recursive_move() returns True.

        Args:
            dest_path: Destination path

        Returns:
            List of errors as (path, DAVError) tuples, empty if successful
        """
        if self._readonly:
            return [(self.path, DAVError(HTTP_FORBIDDEN, "Collection is read-only"))]

        if self.folder_entry is None:
            return [(self.path, DAVError(HTTP_FORBIDDEN, "Cannot move root folder"))]

        try:
            # Use copy_move_single which already handles the move
            self.copy_move_single(dest_path, is_move=True)
            return []
        except DAVError as e:
            return [(self.path, e)]
        except Exception as e:
            return [(self.path, DAVError(HTTP_FORBIDDEN, str(e)))]

    def handle_copy(
        self, dest_path: str, *, depth_infinity: bool
    ) -> bool | list[tuple[str, DAVError]]:
        """Handle COPY request natively for the entire collection.

        For depth=infinity, we use the duplicate API which copies the entire tree.
        For depth=0 (shallow), we return False to let WsgiDAV handle it
        (which will call copy_move_single to create an empty folder).

        Note: The duplicate API doesn't support renaming, so if the destination
        name differs from the source, we let WsgiDAV handle it file-by-file.

        Args:
            dest_path: Destination path
            depth_infinity: True if Depth: infinity header was sent

        Returns:
            True if handled successfully, False to let WsgiDAV handle it,
            or list of errors
        """
        if not depth_infinity:
            # For shallow copy (Depth: 0), let WsgiDAV handle it
            # WsgiDAV will call copy_move_single which creates an empty folder
            return False

        if self._readonly:
            return [(self.path, DAVError(HTTP_FORBIDDEN, "Collection is read-only"))]

        if self.folder_entry is None:
            return [(self.path, DAVError(HTTP_FORBIDDEN, "Cannot copy root folder"))]

        # Parse destination path to get the name
        dest_path_clean = dest_path.rstrip("/")
        dest_parts = dest_path_clean.strip("/").split("/")
        new_name = dest_parts[-1] if dest_parts else self.folder_entry.name

        # If destination name differs from source, let WsgiDAV handle it
        # because the duplicate API doesn't support renaming
        if new_name != self.folder_entry.name:
            return False

        try:
            # For deep copy with same name, use the duplicate API directly
            dest_parent_path = (
                "/" + "/".join(dest_parts[:-1]) if len(dest_parts) > 1 else "/"
            )

            # Check if destination exists and delete it first (overwrite handling)
            # When we handle the copy ourselves, we're responsible for overwrite
            if self._provider is not None:
                dest_res = self._provider.get_resource_inst(
                    dest_path_clean, self.environ
                )
                if dest_res is not None:
                    # Delete the destination first
                    dest_res.delete()

            # Find the destination parent folder
            dest_parent_id: int | None = None
            if dest_parent_path != "/":
                dest_parent_id = self._find_folder_by_path(dest_parent_path)

            # Get the real entry ID (may need API lookup for placeholders)
            entry_id = self._get_real_entry_id()

            # Duplicate the folder (this copies the entire tree)
            self.client.duplicate_file_entries(
                [entry_id],
                destination_id=dest_parent_id,
                workspace_id=self.workspace_id,
            )

            # Register the newly created path for eventual consistency handling
            if self._provider is not None:
                self._provider._register_create(dest_path_clean)

            return True
        except DAVError as e:
            return [(self.path, e)]
        except Exception as e:
            return [(self.path, DAVError(HTTP_FORBIDDEN, str(e)))]

    def handle_move(self, dest_path: str) -> bool | list[tuple[str, DAVError]]:
        """Handle MOVE request natively for the entire collection.

        Args:
            dest_path: Destination path

        Returns:
            True if handled successfully, False to let WsgiDAV handle it,
            or list of errors
        """
        logger.debug(
            f"DrimeCollection.handle_move: source={self.path}, dest={dest_path}, "
            f"folder_entry={self.folder_entry.name if self.folder_entry else 'ROOT'}, "
            f"is_folder={self.folder_entry.is_folder if self.folder_entry else True}"
        )

        if self._readonly:
            return [(self.path, DAVError(HTTP_FORBIDDEN, "Collection is read-only"))]

        if self.folder_entry is None:
            return [(self.path, DAVError(HTTP_FORBIDDEN, "Cannot move root folder"))]

        try:
            # Check if destination exists and delete it first (overwrite handling)
            if self._provider is not None:
                dest_res = self._provider.get_resource_inst(dest_path, self.environ)
                logger.debug(
                    f"DrimeCollection.handle_move: dest_res={dest_res}, "
                    f"type={type(dest_res).__name__ if dest_res else None}"
                )
                if dest_res is not None:
                    # Delete the destination first
                    logger.debug(
                        f"DrimeCollection.handle_move: Deleting dest {dest_path}"
                    )
                    dest_res.delete()

            # Use copy_move_single which already handles the move
            self.copy_move_single(dest_path, is_move=True)
            return True
        except DAVError as e:
            logger.error(f"DrimeCollection.handle_move: DAVError {e}")
            return [(self.path, e)]
        except Exception as e:
            logger.error(f"DrimeCollection.handle_move: Exception {e}")
            return [(self.path, DAVError(HTTP_FORBIDDEN, str(e)))]

    def get_creation_date(self) -> float:
        """Return the creation date as timestamp."""
        if self.folder_entry and self.folder_entry.created_at:
            try:
                from ..utils import parse_iso_timestamp

                dt = parse_iso_timestamp(self.folder_entry.created_at)
                if dt:
                    return dt.timestamp()
            except Exception:
                pass
        return time.time()

    def get_last_modified(self) -> float:
        """Return the last modified date as timestamp."""
        if self.folder_entry and self.folder_entry.updated_at:
            try:
                from ..utils import parse_iso_timestamp

                dt = parse_iso_timestamp(self.folder_entry.updated_at)
                if dt:
                    return dt.timestamp()
            except Exception:
                pass
        return self.get_creation_date()

    def get_display_name(self) -> str:
        """Return the display name."""
        if self.folder_entry:
            return self.folder_entry.name
        return "/"

    def get_etag(self) -> str | None:
        """Return the ETag for this collection.

        Note: WsgiDAV adds quotes automatically, so we return unquoted value.
        """
        if self.folder_entry and self.folder_entry.hash:
            # Strip any existing quotes from the hash
            return self.folder_entry.hash.strip('"')
        if self.folder_entry:
            return f"{self.folder_entry.id}-{self.get_last_modified()}"
        return None  # Root folder has no ETag

    def support_etag(self) -> bool:
        """Return True if ETags are supported."""
        return True

    def get_property_value(self, name: str) -> Any:
        """Return the value of a property with debug logging for locks."""
        if name == "{DAV:}lockdiscovery":
            lm = self.provider.lock_manager
            ref_url = self.get_ref_url()
            logger.debug(
                f"DrimeCollection.get_property_value: name={name}, "
                f"ref_url={ref_url!r}, provider id={id(self.provider)}, "
                f"lock_manager={lm}"
            )
            if lm:
                lock_list = lm.get_url_lock_list(ref_url)
                logger.debug(
                    f"DrimeCollection.get_property_value: found {len(lock_list)} locks"
                )
                for i, lock in enumerate(lock_list):
                    logger.debug(f"  Lock {i}: {lock}")
            else:
                logger.warning(
                    "DrimeCollection.get_property_value: lock_manager is None!"
                )
        return super().get_property_value(name)


class DrimeDAVProvider(DAVProvider):
    """Custom DAV provider for Drime Cloud storage."""

    def __init__(
        self,
        client: DrimeClient,
        workspace_id: int = 0,
        readonly: bool = False,
        cache_ttl: float = DEFAULT_CACHE_TTL,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    ) -> None:
        """Initialize the provider.

        Args:
            client: The Drime API client
            workspace_id: The workspace ID to serve (0 for personal)
            readonly: Whether to allow write operations
            cache_ttl: Cache time-to-live in seconds
            max_file_size: Maximum file size for uploads/downloads in bytes
        """
        super().__init__()
        self.client = client
        self.workspace_id = workspace_id
        self._readonly = readonly
        self._cache_ttl = cache_ttl
        self._max_file_size = max_file_size
        # Track recently created paths to handle API eventual consistency
        # Maps normalized path -> (creation_time, FileEntry or None)
        self._recent_creates: dict[str, tuple[float, FileEntry | None]] = {}
        # Track recently deleted paths
        self._recent_deletes: dict[str, float] = {}

    def _register_create(self, path: str, entry: FileEntry | None = None) -> None:
        """Register a recently created path."""
        path = path.rstrip("/") or "/"
        self._recent_creates[path] = (time.time(), entry)
        # Remove from deletes if present
        self._recent_deletes.pop(path, None)

    def _register_delete(self, path: str) -> None:
        """Register a recently deleted path."""
        path = path.rstrip("/") or "/"
        self._recent_deletes[path] = time.time()
        # Remove from creates if present
        self._recent_creates.pop(path, None)

    def _is_recently_created(self, path: str) -> tuple[bool, FileEntry | None]:
        """Check if path was recently created (within cache TTL)."""
        path = path.rstrip("/") or "/"
        if path in self._recent_creates:
            create_time, entry = self._recent_creates[path]
            if time.time() - create_time < self._cache_ttl:
                return True, entry
            else:
                # Expired, remove it
                del self._recent_creates[path]
        return False, None

    def _is_recently_deleted(self, path: str) -> bool:
        """Check if path was recently deleted (within cache TTL)."""
        path = path.rstrip("/") or "/"
        if path in self._recent_deletes:
            delete_time = self._recent_deletes[path]
            if time.time() - delete_time < self._cache_ttl:
                return True
            else:
                # Expired, remove it
                del self._recent_deletes[path]
        return False

    def get_resource_inst(
        self, path: str, environ: dict[str, Any]
    ) -> DrimeCollection | DrimeResource | None:
        """Return a DAV resource instance for the given path.

        Args:
            path: The resource path
            environ: WSGI environment

        Returns:
            A DrimeCollection or DrimeResource, or None if not found
        """
        # Normalize the path
        original_path = path
        path = path.rstrip("/") or "/"

        logger.debug(
            f"get_resource_inst: original_path={original_path}, normalized={path}"
        )

        # Check if recently deleted - return None immediately
        if self._is_recently_deleted(path):
            logger.debug(
                f"get_resource_inst: {path} was recently deleted, returning None"
            )
            return None

        # Root path
        if path == "/":
            return DrimeCollection(
                path="/",
                environ=environ,
                folder_entry=None,
                client=self.client,
                workspace_id=self.workspace_id,
                readonly=self._readonly,
                cache_ttl=self._cache_ttl,
                max_file_size=self._max_file_size,
                provider=self,
            )

        # Split the path and traverse
        parts = path.strip("/").split("/")
        current_folder_id: int | None = None

        try:
            from ..models import FileEntriesResult

            for i, part in enumerate(parts):
                # Get entries in current folder
                params: dict[str, Any] = {
                    "workspace_id": self.workspace_id,
                    "per_page": 1000,
                }
                if current_folder_id is not None:
                    params["parent_ids"] = [current_folder_id]

                result = self.client.get_file_entries(**params)
                file_entries = FileEntriesResult.from_api_response(result)

                # For root folder, filter to only entries with no parent
                entries = file_entries.entries
                if current_folder_id is None:
                    entries = [
                        entry
                        for entry in entries
                        if entry.parent_id is None or entry.parent_id == 0
                    ]

                # Find the matching entry
                found_entry = None
                for entry in entries:
                    if entry.name == part:
                        found_entry = entry
                        break

                if found_entry is None:
                    # Check if this path was recently created
                    current_path = "/" + "/".join(parts[: i + 1])
                    recently_created, cached_entry = self._is_recently_created(
                        current_path
                    )
                    if recently_created:
                        # If this is the last part, return the resource
                        if i == len(parts) - 1:
                            if cached_entry is not None:
                                if cached_entry.is_folder:
                                    return DrimeCollection(
                                        path=path,
                                        environ=environ,
                                        folder_entry=cached_entry,
                                        client=self.client,
                                        workspace_id=self.workspace_id,
                                        readonly=self._readonly,
                                        cache_ttl=self._cache_ttl,
                                        max_file_size=self._max_file_size,
                                        provider=self,
                                    )
                                else:
                                    return DrimeResource(
                                        path=path,
                                        environ=environ,
                                        file_entry=cached_entry,
                                        client=self.client,
                                        workspace_id=self.workspace_id,
                                        readonly=self._readonly,
                                        max_file_size=self._max_file_size,
                                        provider=self,
                                    )
                            else:
                                # We know it exists but don't have details
                                # Return a minimal placeholder resource
                                from ..models import FileEntry

                                placeholder = FileEntry(
                                    id=0,
                                    name=part,
                                    file_name=part,
                                    mime="application/octet-stream",
                                    file_size=0,
                                    parent_id=current_folder_id,
                                    created_at="",
                                    type="file",
                                    extension=None,
                                    hash="",
                                    url="",
                                    workspace_id=self.workspace_id,
                                )
                                return DrimeResource(
                                    path=path,
                                    environ=environ,
                                    file_entry=placeholder,
                                    client=self.client,
                                    workspace_id=self.workspace_id,
                                    readonly=self._readonly,
                                    max_file_size=self._max_file_size,
                                    provider=self,
                                )
                        else:
                            # Not the last part - this is a parent folder
                            # If we have the cached entry with folder ID,
                            # continue traversal
                            if cached_entry is not None and cached_entry.is_folder:
                                current_folder_id = cached_entry.id
                                continue
                            # Otherwise we can't continue
                    return None

                # If this is the last part, return the appropriate resource
                if i == len(parts) - 1:
                    res_type = (
                        "DrimeCollection" if found_entry.is_folder else "DrimeResource"
                    )
                    logger.debug(
                        f"get_resource_inst: Found entry {found_entry.name}, "
                        f"is_folder={found_entry.is_folder}, returning {res_type}"
                    )
                    if found_entry.is_folder:
                        return DrimeCollection(
                            path=path,
                            environ=environ,
                            folder_entry=found_entry,
                            client=self.client,
                            workspace_id=self.workspace_id,
                            readonly=self._readonly,
                            cache_ttl=self._cache_ttl,
                            max_file_size=self._max_file_size,
                            provider=self,
                        )
                    else:
                        return DrimeResource(
                            path=path,
                            environ=environ,
                            file_entry=found_entry,
                            client=self.client,
                            workspace_id=self.workspace_id,
                            readonly=self._readonly,
                            max_file_size=self._max_file_size,
                            provider=self,
                        )
                else:
                    # Traverse into the folder
                    if not found_entry.is_folder:
                        return None  # Can't traverse into a file
                    current_folder_id = found_entry.id

        except Exception as e:
            logger.error(f"Error resolving path {path}: {e}")
            return None

        return None

    def is_readonly(self) -> bool:
        """Return True if the provider is read-only."""
        return self._readonly
