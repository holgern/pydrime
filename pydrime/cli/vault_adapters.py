"""Adapter classes for vault syncengine compatibility.

This module provides adapters that wrap DrimeClient and FileEntriesManager
to work with encrypted vault storage, making them compatible with syncengine.
"""

from __future__ import annotations

import base64
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from ..models import FileEntry
from ..vault_crypto import VaultKey, decrypt_file_content, encrypt_filename

if TYPE_CHECKING:
    from ..api import DrimeClient


class _VaultFileEntriesManagerAdapter:
    """Adapter to make FileEntriesManager work with encrypted vault storage.

    This adapter wraps pydrime's FileEntriesManager and adds encryption/decryption
    for vault operations, making it compatible with syncengine's protocol.
    """

    def __init__(
        self,
        client: DrimeClient,
        vault_id: int,
        vault_key: VaultKey,
        parent_id: int | None,
    ):
        """Initialize vault file entries manager adapter.

        Args:
            client: DrimeClient instance (may be _VaultClientAdapter with IV cache)
            vault_id: Vault ID
            vault_key: Vault encryption key
            parent_id: Parent folder ID in vault (None for root)
        """
        self._client = client
        self._vault_id = vault_id
        self._vault_key = vault_key
        self._parent_id = parent_id
        # Cache for folder ID to hash mapping
        self._folder_hash_cache: dict[int, str] = {}
        # Try to get IV cache from client adapter (if it's a _VaultClientAdapter)
        self._iv_cache = getattr(client, "_file_iv_cache", None)

    def _get_folder_hash(self, folder_id: int | None) -> str:
        """Get folder hash from folder ID, with caching.

        Args:
            folder_id: Folder ID (None for configured parent or root)

        Returns:
            Folder hash (empty string for root)
        """
        if folder_id is None:
            folder_id = self._parent_id

        if folder_id is None:
            return ""

        # Check cache
        if folder_id in self._folder_hash_cache:
            return self._folder_hash_cache[folder_id]

        # This is a simplified approach - in practice we'd need to build
        # the cache by traversing the tree. For now, return empty string
        # which means we'll query from root.
        return ""

    def find_folder_by_name(self, name: str, parent_id: int = 0) -> FileEntry | None:
        """Find folder by name in vault.

        Args:
            name: Folder name to find
            parent_id: Parent folder ID (0 for root)

        Returns:
            FileEntry if found, None otherwise
        """
        # Convert parent_id: 0 → None for vault root
        actual_parent_id = self._parent_id if parent_id == 0 else parent_id

        # Get folder hash from parent_id if specified
        folder_hash = ""
        if actual_parent_id is not None:
            # Need to get the hash for this parent_id
            # This requires fetching all entries to find the one with matching ID
            all_result = self._client.get_vault_file_entries(
                folder_hash="",
                per_page=1000,
            )
            all_entries = []
            if isinstance(all_result, dict):
                if "data" in all_result:
                    all_entries = all_result["data"]
                elif "pagination" in all_result and isinstance(
                    all_result["pagination"], dict
                ):
                    all_entries = all_result["pagination"].get("data", [])

            for e in all_entries:
                if e.get("id") == actual_parent_id and e.get("type") == "folder":
                    folder_hash = e.get("hash", "")
                    break

        # Get vault entries from the target folder
        result = self._client.get_vault_file_entries(
            folder_hash=folder_hash,
            per_page=1000,
        )

        # Extract entries from response
        entries = []
        if isinstance(result, dict):
            if "data" in result:
                entries = result["data"]
            elif "pagination" in result and isinstance(result["pagination"], dict):
                entries = result["pagination"].get("data", [])

        # Search for folder by comparing entry names
        # Note: Vault API returns decrypted folder names (server handles decryption)
        import logging

        logger = logging.getLogger(__name__)
        logger.debug(f"Searching for folder '{name}' in {len(entries)} entries")

        for entry in entries:
            if entry.get("type") != "folder":
                continue

            entry_name = entry.get("name", "")
            entry_id = entry.get("id")
            name_iv = entry.get("name_iv")

            logger.debug(
                f"Checking folder: '{entry_name}' "
                f"(ID: {entry_id}, has name_iv: {name_iv is not None})"
            )

            # The API returns decrypted names for folders, but encrypted names for files
            # For folders: just compare directly
            # For files: need to decrypt using name_iv
            if entry_name == name:
                logger.debug(f"Found matching folder: '{name}' (ID: {entry_id})")
                return FileEntry(
                    id=entry_id,
                    name=name,
                    file_name=name,
                    mime="application/x-directory",
                    type="folder",
                    extension=None,
                    parent_id=entry.get("parent_id"),
                    hash=entry.get("hash", ""),
                    url="",
                    file_size=0,
                    created_at=entry.get("created_at", ""),
                    updated_at=entry.get("updated_at"),
                )

        logger.debug(f"Folder '{name}' not found")

        return None

    def get_all_recursive(
        self, folder_id: int | None, path_prefix: str
    ) -> list[tuple[FileEntry, str]]:
        """Get all vault entries recursively.

        Args:
            folder_id: Folder ID to start from (None for root)
            path_prefix: Path prefix for relative paths

        Returns:
            List of (FileEntry, relative_path) tuples
        """
        import logging

        logger = logging.getLogger(__name__)

        results: list[tuple[FileEntry, str]] = []
        self._get_recursive_helper(folder_id, path_prefix, results)

        logger.debug(f"get_all_recursive returning {len(results)} entries:")
        for entry, path in results:
            logger.debug(
                f"  - {path}: {entry.file_name} "
                f"(size={entry.file_size}, type={entry.type})"
            )

        return results

    def _get_recursive_helper(
        self, folder_id: int | None, path_prefix: str, results: list
    ) -> None:
        """Helper for recursive vault entry retrieval."""
        # Determine starting folder hash
        folder_hash = ""

        # If folder_id is provided, we need to find its hash
        target_id = folder_id if folder_id is not None else self._parent_id

        if target_id is not None:
            # Need to find the hash for this folder ID
            # Query all entries from root to find it
            root_result = self._client.get_vault_file_entries(
                folder_hash="",
                per_page=1000,
            )
            root_entries = []
            if isinstance(root_result, dict):
                if "data" in root_result:
                    root_entries = root_result["data"]
                elif "pagination" in root_result and isinstance(
                    root_result["pagination"], dict
                ):
                    root_entries = root_result["pagination"].get("data", [])

            for e in root_entries:
                if e.get("id") == target_id and e.get("type") == "folder":
                    folder_hash = e.get("hash", "")
                    # Cache the mapping
                    self._folder_hash_cache[target_id] = folder_hash
                    break

        # Use the hash-based helper to do the actual work
        self._get_recursive_helper_by_hash(folder_hash, path_prefix, results)

    def _get_recursive_helper_by_hash(
        self, folder_hash: str, path_prefix: str, results: list
    ) -> None:
        """Helper for recursive vault entry retrieval using folder hash directly."""
        # Get entries for this folder using the folder hash
        result = self._client.get_vault_file_entries(
            folder_hash=folder_hash,
            per_page=1000,
        )

        entries = []
        if isinstance(result, dict):
            if "data" in result:
                entries = result["data"]
            elif "pagination" in result and isinstance(result["pagination"], dict):
                entries = result["pagination"].get("data", [])

        import logging

        logger = logging.getLogger(__name__)

        for entry in entries:
            entry_type = entry.get("type", "file")
            entry_name = entry.get("name", "")
            entry_id = entry.get("id")
            name_iv = entry.get("name_iv")

            # The vault API returns decrypted names for both files and folders
            # No client-side decryption needed
            decrypted_name = entry_name

            logger.debug(
                f"Processing {entry_type}: '{entry_name}' (ID: {entry_id}, "
                f"has name_iv: {name_iv is not None})"
            )

            # Build relative path
            rel_path = (
                f"{path_prefix}/{decrypted_name}" if path_prefix else decrypted_name
            )

            # Create FileEntry
            # Note: Vault API returns encrypted file size
            # (plaintext + 16 bytes overhead)
            # We need to subtract the encryption overhead for sync comparison
            encrypted_size = entry.get("size", entry.get("file_size", 0))
            # Encryption overhead: 12 bytes IV + 16 bytes GCM tag = 28 bytes total
            # But the IV is prepended, so stored size = plaintext + 16 bytes
            ENCRYPTION_OVERHEAD = 16
            decrypted_size = (
                encrypted_size - ENCRYPTION_OVERHEAD
                if entry_type == "file" and encrypted_size > ENCRYPTION_OVERHEAD
                else encrypted_size
            )

            file_entry = FileEntry(
                id=entry.get("id"),
                name=decrypted_name,
                file_name=decrypted_name,
                mime=entry.get(
                    "mime",
                    "application/octet-stream"
                    if entry_type == "file"
                    else "application/x-directory",
                ),
                type=entry_type,
                extension=Path(decrypted_name).suffix.lstrip(".")
                if entry_type == "file" and "." in decrypted_name
                else None,
                parent_id=entry.get("parent_id"),
                # Vault's hash is base64(file_id|pad), not a content hash
                # We keep it for file identification (needed for download_file)
                # SIZE_ONLY comparison mode will ignore it for content comparison
                hash=entry.get("hash", ""),
                url="",
                file_size=decrypted_size,
                created_at=entry.get("created_at", ""),
                updated_at=entry.get("updated_at") or entry.get("created_at", ""),
            )

            # Cache the IV for this file (needed for decryption during download)
            if entry_type == "file" and self._iv_cache is not None:
                file_hash = file_entry.hash
                # The API might return 'iv', 'content_iv', or 'ivs' field
                raw_iv = entry.get("iv") or entry.get("content_iv") or entry.get("ivs")

                # The 'iv' field contains nameIv + contentIv concatenated
                # (32 base64 chars total)
                # Each IV is 12 bytes = 16 base64 characters
                # We need to extract just the content IV (last 16 characters)
                content_iv = raw_iv
                if raw_iv:
                    if "," in raw_iv:
                        # Format: "nameIv,contentIv" - split and take second part
                        parts = raw_iv.split(",", 1)
                        if len(parts) == 2:
                            content_iv = parts[1]
                            logger.debug(
                                f"Parsed comma-separated IV for {entry_name}: "
                                f"name_iv='{parts[0]}', content_iv='{content_iv}'"
                            )
                    elif len(raw_iv) == 32:
                        # Format: "nameIvContentIv" concatenated (no comma)
                        # Each IV is 16 base64 chars, total 32 chars
                        name_iv = raw_iv[:16]
                        content_iv = raw_iv[16:]
                        logger.debug(
                            f"Parsed concatenated IV for {entry_name}: "
                            f"name_iv='{name_iv}', content_iv='{content_iv}'"
                        )

                self._iv_cache[file_hash] = content_iv
                logger.debug(
                    f"Cached content IV for {file_hash} (from raw_iv={raw_iv})"
                )

            logger.debug(
                f"Added {entry_type} to results: {rel_path} "
                f"(size: {file_entry.file_size}, "
                f"id={file_entry.id}, hash={file_entry.hash}, "
                f"parent_id={file_entry.parent_id}, "
                f"updated_at={file_entry.updated_at})"
            )

            if entry_type == "folder":
                # Add folder and recurse
                results.append((file_entry, rel_path))
                # Use the entry's hash directly for recursion
                entry_hash = entry.get("hash", "")
                self._get_recursive_helper_by_hash(entry_hash, rel_path, results)
            else:
                # Add file
                results.append((file_entry, rel_path))

    def iter_all_recursive(
        self, folder_id: int | None, path_prefix: str, batch_size: int
    ) -> Iterator[list[tuple[FileEntry, str]]]:
        """Iterate all vault entries recursively in batches.

        Args:
            folder_id: Folder ID to start from (None for root)
            path_prefix: Path prefix for relative paths
            batch_size: Number of entries per batch

        Yields:
            Batches of (FileEntry, relative_path) tuples
        """
        # For now, yield all at once (streaming could be added later)
        all_entries = self.get_all_recursive(folder_id, path_prefix)

        # Yield in batches
        for i in range(0, len(all_entries), batch_size):
            yield all_entries[i : i + batch_size]


class _VaultClientAdapter:
    """Adapter to make DrimeClient work with encrypted vault storage.

    This adapter wraps pydrime's DrimeClient and adds encryption/decryption
    for vault operations, making it compatible with syncengine's StorageClientProtocol.
    """

    def __init__(
        self,
        client: DrimeClient,
        vault_id: int,
        vault_key: VaultKey,
        parent_id: int | None,
    ) -> None:
        """Initialize vault client adapter.

        Args:
            client: DrimeClient instance
            vault_id: Vault ID
            vault_key: Vault encryption key
            parent_id: Parent folder ID in vault (None for root)
        """
        self._client = client
        self._vault_id = vault_id
        self._vault_key = vault_key
        self._parent_id = parent_id
        # Cache for file hash -> IV mapping (needed for decryption)
        self._file_iv_cache: dict[str, str | None] = {}

    def __getattr__(self, name: str) -> Any:
        """Forward all other attributes to the wrapped client."""
        return getattr(self._client, name)

    def upload_file(
        self,
        file_path: Path,
        relative_path: str,
        storage_id: int = 0,
        chunk_size: int = 25 * 1024 * 1024,
        use_multipart_threshold: int = 100 * 1024 * 1024,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> dict[str, Any]:
        """Upload file to vault with encryption.

        Args:
            file_path: Local file path
            relative_path: Relative path in vault (e.g., "folder/subfolder/file.txt")
            storage_id: Ignored (uses vault_id)
            chunk_size: Chunk size for uploads
            use_multipart_threshold: Threshold for multipart uploads
            progress_callback: Progress callback function

        Returns:
            Upload result dictionary
        """
        # Read and encrypt file content
        file_content = file_path.read_bytes()

        # Extract just the filename from relative_path for encryption
        # The folder structure should already be created by the sync engine
        filename = Path(relative_path).name
        encrypted_name, name_iv = encrypt_filename(self._vault_key, filename)

        # Encrypt content
        ciphertext, content_iv_bytes = self._vault_key.encrypt(file_content)
        content_iv = base64.b64encode(content_iv_bytes).decode("ascii")

        # Determine parent folder ID from relative_path
        # If the file is in a subfolder, the parent_id should already be
        # set by sync engine through create_folder calls.
        # We use self._parent_id as the base.
        parent_id = self._parent_id

        # Note: The sync engine should have already created the folder structure
        # and will call this with the appropriate parent context. For files in
        # subfolders, the engine manages folder creation and passes the correct
        # parent_id through the FileEntry metadata.

        # Upload encrypted file to vault
        result = self._client.upload_vault_file(
            file_path=file_path,
            encrypted_content=ciphertext,
            encrypted_name=encrypted_name,
            name_iv=name_iv,
            content_iv=content_iv,
            vault_id=self._vault_id,
            parent_id=parent_id,
        )

        # Cache the content IV for this file (needed for future downloads)
        # The file hash will be in the result
        if "fileEntry" in result:
            file_hash = result["fileEntry"].get("hash")
            if file_hash:
                self._file_iv_cache[file_hash] = content_iv
                import logging

                logger = logging.getLogger(__name__)
                logger.debug(
                    f"Cached IV for uploaded file {file_hash}: {content_iv[:20]}..."
                )

        # Call progress callback if provided (with full size as complete)
        if progress_callback:
            progress_callback(len(file_content), len(file_content))

        return result

    def download_file(
        self,
        file_id: str,
        output_path: Path,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Download file from vault with decryption.

        Adapted signature for syncengine protocol.

        Args:
            file_id: File hash identifier (base64-encoded from FileEntry.hash)
            output_path: Local output path
            progress_callback: Progress callback function

        Returns:
            Path where file was saved
        """
        import logging

        logger = logging.getLogger(__name__)

        logger.debug(f"download_file called: hash={file_id}, output={output_path}")

        # Get IV from cache (needed for decryption)
        file_iv = self._file_iv_cache.get(file_id)
        logger.debug(f"IV from cache for {file_id}: {file_iv is not None}")

        # Create parent directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Create temporary file for encrypted download
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            temp_path = Path(tmp_file.name)

        try:
            logger.debug(f"Downloading encrypted file to {temp_path}")

            # Download encrypted file
            self._client.download_vault_file(
                hash_value=file_id,
                output_path=temp_path,
            )

            logger.debug("Download complete, decrypting...")

            # Read encrypted content
            encrypted_content = temp_path.read_bytes()

            # Decrypt content using IV from cache
            # The server stores ciphertext separately from IV metadata
            decrypted_content = decrypt_file_content(
                self._vault_key,
                encrypted_content,
                iv_b64=file_iv,  # Use IV from file entry metadata
            )

            logger.debug(
                f"Decryption complete, writing "
                f"{len(decrypted_content)} bytes to {output_path}"
            )

            # Write decrypted content to output
            output_path.write_bytes(decrypted_content)

            logger.debug(f"File written successfully: {output_path}")

            # Call progress callback if provided
            if progress_callback:
                progress_callback(len(decrypted_content), len(decrypted_content))

            return output_path

        except Exception as e:
            logger.error(f"download_file failed for hash {file_id}: {e}", exc_info=True)
            raise
        finally:
            # Clean up temporary file
            if temp_path.exists():
                temp_path.unlink()

    def create_folder(
        self,
        name: str,
        parent_id: int | None = None,
        storage_id: int = 0,
    ) -> FileEntry:
        """Create folder in vault.

        Note: For vault folders, the server handles encryption automatically
        when vaultId is provided. We send the plain folder name.

        Args:
            name: Folder name (unencrypted - server will encrypt it)
            parent_id: Parent folder ID (None for root)
            storage_id: Ignored (uses vault_id)

        Returns:
            Created folder as FileEntry
        """
        # Use vault parent_id if not specified
        if parent_id is None or parent_id == 0:
            parent_id = self._parent_id

        # Create folder in vault (server will encrypt the name)
        result = self._client.create_vault_folder(
            name=name,  # Send unencrypted name - server handles encryption
            vault_id=self._vault_id,
            parent_id=parent_id,
        )

        # Extract folder info
        folder_info = result.get("folder", {})

        # Return as FileEntry with decrypted name
        return FileEntry(
            id=folder_info.get("id"),
            name=name,  # Store decrypted name
            file_name=name,
            mime="application/x-directory",
            type="folder",
            extension=None,
            parent_id=folder_info.get("parent_id"),
            hash=folder_info.get("hash", ""),
            url="",
            file_size=0,
            created_at=folder_info.get("created_at", ""),
            updated_at=folder_info.get("updated_at"),
        )

    def delete_file(self, file_id: int) -> None:
        """Delete file from vault.

        Args:
            file_id: File entry ID to delete
        """
        self._client.delete_vault_file_entries(
            entry_ids=[file_id],
            delete_forever=True,
        )

    def delete_file_entries(
        self,
        entry_ids: list[int],
        delete_forever: bool = True,
        workspace_id: int = 0,
    ) -> dict[str, Any]:
        """Delete file entries from vault (batch operation).

        This method is called by syncengine for batch deletions.

        Args:
            entry_ids: List of file entry IDs to delete
            delete_forever: Whether to permanently delete (vault always uses True)
            workspace_id: Ignored for vault operations

        Returns:
            Delete result dictionary
        """
        return self._client.delete_vault_file_entries(
            entry_ids=entry_ids,
            delete_forever=True,  # Always permanent for vault
        )

    def rename_file(self, file_id: int, new_name: str) -> None:
        """Rename file in vault.

        Args:
            file_id: File entry ID
            new_name: New name for the file (unencrypted)
        """
        import logging

        logger = logging.getLogger(__name__)

        # Encrypt the new name
        from pydrime.vault_crypto import encrypt_filename

        encrypted_name, name_iv = encrypt_filename(self._vault_key, new_name)

        logger.debug(
            f"Renaming vault file {file_id}: new_name='{new_name}', "
            f"encrypted='{encrypted_name[:20]}...', name_iv='{name_iv[:20]}...'"
        )

        # Call vault update API endpoint
        # The vault uses a special update endpoint with encrypted names
        endpoint = f"/file-entries/{file_id}"
        params = {"workspaceId": None, "_method": "PUT", "vaultId": self._vault_id}
        data = {
            "name": encrypted_name,
            "nameIv": name_iv,
        }

        result = self._client._request("POST", endpoint, params=params, json=data)
        logger.debug(f"Rename result: {result.get('status')}")


def create_vault_entries_manager_factory(
    vault_id: int, vault_key: VaultKey, parent_id: int | None
) -> Callable[[DrimeClient, int], _VaultFileEntriesManagerAdapter]:
    """Create a factory function for VaultFileEntriesManager.

    This factory is required by SyncEngine to create FileEntriesManager instances
    that work with encrypted vault storage.

    Args:
        vault_id: Vault ID
        vault_key: Vault encryption key
        parent_id: Parent folder ID in vault (None for root)

    Returns:
        A callable that takes (client, storage_id) and returns an adapted
        VaultFileEntriesManager
    """

    def factory(
        client: DrimeClient, storage_id: int
    ) -> _VaultFileEntriesManagerAdapter:
        return _VaultFileEntriesManagerAdapter(client, vault_id, vault_key, parent_id)

    return factory
