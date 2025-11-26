"""State management for tracking sync history.

This module provides state tracking for bidirectional sync modes,
enabling proper detection of file deletions and renames by remembering
which files were present in previous sync operations along with their
metadata (size, mtime, file_id, hash).

Inspired by filen-sync's state.ts implementation which stores:
- Full tree structure (path -> item mapping)
- File ID index for local files (file_id -> item mapping)
- ID index for remote files (id -> item mapping)
- Local file hashes for change detection

Cross-platform note:
- On Linux/macOS: file_id is the inode number (st_ino), which persists across renames
- On Windows: file_id is the NTFS file index (st_ino), which should persist across
  renames on NTFS but behavior may vary on other filesystems
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# State format version - increment when breaking changes are made
STATE_VERSION = 2


@dataclass
class LocalItemState:
    """State of a local file/directory from previous sync.

    Stores metadata needed for rename detection and change comparison.
    """

    path: str
    """Relative path (using forward slashes)"""

    size: int
    """File size in bytes (0 for directories)"""

    mtime: float
    """Last modification time (Unix timestamp)"""

    file_id: int
    """Filesystem file identifier (inode on Unix, file index on Windows).

    This value persists across renames on most filesystems, enabling
    rename detection by tracking the same file_id at a different path.
    """

    item_type: str = "file"
    """Type: 'file' or 'directory'"""

    creation_time: Optional[float] = None
    """Creation time (Unix timestamp) if available"""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "path": self.path,
            "size": self.size,
            "mtime": self.mtime,
            "file_id": self.file_id,
            "item_type": self.item_type,
            "creation_time": self.creation_time,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LocalItemState":
        """Create from dictionary."""
        return cls(
            path=data.get("path", ""),
            size=data.get("size", 0),
            mtime=data.get("mtime", 0.0),
            # Support both 'file_id' and legacy 'inode' key for backward compatibility
            file_id=data.get("file_id", data.get("inode", 0)),
            item_type=data.get("item_type", "file"),
            creation_time=data.get("creation_time"),
        )


@dataclass
class RemoteItemState:
    """State of a remote file/directory from previous sync.

    Stores metadata needed for rename detection and change comparison.
    """

    path: str
    """Relative path (using forward slashes)"""

    size: int
    """File size in bytes (0 for directories)"""

    mtime: Optional[float]
    """Last modification time (Unix timestamp) if available"""

    id: int
    """Remote entry ID - persists across renames"""

    item_type: str = "file"
    """Type: 'file' or 'directory'"""

    file_hash: str = ""
    """MD5 hash of file content (empty for directories)"""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "path": self.path,
            "size": self.size,
            "mtime": self.mtime,
            "id": self.id,
            "item_type": self.item_type,
            "file_hash": self.file_hash,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RemoteItemState":
        """Create from dictionary."""
        return cls(
            path=data.get("path", ""),
            size=data.get("size", 0),
            mtime=data.get("mtime"),
            # Support both 'id' and legacy 'uuid' key for backward compatibility
            id=data.get("id", data.get("uuid", 0)),
            item_type=data.get("item_type", "file"),
            file_hash=data.get("file_hash", ""),
        )


@dataclass
class LocalTree:
    """Full local tree state with path and file_id indexes.

    Provides O(1) lookup by both path and file_id for efficient
    rename detection.
    """

    tree: dict[str, LocalItemState] = field(default_factory=dict)
    """Path -> LocalItemState mapping"""

    file_ids: dict[int, LocalItemState] = field(default_factory=dict)
    """File ID -> LocalItemState mapping for rename detection"""

    @property
    def size(self) -> int:
        """Number of items in tree."""
        return len(self.tree)

    def add_item(self, item: LocalItemState) -> None:
        """Add an item to both indexes."""
        self.tree[item.path] = item
        self.file_ids[item.file_id] = item

    def remove_item(self, path: str) -> Optional[LocalItemState]:
        """Remove an item from both indexes by path."""
        item = self.tree.pop(path, None)
        if item:
            self.file_ids.pop(item.file_id, None)
        return item

    def get_by_path(self, path: str) -> Optional[LocalItemState]:
        """Get item by path."""
        return self.tree.get(path)

    def get_by_file_id(self, file_id: int) -> Optional[LocalItemState]:
        """Get item by file_id (for rename detection)."""
        return self.file_ids.get(file_id)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "tree": {k: v.to_dict() for k, v in self.tree.items()},
            "file_ids": {str(k): v.to_dict() for k, v in self.file_ids.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LocalTree":
        """Create from dictionary."""
        tree_data = data.get("tree", {})
        # Support both 'file_ids' and legacy 'inodes' key for backward compatibility
        file_ids_data = data.get("file_ids", data.get("inodes", {}))
        return cls(
            tree={k: LocalItemState.from_dict(v) for k, v in tree_data.items()},
            file_ids={
                int(k): LocalItemState.from_dict(v) for k, v in file_ids_data.items()
            },
        )


@dataclass
class RemoteTree:
    """Full remote tree state with path and ID indexes.

    Provides O(1) lookup by both path and ID for efficient
    rename detection.
    """

    tree: dict[str, RemoteItemState] = field(default_factory=dict)
    """Path -> RemoteItemState mapping"""

    ids: dict[int, RemoteItemState] = field(default_factory=dict)
    """ID -> RemoteItemState mapping for rename detection"""

    @property
    def size(self) -> int:
        """Number of items in tree."""
        return len(self.tree)

    def add_item(self, item: RemoteItemState) -> None:
        """Add an item to both indexes."""
        self.tree[item.path] = item
        self.ids[item.id] = item

    def remove_item(self, path: str) -> Optional[RemoteItemState]:
        """Remove an item from both indexes by path."""
        item = self.tree.pop(path, None)
        if item:
            self.ids.pop(item.id, None)
        return item

    def get_by_path(self, path: str) -> Optional[RemoteItemState]:
        """Get item by path."""
        return self.tree.get(path)

    def get_by_id(self, id: int) -> Optional[RemoteItemState]:
        """Get item by ID (for rename detection)."""
        return self.ids.get(id)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "tree": {k: v.to_dict() for k, v in self.tree.items()},
            "ids": {str(k): v.to_dict() for k, v in self.ids.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RemoteTree":
        """Create from dictionary."""
        tree_data = data.get("tree", {})
        # Support both 'ids' and legacy 'uuids' key for backward compatibility
        ids_data = data.get("ids", data.get("uuids", {}))
        return cls(
            tree={k: RemoteItemState.from_dict(v) for k, v in tree_data.items()},
            ids={int(k): RemoteItemState.from_dict(v) for k, v in ids_data.items()},
        )


@dataclass
class SyncState:
    """Represents the full state of a sync pair from a previous sync.

    Stores complete tree structure for both local and remote, enabling:
    - Deletion detection (file in previous state but not current)
    - Rename detection (same file_id/id, different path)
    - Change detection (same path, different size/mtime/hash)

    This is version 2 of the state format, storing full metadata instead
    of just file paths.
    """

    local_path: str
    """Local directory path that was synced"""

    remote_path: str
    """Remote path that was synced"""

    local_tree: LocalTree = field(default_factory=LocalTree)
    """Full local tree with path and file_id indexes"""

    remote_tree: RemoteTree = field(default_factory=RemoteTree)
    """Full remote tree with path and ID indexes"""

    local_file_hashes: dict[str, str] = field(default_factory=dict)
    """Path -> MD5 hash mapping for local files"""

    last_sync: Optional[str] = None
    """ISO timestamp of last successful sync"""

    version: int = STATE_VERSION
    """State format version"""

    # Legacy field for backward compatibility
    synced_files: set[str] = field(default_factory=set)
    """Set of relative paths (legacy, for backward compatibility)"""

    def to_dict(self) -> dict[str, Any]:
        """Convert state to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "local_path": self.local_path,
            "remote_path": self.remote_path,
            "local_tree": self.local_tree.to_dict(),
            "remote_tree": self.remote_tree.to_dict(),
            "local_file_hashes": self.local_file_hashes,
            "last_sync": self.last_sync,
            # Include legacy synced_files for compatibility
            "synced_files": sorted(self.synced_files),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SyncState":
        """Create SyncState from dictionary."""
        version = data.get("version", 1)

        # Handle legacy v1 format (just synced_files)
        if version == 1 or "local_tree" not in data:
            return cls(
                local_path=data.get("local_path", ""),
                remote_path=data.get("remote_path", ""),
                synced_files=set(data.get("synced_files", [])),
                last_sync=data.get("last_sync"),
                version=1,
            )

        # Handle v2 format with full trees
        return cls(
            local_path=data.get("local_path", ""),
            remote_path=data.get("remote_path", ""),
            local_tree=LocalTree.from_dict(data.get("local_tree", {})),
            remote_tree=RemoteTree.from_dict(data.get("remote_tree", {})),
            local_file_hashes=data.get("local_file_hashes", {}),
            last_sync=data.get("last_sync"),
            version=version,
            synced_files=set(data.get("synced_files", [])),
        )

    def get_synced_paths(self) -> set[str]:
        """Get all synced paths (for backward compatibility).

        Returns paths that exist in both local and remote trees.
        """
        if self.version == 1:
            return self.synced_files

        local_paths = set(self.local_tree.tree.keys())
        remote_paths = set(self.remote_tree.tree.keys())
        return local_paths & remote_paths


class SyncStateManager:
    """Manages sync state persistence for tracking file deletions and renames.

    The state is stored in a JSON file in the user's config directory,
    keyed by a hash of the local and remote paths to support multiple
    sync pairs.

    State is stored in versioned subdirectories to handle format migrations.
    """

    def __init__(self, state_dir: Optional[Path] = None):
        """Initialize state manager.

        Args:
            state_dir: Directory to store state files. Defaults to
                      ~/.config/pydrime/sync_state/
        """
        if state_dir is None:
            state_dir = Path.home() / ".config" / "pydrime" / "sync_state"
        self.state_dir = state_dir / f"v{STATE_VERSION}"
        self.state_dir.mkdir(parents=True, exist_ok=True)

        # Also check legacy directory for migration
        self._legacy_dir = state_dir

    def _get_state_key(self, local_path: Path, remote_path: str) -> str:
        """Generate a unique key for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path

        Returns:
            Hash-based key for the sync pair
        """
        # Use absolute path for consistency
        local_abs = str(local_path.resolve())
        combined = f"{local_abs}:{remote_path}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def _get_state_file(self, local_path: Path, remote_path: str) -> Path:
        """Get the state file path for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path

        Returns:
            Path to the state file
        """
        key = self._get_state_key(local_path, remote_path)
        return self.state_dir / f"{key}.json"

    def _get_legacy_state_file(self, local_path: Path, remote_path: str) -> Path:
        """Get legacy state file path (for migration)."""
        key = self._get_state_key(local_path, remote_path)
        return self._legacy_dir / f"{key}.json"

    def load_state(self, local_path: Path, remote_path: str) -> Optional[SyncState]:
        """Load sync state for a sync pair.

        Automatically migrates from v1 format if needed.

        Args:
            local_path: Local directory path
            remote_path: Remote path

        Returns:
            SyncState if found, None otherwise
        """
        state_file = self._get_state_file(local_path, remote_path)
        legacy_file = self._get_legacy_state_file(local_path, remote_path)

        # Try current version first
        if state_file.exists():
            try:
                with open(state_file, encoding="utf-8") as f:
                    data = json.load(f)
                state = SyncState.from_dict(data)
                logger.debug(
                    f"Loaded sync state v{state.version} with "
                    f"{state.local_tree.size} local, {state.remote_tree.size} remote "
                    f"items from {state.last_sync}"
                )
                return state
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load sync state: {e}")
                return None

        # Try legacy v1 format for migration
        if legacy_file.exists() and legacy_file != state_file:
            try:
                with open(legacy_file, encoding="utf-8") as f:
                    data = json.load(f)
                state = SyncState.from_dict(data)
                logger.info(
                    f"Migrated legacy sync state with {len(state.synced_files)} files"
                )
                return state
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load legacy sync state: {e}")
                return None

        logger.debug(f"No sync state found at {state_file}")
        return None

    def save_state(
        self,
        local_path: Path,
        remote_path: str,
        synced_files: Optional[set[str]] = None,
        local_tree: Optional[LocalTree] = None,
        remote_tree: Optional[RemoteTree] = None,
        local_file_hashes: Optional[dict[str, str]] = None,
    ) -> None:
        """Save sync state for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path
            synced_files: Set of relative paths (legacy, optional)
            local_tree: Full local tree state
            remote_tree: Full remote tree state
            local_file_hashes: MD5 hashes of local files
        """
        state = SyncState(
            local_path=str(local_path.resolve()),
            remote_path=remote_path,
            local_tree=local_tree or LocalTree(),
            remote_tree=remote_tree or RemoteTree(),
            local_file_hashes=local_file_hashes or {},
            synced_files=synced_files or set(),
            last_sync=datetime.now().isoformat(),
            version=STATE_VERSION,
        )

        # If only synced_files provided (backward compat), populate trees
        if synced_files and not local_tree and not remote_tree:
            state.synced_files = synced_files

        state_file = self._get_state_file(local_path, remote_path)

        try:
            # Write atomically using temp file
            tmp_file = state_file.with_suffix(".tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, indent=2)
            tmp_file.replace(state_file)

            logger.debug(
                f"Saved sync state v{STATE_VERSION} with "
                f"{state.local_tree.size} local, {state.remote_tree.size} remote "
                f"items to {state_file}"
            )
        except OSError as e:
            logger.warning(f"Failed to save sync state: {e}")
            # Clean up temp file if it exists
            tmp_file = state_file.with_suffix(".tmp")
            if tmp_file.exists():
                tmp_file.unlink()

    def save_state_from_trees(
        self,
        local_path: Path,
        remote_path: str,
        local_tree: LocalTree,
        remote_tree: RemoteTree,
        local_file_hashes: Optional[dict[str, str]] = None,
    ) -> None:
        """Save sync state with full tree information.

        This is the preferred method for v2 state format.

        Args:
            local_path: Local directory path
            remote_path: Remote path
            local_tree: Full local tree state
            remote_tree: Full remote tree state
            local_file_hashes: MD5 hashes of local files
        """
        # Also compute synced_files for backward compatibility
        local_paths = set(local_tree.tree.keys())
        remote_paths = set(remote_tree.tree.keys())
        synced_files = local_paths & remote_paths

        self.save_state(
            local_path=local_path,
            remote_path=remote_path,
            synced_files=synced_files,
            local_tree=local_tree,
            remote_tree=remote_tree,
            local_file_hashes=local_file_hashes,
        )

    def clear_state(self, local_path: Path, remote_path: str) -> bool:
        """Clear sync state for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path

        Returns:
            True if state was cleared, False if no state existed
        """
        state_file = self._get_state_file(local_path, remote_path)
        legacy_file = self._get_legacy_state_file(local_path, remote_path)
        cleared = False

        if state_file.exists():
            state_file.unlink()
            logger.debug(f"Cleared sync state at {state_file}")
            cleared = True

        # Also clear legacy file if different
        if legacy_file != state_file and legacy_file.exists():
            legacy_file.unlink()
            logger.debug(f"Cleared legacy sync state at {legacy_file}")
            cleared = True

        return cleared


def build_local_tree_from_files(local_files: list) -> LocalTree:
    """Build a LocalTree from a list of LocalFile objects.

    This function creates a LocalTree with proper file_id indexing
    for efficient rename detection.

    Args:
        local_files: List of LocalFile objects from DirectoryScanner.scan_local()

    Returns:
        LocalTree with all files indexed by path and file_id
    """
    tree = LocalTree()

    for local_file in local_files:
        item = LocalItemState(
            path=local_file.relative_path,
            size=local_file.size,
            mtime=local_file.mtime,
            file_id=local_file.file_id,
            item_type="file",
            creation_time=local_file.creation_time,
        )
        tree.add_item(item)

    return tree


def build_remote_tree_from_files(remote_files: list) -> RemoteTree:
    """Build a RemoteTree from a list of RemoteFile objects.

    This function creates a RemoteTree with proper ID indexing
    for efficient rename detection.

    Args:
        remote_files: List of RemoteFile objects from DirectoryScanner.scan_remote()

    Returns:
        RemoteTree with all files indexed by path and ID
    """
    tree = RemoteTree()

    for remote_file in remote_files:
        item = RemoteItemState(
            path=remote_file.relative_path,
            size=remote_file.size,
            mtime=remote_file.mtime,
            id=remote_file.id,
            item_type="file",
            file_hash=remote_file.hash,
        )
        tree.add_item(item)

    return tree
