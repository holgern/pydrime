"""Sync engine for Pydrime - unified upload/download/sync operations."""

from .comparator import FileComparator, SyncDecision
from .concurrency import (
    AsyncSemaphore,
    AsyncSyncPauseController,
    ConcurrencyLimits,
    Semaphore,
    SyncPauseController,
)
from .config import SyncConfigError, load_sync_pairs_from_json
from .engine import SyncEngine
from .ignore import (
    DEFAULT_IGNORE_PATTERNS,
    IGNORE_FILE_NAME,
    LOCAL_TRASH_DIR_NAME,
    IgnoreFileManager,
    IgnoreRule,
    load_ignore_file,
)
from .modes import SyncMode
from .operations import SyncOperations, get_local_trash_path, move_to_local_trash
from .pair import SyncPair
from .progress import (
    SyncProgressCallback,
    SyncProgressEvent,
    SyncProgressInfo,
    SyncProgressTracker,
)
from .scanner import DirectoryScanner, LocalFile, RemoteFile
from .state import (
    LocalItemState,
    LocalTree,
    RemoteItemState,
    RemoteTree,
    SyncState,
    SyncStateManager,
    build_local_tree_from_files,
    build_remote_tree_from_files,
)

__all__ = [
    "SyncEngine",
    "SyncMode",
    "SyncPair",
    "SyncOperations",
    "SyncConfigError",
    "load_sync_pairs_from_json",
    "DirectoryScanner",
    "FileComparator",
    "SyncDecision",
    "LocalFile",
    "RemoteFile",
    "SyncState",
    "SyncStateManager",
    "LocalItemState",
    "LocalTree",
    "RemoteItemState",
    "RemoteTree",
    "build_local_tree_from_files",
    "build_remote_tree_from_files",
    "IgnoreFileManager",
    "IgnoreRule",
    "IGNORE_FILE_NAME",
    "LOCAL_TRASH_DIR_NAME",
    "DEFAULT_IGNORE_PATTERNS",
    "load_ignore_file",
    "get_local_trash_path",
    "move_to_local_trash",
    # Progress tracking
    "SyncProgressCallback",
    "SyncProgressEvent",
    "SyncProgressInfo",
    "SyncProgressTracker",
    # Concurrency utilities
    "Semaphore",
    "AsyncSemaphore",
    "ConcurrencyLimits",
    "SyncPauseController",
    "AsyncSyncPauseController",
]
