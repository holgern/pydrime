"""
Benchmark script to validate sync behavior using Python API directly.

This script performs the same tests as test_sync_modes.py but uses the Python API
directly instead of CLI commands, providing detailed debugging output.

Tests:
1. cloudUpload mode (localToCloud):
   - Creates 15 small files (1KB each) with random content
   - Uploads them to a unique UUID-named folder
   - Attempts sync again to verify nothing is uploaded (idempotency)

2. cloudDownload mode (cloudToLocal):
   - Creates a second local directory
   - Downloads files from the cloud folder
   - Attempts sync again to verify nothing is downloaded (idempotency)

Usage:
    python benchmarks/test_sync_api.py
"""

import logging
import os
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Any

# Configure logging BEFORE imports to capture all debug messages
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

# Enable debug for all pydrime modules
logging.getLogger("pydrime").setLevel(logging.DEBUG)

from syncengine.comparator import FileComparator, SyncAction  # noqa: E402
from syncengine.engine import SyncEngine  # noqa: E402
from syncengine.modes import SyncMode  # noqa: E402
from syncengine.pair import SyncPair  # noqa: E402
from syncengine.scanner import DirectoryScanner  # noqa: E402

from pydrime.api import DrimeClient  # noqa: E402
from pydrime.file_entries_manager import FileEntriesManager  # noqa: E402
from pydrime.models import FileEntriesResult, FileEntry  # noqa: E402
from pydrime.output import OutputFormatter  # noqa: E402

logger = logging.getLogger(__name__)


class _FileEntriesManagerAdapter:
    """Adapter to make FileEntriesManager compatible with syncengine's
    FileEntriesManagerProtocol.

    This adapter wraps pydrime's FileEntriesManager and adapts its method signatures
    to match the protocol expected by syncengine.
    """

    def __init__(self, manager: FileEntriesManager):
        self._manager = manager

    def find_folder_by_name(self, name: str, parent_id: int = 0) -> FileEntry | None:
        """Find folder by name (adapted signature for syncengine protocol)."""
        # Convert parent_id: 0 → None (syncengine uses 0 for root, pydrime uses None)
        actual_parent_id = None if parent_id == 0 else parent_id
        return self._manager.find_folder_by_name(name, parent_id=actual_parent_id)

    def get_all_recursive(
        self, folder_id: int | None, path_prefix: str
    ) -> list[tuple[FileEntry, str]]:
        """Get all entries recursively (adapted signature for syncengine protocol)."""
        return self._manager.get_all_recursive(
            folder_id=folder_id, path_prefix=path_prefix
        )

    def iter_all_recursive(
        self, folder_id: int | None, path_prefix: str, batch_size: int
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

    def __init__(self, client: DrimeClient):
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
        progress_callback: Any = None,
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
        storage_id: int = 0,
        parent_id: int | None = None,
    ):
        """Create folder (adapted signature for syncengine protocol).

        Converts storage_id → workspace_id for DrimeClient.
        """
        return self._client.create_folder(
            name=name,
            workspace_id=storage_id,  # Convert storage_id to workspace_id
            parent_id=parent_id,
        )

    def delete_file_entries(
        self,
        entry_ids: list[int],
        delete_forever: bool = False,
        storage_id: int = 0,
    ):
        """Delete file entries (adapted signature for syncengine protocol).

        Converts storage_id → workspace_id for DrimeClient.
        """
        return self._client.delete_file_entries(
            entry_ids=entry_ids,
            delete_forever=delete_forever,
            workspace_id=storage_id,  # Convert storage_id to workspace_id
        )


logger = logging.getLogger(__name__)


def print_separator(title: str, char: str = "=") -> None:
    """Print a separator line with title."""
    print(f"\n{char * 80}")
    print(f" {title}")
    print(f"{char * 80}")


def print_debug(msg: str, indent: int = 0) -> None:
    """Print a debug message with optional indentation."""
    prefix = "  " * indent
    print(f"{prefix}[DEBUG] {msg}")


def print_info(msg: str, indent: int = 0) -> None:
    """Print an info message."""
    prefix = "  " * indent
    print(f"{prefix}[INFO] {msg}")


def print_success(msg: str) -> None:
    """Print a success message."""
    print(f"[SUCCESS] {msg}")


def print_error(msg: str) -> None:
    """Print an error message."""
    print(f"[ERROR] {msg}")


def print_warning(msg: str) -> None:
    """Print a warning message."""
    print(f"[WARNING] {msg}")


def create_test_files(directory: Path, count: int = 15, size_kb: int = 1) -> list[Path]:
    """Create test files with random content.

    Args:
        directory: Directory to create files in
        count: Number of files to create
        size_kb: Size of each file in KB

    Returns:
        List of created file paths
    """
    print_separator(f"Creating {count} Test Files", "-")
    directory.mkdir(parents=True, exist_ok=True)
    created_files = []

    print_info(f"Directory: {directory}")
    print_info(f"File size: {size_kb}KB each")

    for i in range(count):
        file_path = directory / f"test_file_{i:03d}.txt"
        # Create random content
        content = f"Test file {i}\n" + os.urandom(size_kb * 1024 - 20).hex()
        file_path.write_text(content)
        created_files.append(file_path)
        file_size = file_path.stat().st_size
        print_debug(f"Created: {file_path.name} ({file_size} bytes)", 1)

    print_info(f"Total files created: {len(created_files)}")
    return created_files


def debug_folder_discovery(
    client: DrimeClient,
    manager: FileEntriesManager,
    folder_name: str,
    workspace_id: int,
) -> int | None:
    """Debug folder discovery using multiple methods.

    Args:
        client: DrimeClient instance
        manager: FileEntriesManager instance
        folder_name: Name of the folder to find
        workspace_id: Workspace ID

    Returns:
        Folder ID if found, None otherwise
    """
    print_separator(f"Folder Discovery Debug: '{folder_name}'", "-")

    folder_id = None

    # Method 1: FileEntriesManager.find_folder_by_name with parent_id=0
    print_info("Method 1: find_folder_by_name(parent_id=0)")
    try:
        folder_entry = manager.find_folder_by_name(folder_name, parent_id=0)
        if folder_entry:
            print_success(
                f"Found! id={folder_entry.id}, "
                f"parent_id={folder_entry.parent_id}, "
                f"name='{folder_entry.name}'"
            )
            folder_id = folder_entry.id
        else:
            print_warning("Not found via find_folder_by_name(parent_id=0)")
    except Exception as e:
        print_error(f"Exception: {e}")

    # Method 2: FileEntriesManager.find_folder_by_name without parent_id
    print_info("Method 2: find_folder_by_name(search_in_root=True)")
    try:
        folder_entry = manager.find_folder_by_name(
            folder_name, parent_id=None, search_in_root=True
        )
        if folder_entry:
            print_success(
                f"Found! id={folder_entry.id}, "
                f"parent_id={folder_entry.parent_id}, "
                f"name='{folder_entry.name}'"
            )
            if folder_id is None:
                folder_id = folder_entry.id
        else:
            print_warning("Not found via find_folder_by_name(search_in_root=True)")
    except Exception as e:
        print_error(f"Exception: {e}")

    # Method 3: Direct search API
    print_info("Method 3: client.get_file_entries(query=..., entry_type='folder')")
    try:
        result = client.get_file_entries(
            query=folder_name,
            entry_type="folder",
            workspace_id=workspace_id,
            per_page=50,
        )
        print_debug(f"API response keys: {result.keys() if result else 'None'}", 1)

        if result and result.get("data"):
            entries = FileEntriesResult.from_api_response(result)
            print_debug(f"Found {len(entries.entries)} entries in search results", 1)

            for entry in entries.entries:
                is_match = entry.name == folder_name
                marker = " <-- EXACT MATCH" if is_match else ""
                print_debug(
                    f"Entry: name='{entry.name}', id={entry.id}, "
                    f"parent_id={entry.parent_id}, type={entry.type}{marker}",
                    2,
                )
                if is_match and folder_id is None:
                    folder_id = entry.id
        else:
            print_warning("Search returned no data")
    except Exception as e:
        print_error(f"Exception: {e}")

    # Method 4: List root folder entries
    print_info("Method 4: List all folders in root (no parent_ids filter)")
    try:
        result = client.get_file_entries(
            entry_type="folder",
            workspace_id=workspace_id,
            per_page=100,
        )

        if result and result.get("data"):
            entries = FileEntriesResult.from_api_response(result)
            print_debug(f"Found {len(entries.entries)} folders total", 1)

            matching = [e for e in entries.entries if e.name == folder_name]
            if matching:
                for entry in matching:
                    print_success(
                        f"Found matching folder: id={entry.id}, "
                        f"parent_id={entry.parent_id}"
                    )
                    if folder_id is None:
                        folder_id = entry.id
            else:
                print_warning(f"No folder named '{folder_name}' in results")
                # Show first 10 folders for context
                print_debug("First 10 folders:", 1)
                for entry in entries.entries[:10]:
                    print_debug(
                        f"  - '{entry.name}' (id={entry.id}, "
                        f"parent_id={entry.parent_id})",
                        2,
                    )
    except Exception as e:
        print_error(f"Exception: {e}")

    if folder_id:
        print_success(f"Final folder_id: {folder_id}")
    else:
        print_error("Could not find folder by any method!")

    return folder_id


def debug_remote_scan(
    manager: FileEntriesManager,
    folder_id: int | None,
    folder_name: str,
) -> list:
    """Debug remote file scanning.

    Args:
        manager: FileEntriesManager instance
        folder_id: Folder ID to scan
        folder_name: Folder name (for display)

    Returns:
        List of remote files
    """
    print_separator(f"Remote Scan Debug: '{folder_name}' (id={folder_id})", "-")

    scanner = DirectoryScanner()

    if folder_id is None:
        print_warning("folder_id is None, will scan root")

    # Get all files recursively
    print_info("Calling manager.get_all_recursive()")
    try:
        entries_with_paths = manager.get_all_recursive(
            folder_id=folder_id,
            path_prefix="",
        )
        print_debug(f"get_all_recursive returned {len(entries_with_paths)} entries", 1)

        # Show details of each entry
        for entry, path in entries_with_paths[:20]:  # First 20
            print_debug(
                f"Entry: '{entry.name}' -> path='{path}', "
                f"type={entry.type}, size={entry.file_size}",
                2,
            )
        if len(entries_with_paths) > 20:
            print_debug(f"... and {len(entries_with_paths) - 20} more", 2)

    except Exception as e:
        print_error(f"Exception in get_all_recursive: {e}")
        import traceback

        traceback.print_exc()
        return []

    # Convert to DestinationFile objects
    print_info("Converting to DestinationFile objects via scanner.scan_remote()")
    try:
        remote_files = scanner.scan_remote(entries_with_paths)
        print_debug(
            f"scan_remote returned {len(remote_files)} files (folders filtered out)", 1
        )

        for rf in remote_files[:10]:
            print_debug(
                f"DestinationFile: '{rf.relative_path}', size={rf.size}, "
                f"hash={rf.hash[:20]}...",
                2,
            )
        if len(remote_files) > 10:
            print_debug(f"... and {len(remote_files) - 10} more", 2)

    except Exception as e:
        print_error(f"Exception in scan_remote: {e}")
        import traceback

        traceback.print_exc()
        return []

    return remote_files


def debug_local_scan(local_dir: Path) -> list:
    """Debug local file scanning.

    Args:
        local_dir: Local directory to scan

    Returns:
        List of local files
    """
    print_separator(f"Local Scan Debug: {local_dir}", "-")

    scanner = DirectoryScanner()

    print_info(f"Scanning directory: {local_dir}")
    print_info(f"Directory exists: {local_dir.exists()}")
    print_info(f"Is directory: {local_dir.is_dir()}")

    try:
        local_files = scanner.scan_local(local_dir)
        print_debug(f"scan_local returned {len(local_files)} files", 1)

        for lf in local_files[:10]:
            print_debug(
                f"SourceFile: '{lf.relative_path}', size={lf.size}, mtime={lf.mtime}",
                2,
            )
        if len(local_files) > 10:
            print_debug(f"... and {len(local_files) - 10} more", 2)

    except Exception as e:
        print_error(f"Exception in scan_local: {e}")
        import traceback

        traceback.print_exc()
        return []

    return local_files


def debug_comparison(
    local_files: list,
    remote_files: list,
    sync_mode: SyncMode,
) -> dict:
    """Debug file comparison.

    Args:
        local_files: List of local files
        remote_files: List of remote files
        sync_mode: Sync mode to use

    Returns:
        Dictionary with sync statistics
    """
    print_separator(f"File Comparison Debug: {sync_mode.value}", "-")

    # Build dictionaries for comparison
    local_file_map = {f.relative_path: f for f in local_files}
    remote_file_map = {f.relative_path: f for f in remote_files}

    print_info(f"Local files: {len(local_file_map)}")
    print_info(f"Remote files: {len(remote_file_map)}")

    print_debug("Local paths:", 1)
    for path in sorted(local_file_map.keys())[:10]:
        print_debug(f"  '{path}'", 2)

    print_debug("Remote paths:", 1)
    for path in sorted(remote_file_map.keys())[:10]:
        print_debug(f"  '{path}'", 2)

    # Find common, local-only, and remote-only paths
    local_paths = set(local_file_map.keys())
    remote_paths = set(remote_file_map.keys())

    common = local_paths & remote_paths
    local_only = local_paths - remote_paths
    remote_only = remote_paths - local_paths

    print_info(f"Common paths: {len(common)}")
    print_info(f"Local-only paths: {len(local_only)}")
    print_info(f"Remote-only paths: {len(remote_only)}")

    if local_only:
        print_debug("Local-only:", 1)
        for path in sorted(local_only)[:5]:
            print_debug(f"  '{path}'", 2)

    if remote_only:
        print_debug("Remote-only:", 1)
        for path in sorted(remote_only)[:5]:
            print_debug(f"  '{path}'", 2)

    # Compare files
    print_info("Running FileComparator.compare_files()")
    comparator = FileComparator(sync_mode)
    decisions = comparator.compare_files(local_file_map, remote_file_map)

    # Categorize decisions
    stats = {
        "uploads": 0,
        "downloads": 0,
        "deletes_local": 0,
        "deletes_remote": 0,
        "skips": 0,
        "conflicts": 0,
    }

    print_info(f"Total decisions: {len(decisions)}")
    print_debug("Decisions breakdown:", 1)

    for decision in decisions:
        action = decision.action
        if action == SyncAction.UPLOAD:
            stats["uploads"] += 1
        elif action == SyncAction.DOWNLOAD:
            stats["downloads"] += 1
        elif action == SyncAction.DELETE_SOURCE:
            stats["deletes_local"] += 1
        elif action == SyncAction.DELETE_DESTINATION:
            stats["deletes_remote"] += 1
        elif action == SyncAction.SKIP:
            stats["skips"] += 1
        elif action == SyncAction.CONFLICT:
            stats["conflicts"] += 1

        print_debug(
            f"{action.value}: '{decision.relative_path}' - {decision.reason}", 2
        )

    print_info(f"Summary: {stats}")
    return stats


def run_sync_engine(
    client: DrimeClient,
    local_dir: Path,
    remote_folder: str,
    sync_mode: SyncMode,
    workspace_id: int,
    batch_size: int = 10,
    max_workers: int = 4,
) -> dict:
    """Run the sync engine and return statistics.

    Args:
        client: DrimeClient instance
        local_dir: Local directory path
        remote_folder: Remote folder name
        sync_mode: Sync mode
        workspace_id: Workspace ID
        batch_size: Batch size for streaming
        max_workers: Number of parallel workers

    Returns:
        Dictionary with sync statistics
    """
    print_separator(f"Running Sync Engine: {sync_mode.value}", "-")

    print_info(f"Local: {local_dir}")
    print_info(f"Remote: /{remote_folder}")
    print_info(f"Mode: {sync_mode.value}")
    print_info(f"Batch size: {batch_size}")
    print_info(f"Workers: {max_workers}")

    # Create sync pair
    pair = SyncPair(
        source=local_dir,
        destination=f"/{remote_folder}",
        sync_mode=sync_mode,
        storage_id=workspace_id,
    )

    print_debug(
        f"SyncPair created: source={pair.source}, destination={pair.destination}", 1
    )
    print_debug(f"requires_source_scan: {sync_mode.requires_source_scan}", 1)
    print_debug(f"requires_destination_scan: {sync_mode.requires_destination_scan}", 1)
    print_debug(f"allows_upload: {sync_mode.allows_upload}", 1)
    print_debug(f"allows_download: {sync_mode.allows_download}", 1)

    # Create output formatter (quiet mode for cleaner output)
    output = OutputFormatter(quiet=False)

    # Create client adapter (required by SyncEngine to convert
    # storage_id to workspace_id)
    client_adapter = _DrimeClientAdapter(client)

    # Create entries manager factory (required by SyncEngine)
    def entries_manager_factory(client_inst, storage_id: int):
        manager = FileEntriesManager(client_inst._client, workspace_id=storage_id)
        return _FileEntriesManagerAdapter(manager)

    # Create sync engine with adapted client
    engine = SyncEngine(client_adapter, entries_manager_factory, output=output)

    print_info("Executing sync_pair()...")
    start_time = time.time()

    try:
        stats = engine.sync_pair(
            pair,
            dry_run=False,
            batch_size=batch_size,
            use_streaming=True,
            max_workers=max_workers,
        )

        elapsed = time.time() - start_time
        print_info(f"Sync completed in {elapsed:.2f}s")
        print_info(f"Stats: {stats}")

        return stats

    except Exception as e:
        print_error(f"Sync failed with exception: {e}")
        import traceback

        traceback.print_exc()
        return {"error": str(e)}


def cleanup_remote_folder(
    client: DrimeClient, folder_name: str, workspace_id: int
) -> bool:
    """Delete remote folder permanently.

    Args:
        client: DrimeClient instance
        folder_name: Folder name to delete

    Returns:
        True if deleted, False otherwise
    """
    print_separator(f"Cleanup: Deleting '{folder_name}'", "-")

    try:
        # Search for the folder
        result = client.get_file_entries(query=folder_name, entry_type="folder")
        if result and result.get("data"):
            entries = FileEntriesResult.from_api_response(result)
            for entry in entries.entries:
                if entry.name == folder_name:
                    print_info(f"Found folder id={entry.id}, deleting permanently...")
                    client.delete_file_entries(
                        [entry.id], delete_forever=True, workspace_id=workspace_id
                    )
                    print_success("Folder deleted")
                    return True
        print_warning("Folder not found for cleanup")
        return False
    except Exception as e:
        print_error(f"Cleanup failed: {e}")
        return False


def test_cloud_upload(
    client: DrimeClient,
    manager: FileEntriesManager,
    base_dir: Path,
    remote_folder: str,
    workspace_id: int,
) -> bool:
    """Test cloudUpload (localToCloud) sync mode.

    Args:
        client: DrimeClient instance
        manager: FileEntriesManager instance
        base_dir: Base directory for test files
        remote_folder: Remote folder name
        workspace_id: Workspace ID

    Returns:
        True if test passed, False otherwise
    """
    print_separator("TEST 1: CLOUD UPLOAD (localToCloud) MODE", "=")

    # Create local directory and test files
    local_dir = base_dir / "upload_test"
    create_test_files(local_dir, count=15, size_kb=1)

    # First sync - should upload all 15 files
    print_separator("First Sync (should upload 15 files)", "-")
    stats1 = run_sync_engine(
        client,
        local_dir,
        remote_folder,
        SyncMode.SOURCE_TO_DESTINATION,
        workspace_id,
        batch_size=10,
        max_workers=1,
    )

    if "error" in stats1:
        print_error(f"First sync failed: {stats1['error']}")
        return False

    uploaded1 = stats1.get("uploads", 0)
    print_info(f"First sync uploaded: {uploaded1} files")

    if uploaded1 != 15:
        print_error(f"Expected 15 uploads, got {uploaded1}")
        return False

    print_success("First sync uploaded 15 files as expected")

    # Wait for API to process
    print_info("Waiting 2 seconds for API to process...")
    time.sleep(2)

    # Debug folder discovery after upload
    print_separator("Debugging Folder Discovery After Upload", "-")
    folder_id = debug_folder_discovery(client, manager, remote_folder, workspace_id)

    if folder_id is None:
        print_error("CRITICAL: Folder not found after upload!")
        print_warning("This is the bug we're investigating.")

        # Try to find files and their parent
        print_info("Searching for uploaded files to find parent folder...")
        result = client.get_file_entries(query="test_file_000.txt", per_page=10)
        if result and result.get("data"):
            entries = FileEntriesResult.from_api_response(result)
            for entry in entries.entries:
                print_debug(f"Found file: {entry.name}, parent_id={entry.parent_id}", 1)
                if entry.parent_id:
                    print_info(
                        f"Trying to get parent folder info for id={entry.parent_id}"
                    )
                    try:
                        parent_info = client.get_folder_info(entry.parent_id)
                        print_debug(f"Parent folder: {parent_info}", 1)
                        folder_id = entry.parent_id
                    except Exception as e:
                        print_error(f"Could not get parent info: {e}")
    else:
        # Debug remote scan
        remote_files = debug_remote_scan(manager, folder_id, remote_folder)
        print_info(f"Found {len(remote_files)} remote files")

    # Second sync - should upload nothing (idempotency)
    print_separator("Second Sync (should upload 0 files - idempotency)", "-")
    stats2 = run_sync_engine(
        client,
        local_dir,
        remote_folder,
        SyncMode.SOURCE_TO_DESTINATION,
        workspace_id,
        batch_size=10,
        max_workers=1,
    )

    if "error" in stats2:
        print_error(f"Second sync failed: {stats2['error']}")
        return False

    uploaded2 = stats2.get("uploads", 0)
    print_info(f"Second sync uploaded: {uploaded2} files")

    if uploaded2 != 0:
        print_error(f"Expected 0 uploads (idempotency), got {uploaded2}")
        return False

    print_success("Second sync uploaded 0 files - idempotency confirmed!")
    return True


def test_cloud_download(
    client: DrimeClient,
    manager: FileEntriesManager,
    base_dir: Path,
    remote_folder: str,
    workspace_id: int,
) -> bool:
    """Test cloudDownload (cloudToLocal) sync mode.

    Args:
        client: DrimeClient instance
        manager: FileEntriesManager instance
        base_dir: Base directory for test files
        remote_folder: Remote folder name
        workspace_id: Workspace ID

    Returns:
        True if test passed, False otherwise
    """
    print_separator("TEST 2: CLOUD DOWNLOAD (cloudToLocal) MODE", "=")

    # Create empty local directory
    local_dir = base_dir / "download_test"
    local_dir.mkdir(parents=True, exist_ok=True)
    print_info(f"Created empty download directory: {local_dir}")

    # First sync - should download all 15 files
    print_separator("First Sync (should download 15 files)", "-")
    stats1 = run_sync_engine(
        client,
        local_dir,
        remote_folder,
        SyncMode.DESTINATION_TO_SOURCE,
        workspace_id,
        batch_size=10,
        max_workers=1,
    )

    if "error" in stats1:
        print_error(f"First download sync failed: {stats1['error']}")
        return False

    downloaded1 = stats1.get("downloads", 0)
    print_info(f"First sync downloaded: {downloaded1} files")

    if downloaded1 != 15:
        print_error(f"Expected 15 downloads, got {downloaded1}")
        return False

    # Verify files exist locally
    local_files = list(local_dir.glob("test_file_*.txt"))
    print_info(f"Local files after download: {len(local_files)}")

    if len(local_files) != 15:
        print_error(f"Expected 15 local files, found {len(local_files)}")
        return False

    print_success("First download sync downloaded 15 files as expected")

    # Wait a bit
    print_info("Waiting 2 seconds...")
    time.sleep(2)

    # Second sync - should download nothing (idempotency)
    print_separator("Second Sync (should download 0 files - idempotency)", "-")
    stats2 = run_sync_engine(
        client,
        local_dir,
        remote_folder,
        SyncMode.DESTINATION_TO_SOURCE,
        workspace_id,
        batch_size=10,
        max_workers=1,
    )

    if "error" in stats2:
        print_error(f"Second download sync failed: {stats2['error']}")
        return False

    downloaded2 = stats2.get("downloads", 0)
    print_info(f"Second sync downloaded: {downloaded2} files")

    if downloaded2 != 0:
        print_error(f"Expected 0 downloads (idempotency), got {downloaded2}")
        return False

    print_success("Second download sync downloaded 0 files - idempotency confirmed!")
    return True


def main():
    """Main benchmark function."""
    print_separator("PYDRIME SYNC API BENCHMARK (Direct Python API)", "=")

    # Generate unique test folder name
    test_uuid = str(uuid.uuid4())
    remote_folder = f"benchmark_{test_uuid}"

    print_info(f"Test UUID: {test_uuid}")
    print_info(f"Remote folder: {remote_folder}")

    # Create local base directory
    base_dir = Path.cwd() / f"benchmark_temp_{test_uuid[:8]}"
    base_dir.mkdir(parents=True, exist_ok=True)
    print_info(f"Local base directory: {base_dir}")

    # Initialize client and manager
    workspace_id = 0  # Personal workspace
    print_info(f"Workspace ID: {workspace_id}")

    client = None  # Initialize to None for safe cleanup in exception handlers

    try:
        print_separator("Initializing API Client", "-")
        client = DrimeClient()
        print_success("DrimeClient initialized")

        manager = FileEntriesManager(client, workspace_id)
        print_success("FileEntriesManager initialized")

        # Verify connection
        print_info("Verifying API connection...")
        user_info = client.get_logged_user()
        if user_info and user_info.get("user"):
            user = user_info["user"]
            print_success(f"Connected as: {user.get('email', 'unknown')}")
        else:
            print_error("Could not verify API connection")
            sys.exit(1)

        # Test 1: Cloud Upload
        test1_passed = test_cloud_upload(
            client, manager, base_dir, remote_folder, workspace_id
        )

        if not test1_passed:
            print_error("TEST 1 FAILED - Stopping benchmarks")
            cleanup_remote_folder(client, remote_folder, workspace_id)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        # Wait for API to process uploaded files
        print_separator("Waiting for API Processing", "-")
        print_info("Waiting 10 seconds for API to process uploaded files...")
        time.sleep(10)

        # Test 2: Cloud Download
        test2_passed = test_cloud_download(
            client, manager, base_dir, remote_folder, workspace_id
        )

        if not test2_passed:
            print_error("TEST 2 FAILED")
            cleanup_remote_folder(client, remote_folder, workspace_id)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        # All tests passed
        print_separator("ALL TESTS PASSED", "=")
        print_success("Cloud upload (localToCloud) mode works correctly")
        print_success("Upload idempotency confirmed (no duplicate uploads)")
        print_success("Cloud download (cloudToLocal) mode works correctly")
        print_success("Download idempotency confirmed (no duplicate downloads)")

        # Cleanup
        cleanup_remote_folder(client, remote_folder, workspace_id)

    except KeyboardInterrupt:
        print_warning("\nBenchmark interrupted by user")
        if client is not None:
            cleanup_remote_folder(client, remote_folder, workspace_id)
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        try:
            if client is not None:
                cleanup_remote_folder(client, remote_folder, workspace_id)
        except Exception:
            pass
        sys.exit(1)
    finally:
        # Clean up local files
        if base_dir.exists():
            print_info(f"Removing local directory: {base_dir}")
            shutil.rmtree(base_dir)

    print_separator("BENCHMARK COMPLETE", "=")


if __name__ == "__main__":
    main()
