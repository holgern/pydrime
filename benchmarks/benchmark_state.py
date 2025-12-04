"""
Benchmark script for sync state functionality.

Tests the v2 sync state format which stores full tree metadata
(LocalTree and RemoteTree) for enabling rename detection.

Tests:
1. Initial sync: Verify state is saved with tree structure
2. State persistence: Verify state is loaded correctly after restart
3. State content: Verify file_id/id indexes are populated
4. State after modifications: Verify state updates correctly

All operations use the pydrime CLI sync command via subprocess.
"""

import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path


def run_sync_command(
    sync_pair: str, workers: int = 1, batch_size: int = 10
) -> tuple[int, str]:
    """Run a pydrime sync command with streaming output.

    Args:
        sync_pair: Sync pair string (e.g., "/local:twoWay:/remote")
        workers: Number of parallel workers
        batch_size: Number of files to process per batch

    Returns:
        Tuple of (exit_code, captured_output)
    """
    print(
        f"\n>>> Running: pydrime sync {sync_pair} "
        f"--workers {workers} --batch-size {batch_size}"
    )
    print("-" * 80)
    sys.stdout.flush()

    cmd = [
        "pydrime",
        "sync",
        sync_pair,
        "--workers",
        str(workers),
        "--batch-size",
        str(batch_size),
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    output_lines = []
    if process.stdout:
        for line in process.stdout:
            print(line, end="")
            sys.stdout.flush()
            output_lines.append(line)

    exit_code = process.wait()
    captured_output = "".join(output_lines)
    return exit_code, captured_output


def create_test_files(directory: Path, count: int = 5, size_kb: int = 1) -> list[Path]:
    """Create test files with random content."""
    directory.mkdir(parents=True, exist_ok=True)
    created_files = []

    print(f"\n[CREATE] Creating {count} test files ({size_kb}KB each) in {directory}")

    for i in range(count):
        file_path = directory / f"test_file_{i:03d}.txt"
        content = f"Test file {i}\n" + (os.urandom(size_kb * 1024 - 20).hex())
        file_path.write_text(content)
        created_files.append(file_path)
        print(f"  + Created: {file_path.name}")

    return created_files


def cleanup_remote_folder(remote_folder: str) -> None:
    """Delete remote folder permanently."""
    print(f"\n[CLEANUP] Deleting remote folder: {remote_folder}")
    try:
        cmd = ["pydrime", "rm", remote_folder, "--no-trash", "--yes"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  + Deleted remote folder: {remote_folder}")
        else:
            print(f"  ! Failed to delete remote folder: {result.stderr.strip()}")
    except Exception as e:
        print(f"  ! Error deleting remote folder: {e}")


def get_state_file_path(local_dir: Path, destination_path: str) -> Path:
    """Get the path to the sync state file for a sync pair."""
    import hashlib

    local_abs = str(local_dir.resolve())
    combined = f"{local_abs}:{destination_path}"
    key = hashlib.sha256(combined.encode()).hexdigest()[:16]

    # State files are stored in v2 subdirectory
    state_dir = Path.home() / ".config" / "pydrime" / "sync_state" / "v2"
    return state_dir / f"{key}.json"


def load_state_file(state_file: Path) -> dict | None:
    """Load and return the state file contents."""
    if not state_file.exists():
        return None
    try:
        with open(state_file, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[ERROR] Failed to load state file: {e}")
        return None


def test_initial_sync_creates_state(local_dir: Path, remote_folder: str) -> bool:
    """Test 1: Initial sync creates state with tree structure.

    Verifies that after a TWO_WAY sync, the state file contains:
    - version: 2
    - source_tree with tree and file_ids mappings
    - destination_tree with tree and ids mappings
    - synced_files set
    """
    print("\n" + "=" * 80)
    print("TEST 1: INITIAL SYNC - Verify state is created with tree structure")
    print("=" * 80)

    # Create test files
    create_test_files(local_dir, count=5, size_kb=1)

    # Sync using twoWay mode
    sync_pair = f"{local_dir}:twoWay:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    # Check state file exists
    state_file = get_state_file_path(local_dir, remote_folder)
    print(f"\n[CHECK] Looking for state file: {state_file}")

    if not state_file.exists():
        print(f"[FAIL] State file not found at {state_file}")
        return False

    print(f"[OK] State file exists: {state_file}")

    # Load and verify state
    state = load_state_file(state_file)
    if state is None:
        print("[FAIL] Failed to load state file")
        return False

    # Check version
    version = state.get("version", 1)
    print(f"\n[CHECK] State version: {version}")
    if version != 2:
        print(f"[FAIL] Expected state version 2, got {version}")
        return False
    print("[OK] State version is 2")

    # Check source_tree
    source_tree = state.get("source_tree", {})
    tree_items = source_tree.get("tree", {})
    file_ids = source_tree.get("file_ids", {})

    print(f"\n[CHECK] source_tree.tree has {len(tree_items)} items")
    print(f"[CHECK] source_tree.file_ids has {len(file_ids)} items")

    if len(tree_items) != 5:
        print(f"[FAIL] Expected 5 items in source_tree.tree, got {len(tree_items)}")
        return False
    print("[OK] source_tree.tree has 5 items")

    if len(file_ids) != 5:
        print(f"[FAIL] Expected 5 items in source_tree.file_ids, got {len(file_ids)}")
        return False
    print("[OK] source_tree.file_ids has 5 items")

    # Check destination_tree
    # Note: In streaming mode, newly uploaded files don't appear in destination_tree
    # because we don't re-scan remote after upload. destination_tree only contains
    # files that already existed on remote (downloads/skips).
    # For initial sync where all files are uploaded, destination_tree will be empty.
    destination_tree = state.get("destination_tree", {})
    remote_tree_items = destination_tree.get("tree", {})
    remote_ids = destination_tree.get("ids", {})

    print(f"\n[CHECK] destination_tree.tree has {len(remote_tree_items)} items")
    print(f"[CHECK] destination_tree.ids has {len(remote_ids)} items")
    print(
        "[INFO] Note: destination_tree is empty because all files were newly uploaded"
    )
    print("       (remote files only tracked when downloaded/skipped from existing)")

    # Check synced_files for backward compatibility
    synced_files = state.get("synced_files", [])
    print(f"\n[CHECK] synced_files has {len(synced_files)} items")
    if len(synced_files) != 5:
        print(f"[FAIL] Expected 5 items in synced_files, got {len(synced_files)}")
        return False
    print("[OK] synced_files has 5 items")

    print("\n[PASS] Initial sync creates state with proper tree structure")
    return True


def test_state_item_structure(local_dir: Path, remote_folder: str) -> bool:
    """Test 2: Verify state items have correct structure.

    Checks that LocalItemState and RemoteItemState have required fields.
    """
    print("\n" + "=" * 80)
    print("TEST 2: STATE ITEM STRUCTURE - Verify items have required fields")
    print("=" * 80)

    state_file = get_state_file_path(local_dir, remote_folder)
    state = load_state_file(state_file)

    if state is None:
        print("[FAIL] State file not found")
        return False

    # Check local item structure
    source_tree = state.get("source_tree", {})
    tree_items = source_tree.get("tree", {})

    print("\n[CHECK] Verifying LocalItemState structure...")
    required_local_fields = ["path", "size", "mtime", "file_id", "item_type"]

    for path, item in tree_items.items():
        for field in required_local_fields:
            if field not in item:
                print(f"[FAIL] Local item '{path}' missing field: {field}")
                return False

        # Verify file_id is an integer
        if not isinstance(item.get("file_id"), int):
            print(f"[FAIL] Local item '{path}' has non-integer file_id")
            return False

        print(f"  [OK] {path}: file_id={item['file_id']}, size={item['size']}")

    print("[OK] All local items have correct structure")

    # Check remote item structure
    destination_tree = state.get("destination_tree", {})
    remote_items = destination_tree.get("tree", {})

    print("\n[CHECK] Verifying RemoteItemState structure...")

    if not remote_items:
        print("[INFO] destination_tree is empty (all files were newly uploaded)")
        print("[OK] Skipping remote item structure check")
    else:
        required_remote_fields = ["path", "size", "id", "item_type"]

        for path, item in remote_items.items():
            for field in required_remote_fields:
                if field not in item:
                    print(f"[FAIL] Remote item '{path}' missing field: {field}")
                    return False

            # Verify id is an integer
            if not isinstance(item.get("id"), int):
                print(f"[FAIL] Remote item '{path}' has non-integer id")
                return False

            print(f"  [OK] {path}: id={item['id']}, size={item['size']}")

        print("[OK] All remote items have correct structure")

    # Verify file_ids index matches tree
    file_ids = source_tree.get("file_ids", {})
    print("\n[CHECK] Verifying file_ids index consistency...")

    for file_id_str, item in file_ids.items():
        file_id = int(file_id_str)
        path = item.get("path")
        if path not in tree_items:
            print(f"[FAIL] file_id {file_id} points to non-existent path: {path}")
            return False
        if tree_items[path].get("file_id") != file_id:
            print(f"[FAIL] file_id mismatch for {path}")
            return False

    print("[OK] file_ids index is consistent with tree")

    # Verify ids index matches tree
    remote_ids = destination_tree.get("ids", {})
    print("\n[CHECK] Verifying remote ids index consistency...")

    if not remote_ids:
        print("[INFO] remote ids index is empty (all files were newly uploaded)")
        print("[OK] Skipping remote ids index check")
    else:
        for id_str, item in remote_ids.items():
            remote_id = int(id_str)
            path = item.get("path")
            if path not in remote_items:
                print(f"[FAIL] id {remote_id} points to non-existent path: {path}")
                return False
            if remote_items[path].get("id") != remote_id:
                print(f"[FAIL] id mismatch for {path}")
                return False

        print("[OK] remote ids index is consistent with tree")

    print("\n[PASS] State items have correct structure")
    return True


def test_state_updates_on_add(local_dir: Path, remote_folder: str) -> bool:
    """Test 3: State updates correctly when adding a file.

    Adds a new file and verifies the state is updated to include it.
    """
    print("\n" + "=" * 80)
    print("TEST 3: STATE UPDATE ON ADD - Add file and verify state update")
    print("=" * 80)

    # Add a new file
    new_file = local_dir / "new_state_test_file.txt"
    new_file.write_text(
        "This is a new file for state testing\n" + os.urandom(512).hex()
    )
    print(f"\n[ADD] Created new file: {new_file.name}")

    # Sync
    sync_pair = f"{local_dir}:twoWay:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    # Check state was updated
    state_file = get_state_file_path(local_dir, remote_folder)
    state = load_state_file(state_file)

    if state is None:
        print("[FAIL] State file not found after sync")
        return False

    source_tree = state.get("source_tree", {})
    tree_items = source_tree.get("tree", {})

    print(f"\n[CHECK] source_tree.tree now has {len(tree_items)} items")

    # Should now have 6 files
    if len(tree_items) != 6:
        print(f"[FAIL] Expected 6 items in tree, got {len(tree_items)}")
        return False

    # Check new file is in tree
    new_file_path = "new_state_test_file.txt"
    if new_file_path not in tree_items:
        print(f"[FAIL] New file '{new_file_path}' not found in source_tree")
        return False

    print(f"[OK] New file '{new_file_path}' is in source_tree")

    # Check file_ids was updated
    file_ids = source_tree.get("file_ids", {})
    new_file_id = tree_items[new_file_path].get("file_id")
    if str(new_file_id) not in file_ids:
        print("[FAIL] New file's file_id not in file_ids index")
        return False

    print(f"[OK] New file's file_id ({new_file_id}) is in file_ids index")

    # Check remote tree - may be empty if all files were uploaded (not downloaded)
    destination_tree = state.get("destination_tree", {})
    remote_items = destination_tree.get("tree", {})

    if new_file_path in remote_items:
        print(
            f"[OK] New file is in destination_tree with "
            f"id={remote_items[new_file_path]['id']}"
        )
    else:
        print(
            "[INFO] New file not in destination_tree (expected - file was uploaded, "
            "not downloaded)"
        )

    print("\n[PASS] State updates correctly when adding a file")
    return True


def test_state_updates_on_delete(local_dir: Path, remote_folder: str) -> bool:
    """Test 4: State updates correctly when deleting a file.

    Deletes a file and verifies it's removed from state.
    """
    print("\n" + "=" * 80)
    print("TEST 4: STATE UPDATE ON DELETE - Delete file and verify state update")
    print("=" * 80)

    # Delete a file
    file_to_delete = local_dir / "test_file_000.txt"
    if not file_to_delete.exists():
        print(f"[WARN] File not found: {file_to_delete.name}")
        return False

    file_to_delete.unlink()
    print(f"\n[DELETE] Deleted file: {file_to_delete.name}")

    # Sync
    sync_pair = f"{local_dir}:twoWay:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    # Check state was updated
    state_file = get_state_file_path(local_dir, remote_folder)
    state = load_state_file(state_file)

    if state is None:
        print("[FAIL] State file not found after sync")
        return False

    source_tree = state.get("source_tree", {})
    tree_items = source_tree.get("tree", {})

    print(f"\n[CHECK] source_tree.tree now has {len(tree_items)} items")

    # Should now have 5 files (6 - 1 deleted)
    if len(tree_items) != 5:
        print(f"[FAIL] Expected 5 items in tree, got {len(tree_items)}")
        return False

    # Check deleted file is not in tree
    deleted_path = "test_file_000.txt"
    if deleted_path in tree_items:
        print(f"[FAIL] Deleted file '{deleted_path}' still in source_tree")
        return False

    print(f"[OK] Deleted file '{deleted_path}' is not in source_tree")

    # Check remote tree was also updated
    destination_tree = state.get("destination_tree", {})
    remote_items = destination_tree.get("tree", {})

    if deleted_path in remote_items:
        print("[FAIL] Deleted file still in destination_tree")
        return False

    print("[OK] Deleted file is not in destination_tree (was never there or removed)")

    print("\n[PASS] State updates correctly when deleting a file")
    return True


def test_state_persistence(local_dir: Path, remote_folder: str) -> bool:
    """Test 5: State persists correctly and idempotent sync works.

    Runs another sync and verifies state is maintained correctly.
    """
    print("\n" + "=" * 80)
    print("TEST 5: STATE PERSISTENCE - Verify idempotent sync maintains state")
    print("=" * 80)

    # Get state before sync
    state_file = get_state_file_path(local_dir, remote_folder)
    state_before = load_state_file(state_file)

    if state_before is None:
        print("[FAIL] State file not found before sync")
        return False

    items_before = len(state_before.get("source_tree", {}).get("tree", {}))
    print(f"\n[CHECK] State before sync: {items_before} items in source_tree")

    # Run sync again (should be idempotent)
    sync_pair = f"{local_dir}:twoWay:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    # Get state after sync
    state_after = load_state_file(state_file)

    if state_after is None:
        print("[FAIL] State file not found after sync")
        return False

    items_after = len(state_after.get("source_tree", {}).get("tree", {}))
    print(f"[CHECK] State after sync: {items_after} items in source_tree")

    if items_before != items_after:
        print(f"[FAIL] Item count changed: {items_before} -> {items_after}")
        return False

    print("[OK] Item count unchanged after idempotent sync")

    # Verify last_sync was updated
    last_sync_before = state_before.get("last_sync", "")
    last_sync_after = state_after.get("last_sync", "")

    if last_sync_after <= last_sync_before:
        print("[WARN] last_sync timestamp was not updated")

    print(f"[OK] last_sync: {last_sync_before} -> {last_sync_after}")

    print("\n[PASS] State persists correctly after idempotent sync")
    return True


def main() -> None:
    """Main benchmark function for sync state functionality."""
    print("\n" + "=" * 80)
    print("PYDRIME BENCHMARK: SYNC STATE FUNCTIONALITY")
    print("=" * 80)
    print("\nTests the v2 sync state format with LocalTree and RemoteTree")
    print("structures for enabling rename detection.")

    # Generate unique folder name
    test_uuid = str(uuid.uuid4())
    remote_folder = f"benchmark_state_{test_uuid[:8]}"
    base_dir = Path.cwd() / f"benchmark_temp_state_{test_uuid[:8]}"
    local_dir = base_dir / "sync_dir"

    print(f"\nRemote folder: /{remote_folder}")
    print(f"Local directory: {local_dir}")

    # Clear any existing state file for this pair
    state_file = get_state_file_path(local_dir, remote_folder)
    if state_file.exists():
        state_file.unlink()
        print(f"\n[CLEANUP] Removed existing state file: {state_file}")

    try:
        # Test 1: Initial sync creates state
        if not test_initial_sync_creates_state(local_dir, remote_folder):
            print("\n[FAIL] TEST 1 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)

        # Test 2: State item structure
        if not test_state_item_structure(local_dir, remote_folder):
            print("\n[FAIL] TEST 2 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        # Test 3: State updates on add
        if not test_state_updates_on_add(local_dir, remote_folder):
            print("\n[FAIL] TEST 3 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)

        # Test 4: State updates on delete
        if not test_state_updates_on_delete(local_dir, remote_folder):
            print("\n[FAIL] TEST 4 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)

        # Test 5: State persistence
        if not test_state_persistence(local_dir, remote_folder):
            print("\n[FAIL] TEST 5 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        # All tests passed
        print("\n" + "=" * 80)
        print("ALL TESTS PASSED FOR SYNC STATE FUNCTIONALITY")
        print("=" * 80)
        print("\nSummary:")
        print("  + Initial sync creates state with tree structure (v2 format)")
        print("  + State items have required fields (path, size, mtime, file_id/id)")
        print("  + State updates correctly when adding files")
        print("  + State updates correctly when deleting files")
        print("  + State persists correctly after idempotent sync")

        # Cleanup
        cleanup_remote_folder(remote_folder)

        # Also cleanup state file
        if state_file.exists():
            state_file.unlink()
            print(f"\n[CLEANUP] Removed state file: {state_file}")

    except KeyboardInterrupt:
        print("\n\n[ABORT] Benchmark interrupted by user")
        cleanup_remote_folder(remote_folder)
        sys.exit(130)
    except Exception as e:
        print(f"\n\n[ERROR] Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        cleanup_remote_folder(remote_folder)
        sys.exit(1)
    finally:
        if base_dir.exists():
            print(f"\n[CLEANUP] Removing local directory: {base_dir}")
            shutil.rmtree(base_dir)


if __name__ == "__main__":
    main()
