"""
Benchmark script for LOCAL_TO_CLOUD sync mode.

LOCAL_TO_CLOUD mode: Mirror every action done locally to the cloud
but never act on cloud changes.
Renaming, deleting & moving is only transferred to the cloud.

Tests:
1. Initial state: Create local files, sync to cloud
2. Add file: Add new local file, verify it appears in cloud
3. Delete file: Delete local file, verify it's deleted from cloud
4. Modify file: Modify local file size, verify cloud is updated

All operations use the pydrime CLI sync command via subprocess.
"""

import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

from pydrime.api import DrimeClient
from pydrime.config import config
from pydrime.utils import verify_remote_files_have_users


def run_sync_command(
    sync_pair: str, workers: int = 4, batch_size: int = 10, start_delay: float = 0.0
) -> tuple[int, str]:
    """Run a pydrime sync command with streaming output.

    Args:
        sync_pair: Sync pair string (e.g., "/local:localToCloud:/remote")
        workers: Number of parallel workers
        batch_size: Number of files to process per batch

    Returns:
        Tuple of (exit_code, captured_output)
    """
    print(
        f"\n>>> Running: pydrime sync {sync_pair} "
        f"--workers {workers} --batch-size {batch_size} --start-delay {start_delay}"
    )
    print("-" * 80)
    sys.stdout.flush()

    cmd = [
        "pydrime",
        "sync",
        sync_pair,
        "--workers",
        str(workers),
        "--start-delay",
        str(start_delay),
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


def parse_sync_output(stdout: str) -> dict[str, int]:
    """Parse sync command output to extract statistics."""
    stats = {
        "uploaded": 0,
        "downloaded": 0,
        "deleted_local": 0,
        "deleted_remote": 0,
    }

    for line in stdout.split("\n"):
        line = line.strip()
        if "Uploaded:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["uploaded"] = int(parts[1].strip())
                except ValueError:
                    pass
        elif "Downloaded:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["downloaded"] = int(parts[1].strip())
                except ValueError:
                    pass
        elif "Deleted locally:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["deleted_local"] = int(parts[1].strip())
                except ValueError:
                    pass
        elif "Deleted remotely:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["deleted_remote"] = int(parts[1].strip())
                except ValueError:
                    pass

    return stats


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


def test_initial_state(
    local_dir: Path, remote_folder: str, client: DrimeClient, workspace_id: int
) -> bool:
    """Test 1: Initial state - create local files and sync to cloud.

    LOCAL_TO_CLOUD should upload all local files to cloud.
    """
    print("\n" + "=" * 80)
    print("TEST 1: INITIAL STATE - Upload local files to cloud")
    print("=" * 80)

    # Create test files
    create_test_files(local_dir, count=5, size_kb=1)

    # Sync using localToCloud mode
    sync_pair = f"{local_dir}:localToCloud:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify 5 files were uploaded
    if stats["uploaded"] != 5:
        print(f"[FAIL] Expected 5 uploads, got {stats['uploaded']}")
        return False

    # Verify no downloads (LOCAL_TO_CLOUD never downloads)
    if stats["downloaded"] != 0:
        print(f"[FAIL] Expected 0 downloads, got {stats['downloaded']}")
        return False

    print("[PASS] Initial state: 5 files uploaded successfully")

    # Verify users field is set correctly
    time.sleep(2)  # Wait for API to process
    verification_result = verify_remote_files_have_users(
        client, remote_folder, expected_count=5, workspace_id=workspace_id
    )
    if not verification_result:
        print(
            f"[FAIL] Users field verification failed: "
            f"{verification_result.verified_count}/{verification_result.total_count} "
            f"verified, expected 5"
        )
        return False
    print("[PASS] Users field verification passed")

    # Verify idempotency
    time.sleep(2)
    print("\n[CHECK] Verifying idempotency (second sync should do nothing)...")
    exit_code, stdout = run_sync_command(sync_pair)
    stats = parse_sync_output(stdout)

    if stats["uploaded"] != 0:
        print(f"[FAIL] Expected 0 uploads on second sync, got {stats['uploaded']}")
        return False

    print("[PASS] Idempotency confirmed")
    return True


def test_add_file(local_dir: Path, remote_folder: str) -> bool:
    """Test 2: Add file - add new local file, verify it appears in cloud.

    LOCAL_TO_CLOUD should upload the new file.
    """
    print("\n" + "=" * 80)
    print("TEST 2: ADD FILE - Add new local file")
    print("=" * 80)

    # Add a new file
    new_file = local_dir / "new_file.txt"
    new_file.write_text(
        "This is a new file added for testing\n" + os.urandom(512).hex()
    )
    print(f"\n[ADD] Created new file: {new_file.name}")

    # Sync
    sync_pair = f"{local_dir}:localToCloud:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify 1 file was uploaded
    if stats["uploaded"] != 1:
        print(f"[FAIL] Expected 1 upload, got {stats['uploaded']}")
        return False

    print("[PASS] Add file: new file uploaded successfully")
    return True


def test_delete_file(local_dir: Path, remote_folder: str) -> bool:
    """Test 3: Delete file - delete local file, verify it's deleted from cloud.

    LOCAL_TO_CLOUD should delete the file from cloud (allows_remote_delete=True).
    """
    print("\n" + "=" * 80)
    print("TEST 3: DELETE FILE - Delete local file")
    print("=" * 80)

    # Delete a file locally
    file_to_delete = local_dir / "test_file_000.txt"
    if file_to_delete.exists():
        file_to_delete.unlink()
        print(f"\n[DELETE] Deleted local file: {file_to_delete.name}")
    else:
        print(f"[WARN] File not found: {file_to_delete.name}")
        return False

    # Sync
    sync_pair = f"{local_dir}:localToCloud:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify 1 file was deleted from remote (LOCAL_TO_CLOUD allows remote delete)
    if stats["deleted_remote"] != 1:
        print(f"[FAIL] Expected 1 remote deletion, got {stats['deleted_remote']}")
        return False

    print("[PASS] Delete file: file deleted from cloud successfully")
    return True


def test_modify_file(local_dir: Path, remote_folder: str) -> bool:
    """Test 4: Modify file - modify local file size, verify cloud is updated.

    LOCAL_TO_CLOUD should upload the modified file.
    """
    print("\n" + "=" * 80)
    print("TEST 4: MODIFY FILE - Modify local file size")
    print("=" * 80)

    # Modify a file (change its content/size)
    file_to_modify = local_dir / "test_file_001.txt"
    if file_to_modify.exists():
        original_size = file_to_modify.stat().st_size
        # Add more content to change the size
        new_content = (
            file_to_modify.read_text()
            + "\n\nModified content\n"
            + os.urandom(1024).hex()
        )
        file_to_modify.write_text(new_content)
        new_size = file_to_modify.stat().st_size
        print(
            f"\n[MODIFY] Modified file: {file_to_modify.name} "
            f"({original_size} -> {new_size} bytes)"
        )
    else:
        print(f"[WARN] File not found: {file_to_modify.name}")
        return False

    # Sync
    sync_pair = f"{local_dir}:localToCloud:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify 1 file was uploaded (the modified file)
    if stats["uploaded"] != 1:
        print(f"[FAIL] Expected 1 upload (modified file), got {stats['uploaded']}")
        return False

    print("[PASS] Modify file: modified file uploaded successfully")
    return True


def main() -> None:
    """Main benchmark function for LOCAL_TO_CLOUD sync mode."""
    print("\n" + "=" * 80)
    print("PYDRIME BENCHMARK: LOCAL_TO_CLOUD SYNC MODE")
    print("=" * 80)
    print("\nMode description: Mirror every action done locally to the cloud")
    print("but never act on cloud changes.")
    print("Renaming, deleting & moving is only transferred to the cloud.")

    # Generate unique folder name
    test_uuid = str(uuid.uuid4())
    remote_folder = f"benchmark_ltc_{test_uuid[:8]}"
    base_dir = Path.cwd() / f"benchmark_temp_ltc_{test_uuid[:8]}"
    local_dir = base_dir / "sync_dir"

    print(f"\nRemote folder: /{remote_folder}")
    print(f"Local directory: {local_dir}")

    # Create client for verification
    client = DrimeClient()

    # Get workspace ID from config (same as sync command uses)
    workspace_id = config.get_default_workspace() or 0
    print(f"Workspace ID: {workspace_id}")

    try:
        # Test 1: Initial state
        if not test_initial_state(local_dir, remote_folder, client, workspace_id):
            print("\n[FAIL] TEST 1 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)  # Wait for cloud to process

        # Test 2: Add file
        if not test_add_file(local_dir, remote_folder):
            print("\n[FAIL] TEST 2 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)

        # Test 3: Delete file
        if not test_delete_file(local_dir, remote_folder):
            print("\n[FAIL] TEST 3 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)

        # Test 4: Modify file
        if not test_modify_file(local_dir, remote_folder):
            print("\n[FAIL] TEST 4 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        # All tests passed
        print("\n" + "=" * 80)
        print("ALL TESTS PASSED FOR LOCAL_TO_CLOUD MODE")
        print("=" * 80)
        print("\nSummary:")
        print("  + Initial state: Local files uploaded to cloud")
        print("  + Add file: New file uploaded to cloud")
        print("  + Delete file: Deleted file removed from cloud")
        print("  + Modify file: Modified file re-uploaded to cloud")

        # Cleanup
        cleanup_remote_folder(remote_folder)

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
