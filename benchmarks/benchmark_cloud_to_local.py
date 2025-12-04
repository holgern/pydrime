"""
Benchmark script for CLOUD_TO_LOCAL sync mode.

CLOUD_TO_LOCAL mode: Mirror every action done in the cloud locally
but never act on local changes.
Renaming, deleting & moving is only transferred to the local side.

Tests:
1. Initial state: Create files locally, upload to cloud, move local files away,
   then sync to download from cloud
2. Add file: Add new file to cloud (via upload), verify it downloads locally
3. Delete file: Delete file from cloud, verify it's deleted locally
4. Modify file: Modify file in cloud, verify local is updated

All operations use the pydrime CLI sync command via subprocess.
"""

import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path


def run_sync_command(
    sync_pair: str, workers: int = 4, batch_size: int = 10, start_delay: float = 0.0
) -> tuple[int, str]:
    """Run a pydrime sync command with streaming output.

    Args:
        sync_pair: Sync pair string (e.g., "/local:cloudToLocal:/remote")
        workers: Number of parallel workers
        batch_size: Number of files to process per batch
        start_delay: Delay in seconds before starting the sync

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
        "--start-delay",
        str(start_delay),
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


def run_upload_command(source_path: Path, destination_path: str) -> tuple[int, str]:
    """Run a pydrime upload command.

    Args:
        source_path: Local file or directory path
        destination_path: Remote destination path

    Returns:
        Tuple of (exit_code, captured_output)
    """
    print(f"\n>>> Running: pydrime upload {source_path} -r {destination_path}")
    print("-" * 80)
    sys.stdout.flush()

    cmd = [
        "pydrime",
        "upload",
        str(source_path),
        "-r",
        destination_path,
        "--on-duplicate",
        "replace",
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


def run_rm_command(destination_path: str) -> tuple[int, str]:
    """Run a pydrime rm command.

    Args:
        destination_path: Remote path to delete

    Returns:
        Tuple of (exit_code, captured_output)
    """
    print(f"\n>>> Running: pydrime rm {destination_path} --no-trash --yes")
    print("-" * 80)
    sys.stdout.flush()

    cmd = ["pydrime", "rm", destination_path, "--no-trash", "--yes"]

    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result.returncode, result.stdout


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


def test_initial_state(local_dir: Path, staging_dir: Path, remote_folder: str) -> bool:
    """Test 1: Initial state - upload files to cloud, then download to local.

    CLOUD_TO_LOCAL should download all cloud files to local.
    """
    print("\n" + "=" * 80)
    print("TEST 1: INITIAL STATE - Setup cloud files and download to local")
    print("=" * 80)

    # Create test files in staging directory and upload to cloud
    print("\n[SETUP] Creating files in staging directory and uploading to cloud...")
    created_files = create_test_files(staging_dir, count=5, size_kb=1)

    # Upload each file directly to the remote folder (not the staging dir)
    # This avoids creating a "staging" subfolder in the remote
    # The remote path must include the filename for single file uploads
    for file_path in created_files:
        destination_path = f"/{remote_folder}/{file_path.name}"
        exit_code, _ = run_upload_command(file_path, destination_path)
        if exit_code != 0:
            print(
                f"[FAIL] Upload failed for {file_path.name} with exit code {exit_code}"
            )
            return False

    print("[INFO] Files uploaded to cloud successfully")

    # Wait for cloud to process
    time.sleep(5)

    # Now sync from cloud to local (empty local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    sync_pair = f"{local_dir}:cloudToLocal:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify 5 files were downloaded
    if stats["downloaded"] != 5:
        print(f"[FAIL] Expected 5 downloads, got {stats['downloaded']}")
        return False

    # Verify no uploads (CLOUD_TO_LOCAL never uploads)
    if stats["uploaded"] != 0:
        print(f"[FAIL] Expected 0 uploads, got {stats['uploaded']}")
        return False

    # Verify files exist locally
    local_files = list(local_dir.glob("test_file_*.txt"))
    if len(local_files) != 5:
        print(f"[FAIL] Expected 5 local files, found {len(local_files)}")
        return False

    print("[PASS] Initial state: 5 files downloaded successfully")

    # Verify idempotency
    time.sleep(2)
    print("\n[CHECK] Verifying idempotency (second sync should do nothing)...")
    exit_code, stdout = run_sync_command(sync_pair)
    stats = parse_sync_output(stdout)

    if stats["downloaded"] != 0:
        print(f"[FAIL] Expected 0 downloads on second sync, got {stats['downloaded']}")
        return False

    print("[PASS] Idempotency confirmed")
    return True


def test_add_file(local_dir: Path, staging_dir: Path, remote_folder: str) -> bool:
    """Test 2: Add file - add new file to cloud, verify it downloads locally.

    CLOUD_TO_LOCAL should download the new file.
    """
    print("\n" + "=" * 80)
    print("TEST 2: ADD FILE - Add new file to cloud")
    print("=" * 80)

    # Create a new file in staging and upload to cloud
    new_file = staging_dir / "new_cloud_file.txt"
    new_file.write_text(
        "This is a new file added to cloud for testing\n" + os.urandom(512).hex()
    )
    print(f"\n[ADD] Created new file in staging: {new_file.name}")

    # Upload the new file to cloud (include filename in remote path)
    destination_path = f"/{remote_folder}/{new_file.name}"
    exit_code, _ = run_upload_command(new_file, destination_path)
    if exit_code != 0:
        print(f"[FAIL] Upload failed with exit code {exit_code}")
        return False

    # Wait for cloud to process
    time.sleep(3)

    # Sync
    sync_pair = f"{local_dir}:cloudToLocal:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify 1 file was downloaded
    if stats["downloaded"] != 1:
        print(f"[FAIL] Expected 1 download, got {stats['downloaded']}")
        return False

    # Verify file exists locally
    local_new_file = local_dir / "new_cloud_file.txt"
    if not local_new_file.exists():
        print(f"[FAIL] New file not found locally: {local_new_file}")
        return False

    print("[PASS] Add file: new file downloaded successfully")
    return True


def test_delete_file(local_dir: Path, remote_folder: str) -> bool:
    """Test 3: Delete file - delete file from cloud, verify it's deleted locally.

    CLOUD_TO_LOCAL should delete the local file (allows_local_delete=True).
    """
    print("\n" + "=" * 80)
    print("TEST 3: DELETE FILE - Delete file from cloud")
    print("=" * 80)

    # Delete a file from cloud
    file_to_delete = f"{remote_folder}/test_file_000.txt"
    print(f"\n[DELETE] Deleting cloud file: {file_to_delete}")
    exit_code, _ = run_rm_command(file_to_delete)

    if exit_code != 0:
        print(f"[WARN] rm command returned {exit_code}, continuing anyway...")

    # Wait for cloud to process
    time.sleep(3)

    # Sync
    sync_pair = f"{local_dir}:cloudToLocal:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify 1 file was deleted locally (CLOUD_TO_LOCAL allows local delete)
    if stats["deleted_local"] != 1:
        print(f"[FAIL] Expected 1 local deletion, got {stats['deleted_local']}")
        return False

    # Verify file no longer exists locally
    source_file = local_dir / "test_file_000.txt"
    if source_file.exists():
        print(f"[FAIL] File should be deleted locally: {source_file}")
        return False

    print("[PASS] Delete file: local file deleted successfully")
    return True


def test_modify_file(local_dir: Path, staging_dir: Path, remote_folder: str) -> bool:
    """Test 4: Modify file - modify file in cloud, verify local is updated.

    CLOUD_TO_LOCAL should download the modified file.
    """
    print("\n" + "=" * 80)
    print("TEST 4: MODIFY FILE - Modify file in cloud")
    print("=" * 80)

    # Modify a file in staging and re-upload to cloud
    file_to_modify = staging_dir / "test_file_001.txt"
    if file_to_modify.exists():
        original_size = file_to_modify.stat().st_size
        # Add more content to change the size
        new_content = (
            file_to_modify.read_text()
            + "\n\nModified in cloud\n"
            + os.urandom(1024).hex()
        )
        file_to_modify.write_text(new_content)
        new_size = file_to_modify.stat().st_size
        print(
            f"\n[MODIFY] Modified staging file: {file_to_modify.name} "
            f"({original_size} -> {new_size} bytes)"
        )
    else:
        print(f"[WARN] File not found in staging: {file_to_modify.name}")
        return False

    # Upload the modified file to cloud (replace existing, include filename in path)
    destination_path = f"/{remote_folder}/{file_to_modify.name}"
    exit_code, _ = run_upload_command(file_to_modify, destination_path)
    if exit_code != 0:
        print(f"[FAIL] Upload failed with exit code {exit_code}")
        return False

    # Wait for cloud to process
    time.sleep(3)

    # Sync
    sync_pair = f"{local_dir}:cloudToLocal:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify 1 file was downloaded (the modified file)
    if stats["downloaded"] != 1:
        print(f"[FAIL] Expected 1 download (modified file), got {stats['downloaded']}")
        return False

    print("[PASS] Modify file: modified file downloaded successfully")
    return True


def main() -> None:
    """Main benchmark function for CLOUD_TO_LOCAL sync mode."""
    print("\n" + "=" * 80)
    print("PYDRIME BENCHMARK: CLOUD_TO_LOCAL SYNC MODE")
    print("=" * 80)
    print("\nMode description: Mirror every action done in the cloud locally")
    print("but never act on local changes.")
    print("Renaming, deleting & moving is only transferred to the local side.")

    # Generate unique folder name
    test_uuid = str(uuid.uuid4())
    remote_folder = f"benchmark_ctl_{test_uuid[:8]}"
    base_dir = Path.cwd() / f"benchmark_temp_ctl_{test_uuid[:8]}"
    local_dir = base_dir / "sync_dir"
    staging_dir = base_dir / "staging"

    print(f"\nRemote folder: /{remote_folder}")
    print(f"Local directory: {local_dir}")
    print(f"Staging directory: {staging_dir}")

    try:
        # Test 1: Initial state
        if not test_initial_state(local_dir, staging_dir, remote_folder):
            print("\n[FAIL] TEST 1 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)  # Wait for cloud to process

        # Test 2: Add file
        if not test_add_file(local_dir, staging_dir, remote_folder):
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
        if not test_modify_file(local_dir, staging_dir, remote_folder):
            print("\n[FAIL] TEST 4 FAILED")
            cleanup_remote_folder(remote_folder)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        # All tests passed
        print("\n" + "=" * 80)
        print("ALL TESTS PASSED FOR CLOUD_TO_LOCAL MODE")
        print("=" * 80)
        print("\nSummary:")
        print("  + Initial state: Cloud files downloaded to local")
        print("  + Add file: New cloud file downloaded to local")
        print("  + Delete file: Deleted cloud file removed locally")
        print("  + Modify file: Modified cloud file re-downloaded to local")

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
