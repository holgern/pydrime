"""
Benchmark script to validate sync behavior for different modes.

This script tests:
1. cloudUpload mode (localToCloud):
   - Creates 10 small files (1KB each) with random content
   - Uploads them to a unique UUID-named folder
   - Attempts upload again to verify nothing is uploaded (idempotency)

2. cloudDownload mode (cloudToLocal):
   - Creates a second local directory
   - Downloads files from the cloud folder
   - Attempts download again to verify nothing is downloaded (idempotency)

All operations use the pydrime CLI sync command via subprocess with streaming output.
"""

import os
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
        sync_pair: Sync pair string (e.g., "/local:localToCloud:/remote")
        workers: Number of parallel workers (default: 4)
        batch_size: Number of files to process per batch (default: 10)
        start_delay: Delay in seconds before starting the command (default: 0.0)

    Returns:
        Tuple of (exit_code, captured_output)
    """
    print(
        f"\n>>> Running: pydrime sync {sync_pair} "
        f"--workers {workers} --batch-size {batch_size}"
    )
    print("=" * 80)
    sys.stdout.flush()

    # Run with unbuffered output for real-time streaming
    cmd = [
        "pydrime",
        # "--verbose",
        "sync",
        sync_pair,
        "--workers",
        str(workers),
        "--batch-size",
        str(batch_size),
        "--start-delay",
        str(start_delay),
    ]

    # Capture output while also streaming to console
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # Line buffered
        universal_newlines=True,
    )

    output_lines = []

    # Stream output in real-time
    if process.stdout:
        for line in process.stdout:
            print(line, end="")
            sys.stdout.flush()
            output_lines.append(line)

    # Wait for completion
    exit_code = process.wait()

    captured_output = "".join(output_lines)
    return exit_code, captured_output


def create_test_files(directory: Path, count: int = 10, size_kb: int = 1) -> list[Path]:
    """Create test files with random content.

    Args:
        directory: Directory to create files in
        count: Number of files to create
        size_kb: Size of each file in KB

    Returns:
        List of created file paths
    """
    directory.mkdir(parents=True, exist_ok=True)
    created_files = []

    print(f"\n[INFO] Creating {count} test files ({size_kb}KB each) in {directory}")

    for i in range(count):
        file_path = directory / f"test_file_{i:03d}.txt"
        # Create random content
        content = f"Test file {i}\n" + (os.urandom(size_kb * 1024 - 20).hex())
        file_path.write_text(content)
        created_files.append(file_path)
        print(f"  [OK] Created: {file_path.name}")

    return created_files


def parse_sync_output(stdout: str) -> dict[str, int]:
    """Parse sync command output to extract statistics.

    Expected output contains lines like:
        Total actions: X
          Uploaded: X
          Downloaded: X
          Deleted locally: X
          Deleted remotely: X
    """
    stats = {
        "uploaded": 0,
        "downloaded": 0,
        "deleted": 0,
        "skipped": 0,
        "conflicts": 0,
    }

    for line in stdout.split("\n"):
        line = line.strip()

        # Match "Uploaded: X" format
        if "Uploaded:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["uploaded"] = int(parts[1].strip())
                except ValueError:
                    pass

        # Match "Downloaded: X" format
        elif "Downloaded:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["downloaded"] = int(parts[1].strip())
                except ValueError:
                    pass

        # Match "Deleted locally:" or "Deleted remotely:" format
        elif "Deleted" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["deleted"] += int(parts[1].strip())
                except ValueError:
                    pass

    return stats


def test_cloud_upload(base_dir: Path, remote_folder: str) -> bool:
    """Test cloudUpload (localToCloud) sync mode.

    Args:
        base_dir: Base directory for test files
        remote_folder: Remote folder name (UUID)

    Returns:
        True if test passed, False otherwise
    """
    print("\n" + "=" * 80)
    print("TEST 1: CLOUD UPLOAD (localToCloud) MODE")
    print("=" * 80)

    # Create local directory
    local_dir = base_dir / "upload_test"
    local_dir.mkdir(parents=True, exist_ok=True)

    # Create test files - 15 files to test batching (batch_size=10)
    create_test_files(local_dir, count=15, size_kb=1)

    # First sync - should upload all files
    # Use workers=4 for parallel uploads with staggered starts
    print("\n[SYNC] First sync (should upload 15 files)...")
    sync_pair = f"{local_dir}:localToCloud:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair, workers=1, batch_size=10)

    if exit_code != 0:
        print(f"[FAIL] First sync failed with exit code {exit_code}")
        return False

    stats1 = parse_sync_output(stdout)
    print(f"\n[STATS] First sync stats: {stats1}")

    # Verify 15 files were uploaded
    if stats1["uploaded"] != 15:
        print(f"[FAIL] Expected 15 uploads, got {stats1['uploaded']}")
        return False

    print("[PASS] First sync uploaded 15 files as expected")

    # Wait a bit to ensure cloud sync completes
    time.sleep(2)

    # Second sync - should upload nothing (idempotency)
    print("\n[SYNC] Second sync (should upload 0 files - idempotency test)...")
    exit_code, stdout = run_sync_command(sync_pair, workers=1, batch_size=10)

    if exit_code != 0:
        print(f"[FAIL] Second sync failed with exit code {exit_code}")
        return False

    stats2 = parse_sync_output(stdout)
    print(f"\n[STATS] Second sync stats: {stats2}")

    # Verify nothing was uploaded
    if stats2["uploaded"] != 0:
        print(f"[FAIL] Expected 0 uploads (idempotency), got {stats2['uploaded']}")
        return False

    print("[PASS] Second sync uploaded 0 files - idempotency confirmed")

    return True


def test_cloud_download(base_dir: Path, remote_folder: str) -> bool:
    """Test cloudDownload (cloudToLocal) sync mode.

    Args:
        base_dir: Base directory for test files
        remote_folder: Remote folder name (UUID)

    Returns:
        True if test passed, False otherwise
    """
    print("\n" + "=" * 80)
    print("TEST 2: CLOUD DOWNLOAD (cloudToLocal) MODE")
    print("=" * 80)

    # Create second local directory
    local_dir = base_dir / "download_test"
    local_dir.mkdir(parents=True, exist_ok=True)

    # First sync - should download all files (15 files)
    # Use workers=1 initially to avoid parallel download issues
    print("\n[SYNC] First download sync (should download 15 files)...")
    sync_pair = f"{local_dir}:cloudToLocal:/{remote_folder}"
    exit_code, stdout = run_sync_command(sync_pair, workers=1, batch_size=10)

    if exit_code != 0:
        print(f"[FAIL] First download sync failed with exit code {exit_code}")
        return False

    stats1 = parse_sync_output(stdout)
    print(f"\n[STATS] First download sync stats: {stats1}")

    # Verify files were downloaded
    # With sequential uploads (workers=1), all files should be available
    # Expect 100% download success
    expected = 15
    if stats1["downloaded"] != expected:
        print(f"[FAIL] Expected {expected} downloads, got {stats1['downloaded']}")
        return False

    # Verify files exist locally
    local_files = list(local_dir.glob("test_file_*.txt"))
    if len(local_files) != expected:
        print(f"[FAIL] Expected {expected} local files, found {len(local_files)}")
        return False

    print(f"[PASS] First download sync downloaded {stats1['downloaded']} files")

    # Wait a bit
    time.sleep(2)

    # Second sync - should download nothing (idempotency)
    print(
        "\n[SYNC] Second download sync (should download 0 files - idempotency test)..."
    )
    exit_code, stdout = run_sync_command(sync_pair, workers=1, batch_size=10)

    if exit_code != 0:
        print(f"[FAIL] Second download sync failed with exit code {exit_code}")
        return False

    stats2 = parse_sync_output(stdout)
    print(f"\n[STATS] Second download sync stats: {stats2}")

    # Idempotency check: second sync should download 0 files
    if stats2["downloaded"] != 0:
        print(f"[FAIL] Expected 0 downloads (idempotency), got {stats2['downloaded']}")
        return False

    print("[PASS] Second download sync uploaded 0 files - idempotency confirmed")

    return True


def cleanup_test_folder(remote_folder: str, base_dir: Path) -> None:
    """Clean up the test folder from cloud and local.

    Args:
        remote_folder: Remote folder name to delete (without leading slash)
        base_dir: Local base directory to delete
    """
    import shutil
    import subprocess

    print("\n[CLEANUP] Cleaning up test data...")

    # Delete local directory
    if base_dir.exists():
        try:
            shutil.rmtree(base_dir)
            print(f"   [OK] Deleted local directory: {base_dir}")
        except Exception as e:
            print(f"   [WARN] Failed to delete local directory: {e}")

    # Delete remote folder permanently using --yes to skip confirmation
    # Use folder name without leading slash
    try:
        cmd = ["pydrime", "--verbose", "rm", remote_folder, "--no-trash", "--yes"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   [OK] Deleted remote folder: {remote_folder}")
        else:
            print(f"   [WARN] Failed to delete remote folder: {result.stderr.strip()}")
    except Exception as e:
        print(f"   [WARN] Failed to delete remote folder: {e}")


def main():
    """Main benchmark function."""
    print("\n" + "=" * 80)
    print("PYDRIME SYNC MODE BENCHMARKS")
    print("=" * 80)

    # Generate unique folder name for both local and remote
    test_uuid = str(uuid.uuid4())
    remote_folder = f"benchmark_{test_uuid}"

    print(f"\n[INFO] Test folder: {remote_folder}")
    print("   This folder will be created in the cloud workspace root")

    # Create unique base directory in current workspace
    base_dir = Path.cwd() / f"benchmark_temp_{test_uuid[:8]}"
    base_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[INFO] Local test directory: {base_dir}")

    try:
        # Test 1: Cloud Upload
        test1_passed = test_cloud_upload(base_dir, remote_folder)

        if not test1_passed:
            print("\n[FAIL] TEST 1 FAILED - Stopping benchmarks")
            cleanup_test_folder(remote_folder, base_dir)
            sys.exit(1)

        # Wait for API to process uploaded files before downloading
        # With sequential uploads, files should be ready sooner
        print("\n[WAIT] Waiting 10 seconds for API to process uploaded files...")
        time.sleep(10)

        # Test 2: Cloud Download
        test2_passed = test_cloud_download(base_dir, remote_folder)

        if not test2_passed:
            print("\n[FAIL] TEST 2 FAILED")
            cleanup_test_folder(remote_folder, base_dir)
            sys.exit(1)

        # All tests passed
        print("\n" + "=" * 80)
        print("[PASS] ALL TESTS PASSED")
        print("=" * 80)
        print("\n[SUMMARY]:")
        print("   [OK] Cloud upload (localToCloud) mode works correctly")
        print("   [OK] Upload idempotency confirmed (no duplicate uploads)")
        print("   [OK] Cloud download (cloudToLocal) mode works correctly")
        print("   [OK] Download idempotency confirmed (no duplicate downloads)")
        print("\n[INFO] Test data:")
        print(f"   Local: {base_dir}")
        print(f"   Remote: /{remote_folder}")

        # Cleanup
        cleanup_test_folder(remote_folder, base_dir)

    except KeyboardInterrupt:
        print("\n\n[WARN] Benchmark interrupted by user")
        if "base_dir" in locals() and "remote_folder" in locals():
            cleanup_test_folder(remote_folder, base_dir)
        sys.exit(130)
    except Exception as e:
        print(f"\n\n[ERROR] Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        if "base_dir" in locals() and "remote_folder" in locals():
            cleanup_test_folder(remote_folder, base_dir)
        sys.exit(1)


if __name__ == "__main__":
    main()
