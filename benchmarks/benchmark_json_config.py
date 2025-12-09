"""
Benchmark script for sync command using JSON config file.

This script tests the --config option of the pydrime sync command which allows
specifying multiple sync pairs in a JSON file. Each sync pair is processed
sequentially by the CLI.

Tests:
1. Create multiple local directories with test files
2. Generate a JSON config file with multiple sync pairs (different modes)
3. Run sync using --config option to sync all pairs at once
4. Verify all files are synced correctly
5. Test idempotency (second sync should do nothing)

JSON Config Format:
[
  {
    "workspace": 0,              // optional, default: 0
    "local": "/path/to/local",   // required
    "remote": "remote/path",     // required
    "syncMode": "twoWay",        // required
    "disableLocalTrash": false,  // optional, default: false
    "ignore": ["*.tmp"],         // optional, default: []
    "excludeDotFiles": false     // optional, default: false
  }
]
"""

import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any


def run_sync_with_config(
    config_file: Path,
    workers: int = 1,
    batch_size: int = 10,
    dry_run: bool = False,
) -> tuple[int, str]:
    """Run a pydrime sync command with JSON config file.

    Args:
        config_file: Path to JSON config file
        workers: Number of parallel workers
        batch_size: Number of files to process per batch
        dry_run: If True, run in dry-run mode

    Returns:
        Tuple of (exit_code, captured_output)
    """
    cmd = [
        "pydrime",
        "sync",
        "--config",
        str(config_file),
        "--workers",
        str(workers),
        "--batch-size",
        str(batch_size),
    ]

    if dry_run:
        cmd.append("--dry-run")

    print(f"\n>>> Running: {' '.join(cmd)}")
    print("-" * 80)
    sys.stdout.flush()

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
    """Parse sync command output to extract statistics.

    This aggregates stats from multiple sync pairs in a single run.
    """
    stats = {
        "uploaded": 0,
        "downloaded": 0,
        "deleted_local": 0,
        "deleted_remote": 0,
        "pairs_processed": 0,
    }

    for line in stdout.split("\n"):
        line = line.strip()
        if "Uploaded:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["uploaded"] += int(parts[1].strip())
                except ValueError:
                    pass
        elif "Downloaded:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["downloaded"] += int(parts[1].strip())
                except ValueError:
                    pass
        elif "Deleted locally:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["deleted_local"] += int(parts[1].strip())
                except ValueError:
                    pass
        elif "Deleted remotely:" in line:
            parts = line.split(":")
            if len(parts) >= 2:
                try:
                    stats["deleted_remote"] += int(parts[1].strip())
                except ValueError:
                    pass
        elif "Sync Pair" in line and "/" in line:
            # Count "Sync Pair X/Y" lines
            stats["pairs_processed"] += 1

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


def create_json_config(config_path: Path, sync_pairs: list[dict[str, Any]]) -> None:
    """Create a JSON config file with sync pairs.

    Args:
        config_path: Path to write the JSON config file
        sync_pairs: List of sync pair dictionaries
    """
    print(f"\n[CONFIG] Creating JSON config file: {config_path}")
    with open(config_path, "w") as f:
        json.dump(sync_pairs, f, indent=2)
    print(f"  + Written {len(sync_pairs)} sync pair(s)")


def cleanup_remote_folder(remote_folder: str) -> None:
    """Delete remote folder permanently."""
    print(f"\n[CLEANUP] Deleting remote folder: {remote_folder}")
    try:
        cmd = ["pydrime", "rm", remote_folder, "--no-trash", "--yes"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  + Deleted remote folder: {remote_folder}")
        else:
            # Silently ignore if folder doesn't exist
            if "not found" not in result.stderr.lower():
                print(f"  ! Failed to delete remote folder: {result.stderr.strip()}")
    except Exception as e:
        print(f"  ! Error deleting remote folder: {e}")


def test_multi_pair_sync(
    base_dir: Path,
    remote_folders: list[str],
    config_path: Path,
) -> bool:
    """Test syncing multiple pairs using JSON config.

    Args:
        base_dir: Base directory for local test directories
        remote_folders: List of remote folder names
        config_path: Path to JSON config file

    Returns:
        True if test passed, False otherwise
    """
    print("\n" + "=" * 80)
    print("TEST 1: MULTI-PAIR SYNC - Using JSON config file")
    print("=" * 80)

    # Create local directories and test files
    local_dirs = []
    sync_pairs = []
    total_files = 0

    for i, remote_folder in enumerate(remote_folders):
        local_dir = base_dir / f"sync_pair_{i}"
        local_dirs.append(local_dir)

        # Create different number of files for each pair
        file_count = 3 + i  # 3, 4, 5 files
        total_files += file_count
        create_test_files(local_dir, count=file_count, size_kb=1)

        # Alternate sync modes for variety
        modes = ["sourceToDestination", "sourceBackup", "twoWay"]
        mode = modes[i % len(modes)]

        sync_pairs.append(
            {
                "workspace": 0,
                "local": str(local_dir),
                "remote": f"/{remote_folder}",
                "syncMode": mode,
                "disableLocalTrash": False,
                "ignore": [],
                "excludeDotFiles": False,
            }
        )

        print(f"\n  Pair {i + 1}: {local_dir.name} -> /{remote_folder} ({mode})")

    # Create JSON config file
    create_json_config(config_path, sync_pairs)

    # First sync - should upload all files
    print(f"\n[SYNC] First sync (should upload {total_files} files)...")
    exit_code, stdout = run_sync_with_config(config_path, workers=1, batch_size=10)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Verify all files were uploaded
    if stats["uploaded"] != total_files:
        print(f"[FAIL] Expected {total_files} uploads, got {stats['uploaded']}")
        return False

    # Verify all pairs were processed
    expected_pairs = len(sync_pairs)
    if stats["pairs_processed"] != expected_pairs:
        print(
            f"[FAIL] Expected {expected_pairs} pairs processed, "
            f"got {stats['pairs_processed']}"
        )
        return False

    print(
        f"[PASS] First sync uploaded {total_files} files across {expected_pairs} pairs"
    )

    # Wait for API to process
    time.sleep(2)

    # Second sync - should do nothing (idempotency)
    print("\n[SYNC] Second sync (should upload 0 files - idempotency test)...")
    exit_code, stdout = run_sync_with_config(config_path, workers=1, batch_size=10)

    if exit_code != 0:
        print(f"[FAIL] Second sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    if stats["uploaded"] != 0:
        print(f"[FAIL] Expected 0 uploads (idempotency), got {stats['uploaded']}")
        return False

    print("[PASS] Idempotency confirmed - no duplicate uploads")

    return True


def test_dry_run(
    base_dir: Path,
    remote_folders: list[str],
    config_path: Path,
) -> bool:
    """Test dry-run mode with JSON config.

    Args:
        base_dir: Base directory for local test directories
        remote_folders: List of remote folder names
        config_path: Path to JSON config file

    Returns:
        True if test passed, False otherwise
    """
    print("\n" + "=" * 80)
    print("TEST 2: DRY-RUN MODE - Using JSON config file")
    print("=" * 80)

    # Add a new file to one of the sync directories
    new_file = base_dir / "sync_pair_0" / "new_dry_run_file.txt"
    new_file.write_text("This file should NOT be uploaded in dry-run mode\n")
    print(f"\n[ADD] Created new file: {new_file.name}")

    # Run sync in dry-run mode
    print("\n[SYNC] Running sync in dry-run mode...")
    exit_code, stdout = run_sync_with_config(
        config_path, workers=1, batch_size=10, dry_run=True
    )

    if exit_code != 0:
        print(f"[FAIL] Dry-run sync failed with exit code {exit_code}")
        return False

    # Verify dry-run mode was indicated
    if "dry" not in stdout.lower():
        print("[WARN] Dry-run mode not explicitly indicated in output")

    # Now run actual sync to confirm file would be uploaded
    print("\n[SYNC] Running actual sync to verify file uploads...")
    exit_code, stdout = run_sync_with_config(config_path, workers=1, batch_size=10)

    if exit_code != 0:
        print(f"[FAIL] Actual sync failed with exit code {exit_code}")
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Should have uploaded the new file
    if stats["uploaded"] != 1:
        print(f"[FAIL] Expected 1 upload (new file), got {stats['uploaded']}")
        return False

    print("[PASS] Dry-run mode works correctly")
    return True


def test_ignore_patterns(
    base_dir: Path,
    test_uuid: str,
) -> bool:
    """Test ignore patterns in JSON config.

    Args:
        base_dir: Base directory for local test directories
        test_uuid: UUID for unique folder names

    Returns:
        True if test passed, False otherwise
    """
    print("\n" + "=" * 80)
    print("TEST 3: IGNORE PATTERNS - Using JSON config file")
    print("=" * 80)

    # Create a new directory with files to ignore
    local_dir = base_dir / "sync_pair_ignore"
    local_dir.mkdir(parents=True, exist_ok=True)
    remote_folder = f"benchmark_json_ignore_{test_uuid[:8]}"

    # Create regular files
    (local_dir / "keep_me.txt").write_text("This file should be uploaded\n")
    (local_dir / "another.txt").write_text("This file should also be uploaded\n")

    # Create files that should be ignored
    (local_dir / "ignore_me.tmp").write_text("This should be ignored\n")
    (local_dir / "test.log").write_text("This should be ignored\n")

    print("\n[CREATE] Created 4 files (2 should be ignored)")

    # Create JSON config with ignore patterns
    config_path = base_dir / "ignore_config.json"
    sync_pairs = [
        {
            "workspace": 0,
            "local": str(local_dir),
            "remote": f"/{remote_folder}",
            "syncMode": "sourceToDestination",
            "ignore": ["*.tmp", "*.log"],
            "excludeDotFiles": False,
        }
    ]
    create_json_config(config_path, sync_pairs)

    # Run sync
    print("\n[SYNC] Running sync (should upload 2 files, ignore 2)...")
    exit_code, stdout = run_sync_with_config(config_path, workers=1, batch_size=10)

    if exit_code != 0:
        print(f"[FAIL] Sync failed with exit code {exit_code}")
        cleanup_remote_folder(remote_folder)
        return False

    stats = parse_sync_output(stdout)
    print(f"\n[STATS] {stats}")

    # Should only upload 2 files (ignore *.tmp and *.log)
    if stats["uploaded"] != 2:
        print(f"[FAIL] Expected 2 uploads (ignore patterns), got {stats['uploaded']}")
        cleanup_remote_folder(remote_folder)
        return False

    print("[PASS] Ignore patterns work correctly")

    # Cleanup this test's remote folder
    cleanup_remote_folder(remote_folder)

    return True


def main() -> None:
    """Main benchmark function for JSON config sync."""
    print("\n" + "=" * 80)
    print("PYDRIME BENCHMARK: SYNC WITH JSON CONFIG FILE")
    print("=" * 80)
    print("\nThis benchmark tests the --config option for syncing multiple")
    print("folder pairs defined in a JSON configuration file.")

    # Generate unique identifiers
    test_uuid = str(uuid.uuid4())
    base_dir = Path.cwd() / f"benchmark_temp_json_{test_uuid[:8]}"
    config_path = base_dir / "sync_config.json"

    # Create 3 remote folders for multi-pair test
    remote_folders = [
        f"benchmark_json_{test_uuid[:8]}_pair0",
        f"benchmark_json_{test_uuid[:8]}_pair1",
        f"benchmark_json_{test_uuid[:8]}_pair2",
    ]

    print(f"\nLocal base directory: {base_dir}")
    print(f"Remote folders: {remote_folders}")

    try:
        base_dir.mkdir(parents=True, exist_ok=True)

        # Test 1: Multi-pair sync
        if not test_multi_pair_sync(base_dir, remote_folders, config_path):
            print("\n[FAIL] TEST 1 FAILED")
            for rf in remote_folders:
                cleanup_remote_folder(rf)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)

        # Test 2: Dry-run mode
        if not test_dry_run(base_dir, remote_folders, config_path):
            print("\n[FAIL] TEST 2 FAILED")
            for rf in remote_folders:
                cleanup_remote_folder(rf)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        time.sleep(3)

        # Test 3: Ignore patterns
        if not test_ignore_patterns(base_dir, test_uuid):
            print("\n[FAIL] TEST 3 FAILED")
            for rf in remote_folders:
                cleanup_remote_folder(rf)
            if base_dir.exists():
                shutil.rmtree(base_dir)
            sys.exit(1)

        # All tests passed
        print("\n" + "=" * 80)
        print("ALL TESTS PASSED FOR JSON CONFIG SYNC")
        print("=" * 80)
        print("\nSummary:")
        print("  + Multi-pair sync: Multiple sync pairs processed correctly")
        print("  + Idempotency: No duplicate uploads on second sync")
        print("  + Dry-run mode: Works correctly with JSON config")
        print("  + Ignore patterns: Files matching patterns are ignored")

        # Cleanup
        for rf in remote_folders:
            cleanup_remote_folder(rf)

    except KeyboardInterrupt:
        print("\n\n[ABORT] Benchmark interrupted by user")
        for rf in remote_folders:
            cleanup_remote_folder(rf)
        sys.exit(130)
    except Exception as e:
        print(f"\n\n[ERROR] Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        for rf in remote_folders:
            cleanup_remote_folder(rf)
        sys.exit(1)
    finally:
        if base_dir.exists():
            print(f"\n[CLEANUP] Removing local directory: {base_dir}")
            shutil.rmtree(base_dir)


if __name__ == "__main__":
    main()
