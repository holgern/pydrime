"""
Benchmark script to test timestamp preservation during upload and download.

This script tests:
1. Creates test files with specific creation and modification timestamps
2. Uploads files to the cloud using the CLI
3. Downloads files to a different location
4. Verifies that timestamps are preserved correctly

Usage:
    python benchmarks/benchmark_timestamp_preservation.py
"""

import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


def print_separator(title: str, char: str = "=") -> None:
    """Print a separator line with title."""
    print(f"\n{char * 80}")
    print(f" {title}")
    print(f"{char * 80}")


def print_info(msg: str) -> None:
    """Print an info message."""
    print(f"[INFO] {msg}")


def print_success(msg: str) -> None:
    """Print a success message."""
    print(f"[PASS] {msg}")


def print_error(msg: str) -> None:
    """Print an error message."""
    print(f"[FAIL] {msg}")


def print_warning(msg: str) -> None:
    """Print a warning message."""
    print(f"[WARN] {msg}")


def set_file_timestamps(
    file_path: Path, mtime: datetime, atime: datetime | None = None
) -> None:
    """Set file modification and access timestamps.

    Args:
        file_path: Path to the file
        mtime: Modification time to set
        atime: Access time to set (defaults to mtime if None)
    """
    if atime is None:
        atime = mtime

    # Convert datetime to timestamp
    mtime_ts = mtime.timestamp()
    atime_ts = atime.timestamp()

    # Set timestamps using os.utime
    os.utime(file_path, (atime_ts, mtime_ts))


def get_file_timestamps(file_path: Path) -> dict[str, datetime]:
    """Get file timestamps.

    Args:
        file_path: Path to the file

    Returns:
        Dictionary with 'mtime', 'atime', and 'ctime' as datetime objects
    """
    stat = file_path.stat()
    return {
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
        "atime": datetime.fromtimestamp(stat.st_atime, tz=timezone.utc),
        "ctime": datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc),
    }


def create_test_file_with_timestamp(
    file_path: Path, content: str, mtime: datetime
) -> None:
    """Create a test file with specific content and timestamp.

    Args:
        file_path: Path where to create the file
        content: Content to write to the file
        mtime: Modification time to set
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)
    set_file_timestamps(file_path, mtime)


def run_sync_command(sync_pair: str, workers: int = 4) -> tuple[int, str]:
    """Run a pydrime sync command with streaming output.

    Args:
        sync_pair: Sync pair string (e.g., "/local:localToCloud:/remote")
        workers: Number of parallel workers (default: 4)

    Returns:
        Tuple of (exit_code, captured_output)
    """
    print_info(f"Running: pydrime sync {sync_pair} --workers {workers}")
    print("-" * 80)
    sys.stdout.flush()

    cmd = [
        "pydrime",
        "sync",
        sync_pair,
        "--workers",
        str(workers),
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


def format_datetime(dt: datetime) -> str:
    """Format datetime for display."""
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def compare_timestamps(
    original: datetime, downloaded: datetime, tolerance_seconds: float = 2.0
) -> tuple[bool, float]:
    """Compare two timestamps with tolerance.

    Args:
        original: Original timestamp
        downloaded: Downloaded file timestamp
        tolerance_seconds: Maximum allowed difference in seconds

    Returns:
        Tuple of (is_match, difference_seconds)
    """
    diff = abs((original - downloaded).total_seconds())
    is_match = diff <= tolerance_seconds
    return is_match, diff


def test_timestamp_preservation(base_dir: Path) -> dict:
    """Test timestamp preservation during upload and download.

    Args:
        base_dir: Base directory for test files

    Returns:
        Dictionary with test results
    """
    print_separator("TIMESTAMP PRESERVATION TEST")

    # Generate unique ID for this test run
    test_id = uuid.uuid4().hex[:8]
    cloud_folder = f"benchmark_timestamps_{test_id}"

    # Create test directories
    upload_dir = base_dir / "upload_test"
    download_dir = base_dir / "download_test"

    # Clean up if they exist
    if upload_dir.exists():
        shutil.rmtree(upload_dir)
    if download_dir.exists():
        shutil.rmtree(download_dir)

    upload_dir.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "files": [],
        "all_passed": True,
        "test_id": test_id,
        "cloud_folder": cloud_folder,
    }

    # Create test files with different timestamps
    print_info("Creating test files with specific timestamps...")

    now = datetime.now(timezone.utc)
    test_cases = [
        {
            "name": "recent_file.txt",
            "content": "This file was modified recently",
            "mtime": now - timedelta(hours=1),
            "description": "Recent file (1 hour ago)",
        },
        {
            "name": "old_file.txt",
            "content": "This file is quite old",
            "mtime": now - timedelta(days=365),
            "description": "Old file (1 year ago)",
        },
        {
            "name": "yesterday_file.txt",
            "content": "Modified yesterday",
            "mtime": now - timedelta(days=1),
            "description": "Yesterday's file",
        },
        {
            "name": "last_week.txt",
            "content": "Modified last week",
            "mtime": now - timedelta(days=7),
            "description": "Last week's file",
        },
        {
            "name": "nested/deep/nested_file.txt",
            "content": "This file is in a nested folder",
            "mtime": now - timedelta(days=30),
            "description": "Nested file (30 days ago)",
        },
    ]

    # Create all test files with specific timestamps
    for test_case in test_cases:
        file_path = upload_dir / test_case["name"]
        create_test_file_with_timestamp(
            file_path, test_case["content"], test_case["mtime"]
        )
        original_ts = get_file_timestamps(file_path)
        test_case["original_path"] = file_path
        test_case["original_timestamps"] = original_ts
        print(f"  ✓ Created: {test_case['name']}")
        print(f"    Original mtime: {format_datetime(original_ts['mtime'])}")

    # Step 1: Upload files to cloud
    print_separator("STEP 1: UPLOAD TO CLOUD", "-")
    sync_pair_upload = f"{upload_dir}:localToCloud:/{cloud_folder}"

    exit_code, output = run_sync_command(sync_pair_upload)

    if exit_code != 0:
        print_error(f"Upload failed with exit code {exit_code}")
        results["upload_failed"] = True
        results["all_passed"] = False
        return results

    print_success("Upload completed successfully")

    # Wait a bit to ensure cloud processing is complete
    print_info("Waiting 3 seconds for cloud processing...")
    time.sleep(3)

    # Step 2: Download files to a different location
    print_separator("STEP 2: DOWNLOAD TO DIFFERENT LOCATION", "-")
    sync_pair_download = f"{download_dir}:cloudToLocal:/{cloud_folder}"

    exit_code, output = run_sync_command(sync_pair_download)

    if exit_code != 0:
        print_error(f"Download failed with exit code {exit_code}")
        results["download_failed"] = True
        results["all_passed"] = False
        return results

    print_success("Download completed successfully")

    # Step 3: Compare timestamps
    print_separator("STEP 3: COMPARE TIMESTAMPS", "-")

    for test_case in test_cases:
        file_result = {
            "name": test_case["name"],
            "description": test_case["description"],
        }

        # Get downloaded file path
        downloaded_path = download_dir / test_case["name"]

        if not downloaded_path.exists():
            print_error(f"File not found after download: {test_case['name']}")
            file_result["passed"] = False
            file_result["error"] = "File not found after download"
            results["all_passed"] = False
        else:
            # Get downloaded timestamps
            downloaded_ts = get_file_timestamps(downloaded_path)
            original_mtime = test_case["original_timestamps"]["mtime"]
            downloaded_mtime = downloaded_ts["mtime"]

            # Compare timestamps (allow 2 second tolerance for filesystem precision)
            is_match, diff = compare_timestamps(original_mtime, downloaded_mtime)

            file_result["original_mtime"] = format_datetime(original_mtime)
            file_result["downloaded_mtime"] = format_datetime(downloaded_mtime)
            file_result["difference_seconds"] = diff
            file_result["passed"] = is_match

            # Print comparison
            print(f"\nFile: {test_case['name']} ({test_case['description']})")
            print(f"  Original mtime:    {format_datetime(original_mtime)}")
            print(f"  Downloaded mtime:  {format_datetime(downloaded_mtime)}")
            print(f"  Difference:        {diff:.2f} seconds")

            if is_match:
                print_success("  ✓ Timestamps match (within tolerance)")
            else:
                print_error(f"  ✗ Timestamps differ by {diff:.2f}s (tolerance: 2.0s)")
                results["all_passed"] = False

        results["files"].append(file_result)

    return results


def print_summary(results: dict) -> None:
    """Print test summary.

    Args:
        results: Test results dictionary
    """
    print_separator("TEST SUMMARY")

    total_files = len(results["files"])
    passed_files = sum(1 for f in results["files"] if f.get("passed", False))
    failed_files = total_files - passed_files

    print(f"\nTotal files tested: {total_files}")
    print(f"  Passed: {passed_files}")
    print(f"  Failed: {failed_files}")

    if results.get("upload_failed"):
        print_error("\n✗ Upload to cloud failed")
    elif results.get("download_failed"):
        print_error("\n✗ Download from cloud failed")
    elif results["all_passed"]:
        print_success("\n✓ ALL TESTS PASSED - Timestamps are preserved correctly!")
    else:
        print_error("\n✗ SOME TESTS FAILED - Timestamp preservation has issues")

    # Print details of failed files
    failed = [f for f in results["files"] if not f.get("passed", False)]
    if failed:
        print("\nFailed files:")
        for f in failed:
            print(f"  - {f['name']}: {f.get('error', 'Timestamp mismatch')}")
            if "difference_seconds" in f:
                print(f"    Difference: {f['difference_seconds']:.2f} seconds")

    print(f"\nTest ID: {results.get('test_id', 'N/A')}")
    print(f"Cloud folder: {results.get('cloud_folder', 'N/A')}")
    print("\nNote: Test files are left in place for manual inspection:")
    print("  - Local upload: benchmark_temp/upload_test/")
    print("  - Local download: benchmark_temp/download_test/")
    print(f"  - Cloud: /{results.get('cloud_folder', 'N/A')}/")


def main():
    """Main benchmark function."""
    print_separator("PYDRIME TIMESTAMP PRESERVATION BENCHMARK")

    # Setup base directory for all test files
    base_dir = Path.cwd() / "benchmark_temp"
    base_dir.mkdir(exist_ok=True)

    print_info(f"Test directory: {base_dir}")

    try:
        # Run the test
        results = test_timestamp_preservation(base_dir)

        # Print summary
        print_summary(results)

        # Exit with appropriate code
        sys.exit(0 if results["all_passed"] else 1)

    except KeyboardInterrupt:
        print_warning("\nBenchmark interrupted by user")
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
