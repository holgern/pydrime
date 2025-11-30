"""
Benchmark script to test API upload functions with validation.

This script tests different upload helper functions to verify files are
uploaded correctly to the expected locations and can be replaced properly.

Test scenarios:
1. Upload to root folder
2. Upload to subfolder
3. Upload to sub-subfolder
4. Upload same file twice with different sizes (replace test)
5. Test all upload helper functions

For each test:
- Upload file
- Validate using returned file ID
- Try to replace with different size
- Validate replacement
- Remove file

Usage:
    python benchmarks/benchmark_api_upload.py
"""

import io
import logging
import os
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Configure logging - set to WARNING to reduce verbosity
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

logging.getLogger("pydrime").setLevel(logging.WARNING)

from pydrime.api import DrimeClient  # noqa: E402
from pydrime.config import Config  # noqa: E402

logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes for Test Results
# ============================================================================


@dataclass
class TestResult:
    """Result of a single test case."""

    name: str
    success: bool
    error: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class TestSummary:
    """Summary of all tests."""

    total: int = 0
    passed: int = 0
    failed: int = 0
    results: list[TestResult] = field(default_factory=list)


# ============================================================================
# Utility Functions
# ============================================================================


def print_separator(title: str, char: str = "=") -> None:
    """Print a separator line with title."""
    print(f"\n{char * 80}")
    print(f" {title}")
    print(f"{char * 80}")


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


def create_test_file(size_bytes: int = 1024, content: bytes | None = None) -> Path:
    """Create a temporary test file with specific size.

    Args:
        size_bytes: Size of the file in bytes
        content: Specific content to use (if None, random content is generated)

    Returns:
        Path to the created temp file
    """
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as f:
        if content is not None:
            f.write(content)
        else:
            f.write(os.urandom(size_bytes))
        return Path(f.name)


def cleanup_file(file_path: Path) -> None:
    """Clean up a temporary file."""
    try:
        if file_path.exists():
            os.unlink(file_path)
    except Exception as e:
        print_warning(f"Could not delete temp file {file_path}: {e}")


def validate_upload_by_id(
    client: DrimeClient,
    file_id: int,
    expected_size: int,
    workspace_id: int,
) -> tuple[bool, str]:
    """Validate an uploaded file by its ID.

    Args:
        client: DrimeClient instance
        file_id: ID of the uploaded file
        expected_size: Expected file size in bytes
        workspace_id: Workspace ID

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        # Fetch file entry by ID directly
        result = client.get_file_entry(
            entry_id=file_id,
            workspace_id=workspace_id,
        )

        if not result or not result.get("fileEntry"):
            return False, "Could not fetch file entry"

        entry = result.get("fileEntry", {})
        actual_size = entry.get("file_size", 0)
        has_users = len(entry.get("users", [])) > 0

        if actual_size != expected_size:
            return False, f"Size mismatch: expected {expected_size}, got {actual_size}"

        if not has_users:
            return False, "File has no users (incomplete upload)"

        return True, ""

    except Exception as e:
        return False, f"Validation error: {e}"


def validate_file_in_folder(
    client: DrimeClient,
    folder_id: int | None,
    file_name: str,
    expected_size: int,
    workspace_id: int,
) -> tuple[bool, int | None, str]:
    """Validate a file exists in a specific folder with expected size.

    Args:
        client: DrimeClient instance
        folder_id: Parent folder ID (None for root)
        file_name: Name of the file
        expected_size: Expected file size in bytes
        workspace_id: Workspace ID

    Returns:
        Tuple of (is_valid, file_id, error_message)
    """
    try:
        # Get folder contents using get_file_entries with parent_ids
        if folder_id:
            result = client.get_file_entries(
                parent_ids=[folder_id],
                workspace_id=workspace_id,
            )
        else:
            # For root, just query by name
            result = client.get_file_entries(
                query=file_name,
                workspace_id=workspace_id,
            )

        if not result:
            return False, None, "Could not fetch folder contents"

        # Search for file by name
        for entry in result.get("data", []):
            if entry.get("name") == file_name and not entry.get("is_folder", False):
                file_id = entry.get("id")
                actual_size = entry.get("file_size", 0)
                has_users = len(entry.get("users", [])) > 0

                if actual_size != expected_size:
                    return (
                        False,
                        file_id,
                        f"Size mismatch: expected {expected_size}, got {actual_size}",
                    )

                if not has_users:
                    return False, file_id, "File has no users (incomplete upload)"

                return True, file_id, ""

        return False, None, f"File '{file_name}' not found in folder"

    except Exception as e:
        return False, None, f"Validation error: {e}"


# ============================================================================
# Upload Helper Function Type
# ============================================================================


UploadFunc = Callable[
    [DrimeClient, Path, str, int, int | None],
    dict[str, Any],
]


def upload_with_simple(
    client: DrimeClient,
    file_path: Path,
    relative_path: str,
    workspace_id: int,
    parent_id: int | None,
) -> dict[str, Any]:
    """Upload using upload_file_simple."""
    return client.upload_file_simple(
        file_path=file_path,
        relative_path=relative_path,
        workspace_id=workspace_id,
        parent_id=parent_id,
    )


def upload_with_presign(
    client: DrimeClient,
    file_path: Path,
    relative_path: str,
    workspace_id: int,
    parent_id: int | None,
) -> dict[str, Any]:
    """Upload using upload_file_presign."""
    return client.upload_file_presign(
        file_path=file_path,
        relative_path=relative_path,
        workspace_id=workspace_id,
        parent_id=parent_id,
    )


def upload_with_multipart(
    client: DrimeClient,
    file_path: Path,
    relative_path: str,
    workspace_id: int,
    parent_id: int | None,
) -> dict[str, Any]:
    """Upload using upload_file_multipart."""
    return client.upload_file_multipart(
        file_path=file_path,
        relative_path=relative_path,
        workspace_id=workspace_id,
        parent_id=parent_id,
        chunk_size=1024 * 1024,  # 1MB chunks for testing
    )


def upload_with_auto(
    client: DrimeClient,
    file_path: Path,
    relative_path: str,
    workspace_id: int,
    parent_id: int | None,
) -> dict[str, Any]:
    """Upload using upload_file (auto-select method)."""
    return client.upload_file(
        file_path=file_path,
        relative_path=relative_path,
        workspace_id=workspace_id,
        parent_id=parent_id,
    )


# Map of upload helper functions to test
UPLOAD_HELPERS: dict[str, UploadFunc] = {
    "upload_file_simple": upload_with_simple,
    "upload_file_presign": upload_with_presign,
    "upload_file_multipart": upload_with_multipart,
    "upload_file (auto)": upload_with_auto,
}


# ============================================================================
# Test Functions
# ============================================================================


def test_upload_to_folder_depth(
    client: DrimeClient,
    workspace_id: int,
    upload_func: UploadFunc,
    upload_name: str,
    depth: int,
    unique_id: str,
) -> TestResult:
    """Test uploading to a specific folder depth.

    Args:
        client: DrimeClient instance
        workspace_id: Workspace ID
        upload_func: Upload function to use
        upload_name: Name of the upload function
        depth: Folder depth (0=root, 1=subfolder, 2=sub-subfolder)
        unique_id: Unique identifier for this test run

    Returns:
        TestResult
    """
    depth_names = {
        0: "root",
        1: "subfolder",
        2: "sub-subfolder",
        3: "sub-sub-subfolder",
    }
    depth_name = depth_names.get(depth, f"depth-{depth}")
    test_name = f"{upload_name} -> {depth_name}"

    print(f"  Testing: {test_name}... ", end="", flush=True)

    # Create relative path based on depth
    if depth == 0:
        relative_path = f"test_{unique_id}_file.txt"
    else:
        folders = [f"folder_{unique_id}_{i}" for i in range(depth)]
        relative_path = "/".join(folders) + "/file.txt"

    # Create test file
    file_size = 1024 + depth * 100  # Slightly different size per depth
    temp_file = create_test_file(size_bytes=file_size)
    file_id = None

    try:
        # Upload
        result = upload_func(client, temp_file, relative_path, workspace_id, None)

        file_entry = result.get("fileEntry", {})
        file_id = file_entry.get("id")

        if not file_id:
            print("FAILED (no file ID)")
            return TestResult(
                name=test_name,
                success=False,
                error="No file ID in response",
                details={"result": result},
            )

        # Validate
        actual_size = file_entry.get("file_size", 0)
        has_users = len(file_entry.get("users", [])) > 0

        if actual_size != file_size:
            print(f"FAILED (size: {actual_size} != {file_size})")
            return TestResult(
                name=test_name,
                success=False,
                error=f"Size mismatch: expected {file_size}, got {actual_size}",
                details={"file_id": file_id},
            )

        if not has_users:
            print("FAILED (no users)")
            return TestResult(
                name=test_name,
                success=False,
                error="No users field (incomplete upload)",
                details={"file_id": file_id},
            )

        print(f"OK (id={file_id})")
        return TestResult(
            name=test_name,
            success=True,
            details={"file_id": file_id, "size": file_size},
        )

    except Exception as e:
        print(f"FAILED ({e})")
        return TestResult(
            name=test_name,
            success=False,
            error=str(e),
        )

    finally:
        cleanup_file(temp_file)
        # Cleanup uploaded file
        if file_id:
            try:
                client.delete_file_entries([file_id], delete_forever=True)
            except Exception:
                pass


def test_upload_replace(
    client: DrimeClient,
    workspace_id: int,
    upload_func: UploadFunc,
    upload_name: str,
    unique_id: str,
) -> TestResult:
    """Test uploading same file twice with different sizes (replace test).

    Args:
        client: DrimeClient instance
        workspace_id: Workspace ID
        upload_func: Upload function to use
        upload_name: Name of the upload function
        unique_id: Unique identifier for this test run

    Returns:
        TestResult
    """
    test_name = f"{upload_name} -> replace test"
    print(f"  Testing: {test_name}...")

    relative_path = f"replace_test_{unique_id}/testfile.txt"

    # Create first file (smaller) - use random content for exact size control
    size_v1 = 1024
    temp_file_v1 = create_test_file(size_bytes=size_v1)
    file_id_v1 = None
    file_id_v2 = None

    try:
        # Upload v1
        print(f"    Uploading v1 ({size_v1} bytes)... ", end="", flush=True)
        result_v1 = upload_func(client, temp_file_v1, relative_path, workspace_id, None)

        file_entry_v1 = result_v1.get("fileEntry", {})
        file_id_v1 = file_entry_v1.get("id")

        if not file_id_v1:
            print("FAILED (no file ID)")
            return TestResult(
                name=test_name,
                success=False,
                error="No file ID in v1 response",
            )

        print(f"OK (id={file_id_v1})")

        # Wait a bit
        time.sleep(0.5)

        # Create second file (larger) - use random content for exact size control
        size_v2 = 2048
        temp_file_v2 = create_test_file(size_bytes=size_v2)

        # Upload v2 (replace)
        print(
            f"    Uploading v2 ({size_v2} bytes, should replace)... ",
            end="",
            flush=True,
        )

        try:
            result_v2 = upload_func(
                client, temp_file_v2, relative_path, workspace_id, None
            )

            file_entry_v2 = result_v2.get("fileEntry", {})
            file_id_v2 = file_entry_v2.get("id")

            if not file_id_v2:
                print("FAILED (no file ID)")
                return TestResult(
                    name=test_name,
                    success=False,
                    error="No file ID in v2 response",
                )

            # Check if size was updated
            actual_size = file_entry_v2.get("file_size", 0)
            has_users = len(file_entry_v2.get("users", [])) > 0

            if actual_size != size_v2:
                print(f"FAILED (size: {actual_size} != {size_v2})")
                return TestResult(
                    name=test_name,
                    success=False,
                    error=f"Replace failed: size should be {size_v2}, "
                    f"got {actual_size}",
                    details={"file_id_v1": file_id_v1, "file_id_v2": file_id_v2},
                )

            if not has_users:
                print("FAILED (no users)")
                return TestResult(
                    name=test_name,
                    success=False,
                    error="Replaced file has no users",
                    details={"file_id_v2": file_id_v2},
                )

            # Check if it's same ID (replaced) or different (new file)
            if file_id_v2 == file_id_v1:
                print(f"OK (replaced same id={file_id_v2})")
            else:
                print(f"OK (new id={file_id_v2}, old id={file_id_v1})")

            return TestResult(
                name=test_name,
                success=True,
                details={
                    "file_id_v1": file_id_v1,
                    "file_id_v2": file_id_v2,
                    "size_v1": size_v1,
                    "size_v2": size_v2,
                    "same_id": file_id_v2 == file_id_v1,
                },
            )

        finally:
            cleanup_file(temp_file_v2)

    except Exception as e:
        print(f"FAILED ({e})")
        return TestResult(
            name=test_name,
            success=False,
            error=str(e),
        )

    finally:
        cleanup_file(temp_file_v1)
        # Cleanup uploaded files
        for fid in [file_id_v1, file_id_v2]:
            if fid:
                try:
                    client.delete_file_entries([fid], delete_forever=True)
                except Exception:
                    pass


def test_full_cycle(
    client: DrimeClient,
    workspace_id: int,
    upload_func: UploadFunc,
    upload_name: str,
    unique_id: str,
) -> TestResult:
    """Test full upload cycle: upload -> validate -> replace -> validate -> remove.

    Args:
        client: DrimeClient instance
        workspace_id: Workspace ID
        upload_func: Upload function to use
        upload_name: Name of the upload function
        unique_id: Unique identifier for this test run

    Returns:
        TestResult
    """
    test_name = f"{upload_name} -> full cycle"
    print(f"  Testing: {test_name}...")

    relative_path = f"cycle_test_{unique_id}/sub/testfile.txt"
    details: dict[str, Any] = {"steps": []}
    file_id = None

    # Step 1: Upload
    size_v1 = 1500
    temp_file_v1 = create_test_file(size_bytes=size_v1)

    try:
        print("    Step 1: Upload... ", end="", flush=True)
        result = upload_func(client, temp_file_v1, relative_path, workspace_id, None)

        file_entry = result.get("fileEntry", {})
        file_id = file_entry.get("id")

        if not file_id:
            print("FAILED")
            details["steps"].append(("upload", False, "No file ID"))
            return TestResult(
                name=test_name, success=False, error="Upload failed", details=details
            )

        print(f"OK (id={file_id})")
        details["steps"].append(("upload", True, f"id={file_id}"))

    finally:
        cleanup_file(temp_file_v1)

    # Step 2: Validate
    print("    Step 2: Validate... ", end="", flush=True)
    try:
        # Wait for server to process
        time.sleep(0.5)

        file_entry = result.get("fileEntry", {})
        actual_size = file_entry.get("file_size", 0)
        has_users = len(file_entry.get("users", [])) > 0

        if actual_size != size_v1:
            print(f"FAILED (size: {actual_size} != {size_v1})")
            details["steps"].append(("validate_v1", False, "Size mismatch"))
        elif not has_users:
            print("FAILED (no users)")
            details["steps"].append(("validate_v1", False, "No users"))
        else:
            print("OK")
            details["steps"].append(("validate_v1", True, ""))

    except Exception as e:
        print(f"FAILED ({e})")
        details["steps"].append(("validate_v1", False, str(e)))

    # Step 3: Replace
    size_v2 = 2500
    temp_file_v2 = create_test_file(size_bytes=size_v2)
    file_id_v2 = None

    try:
        print("    Step 3: Replace... ", end="", flush=True)
        result = upload_func(client, temp_file_v2, relative_path, workspace_id, None)

        file_entry = result.get("fileEntry", {})
        file_id_v2 = file_entry.get("id")

        if not file_id_v2:
            print("FAILED")
            details["steps"].append(("replace", False, "No file ID"))
        else:
            print(f"OK (id={file_id_v2})")
            details["steps"].append(("replace", True, f"id={file_id_v2}"))

    except Exception as e:
        print(f"FAILED ({e})")
        details["steps"].append(("replace", False, str(e)))

    finally:
        cleanup_file(temp_file_v2)

    # Step 4: Validate replacement
    print("    Step 4: Validate replacement... ", end="", flush=True)
    try:
        time.sleep(0.5)

        file_entry = result.get("fileEntry", {})
        actual_size = file_entry.get("file_size", 0)
        has_users = len(file_entry.get("users", [])) > 0

        if actual_size != size_v2:
            print(f"FAILED (size: {actual_size} != {size_v2})")
            details["steps"].append(("validate_v2", False, "Size mismatch"))
        elif not has_users:
            print("FAILED (no users)")
            details["steps"].append(("validate_v2", False, "No users"))
        else:
            print("OK")
            details["steps"].append(("validate_v2", True, ""))

    except Exception as e:
        print(f"FAILED ({e})")
        details["steps"].append(("validate_v2", False, str(e)))

    # Step 5: Remove
    print("    Step 5: Remove... ", end="", flush=True)
    try:
        # Use the most recent file ID
        id_to_delete = file_id_v2 or file_id
        if id_to_delete:
            client.delete_file_entries([id_to_delete], delete_forever=True)
            print("OK")
            details["steps"].append(("remove", True, ""))
            file_id = None  # Mark as cleaned up
            file_id_v2 = None
        else:
            print("SKIPPED (no file ID)")
            details["steps"].append(("remove", False, "No file ID"))

    except Exception as e:
        print(f"FAILED ({e})")
        details["steps"].append(("remove", False, str(e)))

    # Calculate overall success
    all_passed = all(step[1] for step in details["steps"])

    if all_passed:
        return TestResult(name=test_name, success=True, details=details)
    else:
        failed_steps = [step[0] for step in details["steps"] if not step[1]]
        return TestResult(
            name=test_name,
            success=False,
            error=f"Failed steps: {', '.join(failed_steps)}",
            details=details,
        )


def run_tests_for_upload_helper(
    client: DrimeClient,
    workspace_id: int,
    upload_name: str,
    upload_func: UploadFunc,
    unique_id: str,
) -> list[TestResult]:
    """Run all tests for a specific upload helper function.

    Args:
        client: DrimeClient instance
        workspace_id: Workspace ID
        upload_name: Name of the upload function
        upload_func: Upload function to use
        unique_id: Unique identifier for this test run

    Returns:
        List of TestResults
    """
    print_separator(f"Testing: {upload_name}", "-")
    results = []

    # Test folder depths
    for depth in [0, 1, 2, 3]:
        result = test_upload_to_folder_depth(
            client, workspace_id, upload_func, upload_name, depth, unique_id
        )
        results.append(result)
        time.sleep(0.3)

    # Test replace
    result = test_upload_replace(
        client, workspace_id, upload_func, upload_name, unique_id
    )
    results.append(result)
    time.sleep(0.3)

    # Test full cycle
    result = test_full_cycle(client, workspace_id, upload_func, upload_name, unique_id)
    results.append(result)

    return results


# ============================================================================
# Summary Printing
# ============================================================================


def print_summary(summary: TestSummary) -> None:
    """Print test summary with failures and successes."""
    print_separator("TEST SUMMARY", "=")

    # Group results by upload helper
    by_helper: dict[str, list[TestResult]] = {}
    for result in summary.results:
        # Extract helper name from test name
        parts = result.name.split(" -> ")
        helper = parts[0] if parts else "unknown"
        if helper not in by_helper:
            by_helper[helper] = []
        by_helper[helper].append(result)

    # Print results by helper
    for helper, results in by_helper.items():
        passed = sum(1 for r in results if r.success)
        failed = len(results) - passed
        status = "PASS" if failed == 0 else "FAIL"

        print(f"\n{helper}: {passed}/{len(results)} passed [{status}]")

        for result in results:
            test_part = (
                result.name.split(" -> ")[1] if " -> " in result.name else result.name
            )
            if result.success:
                print(f"  [PASS] {test_part}")
            else:
                print(f"  [FAIL] {test_part}: {result.error}")

    # Print overall summary
    print(f"\n{'=' * 80}")
    print(f"OVERALL: {summary.passed}/{summary.total} tests passed")
    print(f"  Passed: {summary.passed}")
    print(f"  Failed: {summary.failed}")
    print(f"{'=' * 80}")

    # Print failure details
    if summary.failed > 0:
        print("\n" + "-" * 80)
        print("FAILURE DETAILS:")
        print("-" * 80)
        for result in summary.results:
            if not result.success:
                print(f"\n  {result.name}:")
                print(f"    Error: {result.error}")
                if result.details:
                    if "steps" in result.details:
                        print("    Steps:")
                        for step in result.details["steps"]:
                            status = "OK" if step[1] else "FAILED"
                            print(f"      {step[0]}: {status} {step[2]}")


# ============================================================================
# Main Function
# ============================================================================


def main():
    """Main benchmark function."""
    print_separator("PYDRIME API UPLOAD BENCHMARK", "=")

    # Initialize client
    print_info("Initializing API client...")
    try:
        client = DrimeClient()
        user_info = client.get_logged_user()
        if user_info and user_info.get("user"):
            user = user_info["user"]
            print_success(f"Connected as: {user.get('email', 'unknown')}")
        else:
            print_error("Could not verify API connection")
            sys.exit(1)
    except Exception as e:
        print_error(f"Failed to initialize client: {e}")
        sys.exit(1)

    # Get workspace ID from config
    config = Config()
    workspace_id = config.get_default_workspace() or 0
    print_info(f"Using workspace ID: {workspace_id}")

    # Generate unique ID for this test run
    unique_id = uuid.uuid4().hex[:8]
    print_info(f"Test run ID: {unique_id}")

    summary = TestSummary()

    try:
        # Run tests for each upload helper
        for upload_name, upload_func in UPLOAD_HELPERS.items():
            results = run_tests_for_upload_helper(
                client, workspace_id, upload_name, upload_func, unique_id
            )
            for result in results:
                summary.results.append(result)
                summary.total += 1
                if result.success:
                    summary.passed += 1
                else:
                    summary.failed += 1

            # Small delay between helper tests
            time.sleep(1)

        # Print summary
        print_summary(summary)

    except KeyboardInterrupt:
        print_warning("\nBenchmark interrupted by user")
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        client.close()

    print_separator("BENCHMARK COMPLETE", "=")

    # Exit with error code if any tests failed
    if summary.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
