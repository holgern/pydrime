"""
Benchmark script to test various upload scenarios and identify issues.

This script tests different upload scenarios to help debug issues like:
- Uploading to nested folders (subfolder of subfolder)
- Re-uploading after deletion
- Files with specific names that may conflict
- Files with different content types

Usage:
    python benchmarks/benchmark_upload_scenarios.py
"""

import io
import logging
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

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


def create_test_file(
    content: bytes | None = None,
    size_kb: int = 1,
    suffix: str = ".txt",
) -> Path:
    """Create a temporary test file.

    Args:
        content: Specific content to use (if None, random content is generated)
        size_kb: Size in KB if generating random content
        suffix: File suffix

    Returns:
        Path to the created temp file
    """
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=suffix) as f:
        if content is not None:
            f.write(content)
        else:
            f.write(os.urandom(size_kb * 1024))
        return Path(f.name)


def cleanup_file(file_path: Path) -> None:
    """Clean up a temporary file."""
    try:
        if file_path.exists():
            os.unlink(file_path)
    except Exception as e:
        print_warning(f"Could not delete temp file {file_path}: {e}")


def test_upload_and_cleanup(
    client: DrimeClient,
    file_path: Path,
    relative_path: str,
    workspace_id: int,
    description: str,
) -> tuple[bool, str]:
    """Test uploading a file and clean up afterwards.

    Args:
        client: DrimeClient instance
        file_path: Local file path to upload
        relative_path: Remote relative path
        workspace_id: Workspace ID
        description: Test description

    Returns:
        Tuple of (success, error_message)
    """
    print(f"  Testing: {description}... ", end="", flush=True)
    try:
        result = client.upload_file(
            file_path=file_path,
            relative_path=relative_path,
            workspace_id=workspace_id,
        )
        file_id = result.get("fileEntry", {}).get("id")
        parent_id = result.get("fileEntry", {}).get("parent_id")
        print(f"SUCCESS (id={file_id}, parent={parent_id})")

        # Cleanup
        if file_id:
            client.delete_file_entries([file_id], delete_forever=True)

        return True, ""
    except Exception as e:
        print("FAILED")
        return False, str(e)


def test_folder_depth(client: DrimeClient, workspace_id: int) -> dict:
    """Test uploading to different folder depths.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Folder Depth", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file()
    results = {}

    test_paths = [
        (f"file_{unique_id}.txt", "Depth 0 (root)"),
        (f"folder1_{unique_id}/file.txt", "Depth 1"),
        (f"folder1_{unique_id}/folder2/file.txt", "Depth 2"),
        (f"folder1_{unique_id}/folder2/folder3/file.txt", "Depth 3"),
        (f"folder1_{unique_id}/folder2/folder3/folder4/file.txt", "Depth 4"),
    ]

    for path, desc in test_paths:
        success, error = test_upload_and_cleanup(
            client, temp_file, path, workspace_id, desc
        )
        results[desc] = {"success": success, "error": error}
        time.sleep(0.5)  # Small delay between tests

    cleanup_file(temp_file)
    return results


def test_existing_folder_structure(client: DrimeClient, workspace_id: int) -> dict:
    """Test uploading to existing folder structures.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Existing Folder Structure", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file()
    results = {}

    # First, create a folder structure by uploading a file
    print_info("Creating folder structure first...")
    base_folder = f"test_existing_{unique_id}"

    try:
        result = client.upload_file(
            file_path=temp_file,
            relative_path=f"{base_folder}/subfolder/setup_file.txt",
            workspace_id=workspace_id,
        )
        setup_file_id = result.get("fileEntry", {}).get("id")
        print_success(f"Created folder structure with setup file (id={setup_file_id})")
    except Exception as e:
        print_error(f"Failed to create folder structure: {e}")
        cleanup_file(temp_file)
        return {"error": str(e)}

    time.sleep(1)

    # Now test uploading to the existing structure
    test_cases = [
        (f"{base_folder}/new_file.txt", "New file in existing folder"),
        (f"{base_folder}/subfolder/new_file.txt", "New file in existing subfolder"),
        (
            f"{base_folder}/subfolder/new_subfolder/file.txt",
            "New subfolder in existing subfolder",
        ),
    ]

    for path, desc in test_cases:
        success, error = test_upload_and_cleanup(
            client, temp_file, path, workspace_id, desc
        )
        results[desc] = {"success": success, "error": error}
        time.sleep(0.5)

    # Cleanup setup file
    if setup_file_id:
        try:
            client.delete_file_entries([setup_file_id], delete_forever=True)
        except Exception:
            pass

    cleanup_file(temp_file)
    return results


def test_delete_and_reupload(client: DrimeClient, workspace_id: int) -> dict:
    """Test deleting a file and re-uploading with same name.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Delete and Re-upload", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file()
    results = {}
    relative_path = f"test_reupload_{unique_id}/test_file.txt"

    # Step 1: Initial upload
    print_info("Step 1: Initial upload")
    try:
        result = client.upload_file(
            file_path=temp_file,
            relative_path=relative_path,
            workspace_id=workspace_id,
        )
        file_id1 = result.get("fileEntry", {}).get("id")
        print_success(f"Initial upload successful (id={file_id1})")
        results["initial_upload"] = {"success": True, "file_id": file_id1}
    except Exception as e:
        print_error(f"Initial upload failed: {e}")
        cleanup_file(temp_file)
        return {"initial_upload": {"success": False, "error": str(e)}}

    # Step 2: Delete to trash
    print_info("Step 2: Delete to trash")
    try:
        client.delete_file_entries([file_id1], delete_forever=False)
        print_success("Deleted to trash")
        results["delete_to_trash"] = {"success": True}
    except Exception as e:
        print_error(f"Delete to trash failed: {e}")
        results["delete_to_trash"] = {"success": False, "error": str(e)}

    # Step 3: Immediate re-upload
    print_info("Step 3: Immediate re-upload (no delay)")
    try:
        result = client.upload_file(
            file_path=temp_file,
            relative_path=relative_path,
            workspace_id=workspace_id,
        )
        file_id2 = result.get("fileEntry", {}).get("id")
        print_success(f"Immediate re-upload successful (id={file_id2})")
        results["immediate_reupload"] = {"success": True, "file_id": file_id2}
        # Cleanup
        client.delete_file_entries([file_id2], delete_forever=True)
    except Exception as e:
        print_error(f"Immediate re-upload failed: {e}")
        results["immediate_reupload"] = {"success": False, "error": str(e)}

    # Step 4: Upload again after delay
    print_info("Step 4: Re-upload after 2 second delay")
    time.sleep(2)
    try:
        result = client.upload_file(
            file_path=temp_file,
            relative_path=relative_path,
            workspace_id=workspace_id,
        )
        file_id3 = result.get("fileEntry", {}).get("id")
        print_success(f"Delayed re-upload successful (id={file_id3})")
        results["delayed_reupload"] = {"success": True, "file_id": file_id3}
        # Cleanup
        client.delete_file_entries([file_id3], delete_forever=True)
    except Exception as e:
        print_error(f"Delayed re-upload failed: {e}")
        results["delayed_reupload"] = {"success": False, "error": str(e)}

    cleanup_file(temp_file)
    return results


def test_special_filenames(client: DrimeClient, workspace_id: int) -> dict:
    """Test uploading files with special/problematic filenames.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Special Filenames", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file()
    results = {}

    test_names = [
        "simple.txt",
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.multiple.dots.txt",
        "UPPERCASE.TXT",
        "MixedCase.Txt",
        "123numeric.txt",
        "file123.txt",
        ".hidden",
        "no_extension",
        "very_long_filename_" + "x" * 100 + ".txt",
    ]

    for name in test_names:
        path = f"test_names_{unique_id}/{name}"
        success, error = test_upload_and_cleanup(
            client, temp_file, path, workspace_id, f"Name: {name[:40]}"
        )
        results[name] = {"success": success, "error": error}
        time.sleep(0.3)

    cleanup_file(temp_file)
    return results


def test_file_content_types(client: DrimeClient, workspace_id: int) -> dict:
    """Test uploading files with different content types.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: File Content Types", "-")

    unique_id = uuid.uuid4().hex[:8]
    results = {}

    content_tests = [
        ("Random bytes", os.urandom(1024), ".bin"),
        ("All zeros", b"\x00" * 1024, ".bin"),
        ("All ones (0xFF)", b"\xff" * 1024, ".bin"),
        ("Text content", b"Hello World! " * 100, ".txt"),
        ("JSON content", b'{"key": "value", "number": 123}', ".json"),
        ("Empty file", b"", ".txt"),
        ("Single byte", b"X", ".txt"),
        ("Newlines only", b"\n" * 100, ".txt"),
        ("Unicode text", "Hello \u4e16\u754c \U0001f600".encode(), ".txt"),
    ]

    for desc, content, suffix in content_tests:
        temp_file = create_test_file(content=content, suffix=suffix)
        path = f"test_content_{unique_id}/{desc.replace(' ', '_')}{suffix}"
        success, error = test_upload_and_cleanup(
            client, temp_file, path, workspace_id, desc
        )
        results[desc] = {"success": success, "error": error}
        cleanup_file(temp_file)
        time.sleep(0.3)

    return results


def test_rapid_uploads(client: DrimeClient, workspace_id: int) -> dict:
    """Test rapid consecutive uploads to detect rate limiting.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Rapid Consecutive Uploads", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file()
    results = {"uploads": [], "failures": 0, "successes": 0}

    num_uploads = 10
    print_info(f"Attempting {num_uploads} rapid uploads with no delay...")

    for i in range(num_uploads):
        path = f"test_rapid_{unique_id}/file_{i:03d}.txt"
        try:
            result = client.upload_file(
                file_path=temp_file,
                relative_path=path,
                workspace_id=workspace_id,
            )
            file_id = result.get("fileEntry", {}).get("id")
            results["uploads"].append({"index": i, "success": True, "file_id": file_id})
            results["successes"] += 1
            print(f"  Upload {i}: SUCCESS (id={file_id})")

            # Cleanup immediately
            if file_id:
                client.delete_file_entries([file_id], delete_forever=True)
        except Exception as e:
            results["uploads"].append({"index": i, "success": False, "error": str(e)})
            results["failures"] += 1
            print(f"  Upload {i}: FAILED - {e}")

    cleanup_file(temp_file)
    print_info(
        f"Results: {results['successes']} successes, {results['failures']} failures"
    )
    return results


def test_presign_entry_flow(client: DrimeClient, workspace_id: int) -> dict:
    """Test the presign -> S3 upload -> entry creation flow in detail.

    This helps identify which step is failing.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Presign/Entry Flow Debug", "-")

    import httpx

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file()
    results = {}

    file_size = temp_file.stat().st_size
    mime_type = "application/octet-stream"
    extension = "txt"
    relative_path = f"test_flow_{unique_id}/debug_file.txt"

    # Step 1: Presign
    print_info("Step 1: Get presigned URL")
    presign_payload = {
        "filename": temp_file.name,
        "mime": mime_type,
        "size": file_size,
        "extension": extension,
        "relativePath": relative_path,
        "workspaceId": workspace_id,
        "parentId": None,
    }

    try:
        presign_response = client._request(
            "POST",
            "/s3/simple/presign",
            json=presign_payload,
            params={"workspaceId": workspace_id},
        )
        key = presign_response.get("key")
        presigned_url = presign_response.get("url")
        print_success(f"Presign OK, key: {key}")
        results["presign"] = {"success": True, "key": key}
    except Exception as e:
        print_error(f"Presign failed: {e}")
        cleanup_file(temp_file)
        return {"presign": {"success": False, "error": str(e)}}

    # Step 2: S3 Upload
    print_info("Step 2: Upload to S3")
    try:
        with open(temp_file, "rb") as f:
            file_content = f.read()

        response = httpx.request(
            "PUT",
            presigned_url,
            content=file_content,
            headers={
                "Content-Type": mime_type,
                "Content-Length": str(file_size),
                "x-amz-acl": "private",
            },
            timeout=60,
        )
        response.raise_for_status()
        print_success(f"S3 upload OK, status: {response.status_code}")
        results["s3_upload"] = {"success": True, "status": response.status_code}
    except Exception as e:
        print_error(f"S3 upload failed: {e}")
        cleanup_file(temp_file)
        results["s3_upload"] = {"success": False, "error": str(e)}
        return results

    # Step 3: Create entry
    print_info("Step 3: Create file entry")
    parent_id = presign_response.get("parentId")
    entry_payload = {
        "clientMime": mime_type,
        "clientName": temp_file.name,
        "filename": key.split("/")[-1],
        "size": file_size,
        "clientExtension": extension,
        "relativePath": relative_path,
        "workspaceId": workspace_id,
        "parentId": parent_id,
    }

    print_debug(f"Entry payload: {entry_payload}", 1)

    try:
        entry_response = client._request("POST", "/s3/entries", json=entry_payload)
        file_id = entry_response.get("fileEntry", {}).get("id")
        print_success(f"Entry created, file_id: {file_id}")
        results["entry_creation"] = {"success": True, "file_id": file_id}

        # Cleanup
        if file_id:
            client.delete_file_entries([file_id], delete_forever=True)
    except Exception as e:
        print_error(f"Entry creation failed: {e}")
        results["entry_creation"] = {"success": False, "error": str(e)}

        # Debug: Check raw response
        print_info("Checking raw response...")
        try:
            http_client = client._get_client()
            url = f"{client.api_url}/s3/entries"
            raw_response = http_client.request("POST", url, json=entry_payload)
            print_debug(f"Status: {raw_response.status_code}", 1)
            print_debug(f"Content-Type: {raw_response.headers.get('Content-Type')}", 1)
            if "text/html" in raw_response.headers.get("Content-Type", ""):
                print_warning("Server returned HTML instead of JSON!")
                print_debug(f"First 200 chars: {raw_response.text[:200]}", 1)
        except Exception as e2:
            print_error(f"Raw response check failed: {e2}")

    cleanup_file(temp_file)
    return results


def test_duplicate_replace(client: DrimeClient, workspace_id: int) -> dict:
    """Test uploading a file that already exists with replace behavior.

    This is the failing scenario: upload file, then upload again with same name.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Duplicate Replace (THE FAILING SCENARIO)", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file(content=b"Initial content v1")
    results = {}
    relative_path = f"test_dup_{unique_id}/myfile.txt"

    # Step 1: Initial upload
    print("  Step 1: Initial upload... ", end="", flush=True)
    try:
        result = client.upload_file(
            file_path=temp_file,
            relative_path=relative_path,
            workspace_id=workspace_id,
        )
        file_id1 = result.get("fileEntry", {}).get("id")
        print(f"SUCCESS (id={file_id1})")
        results["step1_initial"] = {"success": True, "file_id": file_id1}
    except Exception as e:
        print(f"FAILED: {e}")
        cleanup_file(temp_file)
        return {"step1_initial": {"success": False, "error": str(e)}}

    time.sleep(1)

    # Step 2: Create new file with different content
    temp_file2 = create_test_file(content=b"Updated content v2")

    # Step 3: Re-upload with same path (this should trigger duplicate handling)
    print("  Step 2: Re-upload same path (replace)... ", end="", flush=True)
    try:
        result = client.upload_file(
            file_path=temp_file2,
            relative_path=relative_path,
            workspace_id=workspace_id,
        )
        file_id2 = result.get("fileEntry", {}).get("id")
        print(f"SUCCESS (id={file_id2})")
        results["step2_replace"] = {"success": True, "file_id": file_id2}

        # Cleanup
        if file_id2:
            client.delete_file_entries([file_id2], delete_forever=True)
    except Exception as e:
        print(f"FAILED: {e}")
        results["step2_replace"] = {"success": False, "error": str(e)}

        # Still cleanup original if possible
        if file_id1:
            try:
                client.delete_file_entries([file_id1], delete_forever=True)
            except Exception:
                pass

    cleanup_file(temp_file)
    cleanup_file(temp_file2)
    return results


def test_duplicate_in_subfolder(client: DrimeClient, workspace_id: int) -> dict:
    """Test duplicate replace specifically in a subfolder (nested path).

    This tests the exact scenario that was failing:
    sync/test_folder/existing_file.txt with --on-duplicate replace

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Duplicate in Subfolder (EXACT FAILING CASE)", "-")

    unique_id = uuid.uuid4().hex[:8]
    results = {}

    # Use nested path like sync/test_folder/
    base_folder = f"sync_test_{unique_id}"
    subfolder = f"{base_folder}/subfolder"
    filename = "testfile.txt"
    relative_path = f"{subfolder}/{filename}"

    # Step 1: Create folder structure with initial file
    temp_file1 = create_test_file(content=b"Version 1")
    print("  Step 1: Create initial file in subfolder... ", end="", flush=True)
    try:
        result = client.upload_file(
            file_path=temp_file1,
            relative_path=relative_path,
            workspace_id=workspace_id,
        )
        file_id1 = result.get("fileEntry", {}).get("id")
        parent_id = result.get("fileEntry", {}).get("parent_id")
        print(f"SUCCESS (id={file_id1}, parent={parent_id})")
        results["step1_create"] = {
            "success": True,
            "file_id": file_id1,
            "parent_id": parent_id,
        }
    except Exception as e:
        print(f"FAILED: {e}")
        cleanup_file(temp_file1)
        return {"step1_create": {"success": False, "error": str(e)}}

    cleanup_file(temp_file1)
    time.sleep(1)

    # Step 2: Upload again with same path - simulating --on-duplicate replace
    temp_file2 = create_test_file(content=b"Version 2 - replaced")
    print("  Step 2: Replace file in subfolder... ", end="", flush=True)
    try:
        result = client.upload_file(
            file_path=temp_file2,
            relative_path=relative_path,
            workspace_id=workspace_id,
        )
        file_id2 = result.get("fileEntry", {}).get("id")
        print(f"SUCCESS (id={file_id2})")
        results["step2_replace"] = {"success": True, "file_id": file_id2}

        # Cleanup
        if file_id2:
            client.delete_file_entries([file_id2], delete_forever=True)
    except Exception as e:
        print(f"FAILED: {e}")
        results["step2_replace"] = {"success": False, "error": str(e)}

        # Cleanup original
        if file_id1:
            try:
                client.delete_file_entries([file_id1], delete_forever=True)
            except Exception:
                pass

    cleanup_file(temp_file2)
    return results


def test_workspace_isolation(client: DrimeClient) -> dict:
    """Test uploading to different workspaces.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Workspace Isolation", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file()
    results = {}

    # Get available workspaces
    try:
        workspaces = client.get_workspaces()
        ws_list = workspaces.get("workspaces", [])
        print_info(f"Found {len(ws_list)} workspaces")
    except Exception as e:
        print_error(f"Could not get workspaces: {e}")
        cleanup_file(temp_file)
        return {"error": str(e)}

    # Test workspace 0 (personal)
    ws_ids_to_test = [0]  # Start with personal workspace

    # Add first 2 workspaces from list
    for ws in ws_list[:2]:
        ws_ids_to_test.append(ws.get("id"))

    for ws_id in ws_ids_to_test:
        ws_name = "personal" if ws_id == 0 else f"ws_{ws_id}"
        path = f"test_ws_{unique_id}/file.txt"
        success, error = test_upload_and_cleanup(
            client, temp_file, path, ws_id, f"Workspace {ws_name}"
        )
        results[f"workspace_{ws_id}"] = {"success": success, "error": error}
        time.sleep(0.5)

    cleanup_file(temp_file)
    return results


def summarize_results(all_results: dict) -> None:
    """Print a summary of all test results."""
    print_separator("TEST SUMMARY", "=")

    total_tests = 0
    total_passed = 0
    total_failed = 0

    for test_name, results in all_results.items():
        print(f"\n{test_name}:")
        if isinstance(results, dict):
            if "error" in results:
                print(f"  ERROR: {results['error']}")
                total_failed += 1
                total_tests += 1
            else:
                for key, value in results.items():
                    if isinstance(value, dict) and "success" in value:
                        total_tests += 1
                        if value["success"]:
                            total_passed += 1
                            print(f"  [PASS] {key}")
                        else:
                            total_failed += 1
                            print(f"  [FAIL] {key}: {value.get('error', 'Unknown')}")
                    elif key in ("successes", "failures"):
                        pass  # Skip summary fields
                    elif isinstance(value, list):
                        # Handle rapid upload results
                        for item in value:
                            if isinstance(item, dict) and "success" in item:
                                total_tests += 1
                                if item["success"]:
                                    total_passed += 1
                                else:
                                    total_failed += 1

    print(f"\n{'=' * 80}")
    print(f"TOTAL: {total_tests} tests, {total_passed} passed, {total_failed} failed")
    print(f"{'=' * 80}")


def main():
    """Main benchmark function."""
    print_separator("PYDRIME UPLOAD SCENARIOS BENCHMARK", "=")

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

    # Use workspace from config or default to 0
    from pydrime.config import Config

    config = Config()
    workspace_id = config.get_default_workspace() or 0
    print_info(f"Using workspace ID: {workspace_id}")

    all_results = {}

    try:
        # Run tests - MOST IMPORTANT FIRST (the failing scenarios)
        all_results["Duplicate Replace"] = test_duplicate_replace(client, workspace_id)

        all_results["Duplicate in Subfolder"] = test_duplicate_in_subfolder(
            client, workspace_id
        )

        # Other tests
        all_results["Folder Depth"] = test_folder_depth(client, workspace_id)

        all_results["Existing Folder Structure"] = test_existing_folder_structure(
            client, workspace_id
        )

        all_results["Delete and Re-upload"] = test_delete_and_reupload(
            client, workspace_id
        )

        all_results["Special Filenames"] = test_special_filenames(client, workspace_id)

        all_results["File Content Types"] = test_file_content_types(
            client, workspace_id
        )

        all_results["Rapid Uploads"] = test_rapid_uploads(client, workspace_id)

        all_results["Presign/Entry Flow"] = test_presign_entry_flow(
            client, workspace_id
        )

        all_results["Workspace Isolation"] = test_workspace_isolation(client)

        # Print summary
        summarize_results(all_results)

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


if __name__ == "__main__":
    main()
