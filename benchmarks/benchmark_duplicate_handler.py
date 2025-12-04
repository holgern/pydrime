"""
Benchmark script to test DuplicateHandler functionality.

This script tests the DuplicateHandler's ability to correctly identify and handle
duplicates, especially in cases involving:
- Files with same names in different Unicode-named folders (u vs ü)
- NFC vs NFD normalized folder names
- Path-aware duplicate detection

Usage:
    python benchmarks/benchmark_duplicate_handler.py
"""

import io
import logging
import os
import sys
import tempfile
import time
import unicodedata
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
from pydrime.duplicate_handler import DuplicateHandler  # noqa: E402
from pydrime.output import OutputFormatter  # noqa: E402

logger = logging.getLogger(__name__)


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


def create_test_file(
    content: bytes | None = None,
    size_kb: int = 1,
    suffix: str = ".txt",
) -> Path:
    """Create a temporary test file."""
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


def test_unicode_folder_duplicate_detection(
    client: DrimeClient, workspace_id: int
) -> dict:
    """Test that DuplicateHandler correctly handles files in Unicode folders.

    This tests the scenario where:
    - Upload u/file.txt (creates folder 'u' with file.txt)
    - Upload ü/file.txt (should create folder 'ü' with file.txt, NOT detect as dup)

    The bug was: DuplicateHandler would mark BOTH files as duplicates because
    it matched by filename only, not by full path.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Unicode Folder Duplicate Detection", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file(content=b"Test content for duplicate check")
    results = {"tests": [], "issues_found": 0}

    # First, upload a file to folder 'u'
    base_path = f"dup_test_{unique_id}"
    path1 = f"{base_path}/u/file.txt"

    print_info(f"Step 1: Upload file to '{base_path}/u/'")
    file_id1 = None
    try:
        result = client.upload_file(
            file_path=temp_file,
            relative_path=path1,
            workspace_id=workspace_id,
        )
        file_id1 = result.get("fileEntry", {}).get("id")
        parent_id1 = result.get("fileEntry", {}).get("parent_id")
        print_success(f"  Uploaded to 'u/' (id={file_id1}, parent={parent_id1})")
    except Exception as e:
        print_error(f"  Failed to upload to 'u/': {e}")
        cleanup_file(temp_file)
        return {"error": str(e)}

    time.sleep(1)

    # Now test DuplicateHandler with a file going to folder 'ü'
    path2 = f"{base_path}/ü/file.txt"
    temp_file2 = create_test_file(content="Different content for ü folder".encode())

    print_info(f"Step 2: Check duplicates for file in '{base_path}/ü/'")

    out = OutputFormatter(json_output=False, quiet=True)
    handler = DuplicateHandler(
        client=client,
        out=out,
        workspace_id=workspace_id,
        on_duplicate="skip",  # Auto-skip mode to test detection
        parent_id=None,
    )

    # Prepare files_to_upload list
    files_to_upload = [(temp_file2, path2)]

    # Run duplicate validation
    handler.validate_and_handle_duplicates(files_to_upload)

    # Check results
    test_result = {
        "path1": path1,
        "path2": path2,
        "files_to_skip": list(handler.files_to_skip),
        "duplicate_rel_paths": dict(handler._duplicate_rel_paths),
        "issue": False,
    }

    if path2 in handler.files_to_skip:
        print_warning(f"  BUG: '{path2}' was incorrectly marked for skipping!")
        print_warning("  The file in 'ü/' should NOT be a duplicate of file in 'u/'")
        test_result["issue"] = True
        results["issues_found"] += 1
    else:
        print_success(f"  Correctly: '{path2}' was NOT marked as duplicate")

    results["tests"].append(test_result)

    # Cleanup
    if file_id1:
        try:
            client.delete_file_entries([file_id1], delete_forever=True)
        except Exception:
            pass

    cleanup_file(temp_file)
    cleanup_file(temp_file2)
    return results


def test_same_folder_real_duplicate(client: DrimeClient, workspace_id: int) -> dict:
    """Test that DuplicateHandler correctly detects actual duplicates.

    This tests the scenario where a file with the same name IS a real duplicate.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Same Folder Real Duplicate Detection", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file(content=b"Original content")
    results = {"tests": [], "issues_found": 0}

    # First, upload a file
    base_path = f"real_dup_test_{unique_id}"
    path1 = f"{base_path}/subfolder/file.txt"

    print_info(f"Step 1: Upload file to '{path1}'")
    file_id1 = None
    try:
        result = client.upload_file(
            file_path=temp_file,
            relative_path=path1,
            workspace_id=workspace_id,
        )
        file_id1 = result.get("fileEntry", {}).get("id")
        print_success(f"  Uploaded (id={file_id1})")
    except Exception as e:
        print_error(f"  Failed to upload: {e}")
        cleanup_file(temp_file)
        return {"error": str(e)}

    time.sleep(1)

    # Now test DuplicateHandler with the SAME path
    temp_file2 = create_test_file(content=b"New content - should replace")

    print_info(f"Step 2: Check duplicates for same path '{path1}'")

    out = OutputFormatter(json_output=False, quiet=True)
    handler = DuplicateHandler(
        client=client,
        out=out,
        workspace_id=workspace_id,
        on_duplicate="skip",
        parent_id=None,
    )

    files_to_upload = [(temp_file2, path1)]
    handler.validate_and_handle_duplicates(files_to_upload)

    test_result = {
        "path": path1,
        "files_to_skip": list(handler.files_to_skip),
        "duplicate_rel_paths": dict(handler._duplicate_rel_paths),
        "issue": False,
    }

    if path1 in handler.files_to_skip:
        print_success(f"  Correctly: '{path1}' WAS marked as duplicate (skip mode)")
    else:
        # This might not be an issue if server doesn't report it
        print_info(f"  '{path1}' was not marked as duplicate (server may not report)")

    results["tests"].append(test_result)

    # Cleanup
    if file_id1:
        try:
            client.delete_file_entries([file_id1], delete_forever=True)
        except Exception:
            pass

    cleanup_file(temp_file)
    cleanup_file(temp_file2)
    return results


def test_multiple_files_same_name_different_folders(
    client: DrimeClient, workspace_id: int
) -> dict:
    """Test handling multiple files with same name in different folders.

    Scenario: Upload folder1/file.txt and folder2/file.txt in the same batch.
    Only files that are ACTUAL duplicates on server should be flagged.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Multiple Files Same Name Different Folders", "-")

    unique_id = uuid.uuid4().hex[:8]
    results = {"tests": [], "issues_found": 0}

    # First, upload a file to folder1
    base_path = f"multi_test_{unique_id}"
    existing_path = f"{base_path}/folder1/data.txt"
    temp_file1 = create_test_file(content=b"Existing file in folder1")

    print_info(f"Step 1: Upload existing file to '{existing_path}'")
    file_id1 = None
    try:
        result = client.upload_file(
            file_path=temp_file1,
            relative_path=existing_path,
            workspace_id=workspace_id,
        )
        file_id1 = result.get("fileEntry", {}).get("id")
        print_success(f"  Uploaded (id={file_id1})")
    except Exception as e:
        print_error(f"  Failed to upload: {e}")
        cleanup_file(temp_file1)
        return {"error": str(e)}

    time.sleep(1)

    # Now prepare batch upload with files in folder1 and folder2
    temp_file2 = create_test_file(content=b"New file for folder1 - duplicate")
    temp_file3 = create_test_file(content=b"New file for folder2 - not duplicate")

    path_dup = f"{base_path}/folder1/data.txt"  # This IS a duplicate
    path_new = f"{base_path}/folder2/data.txt"  # This is NOT a duplicate

    print_info("Step 2: Check duplicates for batch upload")
    print_info(f"  - {path_dup} (should be duplicate)")
    print_info(f"  - {path_new} (should NOT be duplicate)")

    out = OutputFormatter(json_output=False, quiet=True)
    handler = DuplicateHandler(
        client=client,
        out=out,
        workspace_id=workspace_id,
        on_duplicate="skip",
        parent_id=None,
    )

    files_to_upload = [
        (temp_file2, path_dup),
        (temp_file3, path_new),
    ]
    handler.validate_and_handle_duplicates(files_to_upload)

    test_result = {
        "path_dup": path_dup,
        "path_new": path_new,
        "files_to_skip": list(handler.files_to_skip),
        "duplicate_rel_paths": dict(handler._duplicate_rel_paths),
        "issues": [],
    }

    # Check: path_dup should be in files_to_skip (it's a real duplicate)
    # Check: path_new should NOT be in files_to_skip (different folder)

    if path_new in handler.files_to_skip:
        msg = f"BUG: '{path_new}' incorrectly marked as duplicate"
        print_warning(f"  {msg}")
        test_result["issues"].append(msg)
        results["issues_found"] += 1
    else:
        print_success(f"  Correctly: '{path_new}' NOT marked as duplicate")

    # path_dup might or might not be marked depending on server behavior
    if path_dup in handler.files_to_skip:
        print_success(f"  Correctly: '{path_dup}' WAS marked as duplicate")
    else:
        print_info(f"  '{path_dup}' not marked (server may not report)")

    results["tests"].append(test_result)

    # Cleanup
    if file_id1:
        try:
            client.delete_file_entries([file_id1], delete_forever=True)
        except Exception:
            pass

    cleanup_file(temp_file1)
    cleanup_file(temp_file2)
    cleanup_file(temp_file3)
    return results


def test_nfc_nfd_folder_handling(client: DrimeClient, workspace_id: int) -> dict:
    """Test handling of NFC vs NFD normalized folder names.

    Scenario: folder_ü (NFC) vs folder_u\u0308 (NFD) should be treated as same
    by server but DuplicateHandler should handle this correctly.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: NFC vs NFD Folder Handling", "-")

    unique_id = uuid.uuid4().hex[:8]
    results = {"tests": [], "issues_found": 0}

    # Create NFC and NFD versions of 'ü'
    nfc_char = "\u00fc"  # ü as single codepoint
    nfd_char = "\u0075\u0308"  # u + combining diaeresis

    base_path = f"nfc_nfd_test_{unique_id}"
    nfc_path = f"{base_path}/folder_{nfc_char}/file.txt"
    nfd_path = f"{base_path}/folder_{nfd_char}/file.txt"

    print_info("Unicode analysis:")
    print_info(f"  NFC: {repr(nfc_char)} - bytes: {nfc_char.encode('utf-8').hex()}")
    print_info(f"  NFD: {repr(nfd_char)} - bytes: {nfd_char.encode('utf-8').hex()}")
    print_info(f"  NFC == NFD: {nfc_char == nfd_char}")
    print_info(
        f"  normalize(NFC, NFD) == NFC: "
        f"{unicodedata.normalize('NFC', nfd_char) == nfc_char}"
    )

    # Upload NFC version first
    temp_file1 = create_test_file(content=b"NFC folder content")

    print_info(f"\nStep 1: Upload to NFC folder: {repr(nfc_path)}")
    file_id1 = None
    try:
        result = client.upload_file(
            file_path=temp_file1,
            relative_path=nfc_path,
            workspace_id=workspace_id,
        )
        file_id1 = result.get("fileEntry", {}).get("id")
        parent_id1 = result.get("fileEntry", {}).get("parent_id")
        print_success(f"  Uploaded (id={file_id1}, parent={parent_id1})")
    except Exception as e:
        print_error(f"  Failed to upload: {e}")
        cleanup_file(temp_file1)
        return {"error": str(e)}

    time.sleep(1)

    # Now check if NFD path is considered duplicate
    temp_file2 = create_test_file(content=b"NFD folder content")

    print_info(f"\nStep 2: Check duplicates for NFD folder: {repr(nfd_path)}")

    out = OutputFormatter(json_output=False, quiet=True)
    handler = DuplicateHandler(
        client=client,
        out=out,
        workspace_id=workspace_id,
        on_duplicate="skip",
        parent_id=None,
    )

    files_to_upload = [(temp_file2, nfd_path)]
    handler.validate_and_handle_duplicates(files_to_upload)

    test_result = {
        "nfc_path": nfc_path,
        "nfd_path": nfd_path,
        "files_to_skip": list(handler.files_to_skip),
        "duplicate_rel_paths": dict(handler._duplicate_rel_paths),
    }

    # If server normalizes, NFD might be considered duplicate of NFC
    # This is expected server behavior
    if nfd_path in handler.files_to_skip:
        print_info(
            "  NFD path marked as duplicate (server normalizes Unicode - expected)"
        )
    else:
        print_info("  NFD path NOT marked as duplicate")

    results["tests"].append(test_result)

    # Cleanup
    if file_id1:
        try:
            client.delete_file_entries([file_id1], delete_forever=True)
        except Exception:
            pass

    cleanup_file(temp_file1)
    cleanup_file(temp_file2)
    return results


def test_duplicate_rel_paths_mapping(client: DrimeClient, workspace_id: int) -> dict:
    """Test that _duplicate_rel_paths is correctly populated.

    This internal mapping should track which specific files have duplicates.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: _duplicate_rel_paths Mapping", "-")

    unique_id = uuid.uuid4().hex[:8]
    results = {"mapping_correct": False, "details": {}}

    # Create an existing file
    base_path = f"relpath_test_{unique_id}"
    existing_path = f"{base_path}/existing/report.txt"
    temp_file1 = create_test_file(content=b"Existing report")

    print_info(f"Step 1: Upload file to '{existing_path}'")
    file_id1 = None
    try:
        result = client.upload_file(
            file_path=temp_file1,
            relative_path=existing_path,
            workspace_id=workspace_id,
        )
        file_id1 = result.get("fileEntry", {}).get("id")
        print_success(f"  Uploaded (id={file_id1})")
    except Exception as e:
        print_error(f"  Failed: {e}")
        cleanup_file(temp_file1)
        return {"error": str(e)}

    time.sleep(1)

    # Prepare multiple files, only one should be a duplicate
    temp_dup = create_test_file(content=b"Duplicate of existing")
    temp_new1 = create_test_file(content=b"New file 1")
    temp_new2 = create_test_file(content=b"New file 2")

    dup_path = f"{base_path}/existing/report.txt"
    new_path1 = f"{base_path}/new_folder/report.txt"
    new_path2 = f"{base_path}/another/data.txt"

    print_info("\nStep 2: Check _duplicate_rel_paths for batch")
    print_info(f"  - {dup_path} (actual duplicate)")
    print_info(f"  - {new_path1} (same name, different folder)")
    print_info(f"  - {new_path2} (different name)")

    out = OutputFormatter(json_output=False, quiet=True)
    handler = DuplicateHandler(
        client=client,
        out=out,
        workspace_id=workspace_id,
        on_duplicate="skip",
        parent_id=None,
    )

    files_to_upload = [
        (temp_dup, dup_path),
        (temp_new1, new_path1),
        (temp_new2, new_path2),
    ]
    handler.validate_and_handle_duplicates(files_to_upload)

    results["details"] = {
        "files_to_skip": list(handler.files_to_skip),
        "duplicate_rel_paths": dict(handler._duplicate_rel_paths),
    }

    print_info("\nResults:")
    print_info(f"  files_to_skip: {handler.files_to_skip}")
    print_info(f"  _duplicate_rel_paths: {handler._duplicate_rel_paths}")

    # Verify the mapping is correct
    # Only dup_path should be in _duplicate_rel_paths (if server reports duplicate)
    if handler._duplicate_rel_paths:
        if dup_path in handler._duplicate_rel_paths:
            print_success("  Correctly mapped duplicate path")
            results["mapping_correct"] = True
        if new_path1 in handler._duplicate_rel_paths:
            print_warning("  BUG: new_path1 incorrectly in mapping")
        if new_path2 in handler._duplicate_rel_paths:
            print_warning("  BUG: new_path2 incorrectly in mapping")
    else:
        print_info("  _duplicate_rel_paths is empty (server may not report)")

    # Cleanup
    if file_id1:
        try:
            client.delete_file_entries([file_id1], delete_forever=True)
        except Exception:
            pass

    cleanup_file(temp_file1)
    cleanup_file(temp_dup)
    cleanup_file(temp_new1)
    cleanup_file(temp_new2)
    return results


def summarize_results(all_results: dict) -> None:
    """Print a summary of all test results."""
    print_separator("TEST SUMMARY", "=")

    total_issues = 0

    for test_name, results in all_results.items():
        print(f"\n{test_name}:")
        if isinstance(results, dict):
            if "error" in results:
                print(f"  ERROR: {results['error']}")
                total_issues += 1
            elif "issues_found" in results:
                count = results["issues_found"]
                total_issues += count
                status = "ISSUES FOUND" if count > 0 else "OK"
                print(f"  Issues: {count} ({status})")
            elif "mapping_correct" in results:
                status = "OK" if results["mapping_correct"] else "NEEDS REVIEW"
                print(f"  Mapping: {status}")

    print(f"\n{'=' * 80}")
    if total_issues > 0:
        print(f"TOTAL ISSUES FOUND: {total_issues}")
        print("\nThe DuplicateHandler has bugs in path-aware duplicate detection.")
    else:
        print("All DuplicateHandler tests passed!")
    print(f"{'=' * 80}")


def main():
    """Main benchmark function."""
    print_separator("PYDRIME DUPLICATE HANDLER BENCHMARK", "=")

    print_info("This benchmark tests the DuplicateHandler's ability to:")
    print_info("  - Correctly detect real duplicates (same path)")
    print_info("  - NOT flag false duplicates (same name, different folder)")
    print_info("  - Handle Unicode folder names correctly (u vs ü)")
    print_info("  - Track duplicate paths properly via _duplicate_rel_paths")

    # Initialize client
    print_info("\nInitializing API client...")
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
        # Run tests
        all_results["Unicode Folder Duplicate Detection"] = (
            test_unicode_folder_duplicate_detection(client, workspace_id)
        )

        all_results["Same Folder Real Duplicate"] = test_same_folder_real_duplicate(
            client, workspace_id
        )

        all_results["Multiple Files Same Name"] = (
            test_multiple_files_same_name_different_folders(client, workspace_id)
        )

        all_results["NFC vs NFD Folder Handling"] = test_nfc_nfd_folder_handling(
            client, workspace_id
        )

        all_results["Duplicate Rel Paths Mapping"] = test_duplicate_rel_paths_mapping(
            client, workspace_id
        )

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
