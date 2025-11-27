"""
Benchmark script to test parallel file uploads using threading.

This script creates small files and uploads them in parallel to test
server-side handling of concurrent upload requests.

Supports different upload APIs:
- uploads: Direct upload to /uploads endpoint (default)
- presign: Presigned URL upload via S3
- multipart: Chunked multipart upload

Usage:
    python benchmarks/benchmark_parallel_upload.py [--api uploads|presign|multipart]
"""

import argparse
import logging
import mimetypes
import os
import shutil
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Literal, Optional

import httpx

# Configure logging BEFORE imports to capture all debug messages
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

# Enable debug for all pydrime modules
logging.getLogger("pydrime").setLevel(logging.DEBUG)

from pydrime.api import DrimeClient  # noqa: E402
from pydrime.config import config  # noqa: E402
from pydrime.models import FileEntriesResult, FileEntry  # noqa: E402

logger = logging.getLogger(__name__)

# Type aliases
ApiMode = Literal["uploads", "presign", "multipart"]


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


def create_test_files(directory: Path, count: int = 4, size_kb: int = 1) -> list[Path]:
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


def detect_mime_type(file_path: Path) -> str:
    """Detect MIME type of a file.

    Args:
        file_path: Path to the file

    Returns:
        MIME type string
    """
    mime_type, _ = mimetypes.guess_type(str(file_path))
    return mime_type or "application/octet-stream"


def upload_file_direct(
    file_path: Path,
    api_url: str,
    api_key: str,
    relative_path: str,
    workspace_id: int,
) -> dict[str, Any]:
    """Upload file using direct /uploads endpoint.

    Args:
        file_path: Path to the file to upload
        api_url: API base URL
        api_key: API key for authentication
        relative_path: Relative path for the upload
        workspace_id: Workspace ID

    Returns:
        Upload response data
    """
    url = f"{api_url}/uploads"
    mime_type = detect_mime_type(file_path)

    with open(file_path, "rb") as f:
        files = {"file": (file_path.name, f, mime_type)}
        data: dict[str, Any] = {}

        if relative_path:
            data["relativePath"] = relative_path
        if workspace_id:
            data["workspaceId"] = str(workspace_id)

        headers = {"Authorization": f"Bearer {api_key}"}

        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, files=files, data=data, headers=headers)
            response.raise_for_status()
            return response.json()


def upload_file_presign(
    file_path: Path,
    api_url: str,
    api_key: str,
    relative_path: str,
    workspace_id: int,
) -> dict[str, Any]:
    """Upload file using presigned URL method.

    Args:
        file_path: Path to the file to upload
        api_url: API base URL
        api_key: API key for authentication
        relative_path: Relative path for the upload
        workspace_id: Workspace ID

    Returns:
        Upload response data
    """
    file_size = file_path.stat().st_size
    mime_type = detect_mime_type(file_path)
    extension = file_path.suffix.lstrip(".") if file_path.suffix else ""

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=30.0) as client:
        # Step 1: Get presigned URL
        print_debug(f"Getting presigned URL for {file_path.name}", 2)
        presign_url = f"{api_url}/s3/simple/presign"
        presign_payload = {
            "filename": file_path.name,
            "mime": mime_type,
            "size": file_size,
            "extension": extension,
            "relativePath": relative_path,
            "workspaceId": workspace_id,
            "parentId": None,
        }

        response = client.post(
            presign_url,
            json=presign_payload,
            params={"workspaceId": workspace_id},
            headers=headers,
        )
        response.raise_for_status()
        presign_response = response.json()

        presigned_url = presign_response.get("url")
        key = presign_response.get("key")

        if not presigned_url or not key:
            raise ValueError(f"Invalid presign response: {presign_response}")

        print_debug(f"Got presigned URL, key: {key}", 2)

        # Step 2: Upload to presigned URL
        print_debug("Uploading to S3", 2)
        with open(file_path, "rb") as f:
            file_content = f.read()

        s3_headers = {
            "Content-Type": mime_type,
            "x-amz-acl": "private",
        }

        response = client.put(presigned_url, content=file_content, headers=s3_headers)
        response.raise_for_status()
        print_debug("S3 upload complete", 2)

        # Step 3: Create file entry
        print_debug("Creating file entry", 2)
        entry_url = f"{api_url}/s3/entries"
        entry_payload = {
            "clientMime": mime_type,
            "clientName": file_path.name,
            "filename": key.split("/")[-1],
            "size": file_size,
            "clientExtension": extension,
            "relativePath": relative_path,
            "workspaceId": workspace_id,
        }

        response = client.post(entry_url, json=entry_payload, headers=headers)
        response.raise_for_status()
        return response.json()


def upload_file_multipart(
    file_path: Path,
    api_url: str,
    api_key: str,
    relative_path: str,
    workspace_id: int,
    chunk_size: Optional[int] = None,
) -> dict[str, Any]:
    """Upload file using multipart upload (for large files).

    Args:
        file_path: Path to the file to upload
        api_url: API base URL
        api_key: API key for authentication
        relative_path: Relative path for the upload
        workspace_id: Workspace ID
        chunk_size: Size of each chunk in bytes (default: auto-calculated)

    Returns:
        Upload response data
    """
    import math

    file_size = file_path.stat().st_size
    file_name = file_path.name
    extension = file_path.suffix.lstrip(".")
    mime_type = detect_mime_type(file_path)

    # Calculate chunk size
    if chunk_size is None:
        default_chunk_size = 25 * 1024 * 1024  # 25MB default

        if file_size <= default_chunk_size:
            chunk_size = file_size
        else:
            chunk_size = default_chunk_size

    num_parts = math.ceil(file_size / chunk_size) if chunk_size > 0 else 1

    print_debug(
        f"Multipart upload: {file_name} ({file_size} bytes, "
        f"{num_parts} parts, chunk_size={chunk_size})",
        2,
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=60.0) as client:
        # Step 1: Initialize multipart upload
        init_data = {
            "filename": file_name,
            "mime": mime_type,
            "size": file_size,
            "extension": extension,
            "relativePath": relative_path,
            "workspaceId": workspace_id,
        }

        init_url = f"{api_url}/s3/multipart/create"
        response = client.post(init_url, json=init_data, headers=headers)
        response.raise_for_status()
        init_response = response.json()

        upload_id = init_response.get("uploadId")
        key = init_response.get("key")

        if not upload_id or not key:
            raise ValueError("Failed to initialize multipart upload")

        print_debug(f"Multipart upload initialized: uploadId={upload_id}", 2)

        uploaded_parts = []

        try:
            with open(file_path, "rb") as f:
                part_number = 1

                while part_number <= num_parts:
                    # Request signed URLs for batch of parts
                    batch_size = min(10, num_parts - part_number + 1)
                    part_numbers = list(range(part_number, part_number + batch_size))

                    sign_url = f"{api_url}/s3/multipart/batch-sign-part-urls"
                    sign_response = client.post(
                        sign_url,
                        json={
                            "key": key,
                            "uploadId": upload_id,
                            "partNumbers": part_numbers,
                        },
                        headers=headers,
                    )
                    sign_response.raise_for_status()
                    urls_list = sign_response.json().get("urls", [])
                    signed_urls = {u["partNumber"]: u["url"] for u in urls_list}

                    # Upload each part
                    for pn in part_numbers:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break

                        signed_url = signed_urls.get(pn)
                        if not signed_url:
                            raise ValueError(f"No signed URL for part {pn}")

                        # Upload chunk to S3
                        chunk_headers = {
                            "Content-Type": "application/octet-stream",
                            "Content-Length": str(len(chunk)),
                        }

                        put_response = client.put(
                            signed_url, content=chunk, headers=chunk_headers
                        )
                        put_response.raise_for_status()

                        etag = put_response.headers.get("ETag", "").strip('"')
                        uploaded_parts.append(
                            {
                                "PartNumber": pn,
                                "ETag": etag,
                            }
                        )

                        print_debug(f"Uploaded part {pn}/{num_parts}", 3)

                    part_number += batch_size

            # Step 2: Complete multipart upload
            complete_url = f"{api_url}/s3/multipart/complete"
            complete_response = client.post(
                complete_url,
                json={
                    "key": key,
                    "uploadId": upload_id,
                    "parts": uploaded_parts,
                },
                headers=headers,
            )
            complete_response.raise_for_status()

            print_debug("Multipart upload completed", 2)

            # Step 3: Create file entry
            entry_url = f"{api_url}/s3/entries"
            entry_response = client.post(
                entry_url,
                json={
                    "clientMime": mime_type,
                    "clientName": file_name,
                    "filename": key.split("/")[-1],
                    "size": file_size,
                    "clientExtension": extension,
                    "relativePath": relative_path,
                    "workspaceId": workspace_id,
                },
                headers=headers,
            )
            entry_response.raise_for_status()

            print_debug("File entry created", 2)

            return entry_response.json()

        except Exception as e:
            # Abort upload on error
            try:
                abort_url = f"{api_url}/s3/multipart/abort"
                client.post(
                    abort_url,
                    json={"key": key, "uploadId": upload_id},
                    headers=headers,
                )
            except Exception:
                pass
            raise ValueError(f"Multipart upload failed: {e}") from e


def upload_file(
    file_path: Path,
    api_url: str,
    api_key: str,
    relative_path: str,
    workspace_id: int,
    api_mode: ApiMode = "uploads",
) -> dict[str, Any]:
    """Upload file using specified API mode.

    Args:
        file_path: Path to the file to upload
        api_url: API base URL
        api_key: API key for authentication
        relative_path: Relative path for the upload
        workspace_id: Workspace ID
        api_mode: API mode to use ("uploads", "presign", or "multipart")

    Returns:
        Upload response data
    """
    if api_mode == "presign":
        return upload_file_presign(
            file_path, api_url, api_key, relative_path, workspace_id
        )
    elif api_mode == "multipart":
        return upload_file_multipart(
            file_path, api_url, api_key, relative_path, workspace_id
        )
    else:
        return upload_file_direct(
            file_path, api_url, api_key, relative_path, workspace_id
        )


def upload_file_task(
    file_path: Path,
    api_url: str,
    api_key: str,
    relative_path: str,
    workspace_id: int,
    thread_id: int,
    api_mode: ApiMode = "uploads",
    start_delay: float = 0.0,
) -> dict:
    """Upload a single file (to be run in a thread).

    Args:
        file_path: Path to the file to upload
        api_url: API base URL
        api_key: API key for authentication
        relative_path: Relative path for the upload
        workspace_id: Workspace ID
        thread_id: Thread identifier for logging
        api_mode: API mode to use ("uploads", "presign", or "multipart")
        start_delay: Delay in seconds before starting the upload (staggered start)

    Returns:
        Dictionary with upload result info
    """
    thread_name = threading.current_thread().name

    # Apply staggered start delay
    if start_delay > 0:
        print_debug(
            f"[Thread {thread_id}] Waiting {start_delay:.2f}s before starting", 1
        )
        time.sleep(start_delay)

    start_time = time.time()

    print_info(
        f"[Thread {thread_id}] Starting upload: {file_path.name} (API: {api_mode})",
        1,
    )

    try:
        result = upload_file(
            file_path=file_path,
            api_url=api_url,
            api_key=api_key,
            relative_path=relative_path,
            workspace_id=workspace_id,
            api_mode=api_mode,
        )

        elapsed = time.time() - start_time
        status = result.get("status", "unknown")

        print_success(
            f"[Thread {thread_id}] Uploaded {file_path.name} "
            f"in {elapsed:.2f}s - status: {status}"
        )

        return {
            "file": file_path.name,
            "thread_id": thread_id,
            "thread_name": thread_name,
            "status": status,
            "elapsed": elapsed,
            "success": True,
            "result": result,
        }

    except Exception as e:
        elapsed = time.time() - start_time
        print_error(f"[Thread {thread_id}] Failed to upload {file_path.name}: {e}")

        return {
            "file": file_path.name,
            "thread_id": thread_id,
            "thread_name": thread_name,
            "status": "error",
            "elapsed": elapsed,
            "success": False,
            "error": str(e),
        }


def parallel_upload(
    files: list[Path],
    api_url: str,
    api_key: str,
    remote_folder: str,
    workspace_id: int,
    max_workers: int = 1,
    api_mode: ApiMode = "uploads",
    start_delay: float = 0.0,
    client: Optional[DrimeClient] = None,
) -> list[dict]:
    """Upload multiple files in parallel using ThreadPoolExecutor.

    Args:
        files: List of file paths to upload
        api_url: API base URL
        api_key: API key for authentication
        remote_folder: Remote folder name
        workspace_id: Workspace ID
        max_workers: Maximum number of parallel workers
        api_mode: API mode to use ("uploads", "presign", or "multipart")
        start_delay: Delay in seconds between starting each upload (staggered start)
        client: Optional DrimeClient instance for pre-creating folder

    Returns:
        List of upload results
    """
    print_separator(f"Parallel Upload ({max_workers} workers, API: {api_mode})", "-")
    print_info(f"Files to upload: {len(files)}")
    print_info(f"Remote folder: /{remote_folder}")
    print_info(f"API mode: {api_mode}")
    if start_delay > 0:
        print_info(f"Start delay between uploads: {start_delay}s")

    # Pre-create the remote folder to avoid race conditions when multiple
    # uploads try to create the same parent folder simultaneously
    if client is not None and max_workers > 1:
        print_info("Pre-creating remote folder to avoid race conditions...")
        try:
            result = client.create_folder(name=remote_folder, parent_id=None)
            if result.get("status") == "success":
                folder_id = result.get("folder", {}).get("id")
                print_success(f"Created folder '{remote_folder}' (id={folder_id})")
            else:
                print_warning(f"Folder creation returned: {result.get('status')}")
        except Exception as e:
            print_warning(f"Could not pre-create folder: {e}")

    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all upload tasks
        futures = {}
        for i, file_path in enumerate(files):
            relative_path = f"{remote_folder}/{file_path.name}"
            # Calculate staggered delay for this thread
            thread_delay = i * start_delay
            future = executor.submit(
                upload_file_task,
                file_path,
                api_url,
                api_key,
                relative_path,
                workspace_id,
                i,
                api_mode,
                thread_delay,
            )
            futures[future] = file_path

        # Collect results as they complete
        for future in as_completed(futures):
            file_path = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print_error(f"Exception getting result for {file_path.name}: {e}")
                results.append(
                    {
                        "file": file_path.name,
                        "status": "exception",
                        "success": False,
                        "error": str(e),
                    }
                )

    total_elapsed = time.time() - start_time
    print_separator("Upload Summary", "-")
    print_info(f"Total time: {total_elapsed:.2f}s")

    successful = sum(1 for r in results if r.get("success", False))
    failed = len(results) - successful

    print_info(f"Successful: {successful}/{len(files)}")
    if failed > 0:
        print_warning(f"Failed: {failed}/{len(files)}")

    return results


def cleanup_remote_folder(
    client: DrimeClient,
    folder_name: str,
    workspace_id: int,
) -> bool:
    """Delete remote folder permanently.

    Args:
        client: DrimeClient instance
        folder_name: Folder name to delete
        workspace_id: Workspace ID

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


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Benchmark parallel file uploads",
        allow_abbrev=False,  # Disable prefix matching to catch typos like --max-worker
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=None,
        help="Number of files to create and upload (default: same as --max-workers)",
    )
    parser.add_argument(
        "--file-size",
        type=int,
        default=1,
        help="Size of each file in KB (default: 1)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Maximum number of parallel workers (default: 1)",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't cleanup remote folder after upload",
    )
    parser.add_argument(
        "--start-delay",
        type=float,
        default=0.0,
        help="Delay in seconds between starting each upload (default: 0.0)",
    )
    parser.add_argument(
        "--api",
        choices=["uploads", "presign", "multipart"],
        default="uploads",
        help="Upload API mode: 'uploads' for direct, 'presign' for presigned URL, "
        "'multipart' for chunked upload",
    )
    return parser.parse_args()


def main():  # noqa: C901
    """Main benchmark function."""
    args = parse_args()

    api_mode: ApiMode = args.api
    num_files = args.num_files
    file_size_kb = args.file_size
    max_workers = args.max_workers
    start_delay = args.start_delay

    # If num_files not explicitly set, use max_workers as default
    if num_files is None:
        num_files = max_workers

    print_separator("PYDRIME PARALLEL UPLOAD BENCHMARK", "=")

    # Generate unique test folder name
    test_uuid = str(uuid.uuid4())
    remote_folder = f"parallel_upload_test_{test_uuid[:8]}"

    print_info(f"Test UUID: {test_uuid}")
    print_info(f"Remote folder: {remote_folder}")
    print_info(f"Number of files: {num_files}")
    print_info(f"File size: {file_size_kb}KB each")
    print_info(f"Max workers: {max_workers}")
    print_info(f"API mode: {api_mode}")
    if start_delay > 0:
        print_info(f"Start delay: {start_delay}s between each upload")

    # Create local base directory
    base_dir = Path.cwd() / f"parallel_upload_temp_{test_uuid[:8]}"
    base_dir.mkdir(parents=True, exist_ok=True)
    print_info(f"Local base directory: {base_dir}")

    # Get API configuration
    api_url = config.api_url
    api_key = config.api_key

    if not api_key:
        print_error("API key not configured. Set DRIME_API_KEY environment variable.")
        sys.exit(1)

    print_info(f"API URL: {api_url}")

    # Initialize client for non-upload operations
    workspace_id = 0  # Personal workspace
    print_info(f"Workspace ID: {workspace_id}")

    client = None

    try:
        print_separator("Initializing API Client", "-")
        client = DrimeClient()
        print_success("DrimeClient initialized")

        # Verify connection
        print_info("Verifying API connection...")
        user_info = client.get_logged_user()
        if user_info and user_info.get("user"):
            user = user_info["user"]
            print_success(f"Connected as: {user.get('email', 'unknown')}")
        else:
            print_error("Could not verify API connection")
            sys.exit(1)

        # Create test files
        test_files = create_test_files(base_dir, count=num_files, size_kb=file_size_kb)

        # Perform parallel upload
        print_separator(f"Starting Parallel Upload Test (API: {api_mode})", "=")
        results = parallel_upload(
            test_files,
            api_url,
            api_key,
            remote_folder,
            workspace_id,
            max_workers=max_workers,
            api_mode=api_mode,
            start_delay=start_delay,
            client=client,
        )

        # Build file size map for speed calculations
        file_sizes = {f.name: f.stat().st_size for f in test_files}

        # Analyze results and build detailed metrics
        print_separator("Results Analysis", "=")
        upload_metrics = []

        for result in results:
            file_name = result.get("file", "unknown")
            elapsed = result.get("elapsed", 0)
            success = result.get("success", False)
            thread_id = result.get("thread_id", "?")
            file_size = file_sizes.get(file_name, 0)

            # Calculate speed in KB/s
            speed_kbps = (file_size / 1024) / elapsed if elapsed > 0 else 0

            # Check if upload was truly successful (has fileEntry with size > 0)
            verified = False
            has_users = False
            server_size = 0

            if success and result.get("result"):
                file_entry_data = result["result"].get("fileEntry", {})
                server_size = file_entry_data.get("file_size", 0)
                users = file_entry_data.get("users", [])
                has_users = len(users) > 0
                verified = server_size > 0 and has_users

            upload_metrics.append(
                {
                    "file": file_name,
                    "thread_id": thread_id,
                    "size_bytes": file_size,
                    "elapsed": elapsed,
                    "speed_kbps": speed_kbps,
                    "success": success,
                    "verified": verified,
                    "server_size": server_size,
                    "has_users": has_users,
                    "error": result.get("error"),
                }
            )

            if success:
                verified_status = "VERIFIED" if verified else "UNVERIFIED"
                print_success(
                    f"  [{thread_id}] {file_name}: OK "
                    f"({elapsed:.2f}s, {speed_kbps:.1f} KB/s, {verified_status})"
                )
            else:
                error = result.get("error", "unknown error")
                print_error(f"  [{thread_id}] {file_name}: FAILED - {error}")

        # Wait for API to process
        print_info("Waiting 2 seconds for API to process uploads...")
        time.sleep(2)

        # Print file entry info including users field
        print_separator("File Entry Info (from upload results)", "=")
        for result in results:
            if result.get("success") and result.get("result"):
                upload_result = result["result"]
                file_entry_data = upload_result.get("fileEntry")
                if file_entry_data:
                    file_entry = FileEntry.from_dict(file_entry_data)
                    print_info(f"File: {file_entry.name}")
                    print_info(f"  ID: {file_entry.id}", 1)
                    print_info(f"  Hash: {file_entry.hash}", 1)
                    print_info(f"  Size: {file_entry.format_size()}", 1)
                    print_info(f"  Type: {file_entry.type}", 1)
                    print_info(f"  Parent ID: {file_entry.parent_id}", 1)

                    # Print users field
                    print_info(f"  Users ({len(file_entry.users)}):", 1)
                    if file_entry.users:
                        for user in file_entry.users:
                            print_info(f"    - ID: {user.id}", 2)
                            print_info(f"      Email: {user.email}", 2)
                            print_info(f"      Owns Entry: {user.owns_entry}", 2)
                            print_info(f"      Display Name: {user.display_name}", 2)
                            print_info(
                                f"      Entry Permissions: {user.entry_permissions}", 2
                            )
                    else:
                        print_info("    (no users)", 2)

                    # Also print raw users data for debugging
                    print_debug(f"  Raw users data: {file_entry_data.get('users')}", 1)
                else:
                    print_warning(
                        f"No fileEntry in result for {result.get('file', 'unknown')}"
                    )

        # Get the folder hash from upload results to query files properly
        folder_hash = None
        for result in results:
            if result.get("success") and result.get("result"):
                upload_result = result["result"]
                file_entry_data = upload_result.get("fileEntry")
                if file_entry_data:
                    # Get parent folder hash from the uploaded file
                    folder_hash = file_entry_data.get("parent", {}).get("hash")
                    if folder_hash:
                        break

        # Also fetch file entries from server to get updated info
        print_separator("File Entries from Server (after upload)", "=")
        try:
            if folder_hash:
                print_info(f"Fetching files from folder hash: {folder_hash}")
                server_result = client.get_file_entries(
                    page_id=folder_hash,
                    folder_id=folder_hash,
                    workspace_id=workspace_id,
                    backup=0,
                    order_by="updated_at",
                    order_dir="desc",
                    page=1,
                    per_page=50,
                )
            else:
                print_warning("No folder hash found, falling back to query search")
                server_result = client.get_file_entries(
                    query=remote_folder,
                    workspace_id=workspace_id,
                    per_page=50,
                )

            # Update metrics with server-side verification
            server_entries = {}
            if server_result and server_result.get("data"):
                entries = FileEntriesResult.from_api_response(server_result)
                print_info(f"Found {len(entries.entries)} entries on server")

                for entry in entries.entries:
                    server_entries[entry.name] = {
                        "size": entry.file_size,
                        "has_users": len(entry.users) > 0,
                        "users_count": len(entry.users),
                    }
                    print_info(f"File: {entry.name}")
                    print_info(f"  ID: {entry.id}", 1)
                    print_info(f"  Size: {entry.file_size} bytes", 1)
                    print_info(f"  Type: {entry.type}", 1)
                    print_info(f"  Users ({len(entry.users)}):", 1)
                    if entry.users:
                        for user in entry.users:
                            print_info(f"    - ID: {user.id}", 2)
                            print_info(f"      Email: {user.email}", 2)
                            print_info(f"      Owns Entry: {user.owns_entry}", 2)
                    else:
                        print_info("    (no users)", 2)

                # Update metrics with server verification
                for metric in upload_metrics:
                    server_info = server_entries.get(metric["file"])
                    if server_info:
                        metric["server_size"] = server_info["size"]
                        metric["has_users"] = server_info["has_users"]
                        metric["verified"] = (
                            server_info["size"] > 0 and server_info["has_users"]
                        )
            else:
                print_warning("No entries found on server")
        except Exception as e:
            print_error(f"Failed to fetch entries from server: {e}")

        # =================================================================
        # SUMMARY TABLE
        # =================================================================
        print_separator("BENCHMARK RESULTS TABLE", "=")

        # Calculate summary statistics
        successful_uploads = [m for m in upload_metrics if m["success"]]
        verified_uploads = [m for m in upload_metrics if m["verified"]]
        failed_uploads = [m for m in upload_metrics if not m["success"]]

        total_time = sum(m["elapsed"] for m in successful_uploads)
        total_size_bytes = sum(m["size_bytes"] for m in successful_uploads)
        avg_time = total_time / len(successful_uploads) if successful_uploads else 0
        avg_speed = (
            sum(m["speed_kbps"] for m in successful_uploads) / len(successful_uploads)
            if successful_uploads
            else 0
        )
        total_speed_kbps = (total_size_bytes / 1024) / avg_time if avg_time > 0 else 0

        # Print individual file results table
        print("\n" + "=" * 85)
        print(
            f"{'File':<25} {'Size':>10} {'Time':>8} {'Speed':>12} "
            f"{'Status':>10} {'Verified':>10}"
        )
        print("-" * 85)

        for m in sorted(upload_metrics, key=lambda x: x["thread_id"]):
            size_str = f"{m['size_bytes'] / 1024:.1f} KB"
            time_str = f"{m['elapsed']:.2f}s"
            speed_str = f"{m['speed_kbps']:.1f} KB/s"
            status_str = "OK" if m["success"] else "FAILED"
            verified_str = "YES" if m["verified"] else "NO"

            print(
                f"{m['file']:<25} {size_str:>10} {time_str:>8} {speed_str:>12} "
                f"{status_str:>10} {verified_str:>10}"
            )

        print("=" * 85)

        # Print parameters table
        print("\n" + "=" * 60)
        print(" PARAMETERS")
        print("-" * 60)
        print(f"  {'API Mode:':<25} {api_mode}")
        print(f"  {'Max Workers:':<25} {max_workers}")
        print(f"  {'Number of Files:':<25} {num_files}")
        print(f"  {'File Size:':<25} {file_size_kb} KB")
        print(f"  {'Start Delay:':<25} {start_delay}s")
        print(f"  {'Workspace ID:':<25} {workspace_id}")
        print("=" * 60)

        # Print summary statistics table
        print("\n" + "=" * 60)
        print(" SUMMARY STATISTICS")
        print("-" * 60)
        print(f"  {'Total Files:':<25} {len(upload_metrics)}")
        print(f"  {'Successful Uploads:':<25} {len(successful_uploads)}")
        print(f"  {'Failed Uploads:':<25} {len(failed_uploads)}")
        print(
            f"  {'Verified Uploads:':<25} {len(verified_uploads)} "
            f"(size > 0 & users set)"
        )
        print("-" * 60)
        print(f"  {'Total Data Uploaded:':<25} {total_size_bytes / 1024:.1f} KB")
        print(f"  {'Total Upload Time:':<25} {total_time:.2f}s (sum of all)")
        print(f"  {'Average Time per File:':<25} {avg_time:.2f}s")
        print(f"  {'Average Speed per File:':<25} {avg_speed:.1f} KB/s")
        print(f"  {'Effective Throughput:':<25} {total_speed_kbps:.1f} KB/s")
        print("-" * 60)

        # Success rates
        upload_success_rate = (
            len(successful_uploads) / len(upload_metrics) * 100 if upload_metrics else 0
        )
        verified_success_rate = (
            len(verified_uploads) / len(upload_metrics) * 100 if upload_metrics else 0
        )
        print(f"  {'Upload Success Rate:':<25} {upload_success_rate:.1f}%")
        print(f"  {'Verified Success Rate:':<25} {verified_success_rate:.1f}%")
        print("=" * 60)

        # Final status
        if len(verified_uploads) == len(upload_metrics):
            print_success("\nAll files uploaded and verified successfully!")
        elif len(successful_uploads) == len(upload_metrics):
            print_warning(
                f"\nAll files uploaded, but only {len(verified_uploads)}/"
                f"{len(upload_metrics)} verified (size > 0 & users set)"
            )
        else:
            print_error(
                f"\n{len(failed_uploads)} file(s) failed to upload, "
                f"{len(verified_uploads)}/{len(upload_metrics)} verified"
            )

        # Cleanup
        if not args.no_cleanup:
            cleanup_remote_folder(client, remote_folder, workspace_id)
        else:
            print_info(f"Skipping cleanup. Remote folder: {remote_folder}")

    except KeyboardInterrupt:
        print_warning("\nBenchmark interrupted by user")
        if client is not None and not args.no_cleanup:
            cleanup_remote_folder(client, remote_folder, workspace_id)
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        try:
            if client is not None and not args.no_cleanup:
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
