"""CLI interface for Drime Cloud uploader."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, cast

import click
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .api import DrimeClient
from .auth import require_api_key
from .config import config
from .duplicate_handler import DuplicateHandler
from .exceptions import DrimeAPIError, DrimeNotFoundError
from .file_entries_manager import FileEntriesManager
from .models import FileEntriesResult, FileEntry, SchemaValidationWarning, UserStatus
from .output import OutputFormatter
from .progress import UploadProgressTracker
from .upload_preview import display_upload_preview
from .utils import is_file_id, normalize_to_hash
from .workspace_utils import format_workspace_display, get_folder_display_name


def parse_iso_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO format timestamp from Drime API.

    Args:
        timestamp_str: ISO format timestamp string (e.g., "2025-01-15T10:30:00.000000Z")

    Returns:
        datetime object or None if parsing fails
    """
    if not timestamp_str:
        return None

    try:
        # Handle various ISO formats
        # Remove 'Z' suffix and parse
        timestamp_str = timestamp_str.rstrip("Z")

        # Try parsing with microseconds
        try:
            return datetime.fromisoformat(timestamp_str)
        except ValueError:
            # Try without microseconds
            if "." in timestamp_str:
                timestamp_str = timestamp_str.split(".")[0]
            return datetime.fromisoformat(timestamp_str)
    except (ValueError, AttributeError):
        return None


def scan_directory(
    path: Path, base_path: Path, out: OutputFormatter
) -> list[tuple[Path, str]]:
    """Recursively scan directory and return list of (file_path, relative_path) tuples.

    Args:
        path: Directory to scan
        base_path: Base path for calculating relative paths
        out: Output formatter for warnings

    Returns:
        List of tuples containing file paths and their relative paths
        (paths use forward slashes for cross-platform compatibility)
    """
    files = []

    try:
        for item in path.iterdir():
            if item.is_file():
                # Use as_posix() to ensure forward slashes on all platforms
                relative_path = item.relative_to(base_path).as_posix()
                files.append((item, relative_path))
            elif item.is_dir():
                files.extend(scan_directory(item, base_path, out))
    except PermissionError as e:
        out.warning(f"Permission denied: {e}")

    return files


@click.group()
@click.option("--api-key", "-k", envvar="DRIME_API_KEY", help="Drime Cloud API key")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-essential output")
@click.option("--json", is_flag=True, help="Output in JSON format")
@click.option(
    "--validate-schema",
    is_flag=True,
    help="Enable API schema validation warnings (for debugging)",
)
@click.version_option()
@click.pass_context
def main(
    ctx: Any, api_key: Optional[str], quiet: bool, json: bool, validate_schema: bool
) -> None:
    """PyDrime - Upload & Download files and directories to Drime Cloud."""
    # Store settings in context for subcommands to access
    ctx.ensure_object(dict)
    ctx.obj["api_key"] = api_key
    ctx.obj["out"] = OutputFormatter(json_output=json, quiet=quiet)
    ctx.obj["validate_schema"] = validate_schema

    # Enable schema validation if flag is set
    if validate_schema:
        SchemaValidationWarning.enable()
        SchemaValidationWarning.clear_warnings()  # Clear any previous warnings


@main.command()
@click.option(
    "--api-key",
    "-k",
    prompt="Enter your Drime Cloud API key",
    help="Drime Cloud API key",
)
@click.pass_context
def init(ctx: Any, api_key: str) -> None:
    """Initialize Drime Cloud configuration.

    Stores your API key in ~/.config/pydrime/config for future use.
    """
    out: OutputFormatter = ctx.obj["out"]

    try:
        # Validate API key by attempting to use it
        out.info("Validating API key...")
        client = DrimeClient(api_key=api_key)

        # Try to make a simple API call to validate the key
        try:
            user_info = client.get_logged_user()
            # Check if user is null (invalid API key)
            if not user_info or not user_info.get("user"):
                out.error("API key validation failed: Invalid API key")
                if not click.confirm("Save API key anyway?", default=False):
                    out.warning("Configuration cancelled.")
                    ctx.exit(1)
            else:
                out.success("✓ API key is valid")
        except DrimeAPIError as e:
            out.error(f"API key validation failed: {e}")
            if not click.confirm("Save API key anyway?", default=False):
                out.warning("Configuration cancelled.")
                ctx.exit(1)

        # Save the API key
        config.save_api_key(api_key)
        config_path = config.get_config_path()

        out.print_summary(
            "Initialization Complete",
            [
                ("Status", "✓ Configuration saved successfully"),
                ("Config file", str(config_path)),
                ("Note", "You can now use drime commands without specifying --api-key"),
            ],
        )

    except Exception as e:
        out.error(f"Initialization failed: {e}")
        ctx.exit(1)


@main.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--remote-path", "-r", help="Remote destination path")
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses default workspace if not specified)",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be uploaded without uploading"
)
@click.option(
    "--on-duplicate",
    type=click.Choice(["ask", "replace", "rename", "skip"]),
    default="ask",
    help="What to do when duplicate files are detected (default: ask)",
)
@click.option(
    "--workers",
    "-j",
    type=int,
    default=1,
    help="Number of parallel workers (default: 1, use 4-8 for parallel uploads)",
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Disable progress bars",
)
@click.option(
    "--chunk-size",
    "-c",
    type=int,
    default=25,
    help="Chunk size in MB for multipart uploads (default: 25MB)",
)
@click.option(
    "--multipart-threshold",
    "-m",
    type=int,
    default=30,
    help="File size threshold in MB for using multipart upload (default: 30MB)",
)
@click.pass_context
def upload(  # noqa: C901
    ctx: Any,
    path: str,
    remote_path: Optional[str],
    workspace: Optional[int],
    dry_run: bool,
    on_duplicate: str,
    workers: int,
    no_progress: bool,
    chunk_size: int,
    multipart_threshold: int,
) -> None:
    """Upload a file or directory to Drime Cloud.

    PATH: Local file or directory to upload
    """
    out: OutputFormatter = ctx.obj["out"]
    local_path = Path(path)

    # Validate and convert MB to bytes
    if chunk_size < 5:
        out.error("Chunk size must be at least 5MB")
        ctx.exit(1)
    if chunk_size > 100:
        out.error("Chunk size cannot exceed 100MB")
        ctx.exit(1)
    if multipart_threshold < 1:
        out.error("Multipart threshold must be at least 1MB")
        ctx.exit(1)
    if chunk_size >= multipart_threshold:
        out.error("Chunk size must be smaller than multipart threshold")
        ctx.exit(1)

    chunk_size_bytes = chunk_size * 1024 * 1024
    multipart_threshold_bytes = multipart_threshold * 1024 * 1024

    # Use auth helper
    api_key = require_api_key(ctx, out)

    # Use default workspace if none specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

    # Initialize client early to check parent folder context
    client = DrimeClient(api_key=api_key)

    # Get current folder context for display
    current_folder_id = config.get_current_folder()
    current_folder_name = None

    if not out.quiet:
        # Show workspace information
        workspace_display, _ = format_workspace_display(client, workspace)
        out.info(f"Workspace: {workspace_display}")

        # Show parent folder information
        folder_display, current_folder_name = get_folder_display_name(
            client, current_folder_id
        )
        out.info(f"Parent folder: {folder_display}")

        if remote_path:
            out.info(f"Remote path structure: {remote_path}")

        out.info("")  # Empty line for readability

    # Collect files to upload
    if local_path.is_file():
        files_to_upload = [(local_path, remote_path or local_path.name)]
    else:
        out.info(f"Scanning directory: {local_path}")
        # Always use parent as base_path so the folder name is included in
        # relative paths
        base_path = local_path.parent
        files_to_upload = scan_directory(local_path, base_path, out)

        # If remote_path is specified, prepend it to all relative paths
        if remote_path:
            files_to_upload = [
                (file_path, f"{remote_path}/{rel_path}")
                for file_path, rel_path in files_to_upload
            ]

    if not files_to_upload:
        out.warning("No files found to upload.")
        return

    # Calculate total size
    total_size = sum(f[0].stat().st_size for f in files_to_upload)

    if dry_run:
        # Use the display_upload_preview function
        display_upload_preview(
            out,
            client,
            files_to_upload,
            workspace,
            current_folder_id,
            current_folder_name,
            is_dry_run=True,
        )
        out.warning("Dry run mode - no files were uploaded.")
        return

    # Display summary for actual upload (using same preview function)
    display_upload_preview(
        out,
        client,
        files_to_upload,
        workspace,
        current_folder_id,
        current_folder_name,
        is_dry_run=False,
    )

    # Validate uploads and handle duplicates
    try:
        # Use DuplicateHandler class
        dup_handler = DuplicateHandler(
            client, out, workspace, on_duplicate, current_folder_id
        )
        dup_handler.validate_and_handle_duplicates(files_to_upload)

        # Delete entries marked for replacement
        if not dup_handler.delete_marked_entries():
            ctx.exit(1)

        success_count = 0
        error_count = 0
        skipped_count = 0
        uploaded_files = []

        # Prepare files for upload (filter skipped, apply renames)
        files_to_process = []
        for file_path, rel_path in files_to_upload:
            if rel_path in dup_handler.files_to_skip:
                skipped_count += 1
                if not out.quiet:
                    out.info(f"Skipping: {rel_path}")
                continue

            upload_path = dup_handler.apply_renames(rel_path)
            files_to_process.append((file_path, upload_path, rel_path))

        # Create progress display
        if not no_progress and not out.quiet:
            progress_display = Progress(
                "[progress.description]{task.description}",
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                # Update 10 times per second for smoother speed calculation
                refresh_per_second=10,
            )
        else:
            progress_display = None

        # Create progress tracker for parallel uploads
        progress_tracker = UploadProgressTracker()

        # Helper function to upload a single file with progress
        def upload_single_file_with_progress(
            file_path: Path, upload_path: str, task_id: Optional[Any]
        ) -> Any:
            callback: Optional[Callable[[int, int], None]] = None
            if progress_display and task_id is not None:
                # Create file-specific progress callback with individual task tracking
                def file_progress_callback(
                    bytes_uploaded: int, total_bytes: int
                ) -> None:
                    # Update individual file progress
                    if progress_display:
                        progress_display.update(
                            task_id, completed=bytes_uploaded, total=total_bytes
                        )

                # Create overall progress callback using tracker
                overall_callback = progress_tracker.create_file_callback(
                    file_path, progress_display
                )

                # Combine both callbacks
                def callback_combined(bytes_uploaded: int, total_bytes: int) -> None:
                    file_progress_callback(bytes_uploaded, total_bytes)
                    overall_callback(bytes_uploaded, total_bytes)

                callback = callback_combined

            return client.upload_file(
                file_path,
                parent_id=current_folder_id,
                relative_path=upload_path,
                workspace_id=workspace,
                progress_callback=callback,
                chunk_size=chunk_size_bytes,
                use_multipart_threshold=multipart_threshold_bytes,
            )

        # Parallel or sequential upload
        try:
            if progress_display:
                progress_display.start()

            if workers > 1:
                # Parallel upload with overall progress
                total_size = sum(f[0].stat().st_size for f in files_to_process)
                total_files = len(files_to_process)

                # Define upload wrapper function (will be conditionally used)
                upload_with_task_pool: Optional[Callable[[Path, str], Any]] = None
                overall_task_id: Optional[Any] = None

                # Add overall progress bar if progress display is enabled
                if progress_display:
                    overall_task_id = progress_display.add_task(
                        f"[green]Overall Progress (0/{total_files} files)",
                        total=total_size,
                    )
                    progress_tracker.set_overall_task(overall_task_id)

                    # Create a limited pool of progress bars (one per worker)
                    # to avoid cluttering the screen
                    import queue

                    progress_task_pool = []
                    for i in range(workers):
                        task_id = progress_display.add_task(
                            f"[dim]Worker {i + 1}: Waiting...",
                            total=100,
                            visible=True,
                        )
                        progress_task_pool.append(task_id)

                    # Track available task IDs - use thread-safe queue
                    available_tasks_queue: queue.Queue[Any] = queue.Queue()
                    for task_id in progress_task_pool:
                        available_tasks_queue.put(task_id)

                    # Wrapper to handle task allocation
                    def upload_with_task_pool_impl(
                        file_path: Path, upload_path: str
                    ) -> Any:
                        # Get a task ID from the pool
                        task_id = available_tasks_queue.get()
                        try:
                            # Update task to show current file
                            file_size = file_path.stat().st_size
                            if progress_display:
                                progress_display.update(
                                    task_id,
                                    description=f"[cyan]{file_path.name}",
                                    completed=0,
                                    total=file_size,
                                )

                            # Upload the file
                            result = upload_single_file_with_progress(
                                file_path, upload_path, task_id
                            )

                            # Mark task as complete
                            if progress_display:
                                progress_display.update(
                                    task_id,
                                    description="[dim]Worker: Waiting...",
                                    completed=0,
                                    total=100,
                                )
                            return result
                        finally:
                            # Return task ID to pool
                            available_tasks_queue.put(task_id)

                    upload_with_task_pool = upload_with_task_pool_impl

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {}

                    for file_path, upload_path, rel_path in files_to_process:
                        if upload_with_task_pool:
                            future = executor.submit(
                                upload_with_task_pool, file_path, upload_path
                            )
                        else:
                            future = executor.submit(
                                upload_single_file_with_progress,
                                file_path,
                                upload_path,
                                None,
                            )
                        futures[future] = (file_path, upload_path, rel_path)

                    try:
                        for future in as_completed(futures):
                            file_path, upload_path, rel_path = futures[future]
                            try:
                                result = future.result()
                                success_count += 1

                                # Update overall progress description with file count
                                if progress_display and overall_task_id is not None:
                                    completed = success_count + error_count
                                    desc = (
                                        f"[green]Overall Progress "
                                        f"({completed}/{total_files} files)"
                                    )
                                    progress_display.update(
                                        overall_task_id,
                                        description=desc,
                                    )

                                # Extract file entry info if available
                                if isinstance(result, dict) and "fileEntry" in result:
                                    entry = result["fileEntry"]
                                    uploaded_files.append(
                                        {
                                            "path": upload_path,
                                            "id": entry.get("id"),
                                            "hash": entry.get("hash"),
                                        }
                                    )
                            except (DrimeAPIError, Exception) as e:
                                error_count += 1

                                # Update overall progress description with file count
                                if progress_display and overall_task_id is not None:
                                    completed = success_count + error_count
                                    desc = (
                                        f"[green]Overall Progress "
                                        f"({completed}/{total_files} files)"
                                    )
                                    progress_display.update(
                                        overall_task_id,
                                        description=desc,
                                    )

                                # Rollback progress for failed file
                                if progress_display:
                                    progress_tracker.rollback_file_progress(
                                        file_path, progress_display
                                    )

                                if not out.quiet:
                                    error_type = (
                                        "API error"
                                        if isinstance(e, DrimeAPIError)
                                        else "Unexpected error"
                                    )
                                    out.error(
                                        f"{error_type} uploading {file_path.name}: {e}"
                                    )
                    except KeyboardInterrupt:
                        out.warning("\nUpload interrupted by user. Cancelling...")
                        # Cancel all pending futures
                        for future in futures:
                            future.cancel()
                        raise
            else:
                # Sequential upload
                total_size = sum(f[0].stat().st_size for f in files_to_process)

                # Add overall progress bar if progress display is enabled
                # and multiple files
                if progress_display and len(files_to_process) > 1:
                    overall_task_id = progress_display.add_task(
                        "[green]Overall Progress", total=total_size
                    )
                    progress_tracker.set_overall_task(overall_task_id)

                for idx, (file_path, upload_path, rel_path) in enumerate(
                    files_to_process, 1
                ):
                    try:
                        file_size = file_path.stat().st_size
                        size_str = out.format_size(file_size)
                        progress_str = f"[{idx}/{len(files_to_process)}]"

                        display_path = (
                            upload_path if upload_path != rel_path else rel_path
                        )

                        if progress_display:
                            task_id = progress_display.add_task(
                                f"[cyan]{file_path.name} {progress_str}",
                                total=file_size,
                            )
                        else:
                            task_id = None
                            out.progress_message(
                                f"Uploading {display_path} ({size_str}) {progress_str}"
                            )

                        result = upload_single_file_with_progress(
                            file_path, upload_path, task_id
                        )
                        success_count += 1

                        # Extract file entry info if available
                        if isinstance(result, dict) and "fileEntry" in result:
                            entry = result["fileEntry"]
                            uploaded_files.append(
                                {
                                    "path": upload_path,
                                    "id": entry.get("id"),
                                    "hash": entry.get("hash"),
                                }
                            )

                    except DrimeAPIError as e:
                        out.error(f"Error uploading {upload_path}: {e}")
                        error_count += 1
        finally:
            if progress_display:
                progress_display.stop()

        # Show summary
        if out.json_output:
            out.output_json(
                {
                    "success": success_count,
                    "failed": error_count,
                    "skipped": skipped_count,
                    "files": uploaded_files,
                }
            )
        else:
            summary_items = [
                ("Successfully uploaded", f"{success_count} files"),
            ]
            if skipped_count > 0:
                summary_items.append(("Skipped", f"{skipped_count} files"))
            if error_count > 0:
                summary_items.append(("Failed", f"{error_count} files"))

            out.print_summary("Upload Complete", summary_items)

        if error_count > 0:
            ctx.exit(1)

    except KeyboardInterrupt:
        out.warning("\nUpload cancelled by user")
        ctx.exit(130)  # Standard exit code for SIGINT
    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)


@main.command()
@click.argument("parent_identifier", type=str, required=False, default=None)
@click.option("--deleted", "-d", is_flag=True, help="Show deleted files")
@click.option("--starred", "-s", is_flag=True, help="Show starred files")
@click.option("--recent", "-r", is_flag=True, help="Show recent files")
@click.option("--shared", "-S", is_flag=True, help="Show shared files")
@click.option(
    "--page", "-p", type=str, help="Display files in specified folder hash/page"
)
@click.option("--workspace", "-w", type=int, default=None, help="Workspace ID")
@click.option("--query", "-q", help="Search by name")
@click.option(
    "--type",
    "-t",
    type=click.Choice(["folder", "image", "text", "audio", "video", "pdf"]),
    help="Filter by file type",
)
@click.option("--recursive", is_flag=True, help="List files recursively")
@click.pass_context
def ls(  # noqa: C901
    ctx: Any,
    parent_identifier: Optional[str],
    deleted: bool,
    starred: bool,
    recent: bool,
    shared: bool,
    page: Optional[str],
    workspace: Optional[int],
    query: Optional[str],
    type: Optional[str],
    recursive: bool,
) -> None:
    """List files and folders in a Drime Cloud directory.

    PARENT_IDENTIFIER: ID or name of parent folder (omit to list current directory)

    Similar to Unix ls command, shows file and folder names in a columnar format.
    Use 'du' command for detailed disk usage information.

    Examples:
        pydrime ls                  # List current directory
        pydrime ls 480432024        # List folder by ID
        pydrime ls test_folder      # List folder by name
        pydrime ls Documents        # List folder by name
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Use default workspace if none specified
        if workspace is None:
            workspace = config.get_default_workspace() or 0

        # Resolve parent_identifier to parent_id
        parent_id = None
        if parent_identifier is not None:
            # Resolve identifier (ID or name) to folder ID
            current_folder = config.get_current_folder()
            parent_id = client.resolve_folder_identifier(
                identifier=parent_identifier,
                parent_id=current_folder,
                workspace_id=workspace,
            )
            if not out.quiet and not parent_identifier.isdigit():
                out.info(f"Resolved '{parent_identifier}' to folder ID: {parent_id}")
        elif not any([deleted, starred, recent, shared, page, query]):
            # If no parent_identifier specified, use current working directory
            parent_id = config.get_current_folder()

        # Build parameters for API call
        params: dict[str, Any] = {
            "deleted_only": deleted or None,
            "starred_only": starred or None,
            "recent_only": recent or None,
            "shared_only": shared or None,
            "query": query,
            "entry_type": type,
            "workspace_id": workspace,
            "folder_id": page,
            "page_id": page,
        }

        # Add parent_id if specified
        if parent_id is not None:
            params["parent_ids"] = [parent_id]

        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}

        result = client.get_file_entries(**params)

        # Parse the response into our data model
        file_entries = FileEntriesResult.from_api_response(result)

        # If recursive, we need to get entries from subfolders too
        if recursive:
            all_entries = list(file_entries.entries)
            folders_to_process = [e for e in file_entries.entries if e.is_folder]

            while folders_to_process:
                folder = folders_to_process.pop(0)
                try:
                    sub_result = client.get_file_entries(parent_ids=[folder.id])
                    sub_entries = FileEntriesResult.from_api_response(sub_result)
                    all_entries.extend(sub_entries.entries)
                    # Add subfolders to the list to process
                    folders_to_process.extend(
                        [e for e in sub_entries.entries if e.is_folder]
                    )
                except DrimeAPIError:
                    # Skip folders we can't access
                    pass

            # Update file_entries with all collected entries
            file_entries.entries = all_entries

        # Output based on format
        if out.json_output:
            out.output_json(file_entries.to_dict())
            return

        if file_entries.is_empty:
            # For empty directory, output nothing (like Unix ls)
            return

        # Text format - simple list of names (like Unix ls)
        table_data = file_entries.to_table_data()
        out.output_table(
            table_data,
            ["name"],
            {"name": "Name"},
        )

    except DrimeNotFoundError as e:
        out.error(str(e))
        ctx.exit(1)
    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("parent_identifier", type=str, required=False, default=None)
@click.option("--deleted", "-d", is_flag=True, help="Show deleted files")
@click.option("--starred", "-s", is_flag=True, help="Show starred files")
@click.option("--recent", "-r", is_flag=True, help="Show recent files")
@click.option("--shared", "-S", is_flag=True, help="Show shared files")
@click.option(
    "--page", "-p", type=str, help="Display files in specified folder hash/page"
)
@click.option("--workspace", "-w", type=int, default=None, help="Workspace ID")
@click.option("--query", "-q", help="Search by name")
@click.option(
    "--type",
    "-t",
    type=click.Choice(["folder", "image", "text", "audio", "video", "pdf"]),
    help="Filter by file type",
)
@click.pass_context
def du(
    ctx: Any,
    parent_identifier: Optional[str],
    deleted: bool,
    starred: bool,
    recent: bool,
    shared: bool,
    page: Optional[str],
    workspace: Optional[int],
    query: Optional[str],
    type: Optional[str],
) -> None:
    """Show disk usage information for files and folders.

    PARENT_IDENTIFIER: ID or name of parent folder (omit to show current directory)

    Similar to Unix du command, shows detailed information about files and folders
    including size, type, and metadata. Folder sizes already include all files inside.
    Use 'ls' command for simple file listing.

    Examples:
        pydrime du                  # Show current directory info
        pydrime du 480432024        # Show folder by ID
        pydrime du test_folder      # Show folder by name
        pydrime du Documents        # Show folder by name
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Use default workspace if none specified
        if workspace is None:
            workspace = config.get_default_workspace() or 0

        # Resolve parent_identifier to parent_id
        parent_id = None
        if parent_identifier is not None:
            # Resolve identifier (ID or name) to folder ID
            current_folder = config.get_current_folder()
            parent_id = client.resolve_folder_identifier(
                identifier=parent_identifier,
                parent_id=current_folder,
                workspace_id=workspace,
            )
            if not out.quiet and not parent_identifier.isdigit():
                out.info(f"Resolved '{parent_identifier}' to folder ID: {parent_id}")
        elif not any([deleted, starred, recent, shared, page, query]):
            # If no parent_identifier specified, use current working directory
            parent_id = config.get_current_folder()

        # Build parameters for API call
        params: dict[str, Any] = {
            "deleted_only": deleted or None,
            "starred_only": starred or None,
            "recent_only": recent or None,
            "shared_only": shared or None,
            "query": query,
            "entry_type": type,
            "workspace_id": workspace,
            "folder_id": page,
            "page_id": page,
        }

        # Add parent_id if specified
        if parent_id is not None:
            params["parent_ids"] = [parent_id]

        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}

        result = client.get_file_entries(**params)

        # Parse the response into our data model
        file_entries = FileEntriesResult.from_api_response(result)

        # Output based on format
        if out.json_output:
            out.output_json(file_entries.to_dict())
            return

        if file_entries.is_empty:
            out.warning("No files found")
            return

        # Text format - one-liner summary for du
        out.print(file_entries.to_text_summary())

    except DrimeNotFoundError as e:
        out.error(str(e))
        ctx.exit(1)
    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("workspace_identifier", type=str, required=False)
@click.pass_context
def workspace(ctx: Any, workspace_identifier: Optional[str]) -> None:
    """Set or show the default workspace.

    WORKSPACE_IDENTIFIER: ID or name of the workspace to set as default
    (omit to show current default)

    Supports both numeric IDs and workspace names. Names are matched
    case-insensitively.

    Examples:
        pydrime workspace           # Show current default workspace
        pydrime workspace 5         # Set workspace 5 as default
        pydrime workspace 0         # Set personal workspace as default
        pydrime workspace test      # Set "test" workspace as default by name
        pydrime workspace "My Team" # Set workspace by name with spaces
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    # If no workspace_identifier provided, show current default
    if workspace_identifier is None:
        current_default = config.get_default_workspace()
        if current_default is None:
            out.info("Default workspace: Personal (0)")
        else:
            # Try to get workspace name
            workspace_name = None
            try:
                client = DrimeClient(api_key=api_key)
                result = client.get_workspaces()
                if isinstance(result, dict) and "workspaces" in result:
                    workspaces_list = result["workspaces"]
                    for ws in workspaces_list:
                        if ws.get("id") == current_default:
                            workspace_name = ws.get("name")
                            break
            except (DrimeAPIError, Exception):
                # If we can't get the name, just show the ID
                pass

            if workspace_name:
                out.info(f"Default workspace: {workspace_name} ({current_default})")
            else:
                out.info(f"Default workspace: {current_default}")
        return

    try:
        client = DrimeClient(api_key=api_key)

        # Try to parse as integer first
        workspace_id: Optional[int] = None
        if workspace_identifier.isdigit():
            workspace_id = int(workspace_identifier)
        else:
            # Try to resolve as workspace name
            result = client.get_workspaces()
            if isinstance(result, dict) and "workspaces" in result:
                workspaces_list = result["workspaces"]
                # Case-insensitive match
                workspace_identifier_lower = workspace_identifier.lower()
                for ws in workspaces_list:
                    if ws.get("name", "").lower() == workspace_identifier_lower:
                        workspace_id = ws.get("id")
                        if not out.quiet:
                            out.info(
                                f"Resolved workspace '{workspace_identifier}' "
                                f"to ID: {workspace_id}"
                            )
                        break

                if workspace_id is None:
                    out.error(
                        f"Workspace '{workspace_identifier}' not found. "
                        f"Use 'pydrime workspaces' to list available workspaces."
                    )
                    ctx.exit(1)
            else:
                out.error("Could not retrieve workspaces")
                ctx.exit(1)

        # Verify the workspace exists if not 0 (personal)
        workspace_name = None
        if workspace_id != 0:
            result = client.get_workspaces()
            if isinstance(result, dict) and "workspaces" in result:
                workspaces_list = result["workspaces"]
                workspace_ids = [ws.get("id") for ws in workspaces_list]

                if workspace_id not in workspace_ids:
                    out.error(f"Workspace {workspace_id} not found or not accessible")
                    ctx.exit(1)

                # Get workspace name for success message
                for ws in workspaces_list:
                    if ws.get("id") == workspace_id:
                        workspace_name = ws.get("name")
                        break

        # Save the default workspace (None for 0, actual ID otherwise)
        config.save_default_workspace(workspace_id if workspace_id != 0 else None)

        if workspace_id == 0:
            out.success("Set default workspace to: Personal (0)")
        else:
            if workspace_name:
                out.success(
                    f"Set default workspace to: {workspace_name} ({workspace_id})"
                )
            else:
                out.success(f"Set default workspace to: {workspace_id}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("name")
@click.option("--parent-id", "-p", type=int, help="Parent folder ID (omit for root)")
@click.pass_context
def mkdir(ctx: Any, name: str, parent_id: Optional[int]) -> None:
    """Create a directory in Drime Cloud.

    NAME: Name of the directory to create
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        result = client.create_directory(name=name, parent_id=parent_id)

        if out.json_output:
            out.output_json(result)
        else:
            out.success(f"Directory created: {name}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.pass_context
def status(ctx: Any) -> None:
    """Check API key validity and connection status.

    Verifies that your API key is valid and displays information
    about the logged-in user.
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]
    validate_schema = ctx.obj.get("validate_schema", False)

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        user_info = client.get_logged_user()

        # Check if user is null (invalid API key)
        if not user_info or not user_info.get("user"):
            out.error("Invalid API key")
            ctx.exit(1)

        # Parse the response into our data model
        user_status = UserStatus.from_api_response(user_info)

        # Output based on format
        if out.json_output:
            out.output_json(user_status.to_dict())
        else:
            out.print(user_status.to_text_summary())

        # Display schema validation warnings if enabled
        if validate_schema and SchemaValidationWarning.has_warnings():
            warnings = SchemaValidationWarning.get_warnings()
            out.warning(f"\n⚠ Schema Validation: {len(warnings)} issue(s) detected:")
            for warning in warnings:
                out.warning(f"  • {warning}")
            out.info(
                "\nThese warnings indicate the API response structure has changed."
            )
            out.info("Consider updating the data models in pydrime/models.py")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("entry_identifiers", nargs=-1, required=True)
@click.option(
    "--output", "-o", help="Output directory path (for folders or multiple files)"
)
@click.option(
    "--on-duplicate",
    "-d",
    type=click.Choice(["skip", "overwrite", "rename"], case_sensitive=False),
    default="overwrite",
    help="Action when file exists locally (default: overwrite)",
)
@click.option(
    "--workers",
    "-j",
    type=int,
    default=1,
    help="Number of parallel workers (default: 1, use 4-8 for parallel downloads)",
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Disable progress bars",
)
@click.pass_context
def download(
    ctx: Any,
    entry_identifiers: tuple[str, ...],
    output: Optional[str],
    on_duplicate: str,
    workers: int,
    no_progress: bool,
) -> None:
    """Download file(s) or folder(s) from Drime Cloud.

    ENTRY_IDENTIFIERS: One or more file/folder names, hashes, or numeric IDs

    Supports file/folder names (resolved in current directory), numeric IDs,
    and hashes. Names are resolved in the current working directory.
    Folders are automatically downloaded recursively with all their contents.

    Examples:
        pydrime download 480424796                  # Download file by ID
        pydrime download NDgwNDI0Nzk2fA             # Download file by hash
        pydrime download test1.txt                  # Download file by name
        pydrime download test_folder                # Download folder
        pydrime download 480424796 480424802        # Multiple files by ID
        pydrime download 480424796 NDgwNDI0ODAyfA   # Mixed IDs and hashes
        pydrime download test1.txt test2.txt        # Multiple files by name
        pydrime download 480432024                  # Download folder by ID
        pydrime download -o ./dest test_folder      # Download to dir
        pydrime download test_folder --on-duplicate skip    # Skip existing
        pydrime download test_folder --on-duplicate rename  # Rename if exists
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        downloaded_files = []
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        # Create output directory if specified
        output_dir = Path(output) if output else Path.cwd()
        if output and not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        # Shared progress display and task pool for parallel downloads
        # These will be initialized if workers > 1
        shared_progress_display: Optional[Any] = None
        shared_task_queue: Optional[Any] = None

        def resolve_identifier_to_hash(identifier: str) -> str:
            """Resolve identifier (name/ID/hash) to hash value."""
            try:
                # Try resolving as entry identifier (supports names, IDs, hashes)
                entry_id = client.resolve_entry_identifier(
                    identifier=identifier,
                    parent_id=current_folder,
                    workspace_id=workspace,
                )
                if (
                    not out.quiet
                    and not identifier.isdigit()
                    and not is_file_id(identifier)
                ):
                    out.info(f"Resolved '{identifier}' to entry ID: {entry_id}")
                return normalize_to_hash(str(entry_id))
            except DrimeNotFoundError:
                # Not found by name, try as hash or ID directly
                if is_file_id(identifier):
                    hash_value = normalize_to_hash(identifier)
                    if not out.quiet:
                        out.info(f"Converting ID {identifier} to hash {hash_value}")
                    return hash_value
                return identifier  # Already a hash

        def get_entry_from_hash(
            hash_value: str, identifier: str
        ) -> Optional[FileEntry]:
            """Get entry object from hash value."""
            # Try searching by query first (works for files)
            result = client.get_file_entries(query=hash_value)
            if result and result.get("data"):
                file_entries = FileEntriesResult.from_api_response(result)
                if not file_entries.is_empty:
                    return file_entries.entries[0]

            # Try using folder_id (works for folders)
            result = client.get_file_entries(folder_id=hash_value)
            if result and result.get("folder"):
                folder_data = result["folder"]
                return FileEntriesResult.from_api_response(
                    {"data": [folder_data]}
                ).entries[0]

            out.error(f"Entry not found: {identifier}")
            return None

        def get_unique_filename(base_path: Path) -> Path:
            """Generate a unique filename if the file already exists."""
            if not base_path.exists():
                return base_path

            # Split name and extension
            stem = base_path.stem
            suffix = base_path.suffix
            parent = base_path.parent
            counter = 1

            # Find a unique name
            while True:
                new_name = f"{stem} ({counter}){suffix}"
                new_path = parent / new_name
                if not new_path.exists():
                    return new_path
                counter += 1

        def download_folder(
            entry: FileEntry,
            folder_path: Path,
            identifier: str,
            entry_obj: Optional[FileEntry] = None,
        ) -> None:
            """Download folder and its contents recursively."""
            # Check if a file exists with the folder name
            if folder_path.exists() and folder_path.is_file():
                out.error(
                    f"Cannot download folder '{entry.name}': "
                    f"a file with this name already exists at {folder_path}"
                )
                return

            folder_path.mkdir(parents=True, exist_ok=True)
            if not out.quiet:
                out.info(f"Downloading folder: {entry.name}")

            try:
                folder_result = client.get_file_entries(
                    parent_ids=[entry.id], workspace_id=entry.workspace_id or workspace
                )
                folder_entries = FileEntriesResult.from_api_response(folder_result)

                for sub_entry in folder_entries.entries:
                    if sub_entry.is_folder:
                        download_entry(sub_entry.hash, folder_path, entry_obj=sub_entry)
                    else:
                        # Use the download_file function which has progress support
                        download_file(
                            sub_entry.hash,
                            identifier if not entry_obj else entry.hash,
                            folder_path,
                            sub_entry.name,
                            show_progress=True,
                        )
            except DrimeAPIError as e:
                out.error(f"Error downloading folder contents: {e}")

        def download_file(
            hash_value: str,
            identifier: str,
            dest_path: Optional[Path],
            entry_name: str,
            show_progress: bool = True,
        ) -> None:
            """Download a single file.

            Args:
                hash_value: Hash of the file to download
                identifier: Original identifier used to request this file
                dest_path: Destination path for the file
                entry_name: Name of the file entry
                show_progress: Whether to show progress bar
            """
            # Determine output path
            if dest_path:
                # If dest_path is a directory, join it with the filename
                if dest_path.is_dir():
                    output_path = dest_path / entry_name
                else:
                    output_path = dest_path
            elif output and len(entry_identifiers) == 1:
                output_path = Path(output)
            else:
                output_path = Path(entry_name)

            # Check for duplicate (only files, not directories)
            if output_path.exists():
                # If a directory exists with this name, we need to rename the file
                # (can't write a file where a directory exists)
                if output_path.is_dir():
                    output_path = get_unique_filename(output_path)
                    out.info(
                        f"Directory exists with same name, renaming file to: "
                        f"{output_path.name}"
                    )
                # If a file exists, apply the duplicate strategy
                elif on_duplicate == "skip":
                    out.info(f"Skipped (already exists): {output_path}")
                    downloaded_files.append(
                        {
                            "hash": hash_value,
                            "path": str(output_path),
                            "input": identifier,
                            "skipped": True,
                        }
                    )
                    return
                elif on_duplicate == "rename":
                    output_path = get_unique_filename(output_path)
                    out.info(f"Renaming to avoid duplicate: {output_path.name}")

            # Try to get a task from the shared queue for parallel downloads
            task_id: Optional[Any] = None
            if shared_task_queue is not None:
                try:
                    # Non-blocking get - if queue is empty, we'll create
                    # individual progress
                    task_id = shared_task_queue.get_nowait()
                except:  # noqa: E722
                    task_id = None

            try:
                # Use shared progress display if we got a task ID
                if task_id is not None and shared_progress_display is not None:
                    # Capture progress display for use in callback
                    progress_display_ref = shared_progress_display

                    # Update the reusable task bar with this file's info
                    progress_display_ref.update(
                        task_id,
                        description=f"[cyan]{entry_name}",
                        completed=0,
                        total=None,
                    )

                    def progress_callback(
                        bytes_downloaded: int, total_bytes: int
                    ) -> None:
                        progress_display_ref.update(
                            task_id, completed=bytes_downloaded, total=total_bytes
                        )

                    saved_path = client.download_file(
                        hash_value, output_path, progress_callback=progress_callback
                    )

                    # Reset the task bar
                    progress_display_ref.update(
                        task_id,
                        description="[dim]Worker: Waiting...",
                        completed=0,
                        total=100,
                    )

                elif show_progress and not no_progress:
                    # Create individual progress bar (for sequential downloads)
                    with Progress(
                        "[progress.description]{task.description}",
                        BarColumn(),
                        "[progress.percentage]{task.percentage:>3.0f}%",
                        DownloadColumn(),
                        TransferSpeedColumn(),
                        TimeElapsedColumn(),
                        TimeRemainingColumn(),
                        # Update 10 times per second for smoother
                        # speed calculation
                        refresh_per_second=10,
                    ) as progress:
                        task = progress.add_task(f"[cyan]{entry_name}", total=None)

                        def progress_callback(
                            bytes_downloaded: int, total_bytes: int
                        ) -> None:
                            # Update with both completed and total to ensure
                            # speed calculation works
                            progress.update(
                                task, completed=bytes_downloaded, total=total_bytes
                            )

                        saved_path = client.download_file(
                            hash_value, output_path, progress_callback=progress_callback
                        )
                        # Don't print "Downloaded:" message when using progress bar
                        # as it breaks the terminal output
                else:
                    if not no_progress:
                        out.progress_message(f"Downloading {entry_name}...")
                    saved_path = client.download_file(hash_value, output_path)
                    if not out.quiet:
                        out.success(f"Downloaded: {saved_path}")

                downloaded_files.append(
                    {"hash": hash_value, "path": str(saved_path), "input": identifier}
                )
            except DrimeAPIError as e:
                out.error(f"Error downloading file: {e}")
            finally:
                # Return task ID to the pool if we used one
                if task_id is not None and shared_task_queue is not None:
                    shared_task_queue.put(task_id)

        def download_entry(
            identifier: str,
            dest_path: Optional[Path] = None,
            entry_obj: Optional[FileEntry] = None,
        ) -> None:
            """Helper function to download a single entry.

            Args:
                identifier: Entry name, hash, or ID string
                dest_path: Destination directory path
                entry_obj: Optional pre-fetched entry object to avoid API lookup
            """
            # If we already have the entry object, use it
            if entry_obj:
                entry: FileEntry = entry_obj
                hash_value = entry.hash
            else:
                # Resolve identifier to hash
                hash_value = resolve_identifier_to_hash(identifier)
                if not hash_value:
                    return

                # Get entry info
                try:
                    entry_maybe: Optional[FileEntry] = get_entry_from_hash(
                        hash_value, identifier
                    )
                    if not entry_maybe:
                        return
                    entry = entry_maybe
                except DrimeAPIError as e:
                    out.error(f"Error downloading {identifier}: {e}")
                    return

            # Handle folders vs files
            if entry.is_folder:
                # Folders are always downloaded recursively
                folder_path = dest_path / entry.name if dest_path else Path(entry.name)
                download_folder(entry, folder_path, identifier, entry_obj)
            else:
                file_path = dest_path / entry.name if dest_path else None
                download_file(
                    hash_value, identifier, file_path, entry.name, show_progress=True
                )

        # Parallel or sequential download
        if workers > 1 and len(entry_identifiers) > 1:
            # Parallel download for multiple entries with progress pooling
            if not out.quiet and not no_progress:
                total_entries = len(entry_identifiers)
                completed_entries = 0

                # Create shared progress display with task pool
                shared_progress_display = Progress(
                    "[progress.description]{task.description}",
                    BarColumn(),
                    "[progress.percentage]{task.percentage:>3.0f}%",
                    DownloadColumn(),
                    TransferSpeedColumn(),
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    refresh_per_second=10,
                )
                shared_progress_display.start()

                # Add overall progress bar
                overall_task_id = shared_progress_display.add_task(
                    f"[green]Overall Download Progress (0/{total_entries} entries)",
                    total=total_entries,
                    completed=0,
                )

                # Create a limited pool of progress bars (one per worker)
                import queue
                import threading

                progress_task_pool = []
                for i in range(workers):
                    task_id = shared_progress_display.add_task(
                        f"[dim]Worker {i + 1}: Waiting...",
                        total=100,
                        visible=True,
                    )
                    progress_task_pool.append(task_id)

                # Track available task IDs - use thread-safe queue
                shared_task_queue = queue.Queue()
                for task_id in progress_task_pool:
                    shared_task_queue.put(task_id)

                # Counter lock for thread-safe increment
                counter_lock = threading.Lock()

                # Wrapper to track completion for overall progress
                def download_with_progress_tracking(
                    identifier: str, dest_path: Optional[Path]
                ) -> None:
                    nonlocal completed_entries
                    try:
                        download_entry(identifier, dest_path)
                    finally:
                        # Update overall progress with counter
                        with counter_lock:
                            completed_entries += 1
                            if shared_progress_display:
                                desc = (
                                    f"[green]Overall Download Progress "
                                    f"({completed_entries}/{total_entries} entries)"
                                )
                                shared_progress_display.update(
                                    overall_task_id,
                                    advance=1,
                                    description=desc,
                                )

                try:
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {}
                        for identifier in entry_identifiers:
                            future = executor.submit(
                                download_with_progress_tracking,
                                identifier,
                                output_dir if output else None,
                            )
                            futures[future] = identifier

                        try:
                            for future in as_completed(futures):
                                identifier = futures[future]
                                try:
                                    future.result()
                                except Exception as e:
                                    out.error(f"Error downloading {identifier}: {e}")
                        except KeyboardInterrupt:
                            out.warning(
                                "\nDownload interrupted by user. "
                                "Cancelling pending downloads..."
                            )
                            # Cancel all pending futures
                            for future in futures:
                                future.cancel()
                            raise
                finally:
                    shared_progress_display.stop()
                    # Reset shared variables
                    shared_progress_display = None
                    shared_task_queue = None
            else:
                # No progress display - simple parallel download
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {}
                    for identifier in entry_identifiers:
                        future = executor.submit(
                            download_entry, identifier, output_dir if output else None
                        )
                        futures[future] = identifier

                    try:
                        for future in as_completed(futures):
                            identifier = futures[future]
                            try:
                                future.result()
                            except Exception as e:
                                out.error(f"Error downloading {identifier}: {e}")
                    except KeyboardInterrupt:
                        out.warning(
                            "\nDownload interrupted by user. "
                            "Cancelling pending downloads..."
                        )
                        # Cancel all pending futures
                        for future in futures:
                            future.cancel()
                        raise
        else:
            # Sequential download
            for identifier in entry_identifiers:
                download_entry(identifier, output_dir if output else None)

        if out.json_output:
            out.output_json({"files": downloaded_files})

    except KeyboardInterrupt:
        out.warning("\nDownload cancelled by user")
        ctx.exit(130)  # Standard exit code for SIGINT
    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--remote-path", "-r", help="Remote destination path")
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses default workspace if not specified)",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be synced without syncing"
)
@click.option(
    "--workers",
    "-j",
    type=int,
    default=1,
    help="Number of parallel workers (default: 1, use 4-8 for parallel transfers)",
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Disable progress bars",
)
@click.option(
    "--chunk-size",
    "-c",
    type=int,
    default=25,
    help="Chunk size in MB for multipart uploads (default: 25MB)",
)
@click.option(
    "--multipart-threshold",
    "-m",
    type=int,
    default=30,
    help="File size threshold in MB for using multipart upload (default: 30MB)",
)
@click.pass_context
def sync(  # noqa: C901
    ctx: Any,
    path: str,
    remote_path: Optional[str],
    workspace: Optional[int],
    dry_run: bool,
    workers: int,
    no_progress: bool,
    chunk_size: int,
    multipart_threshold: int,
) -> None:
    """Sync files between local directory and Drime Cloud.

    PATH: Local directory to sync

    Automatically syncs files based on modification time and size:
    - Uploads new local files
    - Downloads new cloud files
    - For existing files: newer file wins (based on modification time)
    - Files with same timestamp but different size are flagged as conflicts

    No user interaction required - all decisions are made automatically.

    Examples:
        pydrime sync ./my_folder                    # Sync folder bidirectionally
        pydrime sync ./docs -r remote_docs          # Sync with remote path
        pydrime sync . -w 5                         # Sync in workspace 5
        pydrime sync ./data --dry-run               # Preview sync changes
        pydrime sync ./large_folder -j 4            # Parallel sync with 4 workers
    """
    out: OutputFormatter = ctx.obj["out"]
    local_path = Path(path)

    # Validate path is a directory
    if not local_path.is_dir():
        out.error("PATH must be a directory for syncing")
        ctx.exit(1)

    # Validate and convert MB to bytes
    if chunk_size < 5:
        out.error("Chunk size must be at least 5MB")
        ctx.exit(1)
    if chunk_size > 100:
        out.error("Chunk size cannot exceed 100MB")
        ctx.exit(1)
    if multipart_threshold < 1:
        out.error("Multipart threshold must be at least 1MB")
        ctx.exit(1)
    if chunk_size >= multipart_threshold:
        out.error("Chunk size must be smaller than multipart threshold")
        ctx.exit(1)

    chunk_size_bytes = chunk_size * 1024 * 1024
    multipart_threshold_bytes = multipart_threshold * 1024 * 1024

    # Use auth helper
    api_key = require_api_key(ctx, out)

    # Use default workspace if none specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

    # Initialize client
    client = DrimeClient(api_key=api_key)

    # Get current folder context
    current_folder_id = config.get_current_folder()

    if not out.quiet:
        # Show workspace information
        workspace_display, _ = format_workspace_display(client, workspace)
        out.info(f"Workspace: {workspace_display}")

        # Show parent folder information
        folder_display, _ = get_folder_display_name(client, current_folder_id)
        out.info(f"Parent folder: {folder_display}")

        if remote_path:
            out.info(f"Remote path structure: {remote_path}")

        out.info("")  # Empty line for readability

    try:
        # Step 1: Scan local directory
        if not out.quiet:
            out.info(f"Scanning local directory: {local_path}")

        # Scan with the local_path itself as base_path to get relative paths
        # without the folder name
        local_files = scan_directory(local_path, local_path, out)

        # Apply remote path prefix if specified
        if remote_path:
            local_files = [
                (file_path, f"{remote_path}/{rel_path}")
                for file_path, rel_path in local_files
            ]

        # Build local file mapping: {relative_path: (Path, size, mtime)}
        local_file_map: dict[str, tuple[Path, int, float]] = {}
        for file_path, rel_path in local_files:
            stat = file_path.stat()
            local_file_map[rel_path] = (file_path, stat.st_size, stat.st_mtime)

        if not out.quiet:
            out.info(f"Found {len(local_file_map)} local file(s)")

        # Step 2: Get remote files
        if not out.quiet:
            out.info("Fetching remote files...")

        # Get all remote files recursively with their paths
        remote_entries_with_paths: list[tuple[FileEntry, str]] = []

        def fetch_remote_recursive(parent_id: Optional[int], path_prefix: str) -> None:
            """Recursively fetch all remote files with path tracking."""
            try:
                result = client.get_file_entries(
                    parent_ids=[parent_id] if parent_id else None,
                    workspace_id=workspace,
                )
                file_entries = FileEntriesResult.from_api_response(result)

                for entry in file_entries.entries:
                    entry_path = (
                        f"{path_prefix}/{entry.name}" if path_prefix else entry.name
                    )
                    if entry.is_folder:
                        # Recursively fetch folder contents
                        fetch_remote_recursive(entry.id, entry_path)
                    else:
                        remote_entries_with_paths.append((entry, entry_path))
            except DrimeAPIError:
                pass  # Skip inaccessible folders

        # Determine the remote folder to sync with
        # If remote_path is specified, use it
        # Otherwise, look for a folder with the same name as local_path
        # in current_folder_id
        remote_sync_folder_id = current_folder_id
        remote_base_path = remote_path if remote_path else local_path.name

        if not remote_path:
            # Try to find a matching folder in the current directory
            try:
                result = client.get_file_entries(
                    parent_ids=[current_folder_id] if current_folder_id else None,
                    workspace_id=workspace,
                )
                file_entries = FileEntriesResult.from_api_response(result)

                for entry in file_entries.entries:
                    if entry.is_folder and entry.name == local_path.name:
                        # Found matching folder - sync into it
                        remote_sync_folder_id = entry.id
                        remote_base_path = ""  # We're already inside the folder
                        if not out.quiet:
                            out.info(
                                f"Found remote folder '{entry.name}' (ID: {entry.id})"
                            )
                        break
            except DrimeAPIError:
                pass

        fetch_remote_recursive(remote_sync_folder_id, remote_base_path)

        # Build remote file mapping: {relative_path: FileEntry}
        remote_file_map: dict[str, FileEntry] = {}

        for entry, entry_path in remote_entries_with_paths:
            remote_file_map[entry_path] = entry

        if not out.quiet:
            out.info(f"Found {len(remote_file_map)} remote file(s)")
            out.info("")

        # Step 3: Compare and categorize files
        files_to_upload: list[tuple[Path, str]] = []  # (local_path, remote_path)
        files_to_download: list[
            tuple[FileEntry, Path]
        ] = []  # (remote_entry, local_path)
        files_in_sync: list[str] = []
        files_conflict: list[tuple[str, str]] = []  # (path, reason)

        # Find files only in local (need upload) or compare existing
        for rel_path, (file_path, local_size, local_mtime) in local_file_map.items():
            if rel_path not in remote_file_map:
                # File only exists locally - upload it
                files_to_upload.append((file_path, rel_path))
            else:
                # File exists in both - compare
                remote_entry = remote_file_map[rel_path]
                remote_size = remote_entry.file_size or 0
                remote_updated = parse_iso_timestamp(remote_entry.updated_at)

                # Compare by size first
                if remote_size != local_size:
                    # Size mismatch - use modification time to decide
                    if remote_updated:
                        local_dt = datetime.fromtimestamp(local_mtime)
                        time_diff = (local_dt - remote_updated).total_seconds()

                        if time_diff > 2:
                            # Local is newer
                            files_to_upload.append((file_path, rel_path))
                        elif time_diff < -2:
                            # Remote is newer
                            files_to_download.append((remote_entry, file_path))
                        else:
                            # Same time but different size - conflict
                            files_conflict.append(
                                (
                                    rel_path,
                                    f"Same timestamp but size differs: "
                                    f"local={out.format_size(local_size)}, "
                                    f"remote={out.format_size(remote_size)}",
                                )
                            )
                    else:
                        # No remote timestamp - upload local
                        # (assume local is source of truth)
                        files_to_upload.append((file_path, rel_path))
                else:
                    # Same size - check modification time
                    if remote_updated:
                        local_dt = datetime.fromtimestamp(local_mtime)
                        time_diff = (local_dt - remote_updated).total_seconds()

                        # Consider in sync if within 2 seconds (filesystem precision)
                        if abs(time_diff) <= 2:
                            files_in_sync.append(rel_path)
                        elif time_diff > 2:
                            # Local is newer
                            files_to_upload.append((file_path, rel_path))
                        else:
                            # Remote is newer
                            files_to_download.append((remote_entry, file_path))
                    else:
                        # Same size, no timestamp difference detectable
                        files_in_sync.append(rel_path)

        # Find files only in remote (need download)
        for rel_path, remote_entry in remote_file_map.items():
            if rel_path not in local_file_map:
                # Construct local path
                # Remove remote_path prefix if present
                local_rel_path = rel_path
                if remote_path and local_rel_path.startswith(remote_path + "/"):
                    local_rel_path = local_rel_path[len(remote_path) + 1 :]
                elif remote_path and local_rel_path.startswith(remote_path):
                    local_rel_path = local_rel_path[len(remote_path) :]

                local_dest = local_path / local_rel_path
                files_to_download.append((remote_entry, local_dest))

        # Display sync plan
        if not out.quiet:
            out.print("=" * 60)
            out.print("Sync Plan")
            out.print("=" * 60)
            out.print(f"Files to upload:      {len(files_to_upload)}")
            out.print(f"Files to download:    {len(files_to_download)}")
            out.print(f"Files in sync:        {len(files_in_sync)}")
            out.print(f"Files with conflicts: {len(files_conflict)}")
            out.print("=" * 60)
            out.print("")

            if files_to_upload:
                out.info("Files to upload:")
                for file_path, rel_path in files_to_upload[:10]:  # Show first 10
                    size = file_path.stat().st_size
                    out.print(f"  ↑ {rel_path} ({out.format_size(size)})")
                if len(files_to_upload) > 10:
                    out.print(f"  ... and {len(files_to_upload) - 10} more")
                out.print("")

            if files_to_download:
                out.info("Files to download:")
                for remote_entry, _local_dest in files_to_download[:10]:
                    out.print(
                        f"  ↓ {remote_entry.name} "
                        f"({out.format_size(remote_entry.file_size or 0)})"
                    )
                if len(files_to_download) > 10:
                    out.print(f"  ... and {len(files_to_download) - 10} more")
                out.print("")

            if files_conflict:
                out.warning("Files with conflicts (skipped):")
                for rel_path, reason in files_conflict:
                    out.print(f"  ⚠ {rel_path}: {reason}")
                out.print("")

        if dry_run:
            out.warning("Dry run mode - no files were synced.")
            return

        # Step 4: Execute sync
        upload_count = 0
        download_count = 0
        error_count = 0

        # Upload files
        if files_to_upload:
            if not out.quiet:
                out.info(f"Uploading {len(files_to_upload)} file(s)...")

            for file_path, rel_path in files_to_upload:
                try:
                    if not no_progress and not out.quiet:
                        size_str = out.format_size(file_path.stat().st_size)
                        out.progress_message(f"Uploading {rel_path} ({size_str})")

                    client.upload_file(
                        file_path,
                        parent_id=remote_sync_folder_id,
                        relative_path=rel_path,
                        workspace_id=workspace,
                        chunk_size=chunk_size_bytes,
                        use_multipart_threshold=multipart_threshold_bytes,
                    )
                    upload_count += 1
                except DrimeAPIError as e:
                    error_count += 1
                    out.error(f"Error uploading {rel_path}: {e}")

        # Download files
        if files_to_download:
            if not out.quiet:
                out.info(f"Downloading {len(files_to_download)} file(s)...")

            for remote_entry, local_dest in files_to_download:
                try:
                    # Ensure parent directory exists
                    local_dest.parent.mkdir(parents=True, exist_ok=True)

                    if not no_progress and not out.quiet:
                        size_str = out.format_size(remote_entry.file_size or 0)
                        out.progress_message(
                            f"Downloading {remote_entry.name} ({size_str})"
                        )

                    client.download_file(remote_entry.hash, local_dest)
                    download_count += 1
                except DrimeAPIError as e:
                    error_count += 1
                    out.error(f"Error downloading {remote_entry.name}: {e}")

        # Show summary
        if out.json_output:
            out.output_json(
                {
                    "uploaded": upload_count,
                    "downloaded": download_count,
                    "in_sync": len(files_in_sync),
                    "conflicts": len(files_conflict),
                    "errors": error_count,
                }
            )
        else:
            out.print("")
            summary_items = [
                ("Uploaded", f"{upload_count} file(s)"),
                ("Downloaded", f"{download_count} file(s)"),
                ("Already in sync", f"{len(files_in_sync)} file(s)"),
            ]
            if files_conflict:
                summary_items.append(
                    ("Conflicts (skipped)", f"{len(files_conflict)} file(s)")
                )
            if error_count > 0:
                summary_items.append(("Errors", f"{error_count} file(s)"))

            out.print_summary("Sync Complete", summary_items)

        if error_count > 0:
            ctx.exit(1)

    except KeyboardInterrupt:
        out.warning("\nSync cancelled by user")
        ctx.exit(130)
    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)


@main.command()
@click.argument("hash_or_id_value", type=str)
@click.pass_context
def info(ctx: Any, hash_or_id_value: str) -> None:
    """Get detailed information about a file or folder.

    HASH_OR_ID_VALUE: File hash or numeric file ID

    Examples:
        pydrime info 480424796          # Get info by ID
        pydrime info NDgwNDI0Nzk2fA     # Get info by hash
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Convert ID to hash if needed
        if is_file_id(hash_or_id_value):
            hash_value = normalize_to_hash(hash_or_id_value)
            if not out.quiet:
                out.info(f"Converting ID {hash_or_id_value} to hash {hash_value}")
        else:
            hash_value = hash_or_id_value

        # Search for the entry using the hash
        result = client.get_file_entries(query=hash_value)

        if not result or not result.get("data"):
            out.error(f"No file found with hash/ID: {hash_or_id_value}")
            ctx.exit(1)

        # Parse the response
        file_entries = FileEntriesResult.from_api_response(result)

        if file_entries.is_empty:
            out.error(f"No file found with hash/ID: {hash_or_id_value}")
            ctx.exit(1)

        # Get the first entry (should be the only one for exact hash match)
        entry = file_entries.entries[0]

        # Output based on format
        if out.json_output:
            # Convert entry back to dict format
            entry_dict = {
                "id": entry.id,
                "name": entry.name,
                "type": entry.type,
                "hash": entry.hash,
                "size": entry.file_size,
                "parent_id": entry.parent_id,
                "created_at": entry.created_at,
                "updated_at": entry.updated_at,
                "owner": entry.owner.email if entry.owner else None,
                "public": entry.public,
                "description": entry.description,
            }
            out.output_json(entry_dict)
        else:
            # Text format with detailed info
            icon = "📁" if entry.type == "folder" else "📄"
            out.print(f"\n{icon} {entry.name}")
            out.print(f"  ID: {entry.id}")
            out.print(f"  Hash: {entry.hash or 'N/A'}")
            out.print(f"  Type: {entry.type}")
            if entry.file_size:
                out.print(f"  Size: {out.format_size(entry.file_size)}")
            if entry.parent_id:
                out.print(f"  Parent ID: {entry.parent_id}")
            else:
                out.print("  Parent ID: Root")
            if entry.owner:
                out.print(f"  Owner: {entry.owner.email}")
            out.print(f"  Created: {entry.created_at or 'N/A'}")
            if entry.updated_at:
                out.print(f"  Updated: {entry.updated_at}")
            if entry.public:
                out.print("  🌐 Public")
            if entry.description:
                out.print(f"  Description: {entry.description}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("folder_identifier", type=str, required=False)
@click.pass_context
def cd(ctx: Any, folder_identifier: Optional[str]) -> None:
    """Change current working directory (folder).

    FOLDER_IDENTIFIER: ID or name of the folder to navigate to
                       (omit or use 0 or / for root)

    Examples:
        pydrime cd 480432024    # Navigate to folder with ID 480432024
        pydrime cd .            # Navigate to folder named "."
        pydrime cd "My Folder"  # Navigate to folder named "My Folder"
        pydrime cd              # Navigate to root directory
        pydrime cd 0            # Navigate to root directory
        pydrime cd /            # Navigate to root directory
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    # If no folder_identifier is provided or it's "0" or "/", go to root
    if folder_identifier is None or folder_identifier in ("0", "/"):
        config.save_current_folder(None)
        out.success("Changed to root directory")
        return

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()

        # Get default workspace
        workspace_id = config.get_default_workspace() or 0

        # Resolve folder identifier (ID or name) to folder ID
        folder_id = client.resolve_folder_identifier(
            identifier=folder_identifier,
            parent_id=current_folder,
            workspace_id=workspace_id,
        )
        if not out.quiet and not folder_identifier.isdigit():
            out.info(f"Resolved '{folder_identifier}' to folder ID: {folder_id}")

        # Verify the folder exists by trying to list its contents
        result = client.get_file_entries(parent_ids=[folder_id])

        # Check if this is a valid folder
        if result is None:
            out.error(f"Folder with ID {folder_id} not found or is not accessible")
            ctx.exit(1)

        # Save the current folder to config
        config.save_current_folder(folder_id)
        out.success(f"Changed to folder ID: {folder_id}")

        # Show folder contents if not in quiet mode
        if not out.quiet:
            file_entries = FileEntriesResult.from_api_response(result)
            if not file_entries.is_empty:
                out.print(f"\n{file_entries.to_text_summary()}")

    except DrimeNotFoundError as e:
        out.error(str(e))
        ctx.exit(1)
    except DrimeAPIError as e:
        out.error(f"Error changing directory: {e}")
        ctx.exit(1)


@main.command()
@click.option("--id-only", is_flag=True, help="Output only the folder ID")
@click.pass_context
def pwd(ctx: Any, id_only: bool) -> None:
    """Print current working directory and workspace.

    Shows the current folder ID, name, and default workspace.

    Examples:
        pydrime pwd             # Show current folder with ID
        pydrime pwd --id-only   # Show only the folder ID
        pydrime --json pwd      # Show details in JSON format
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    current_folder = config.get_current_folder()
    default_workspace = config.get_default_workspace()
    folder_name = None
    workspace_name = None

    # If --id-only flag is set, just print the ID and exit
    if id_only:
        if current_folder is None:
            out.print("0")  # Root folder
        else:
            out.print(str(current_folder))
        return

    # Get folder name and workspace name if configured
    if config.is_configured() or api_key:
        try:
            client = DrimeClient(api_key=api_key)

            # Get folder name if we have a current folder
            if current_folder is not None:
                folder_info = client.get_folder_info(current_folder)
                folder_name = folder_info["name"]

            # Get workspace name
            if default_workspace:
                workspaces_result = client.get_workspaces()
                if (
                    isinstance(workspaces_result, dict)
                    and "workspaces" in workspaces_result
                ):
                    for ws in workspaces_result["workspaces"]:
                        if ws["id"] == default_workspace:
                            workspace_name = ws["name"]
                            break
        except (DrimeAPIError, DrimeNotFoundError):
            # If we can't get the folder/workspace name, just continue without it
            pass

    if out.json_output:
        # JSON format
        out.output_json(
            {
                "current_folder": current_folder,
                "folder_name": folder_name,
                "default_workspace": default_workspace or 0,
                "workspace_name": workspace_name,
            }
        )
    else:
        # Text format (default) - show folder path with ID
        if current_folder is None:
            out.print("/ (ID: 0)")
        else:
            if folder_name:
                out.print(f"/{folder_name} (ID: {current_folder})")
            else:
                out.print(f"/{current_folder} (ID: {current_folder})")

        # Show workspace information
        if workspace_name:
            out.print(f"Workspace: {workspace_name} ({default_workspace})")
        else:
            out.print(f"Workspace: {default_workspace or 0}")


@main.command()
@click.argument("entry_identifier", type=str)
@click.argument("new_name")
@click.option("--description", "-d", help="New description for the entry")
@click.pass_context
def rename(
    ctx: Any, entry_identifier: str, new_name: str, description: Optional[str]
) -> None:
    """Rename a file or folder entry.

    ENTRY_IDENTIFIER: ID or name of the entry to rename
    NEW_NAME: New name for the entry

    Supports both numeric IDs and file/folder names. Names are resolved
    in the current working directory.

    Examples:
        pydrime rename 480424796 newfile.txt         # Rename by ID
        pydrime rename test1.txt newfile.txt         # Rename by name
        pydrime rename drime_test my_folder          # Rename folder by name
        pydrime rename test.txt file.txt -d "Desc"   # Rename with description
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        # Resolve identifier to entry ID
        try:
            entry_id = int(entry_identifier)
        except ValueError:
            # Not a numeric ID, resolve by name
            entry_id = client.resolve_entry_identifier(
                entry_identifier, current_folder, workspace
            )

        result = client.update_file_entry(
            entry_id, name=new_name, description=description
        )

        if out.json_output:
            out.output_json(result)
        else:
            out.success(f"✓ Entry renamed to: {new_name}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("entry_identifiers", nargs=-1, type=str, required=True)
@click.option("--permanent", is_flag=True, help="Delete permanently (cannot be undone)")
@click.pass_context
def rm(ctx: Any, entry_identifiers: tuple[str, ...], permanent: bool) -> None:
    """Delete one or more file or folder entries.

    ENTRY_IDENTIFIERS: One or more entry IDs or names to delete

    Supports both numeric IDs and file/folder names. Names are resolved
    in the current working directory.

    Examples:
        pydrime rm 480424796                    # Delete by ID
        pydrime rm test1.txt                    # Delete by name
        pydrime rm drime_test                   # Delete folder by name
        pydrime rm test1.txt test2.txt          # Delete multiple files
        pydrime rm 480424796 drime_test         # Mix IDs and names
        pydrime rm --permanent test1.txt        # Permanent deletion
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        # Resolve all identifiers to entry IDs
        entry_ids = []
        for identifier in entry_identifiers:
            try:
                entry_id = client.resolve_entry_identifier(
                    identifier=identifier,
                    parent_id=current_folder,
                    workspace_id=workspace,
                )
                if not out.quiet and not identifier.isdigit():
                    out.info(f"Resolved '{identifier}' to entry ID: {entry_id}")
                entry_ids.append(entry_id)
            except DrimeNotFoundError as e:
                out.error(str(e))
                ctx.exit(1)

        # Confirm deletion
        action = "permanently delete" if permanent else "move to trash"
        if not out.quiet and not click.confirm(
            f"Are you sure you want to {action} {len(entry_ids)} item(s)?"
        ):
            out.warning("Deletion cancelled.")
            return

        result = client.delete_file_entries(entry_ids, delete_forever=permanent)

        if out.json_output:
            out.output_json(result)
        else:
            if permanent:
                out.success(f"✓ Permanently deleted {len(entry_ids)} item(s)")
            else:
                out.success(f"✓ Moved {len(entry_ids)} item(s) to trash")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("entry_identifier", type=str)
@click.option("--password", "-p", help="Optional password for the link")
@click.option(
    "--expires", "-e", help="Expiration date (format: 2025-12-31T23:59:59.000000Z)"
)
@click.option("--allow-edit", is_flag=True, help="Allow editing through the link")
@click.option(
    "--allow-download",
    is_flag=True,
    default=True,
    help="Allow downloading through the link",
)
@click.pass_context
def share(
    ctx: Any,
    entry_identifier: str,
    password: Optional[str],
    expires: Optional[str],
    allow_edit: bool,
    allow_download: bool,
) -> None:
    """Create a shareable link for a file or folder.

    ENTRY_IDENTIFIER: ID or name of the entry to share

    Supports both numeric IDs and file/folder names. Names are resolved
    in the current working directory.

    Examples:
        pydrime share 480424796                   # Share by ID
        pydrime share test1.txt                   # Share by name
        pydrime share drime_test                  # Share folder by name
        pydrime share test.txt -p mypass123       # Share with password
        pydrime share test.txt -e 2025-12-31      # Share with expiration
        pydrime share test.txt --allow-edit       # Allow editing
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        # Resolve identifier to entry ID
        try:
            entry_id = int(entry_identifier)
        except ValueError:
            # Not a numeric ID, resolve by name
            entry_id = client.resolve_entry_identifier(
                entry_identifier, current_folder, workspace
            )

        result = client.create_shareable_link(
            entry_id=entry_id,
            password=password,
            expires_at=expires,
            allow_edit=allow_edit,
            allow_download=allow_download,
        )

        if out.json_output:
            out.output_json(result)
        else:
            if isinstance(result, dict) and "link" in result:
                link_hash = result["link"].get("hash", "")
                out.success("✓ Shareable link created:")
                out.print(f"https://dri.me/{link_hash}")
            else:
                out.warning("Link created but format unexpected")
                out.output_json(result)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.pass_context
def workspaces(ctx: Any) -> None:
    """List all workspaces you have access to.

    Shows workspace name, ID, your role, and owner information.
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        result = client.get_workspaces()

        if out.json_output:
            out.output_json(result)
            return

        if isinstance(result, dict) and "workspaces" in result:
            workspaces_list = result["workspaces"]

            if not workspaces_list:
                out.warning("No workspaces found")
                return

            table_data = []
            for ws in workspaces_list:
                table_data.append(
                    {
                        "id": str(ws.get("id", "")),
                        "name": ws.get("name", ""),
                        "role": ws.get("currentUser", {}).get("role_name", ""),
                        "owner": ws.get("owner", {}).get("email", ""),
                    }
                )

            out.output_table(
                table_data,
                ["id", "name", "role", "owner"],
                {"id": "ID", "name": "Name", "role": "Your Role", "owner": "Owner"},
            )
        else:
            out.warning("Unexpected response format")
            out.output_json(result)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.pass_context
def usage(ctx: Any) -> None:
    """Display storage space usage information.

    Shows how much storage you've used and how much is available.
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        result = client.get_space_usage()

        if out.json_output:
            out.output_json(result)
            return

        if isinstance(result, dict):
            used = result.get("used", 0)
            available = result.get("available", 0)
            total = used + available
            percentage = (used / total * 100) if total > 0 else 0

            # Text format - one-liner
            out.print(
                f"Used: {out.format_size(used)} | "
                f"Available: {out.format_size(available)} | "
                f"Total: {out.format_size(total)} | "
                f"Usage: {percentage:.1f}%"
            )
        else:
            out.warning("Unexpected response format")
            out.output_json(result)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--remote-path", "-r", help="Remote destination path")
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses default workspace if not specified)",
)
@click.pass_context
def validate(
    ctx: Any, path: str, remote_path: Optional[str], workspace: Optional[int]
) -> None:  # noqa: C901
    """Validate that local files/folders are uploaded with correct size.

    PATH: Local file or directory to validate

    Checks if every file in the given path exists in Drime Cloud
    and has the same size as the local file.

    Examples:
        pydrime validate drime_test              # Validate folder
        pydrime validate drime_test/test1.txt    # Validate single file
        pydrime validate . -w 5                  # Validate current dir in workspace 5
        pydrime validate /path/to/local -r remote_folder  # Validate with remote path
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]
    local_path = Path(path)

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    # Use default workspace if none specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

    try:
        client = DrimeClient(api_key=api_key)

        # Show info about workspace and remote path if not in quiet mode
        if not out.quiet:
            # Show workspace information
            if workspace == 0:
                out.info("Workspace: Personal (0)")
            else:
                # Try to get workspace name
                workspace_name = None
                try:
                    result = client.get_workspaces()
                    if isinstance(result, dict) and "workspaces" in result:
                        for ws in result["workspaces"]:
                            if ws.get("id") == workspace:
                                workspace_name = ws.get("name")
                                break
                except (DrimeAPIError, Exception):
                    pass

                if workspace_name:
                    out.info(f"Workspace: {workspace_name} ({workspace})")
                else:
                    out.info(f"Workspace: {workspace}")

            if remote_path:
                out.info(f"Remote path structure: {remote_path}")

            out.info("")  # Empty line for readability

        # Collect files to validate
        if local_path.is_file():
            files_to_validate = [(local_path, remote_path or local_path.name)]
        else:
            out.info(f"Scanning directory: {local_path}")
            # Use parent as base_path so the folder name is included in relative paths
            base_path = local_path.parent if remote_path is None else local_path
            files_to_validate = scan_directory(local_path, base_path, out)

        if not files_to_validate:
            out.warning("No files found to validate.")
            return

        out.info(f"Validating {len(files_to_validate)} file(s)...\n")

        # Use FileEntriesManager to fetch all remote files once
        file_manager = FileEntriesManager(client, workspace)

        # Get current folder context to determine where to start the search
        current_folder_id = config.get_current_folder()

        # Determine the remote folder to validate against
        # If remote_path is specified, find that folder
        # Otherwise, use current folder or root
        remote_folder_id = current_folder_id
        remote_base_path = ""

        if remote_path:
            # Try to find the remote folder by path
            path_parts = remote_path.split("/")
            folder_id = current_folder_id

            for part in path_parts:
                if part:  # Skip empty parts
                    folder_entry = file_manager.find_folder_by_name(part, folder_id)
                    if folder_entry:
                        folder_id = folder_entry.id
                    else:
                        out.warning(
                            f"Remote path '{remote_path}' not found, using root"
                        )
                        folder_id = current_folder_id
                        break

            remote_folder_id = folder_id
            remote_base_path = remote_path
        elif not local_path.is_file():
            # If validating a directory without remote_path, look for matching folder
            folder_entry = file_manager.find_folder_by_name(
                local_path.name, current_folder_id
            )
            if folder_entry:
                remote_folder_id = folder_entry.id
                remote_base_path = ""
                if not out.quiet:
                    folder_info = f"'{folder_entry.name}' (ID: {folder_entry.id})"
                    out.info(f"Found remote folder {folder_info}")

        out.progress_message("Fetching remote files...")

        # Get all remote files recursively
        remote_files_with_paths = file_manager.get_all_recursive(
            folder_id=remote_folder_id, path_prefix=remote_base_path
        )

        # Build a map of remote files: {path: FileEntry}
        remote_file_map: dict[str, FileEntry] = {}
        for entry, entry_path in remote_files_with_paths:
            # Normalize path for comparison
            normalized_path = entry_path
            if remote_base_path and normalized_path.startswith(remote_base_path + "/"):
                normalized_path = normalized_path[len(remote_base_path) + 1 :]
            remote_file_map[normalized_path] = entry

        if not out.quiet:
            out.info(f"Found {len(remote_file_map)} remote file(s)\n")

        # Track validation results
        valid_files = []
        missing_files = []
        size_mismatch_files = []

        for idx, (file_path, rel_path) in enumerate(files_to_validate, 1):
            local_size = file_path.stat().st_size

            out.progress_message(
                f"Validating [{idx}/{len(files_to_validate)}]: {rel_path}"
            )

            # Look up the file in the remote map
            # Try with and without remote_path prefix
            lookup_path = rel_path
            if remote_base_path and lookup_path.startswith(remote_base_path + "/"):
                lookup_path = lookup_path[len(remote_base_path) + 1 :]

            matching_entry = remote_file_map.get(lookup_path)

            if not matching_entry:
                # Also try looking up just the filename if full path doesn't match
                file_name = Path(rel_path).name
                matching_entry = None
                for path, entry in remote_file_map.items():
                    if Path(path).name == file_name:
                        matching_entry = entry
                        break

            if not matching_entry:
                missing_files.append(
                    {
                        "path": rel_path,
                        "local_size": local_size,
                        "reason": "Not found in cloud",
                    }
                )
                continue

            # Check size
            cloud_size = matching_entry.file_size or 0
            if cloud_size != local_size:
                size_mismatch_files.append(
                    {
                        "path": rel_path,
                        "local_size": local_size,
                        "cloud_size": cloud_size,
                        "cloud_id": matching_entry.id,
                    }
                )
            else:
                valid_files.append(
                    {
                        "path": rel_path,
                        "size": local_size,
                        "cloud_id": matching_entry.id,
                    }
                )

        # Output results
        if out.json_output:
            out.output_json(
                {
                    "total": len(files_to_validate),
                    "valid": len(valid_files),
                    "missing": len(missing_files),
                    "size_mismatch": len(size_mismatch_files),
                    "valid_files": valid_files,
                    "missing_files": missing_files,
                    "size_mismatch_files": size_mismatch_files,
                }
            )
        else:
            out.print("\n" + "=" * 60)
            out.print("Validation Results")
            out.print("=" * 60 + "\n")

            # Show valid files
            if valid_files:
                out.success(f"✓ Valid: {len(valid_files)} file(s)")
                out.print("")

            # Show missing files
            if missing_files:
                out.error(f"✗ Missing: {len(missing_files)} file(s)")
                for f in missing_files:
                    local_size = cast(int, f["local_size"])
                    out.print(
                        f"  ✗ {f['path']} ({out.format_size(local_size)}) "
                        f"- {f['reason']}"
                    )
                out.print("")

            # Show size mismatches
            if size_mismatch_files:
                out.warning(f"⚠ Size mismatch: {len(size_mismatch_files)} file(s)")
                for f in size_mismatch_files:
                    local_size = cast(int, f["local_size"])
                    cloud_size = cast(int, f["cloud_size"])
                    out.print(
                        f"  ⚠ {f['path']} [ID: {f['cloud_id']}]\n"
                        f"    Local:  {out.format_size(local_size)}\n"
                        f"    Cloud:  {out.format_size(cloud_size)}"
                    )
                out.print("")

            # Summary
            total = len(files_to_validate)
            valid = len(valid_files)
            issues = len(missing_files) + len(size_mismatch_files)

            out.print("=" * 60)
            if issues == 0:
                out.success(f"All {total} file(s) validated successfully!")
            else:
                msg = f"Validation complete: {valid}/{total} valid, {issues} issue(s)"
                out.warning(msg)
            out.print("=" * 60)

        # Exit with error code if there are issues
        if missing_files or size_mismatch_files:
            ctx.exit(1)

    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)


if __name__ == "__main__":
    main()
