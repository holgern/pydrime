"""CLI interface for Drime Cloud uploader."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional, cast

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
from .duplicate_finder import DuplicateFileFinder
from .duplicate_handler import DuplicateHandler
from .exceptions import DrimeAPIError, DrimeNotFoundError
from .file_entries_manager import FileEntriesManager
from .models import FileEntriesResult, FileEntry, SchemaValidationWarning, UserStatus
from .output import OutputFormatter
from .upload_preview import display_upload_preview
from .utils import calculate_drime_hash, parse_iso_timestamp
from .workspace_utils import format_workspace_display, get_folder_display_name

logger = logging.getLogger(__name__)


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
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose/debug logging output",
)
@click.version_option()
@click.pass_context
def main(
    ctx: Any,
    api_key: Optional[str],
    quiet: bool,
    json: bool,
    validate_schema: bool,
    verbose: bool,
) -> None:
    """PyDrime - Upload & Download files and directories to Drime Cloud."""
    # Store settings in context for subcommands to access
    ctx.ensure_object(dict)
    ctx.obj["api_key"] = api_key
    ctx.obj["out"] = OutputFormatter(json_output=json, quiet=quiet)
    ctx.obj["validate_schema"] = validate_schema
    ctx.obj["verbose"] = verbose

    # Configure logging based on verbose flag
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%H:%M:%S",
        )
        # Enable debug logging for pydrime modules
        logging.getLogger("pydrime").setLevel(logging.DEBUG)
    else:
        # Set default logging level to WARNING to suppress debug/info messages
        logging.basicConfig(level=logging.WARNING)

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
@click.option(
    "--start-delay",
    type=float,
    default=0.0,
    help="Delay in seconds between starting each parallel upload (default: 0.0)",
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
    start_delay: float,
) -> None:
    """Upload a file or directory to Drime Cloud.

    PATH: Local file or directory to upload
    """
    from .sync import SyncEngine

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

        # Show parallel upload settings
        if workers > 1:
            out.info(f"Parallel workers: {workers}")
            if start_delay > 0:
                out.info(f"Start delay between uploads: {start_delay}s")

        out.info("")  # Empty line for readability

    # Handle single file upload separately (not using sync engine)
    if local_path.is_file():
        _upload_single_file(
            ctx=ctx,
            client=client,
            local_path=local_path,
            remote_path=remote_path,
            workspace=workspace,
            current_folder_id=current_folder_id,
            current_folder_name=current_folder_name,
            dry_run=dry_run,
            on_duplicate=on_duplicate,
            no_progress=no_progress,
            chunk_size_bytes=chunk_size_bytes,
            multipart_threshold_bytes=multipart_threshold_bytes,
            out=out,
        )
        return

    # Directory upload - collect files for preview and duplicate handling
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

        # Build skip set and rename map for sync engine
        # The DuplicateHandler uses paths like "sync/test_folder/file.txt"
        # (with folder name prefix from scan_directory using base_path=parent)
        # but SyncEngine.upload_folder scans relative to local_path, so paths
        # are like "test_folder/file.txt". We need to strip the folder prefix.
        folder_prefix = f"{local_path.name}/"
        files_to_skip = {
            p[len(folder_prefix) :] if p.startswith(folder_prefix) else p
            for p in dup_handler.files_to_skip
        }
        file_renames = {
            (k[len(folder_prefix) :] if k.startswith(folder_prefix) else k): v
            for k, v in dup_handler.rename_map.items()
        }

        # Determine the remote path for the sync engine
        # When remote_path is specified, include the local folder name
        # e.g., uploading "test/" with remote_path="dest" -> "dest/test/..."
        if remote_path:
            effective_remote_path = f"{remote_path}/{local_path.name}"
        else:
            effective_remote_path = local_path.name

        # Create output formatter for engine (respect no_progress)
        engine_out = OutputFormatter(
            json_output=out.json_output, quiet=no_progress or out.quiet
        )

        # Create sync engine and upload using sync infrastructure
        engine = SyncEngine(client, engine_out)

        stats = engine.upload_folder(
            local_path=local_path,
            remote_path=effective_remote_path,
            workspace_id=workspace,
            parent_id=current_folder_id,
            max_workers=workers,
            start_delay=start_delay,
            chunk_size=chunk_size_bytes,
            multipart_threshold=multipart_threshold_bytes,
            files_to_skip=files_to_skip,
            file_renames=file_renames,
        )

        # Show summary
        if out.json_output:
            out.output_json(
                {
                    "success": stats["uploads"],
                    "failed": stats["errors"],
                    "skipped": stats["skips"],
                }
            )
        else:
            if engine_out.quiet:
                # Engine didn't show summary, show it here
                summary_items = [
                    ("Successfully uploaded", f"{stats['uploads']} files"),
                ]
                if stats["skips"] > 0:
                    summary_items.append(("Skipped", f"{stats['skips']} files"))
                if stats["errors"] > 0:
                    summary_items.append(("Failed", f"{stats['errors']} files"))

                out.print_summary("Upload Complete", summary_items)

        if stats["errors"] > 0:
            ctx.exit(1)

    except KeyboardInterrupt:
        out.warning("\nUpload cancelled by user")
        ctx.exit(130)  # Standard exit code for SIGINT
    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)


def _upload_single_file(
    ctx: Any,
    client: DrimeClient,
    local_path: Path,
    remote_path: Optional[str],
    workspace: int,
    current_folder_id: Optional[int],
    current_folder_name: Optional[str],
    dry_run: bool,
    on_duplicate: str,
    no_progress: bool,
    chunk_size_bytes: int,
    multipart_threshold_bytes: int,
    out: OutputFormatter,
) -> None:
    """Handle single file upload (not using sync engine).

    This function handles the upload of a single file, including
    dry-run preview, duplicate handling, and progress display.
    """

    files_to_upload = [(local_path, remote_path or local_path.name)]

    if dry_run:
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

    # Display summary for actual upload
    display_upload_preview(
        out,
        client,
        files_to_upload,
        workspace,
        current_folder_id,
        current_folder_name,
        is_dry_run=False,
    )

    try:
        # Handle duplicates for single file
        dup_handler = DuplicateHandler(
            client, out, workspace, on_duplicate, current_folder_id
        )
        dup_handler.validate_and_handle_duplicates(files_to_upload)

        if not dup_handler.delete_marked_entries():
            ctx.exit(1)

        file_path, rel_path = files_to_upload[0]

        # Check if file should be skipped
        if rel_path in dup_handler.files_to_skip:
            if not out.quiet:
                out.info(f"Skipping: {rel_path}")
            if out.json_output:
                out.output_json({"success": 0, "failed": 0, "skipped": 1})
            else:
                out.print_summary("Upload Complete", [("Skipped", "1 file")])
            return

        # Apply rename if needed
        upload_path = dup_handler.apply_renames(rel_path)

        # Create progress display for single file
        if not no_progress and not out.quiet:
            progress_display = Progress(
                "[progress.description]{task.description}",
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                refresh_per_second=10,
            )
        else:
            progress_display = None

        try:
            if progress_display:
                progress_display.start()
                file_size = file_path.stat().st_size
                task_id = progress_display.add_task(
                    f"[cyan]{file_path.name}",
                    total=file_size,
                )

                def progress_callback(bytes_uploaded: int, total_bytes: int) -> None:
                    progress_display.update(
                        task_id, completed=bytes_uploaded, total=total_bytes
                    )
            else:
                task_id = None
                progress_callback = None
                file_size = file_path.stat().st_size
                size_str = out.format_size(file_size)
                out.progress_message(f"Uploading {upload_path} ({size_str})")

            result = client.upload_file(
                file_path,
                parent_id=current_folder_id,
                relative_path=upload_path,
                workspace_id=workspace,
                progress_callback=progress_callback,
                chunk_size=chunk_size_bytes,
                use_multipart_threshold=multipart_threshold_bytes,
            )

            # Show summary
            if out.json_output:
                uploaded_files = []
                if isinstance(result, dict) and "fileEntry" in result:
                    entry = result["fileEntry"]
                    uploaded_files.append(
                        {
                            "path": upload_path,
                            "id": entry.get("id"),
                            "hash": entry.get("hash"),
                        }
                    )
                out.output_json(
                    {"success": 1, "failed": 0, "skipped": 0, "files": uploaded_files}
                )
            else:
                out.print_summary(
                    "Upload Complete", [("Successfully uploaded", "1 file")]
                )

        finally:
            if progress_display:
                progress_display.stop()

    except KeyboardInterrupt:
        out.warning("\nUpload cancelled by user")
        ctx.exit(130)
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
    "--folder-hash", type=str, help="Display files in specified folder hash/page"
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
@click.option("--page", "-p", type=int, default=1, help="Page number (1-based)")
@click.option(
    "--page-size", type=int, default=50, help="Number of items per page (default: 50)"
)
@click.pass_context
def ls(  # noqa: C901
    ctx: Any,
    parent_identifier: Optional[str],
    deleted: bool,
    starred: bool,
    recent: bool,
    shared: bool,
    folder_hash: Optional[str],
    workspace: Optional[int],
    query: Optional[str],
    type: Optional[str],
    recursive: bool,
    page: int,
    page_size: int,
) -> None:
    """List files and folders in a Drime Cloud directory.

    PARENT_IDENTIFIER: ID or name of parent folder (omit to list current directory)

    Similar to Unix ls command, shows file and folder names in a columnar format.
    Use 'du' command for detailed disk usage information.

    Examples:
        pydrime ls                          # List current directory
        pydrime ls 480432024                # List folder by ID
        pydrime ls test_folder              # List folder by name
        pydrime ls Documents                # List folder by name
        pydrime ls --page 2 --page-size 100 # List page 2 with 100 items per page
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
        elif not any([deleted, starred, recent, shared, folder_hash, query]):
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
            "folder_id": folder_hash,
            "page_id": folder_hash,
            "per_page": page_size,
            "page": page,
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
            if not out.quiet and file_entries.pagination:
                # Show pagination info even if no results on this page
                pagination = file_entries.pagination
                if pagination.get("total"):
                    out.info(f"No results on page {page}")
                    out.info(
                        f"Total: {pagination['total']} items across "
                        f"{pagination.get('last_page', '?')} pages"
                    )
            return

        # Text format - simple list of names (like Unix ls)
        table_data = file_entries.to_table_data()
        out.output_table(
            table_data,
            ["name"],
            {"name": "Name"},
        )

        # Display pagination info if not recursive
        if not out.quiet and not recursive and file_entries.pagination:
            pagination = file_entries.pagination
            current = pagination.get("current_page", page)
            total_pages = pagination.get("last_page")
            total_items = pagination.get("total")
            from_item = pagination.get("from")
            to_item = pagination.get("to")
            next_page = pagination.get("next_page")
            prev_page = pagination.get("prev_page")

            if total_items is not None:
                out.info("")
                out.info(f"Page {current} of {total_pages}")
                out.info(f"Showing items {from_item}-{to_item} of {total_items} total")

                # Show navigation hints
                hints = []
                if next_page:
                    hints.append(f"--page {next_page} for next page")
                if prev_page:
                    hints.append(f"--page {prev_page} for previous page")
                if hints:
                    out.info(f"Use {' or '.join(hints)}")

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

    ENTRY_IDENTIFIERS: One or more file/folder paths, names, hashes, or numeric IDs

    Supports file/folder paths (e.g., folder/file.txt), names (resolved in current
    directory), numeric IDs, and hashes. Folders are automatically downloaded
    recursively with all their contents using the sync engine for reliable downloads.

    Examples:
        pydrime download folder/file.txt              # Download by path
        pydrime download a/b/c/file.txt               # Download from nested path
        pydrime download 480424796                    # Download file by ID
        pydrime download NDgwNDI0Nzk2fA               # Download file by hash
        pydrime download test1.txt                    # Download file by name
        pydrime download test_folder                  # Download folder (uses sync)
        pydrime download 480424796 480424802          # Multiple files by ID
        pydrime download -o ./dest test_folder        # Download to dir
        pydrime download test_folder --on-duplicate skip    # Skip existing
    """
    from .download_helpers import (
        download_single_file,
        get_entry_from_hash,
        resolve_identifier_to_hash,
    )
    from .sync import SyncEngine

    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        downloaded_files: list[dict] = []
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        # Create output directory if specified
        output_dir = Path(output) if output else Path.cwd()
        if output and not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        def resolve_and_get_entry(identifier: str) -> Optional[FileEntry]:
            """Resolve identifier and get the FileEntry object."""
            hash_value = resolve_identifier_to_hash(
                client, identifier, current_folder, workspace, out
            )
            if not hash_value:
                return None
            return get_entry_from_hash(client, hash_value, identifier, out)

        def download_entry_with_sync(
            identifier: str,
            dest_path: Optional[Path] = None,
        ) -> bool:
            """Download entry using sync engine for folders, direct for files.

            Returns:
                True if download succeeded, False if entry not found or error occurred.
            """
            entry = resolve_and_get_entry(identifier)
            if not entry:
                return False

            if entry.is_folder:
                # Use sync engine for folder downloads (tested, parallel support)
                folder_path = dest_path / entry.name if dest_path else Path(entry.name)

                # Check if a file exists with the folder name
                if folder_path.exists() and folder_path.is_file():
                    out.error(
                        f"Cannot download folder '{entry.name}': "
                        f"a file with this name already exists at {folder_path}"
                    )
                    return False

                # Create engine with appropriate output settings
                engine_out = OutputFormatter(
                    json_output=out.json_output,
                    quiet=no_progress or out.quiet,
                )
                engine = SyncEngine(client, engine_out)

                # Use sync engine's download_folder method
                # overwrite=True uses CLOUD_BACKUP mode (download only, no delete)
                stats = engine.download_folder(
                    remote_entry=entry,
                    local_path=folder_path,
                    workspace_id=workspace,
                    overwrite=(on_duplicate == "overwrite"),
                    max_workers=workers,
                )

                # Track downloaded files for JSON output
                downloaded_files.append(
                    {
                        "type": "folder",
                        "name": entry.name,
                        "path": str(folder_path),
                        "input": identifier,
                        "downloads": stats["downloads"],
                        "skips": stats["skips"],
                        "errors": stats.get("errors", 0),
                    }
                )
            else:
                # Use direct download for single files
                file_path = dest_path / entry.name if dest_path else None
                download_single_file(
                    client=client,
                    hash_value=entry.hash,
                    identifier=identifier,
                    dest_path=file_path,
                    entry_name=entry.name,
                    downloaded_files=downloaded_files,
                    out=out,
                    on_duplicate=on_duplicate,
                    no_progress=no_progress,
                    output_override=output if len(entry_identifiers) == 1 else None,
                    single_file=(len(entry_identifiers) == 1),
                    show_progress=True,
                )
            return True

        # Process all identifiers
        nonlocal_error_count = [0]  # Use list to allow modification in nested function

        def process_identifier(identifier: str, dest: Optional[Path]) -> None:
            """Process a single identifier, tracking errors."""
            success = download_entry_with_sync(identifier, dest)
            if not success:
                nonlocal_error_count[0] += 1

        if workers > 1 and len(entry_identifiers) > 1:
            # Parallel download for multiple entries
            # Note: For folders, each folder download already uses parallel workers
            # internally via SyncEngine, so we use sequential here to avoid
            # over-parallelization
            has_folders = False
            for identifier in entry_identifiers:
                entry = resolve_and_get_entry(identifier)
                if entry and entry.is_folder:
                    has_folders = True
                    break

            if has_folders:
                # Sequential for folder downloads (they parallelize internally)
                for identifier in entry_identifiers:
                    process_identifier(identifier, output_dir if output else None)
            else:
                # Parallel for multiple file downloads
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {}
                    for identifier in entry_identifiers:
                        future = executor.submit(
                            process_identifier,
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
                                nonlocal_error_count[0] += 1
                    except KeyboardInterrupt:
                        out.warning(
                            "\nDownload interrupted by user. "
                            "Cancelling pending downloads..."
                        )
                        for future in futures:
                            future.cancel()
                        raise
        else:
            # Sequential download
            for identifier in entry_identifiers:
                process_identifier(identifier, output_dir if output else None)

        if out.json_output:
            out.output_json({"files": downloaded_files})

        # Exit with error if all downloads failed
        if nonlocal_error_count[0] == len(entry_identifiers):
            ctx.exit(1)

    except KeyboardInterrupt:
        out.warning("\nDownload cancelled by user")
        ctx.exit(130)  # Standard exit code for SIGINT
    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.command()
@click.argument("path", type=str)
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
@click.option(
    "--batch-size",
    "-b",
    type=int,
    default=50,
    help="Number of remote files to process per batch in streaming mode (default: 50)",
)
@click.option(
    "--no-streaming",
    is_flag=True,
    help="Disable streaming mode (scan all files upfront instead of batch processing)",
)
@click.option(
    "--workers",
    type=int,
    default=1,
    help="Number of parallel workers for uploads/downloads (default: 1)",
)
@click.option(
    "--start-delay",
    type=float,
    default=0.0,
    help="Delay in seconds between starting each parallel operation (default: 0.0)",
)
@click.pass_context
def sync(
    ctx: Any,
    path: str,
    remote_path: Optional[str],
    workspace: Optional[int],
    dry_run: bool,
    no_progress: bool,
    chunk_size: int,
    multipart_threshold: int,
    batch_size: int,
    no_streaming: bool,
    workers: int,
    start_delay: float,
) -> None:
    """Sync files between local directory and Drime Cloud.

    PATH: Local directory to sync OR literal sync pair in format:
          /local/path:syncMode:/remote/path

    Sync Modes:
      - twoWay (tw): Mirror every action in both directions
      - localToCloud (ltc): Mirror local actions to cloud only
      - localBackup (lb): Upload to cloud, never delete
      - cloudToLocal (ctl): Mirror cloud actions to local only
      - cloudBackup (cb): Download from cloud, never delete

    Examples:
        # Directory path with default two-way sync
        pydrime sync ./my_folder
        pydrime sync ./docs -r remote_docs

        # Literal sync pairs with explicit modes
        pydrime sync /home/user/docs:twoWay:/Documents
        pydrime sync /home/user/pics:localToCloud:/Pictures
        pydrime sync ./local:localBackup:/Backup
        pydrime sync ./data:cloudToLocal:/CloudData
        pydrime sync ./archive:cloudBackup:/Archive

        # With abbreviations
        pydrime sync /home/user/pics:tw:/Pictures
        pydrime sync ./backup:ltc:/CloudBackup
        pydrime sync ./local:lb:/Backup

        # Other options
        pydrime sync . -w 5                          # Sync in workspace 5
        pydrime sync ./data --dry-run                # Preview sync changes
        pydrime sync ./data -b 100                   # Process 100 files per batch
        pydrime sync ./data --no-streaming           # Scan all files upfront
    """
    from .sync import SyncEngine, SyncMode, SyncPair

    out: OutputFormatter = ctx.obj["out"]

    # Detect if path is a literal sync pair format
    # Format: /local:mode:/remote (3 parts) or /local:/remote (2 parts)
    # On Windows, paths like C:\Users\... have a colon after drive letter,
    # so we need to handle this case specially.
    # A literal pair must have colons that are NOT drive letter colons.
    # We detect this by checking if the path looks like a Windows drive path.
    import re

    # Check if path starts with a Windows drive letter (e.g., C:, D:)
    # If so, only consider colons after the drive letter for splitting
    windows_drive_match = re.match(r"^([A-Za-z]:)", path)
    if windows_drive_match:
        # Windows path: split only on colons after the drive letter
        drive = windows_drive_match.group(1)
        rest_of_path = path[len(drive) :]
        parts = rest_of_path.split(":")
        # Prepend the drive to the first part
        if parts:
            parts[0] = drive + parts[0]
    else:
        parts = path.split(":")

    is_literal_pair = len(parts) >= 2 and len(parts) <= 3

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
    if batch_size < 1:
        out.error("Batch size must be at least 1")
        ctx.exit(1)
    if batch_size > 1000:
        out.error("Batch size cannot exceed 1000")
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

    # Parse path and create sync pair
    pair: SyncPair  # Type hint to satisfy type checker
    if is_literal_pair:
        # Path is a literal sync pair: /local:mode:/remote
        if remote_path is not None:
            out.error(
                "Cannot use --remote-path with literal sync pair format. "
                "Use '/local:mode:/remote' format instead."
            )
            ctx.exit(1)

        try:
            pair = SyncPair.parse_literal(path)
            pair.workspace_id = workspace
            local_path = pair.local

            if not out.quiet:
                out.info(f"Parsed sync pair: {pair.sync_mode.value}")
                out.info(f"  Local:  {pair.local}")
                out.info(f"  Remote: {pair.remote}")
        except ValueError as e:
            out.error(f"Invalid sync pair format: {e}")
            ctx.exit(1)
            return  # Unreachable, but helps type checker
    else:
        # Path is a simple directory path
        local_path = Path(path)

        # Validate path exists and is a directory
        if not local_path.exists():
            out.error(f"Path does not exist: {path}")
            ctx.exit(1)

        if not local_path.is_dir():
            out.error(f"Path is not a directory: {path}")
            ctx.exit(1)

        # Determine remote path
        if remote_path is None:
            # Use folder name as remote path
            remote_path = local_path.name

        # Create sync pair with TWO_WAY as default
        pair = SyncPair(
            local=local_path,
            remote=remote_path,
            sync_mode=SyncMode.TWO_WAY,
            workspace_id=workspace,
        )

    if not out.quiet:
        # Show workspace information
        workspace_display, _ = format_workspace_display(client, workspace)
        out.info(f"Workspace: {workspace_display}")
        out.info(f"Sync mode: {pair.sync_mode.value}")
        out.info(f"Local path: {pair.local}")
        out.info(f"Remote path: {pair.remote}")
        out.info("")  # Empty line for readability

    try:
        # Create output formatter for engine (respect no_progress)
        engine_out = OutputFormatter(
            json_output=out.json_output, quiet=no_progress or out.quiet
        )

        # Create sync engine
        engine = SyncEngine(client, engine_out)

        # Execute sync
        stats = engine.sync_pair(
            pair,
            dry_run=dry_run,
            chunk_size=chunk_size_bytes,
            multipart_threshold=multipart_threshold_bytes,
            batch_size=batch_size,
            use_streaming=not no_streaming,
            max_workers=workers,
            start_delay=start_delay,
        )

        # Output results
        if out.json_output:
            out.output_json(stats)

        # Exit with warning if there were conflicts
        if stats.get("conflicts", 0) > 0 and not out.quiet:
            out.warning(
                f"\n⚠  {stats['conflicts']} conflict(s) were skipped. "
                "Please resolve conflicts manually."
            )

    except KeyboardInterrupt:
        out.warning("\nSync cancelled by user")
        ctx.exit(130)
    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)
    except Exception as e:
        out.error(f"Error: {e}")
        ctx.exit(1)


@main.command()
@click.argument("identifier", type=str)
@click.pass_context
def stat(ctx: Any, identifier: str) -> None:
    """Show detailed statistics for a file or folder.

    IDENTIFIER: File/folder path, name, hash, or numeric ID

    Supports paths (folder/file.txt), names (resolved in current directory),
    numeric IDs, and hashes.

    Examples:
        pydrime stat my-file.txt             # By name in current folder
        pydrime stat myfolder/my-file.txt    # By path
        pydrime stat 480424796               # By numeric ID
        pydrime stat NDgwNDI0Nzk2fA          # By hash
        pydrime stat "My Documents"          # Folder by name
    """
    from .download_helpers import get_entry_from_hash, resolve_identifier_to_hash

    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        # Resolve identifier to hash (supports paths, names, IDs, hashes)
        hash_value = resolve_identifier_to_hash(
            client, identifier, current_folder, workspace, out
        )

        if not hash_value:
            out.error(f"Entry not found: {identifier}")
            ctx.exit(1)
            return  # For type checker

        # Get the entry details
        entry = get_entry_from_hash(client, hash_value, identifier, out)

        if not entry:
            ctx.exit(1)
            return  # For type checker

        # Get owner info from users list
        owner_email = None
        for user in entry.users:
            if user.owns_entry:
                owner_email = user.email
                break

        # Output based on format
        if out.json_output:
            # Convert entry back to dict format
            entry_dict = {
                "id": entry.id,
                "name": entry.name,
                "type": entry.type,
                "hash": entry.hash,
                "size": entry.file_size,
                "size_formatted": (
                    out.format_size(entry.file_size) if entry.file_size else None
                ),
                "parent_id": entry.parent_id,
                "created_at": entry.created_at,
                "updated_at": entry.updated_at,
                "owner": owner_email,
                "public": entry.public,
                "description": entry.description,
                "extension": entry.extension,
                "mime": entry.mime,
                "workspace_id": entry.workspace_id,
            }
            out.output_json(entry_dict)
        else:
            # Text format - display as a table
            icon = "📁" if entry.type == "folder" else "📄"

            # Format timestamps
            created_dt = parse_iso_timestamp(entry.created_at)
            created_str = (
                created_dt.strftime("%Y-%m-%d %H:%M:%S") if created_dt else "-"
            )

            updated_dt = parse_iso_timestamp(entry.updated_at)
            updated_str = (
                updated_dt.strftime("%Y-%m-%d %H:%M:%S") if updated_dt else "-"
            )

            # Build table data
            table_data = [
                {"field": "Name", "value": f"{icon} {entry.name}"},
                {"field": "Type", "value": entry.type or "-"},
                {"field": "ID", "value": str(entry.id)},
                {"field": "Hash", "value": entry.hash or "-"},
            ]

            # Add size (for files)
            if entry.file_size:
                size_str = (
                    f"{out.format_size(entry.file_size)} ({entry.file_size:,} bytes)"
                )
                table_data.append({"field": "Size", "value": size_str})

            # Add extension and mime type (for files)
            if entry.extension:
                table_data.append({"field": "Extension", "value": entry.extension})
            if entry.mime:
                table_data.append({"field": "MIME Type", "value": entry.mime})

            # Add location info
            if entry.parent_id:
                table_data.append({"field": "Parent ID", "value": str(entry.parent_id)})
            else:
                table_data.append({"field": "Parent ID", "value": "Root"})

            if entry.workspace_id:
                table_data.append(
                    {"field": "Workspace ID", "value": str(entry.workspace_id)}
                )

            # Add timestamps
            table_data.append({"field": "Created", "value": created_str})
            table_data.append({"field": "Updated", "value": updated_str})

            # Add owner
            if owner_email:
                table_data.append({"field": "Owner", "value": owner_email})

            # Add flags
            flags = []
            if entry.public:
                flags.append("Public")
            if flags:
                table_data.append({"field": "Flags", "value": ", ".join(flags)})

            # Add description
            if entry.description:
                table_data.append({"field": "Description", "value": entry.description})

            # Output the table
            out.output_table(
                table_data,
                ["field", "value"],
                {"field": "Field", "value": "Value"},
            )

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
@click.option("--no-trash", is_flag=True, help="Delete permanently (cannot be undone)")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses default workspace if not specified)",
)
@click.pass_context
def rm(
    ctx: Any,
    entry_identifiers: tuple[str, ...],
    no_trash: bool,
    yes: bool,
    workspace: Optional[int],
) -> None:
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
        pydrime rm --no-trash test1.txt         # Permanent deletion
        pydrime rm -w 5 test.txt                # Delete in workspace 5
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    # Use default workspace if none specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()

        # Resolve all identifiers to entry IDs
        entry_ids = []
        for identifier in entry_identifiers:
            try:
                # Check if identifier is a path (contains /)
                if "/" in identifier:
                    entry_id = client.resolve_path_to_id(
                        path=identifier,
                        workspace_id=workspace,
                    )
                else:
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
        action = "permanently delete" if no_trash else "move to trash"
        if (
            not yes
            and not out.quiet
            and not click.confirm(
                f"Are you sure you want to {action} {len(entry_ids)} item(s)?"
            )
        ):
            out.warning("Deletion cancelled.")
            return

        result = client.delete_file_entries(
            entry_ids, delete_forever=no_trash, workspace_id=workspace
        )

        if out.json_output:
            out.output_json(result)
        else:
            if no_trash:
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
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=0,
    help="Workspace ID (default: 0 for personal workspace)",
)
@click.pass_context
def folders(ctx: Any, workspace: int) -> None:
    """List all folders in a workspace.

    Shows folder ID, name, parent ID, and path for all folders
    accessible to the current user in the specified workspace.
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'drime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get current user ID
        user_info = client.get_logged_user()
        if not user_info or not user_info.get("user"):
            out.error("Failed to get user information")
            ctx.exit(1)

        user_id = user_info["user"].get("id")
        if not user_id:
            out.error("User ID not found in response")
            ctx.exit(1)

        # Get folders for the user
        result = client.get_user_folders(user_id, workspace)

        if out.json_output:
            out.output_json(result)
            return

        if isinstance(result, dict) and "folders" in result:
            folders_list = result["folders"]

            if not folders_list:
                out.warning("No folders found")
                return

            table_data = []
            for folder in folders_list:
                table_data.append(
                    {
                        "id": str(folder.get("id", "")),
                        "name": folder.get("name", ""),
                        "parent_id": str(folder.get("parent_id") or "root"),
                        "path": folder.get("path", "/"),
                    }
                )

            out.output_table(
                table_data,
                ["id", "name", "parent_id", "path"],
                {
                    "id": "ID",
                    "name": "Name",
                    "parent_id": "Parent",
                    "path": "Path",
                },
            )
        else:
            out.warning("Unexpected response format")
            out.output_json(result)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@main.group()
@click.pass_context
def vault(ctx: Any) -> None:
    """Manage encrypted vault storage.

    Commands for working with your encrypted vault.

    Examples:
        pydrime vault show                # Show vault info
        pydrime vault ls                  # List vault root
        pydrime vault ls Test1            # List folder by name
    """
    pass


# Environment variable name for vault password (in-memory only, not stored to disk)
VAULT_PASSWORD_ENV_VAR = "PYDRIME_VAULT_PASSWORD"


def get_vault_password_from_env() -> Optional[str]:
    """Get vault password from environment variable if set."""
    import os

    return os.environ.get(VAULT_PASSWORD_ENV_VAR)


@vault.command("unlock")
@click.pass_context
def vault_unlock(ctx: Any) -> None:
    """Unlock the vault for the current shell session.

    Prompts for your vault password and outputs shell commands to set
    an environment variable. The password is stored in memory only
    and never written to disk.

    Usage (bash/zsh):
        eval $(pydrime vault unlock)

    Usage (fish):
        pydrime vault unlock | source

    After unlocking, vault commands won't prompt for password.
    Use 'pydrime vault lock' to clear the password from your session.
    """
    from .vault_crypto import VaultPasswordError, unlock_vault

    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info for password verification
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        # Get encryption parameters from vault
        salt = vault_info.get("salt")
        check = vault_info.get("check")
        iv = vault_info.get("iv")

        if not all([salt, check, iv]):
            out.error("Vault encryption parameters not found.")
            ctx.exit(1)
            return

        # Prompt for password
        password = click.prompt("Vault password", hide_input=True, err=True)

        # Verify the password
        try:
            unlock_vault(password, salt, check, iv)
        except VaultPasswordError:
            out.error("Invalid vault password.")
            ctx.exit(1)
            return

        # Output shell command to set environment variable
        # Using click.echo to bypass OutputFormatter and write to stdout
        click.echo(f"export {VAULT_PASSWORD_ENV_VAR}='{password}'")

        # Print success message to stderr so it doesn't interfere with eval
        click.echo("Vault unlocked. Password stored in shell session.", err=True)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("lock")
@click.pass_context
def vault_lock(ctx: Any) -> None:
    """Lock the vault and clear password from shell session.

    Outputs shell commands to unset the vault password environment variable.

    Usage (bash/zsh):
        eval $(pydrime vault lock)

    Usage (fish):
        pydrime vault lock | source
    """
    # Output shell command to unset environment variable
    click.echo(f"unset {VAULT_PASSWORD_ENV_VAR}")

    # Print message to stderr so it doesn't interfere with eval
    click.echo("Vault locked. Password cleared from shell session.", err=True)


@vault.command("show")
@click.pass_context
def vault_show(ctx: Any) -> None:
    """Show vault information.

    Displays metadata about your encrypted vault including ID and timestamps.

    Examples:
        pydrime vault show                # Show vault info
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info
        vault_result = client.get_vault()

        if out.json_output:
            out.output_json(vault_result)
            return

        vault_info = vault_result.get("vault")
        if not vault_info:
            out.warning("No vault found. You may need to set up a vault first.")
            return

        # Display vault info
        out.print(f"ID: {vault_info.get('id')}")
        out.print(f"User ID: {vault_info.get('user_id')}")
        out.print(f"Created: {vault_info.get('created_at', 'N/A')}")
        out.print(f"Updated: {vault_info.get('updated_at', 'N/A')}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("ls")
@click.argument("folder_identifier", type=str, default="", required=False)
@click.option(
    "--page",
    "-p",
    type=int,
    default=1,
    help="Page number (default: 1)",
)
@click.option(
    "--page-size",
    type=int,
    default=50,
    help="Number of items per page (default: 50)",
)
@click.option(
    "--order-by",
    type=click.Choice(["updated_at", "created_at", "name", "file_size"]),
    default="updated_at",
    help="Field to order by (default: updated_at)",
)
@click.option(
    "--order",
    type=click.Choice(["asc", "desc"]),
    default="desc",
    help="Order direction (default: desc)",
)
@click.pass_context
def vault_ls(
    ctx: Any,
    folder_identifier: str,
    page: int,
    page_size: int,
    order_by: str,
    order: str,
) -> None:
    """List files and folders in the vault.

    Lists encrypted files and folders stored in your vault.

    FOLDER_IDENTIFIER: Folder name, ID, or hash to list (default: root)

    Examples:
        pydrime vault ls                  # List root vault folder
        pydrime vault ls Test1            # List folder by name
        pydrime vault ls 34430            # List folder by ID
        pydrime vault ls MzQ0MzB8cGFkZA   # List folder by hash
        pydrime vault ls --page 2         # Show page 2 of results
        pydrime vault ls --order-by name  # Sort by name
    """
    from typing import Literal

    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info to get vault_id
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")
        vault_id: Optional[int] = vault_info.get("id") if vault_info else None

        # Resolve folder identifier to folder hash
        folder_hash: str = ""
        resolved_folder_name: Optional[str] = None

        if folder_identifier:
            # Check if it's a numeric ID - convert to hash
            if folder_identifier.isdigit():
                folder_hash = calculate_drime_hash(int(folder_identifier))
            else:
                # Could be a name or already a hash
                # First check if it looks like a hash (base64-like)
                # Try to find folder by name in vault root
                search_result = client.get_vault_file_entries(per_page=1000)

                # Handle response format
                search_entries = []
                if isinstance(search_result, dict):
                    if "data" in search_result:
                        search_entries = search_result["data"]
                    elif "pagination" in search_result and isinstance(
                        search_result["pagination"], dict
                    ):
                        search_entries = search_result["pagination"].get("data", [])

                found = False
                for entry in search_entries:
                    entry_name = entry.get("name", "")
                    entry_hash = entry.get("hash", "")
                    entry_type = entry.get("type", "")

                    # Match by name or hash, only folders
                    if entry_type == "folder" and (
                        entry_name == folder_identifier
                        or entry_hash == folder_identifier
                    ):
                        folder_hash = entry_hash
                        resolved_folder_name = entry_name
                        found = True
                        if not out.quiet:
                            out.info(
                                f"Resolved '{folder_identifier}' to folder "
                                f"hash: {folder_hash}"
                            )
                        break

                if not found:
                    # Maybe it's already a valid hash, try using it directly
                    folder_hash = folder_identifier

        # Show current path if we're in a subfolder
        if folder_hash and vault_id and not out.quiet:
            try:
                path_result = client.get_folder_path(folder_hash, vault_id=vault_id)
                if isinstance(path_result, dict) and "path" in path_result:
                    path_parts = [f.get("name", "?") for f in path_result["path"]]
                    current_path = "/" + "/".join(path_parts)
                    out.info(f"Path: {current_path}")
                    out.info("")
            except DrimeAPIError:
                # Silently ignore path errors, just don't show path
                pass

        # Cast order to Literal type for type checker
        order_dir = cast(Literal["asc", "desc"], order)

        # Get vault file entries
        result = client.get_vault_file_entries(
            folder_hash=folder_hash,
            page=page,
            per_page=page_size,
            order_by=order_by,
            order_dir=order_dir,
        )

        if out.json_output:
            out.output_json(result)
            return

        # Handle different response formats:
        # - {"data": [...]} - data at top level
        # - {"pagination": {"data": [...], ...}} - data nested in pagination
        entries = None
        pagination = None

        if isinstance(result, dict):
            if "data" in result:
                entries = result["data"]
                pagination = result.get("pagination") or result.get("meta")
            elif "pagination" in result and isinstance(result["pagination"], dict):
                pagination = result["pagination"]
                entries = pagination.get("data", [])

        if entries is None:
            out.warning("Unexpected response format")
            out.output_json(result)
            return

        if not entries:
            if not folder_hash:
                out.info("Vault is empty")
            else:
                folder_display = (
                    resolved_folder_name or folder_identifier or folder_hash
                )
                out.info(f"No files in vault folder '{folder_display}'")
            return

        # Display files in table format (same as regular ls)
        table_data = []
        for entry in entries:
            entry_type = entry.get("type", "file")
            name = entry.get("name", "Unknown")

            # Format created timestamp
            created_at = entry.get("created_at", "")
            created_str = ""
            if created_at:
                created_dt = parse_iso_timestamp(created_at)
                created_str = (
                    created_dt.strftime("%Y-%m-%d %H:%M:%S") if created_dt else ""
                )

            table_data.append(
                {
                    "id": str(entry.get("id", "")),
                    "name": name,
                    "type": entry_type,
                    "size": out.format_size(entry.get("file_size", 0)),
                    "hash": entry.get("hash", ""),
                    "parent_id": str(entry.get("parent_id", ""))
                    if entry.get("parent_id")
                    else "-",
                    "created": created_str,
                }
            )

        out.output_table(
            table_data,
            ["id", "name", "type", "size", "hash", "parent_id", "created"],
            {
                "id": "ID",
                "name": "Name",
                "type": "Type",
                "size": "Size",
                "hash": "Hash",
                "parent_id": "Parent ID",
                "created": "Created",
            },
        )

        # Show pagination info if available
        if pagination:
            current = pagination.get("current_page", page)
            total_pages = pagination.get("last_page")
            total_items = pagination.get("total")
            if total_items is not None and total_pages is not None:
                out.info("")
                out.info(f"Page {current} of {total_pages} ({total_items} total)")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("download")
@click.argument("file_identifier", type=str)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output file path (default: current directory with original filename)",
)
@click.option(
    "--password",
    "-p",
    type=str,
    default=None,
    help="Vault password (will prompt if not provided)",
)
@click.pass_context
def vault_download(
    ctx: Any,
    file_identifier: str,
    output: Optional[str],
    password: Optional[str],
) -> None:
    """Download a file from the vault.

    Downloads an encrypted file from your vault and decrypts it locally.
    You will be prompted for your vault password.

    FILE_IDENTIFIER: File path, name, ID, or hash to download

    Examples:
        pydrime vault download document.pdf              # Download from root
        pydrime vault download Test1/document.pdf        # Download from subfolder
        pydrime vault download 34431                     # Download by ID
        pydrime vault download MzQ0MzF8cGFkZA           # Download by hash
        pydrime vault download doc.pdf -o out.pdf       # Download to specific path
    """
    from .vault_crypto import VaultPasswordError, decrypt_file_content, unlock_vault

    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info for password verification
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        # Get encryption parameters from vault
        salt = vault_info.get("salt")
        check = vault_info.get("check")
        iv = vault_info.get("iv")

        if not all([salt, check, iv]):
            out.error("Vault encryption parameters not found.")
            ctx.exit(1)
            return

        # Get password from: CLI option > environment variable > prompt
        if not password:
            password = get_vault_password_from_env()
        if not password:
            password = click.prompt("Vault password", hide_input=True)

        # Unlock the vault (verify password)
        if not out.quiet:
            out.info("Verifying vault password...")

        try:
            vault_key = unlock_vault(password, salt, check, iv)
        except VaultPasswordError:
            out.error("Invalid vault password")
            ctx.exit(1)
            return

        # Resolve file identifier to hash
        file_hash: Optional[str] = None
        original_filename: Optional[str] = None
        file_iv: Optional[str] = None  # IV from file entry for decryption

        # Check if it's a numeric ID - convert to hash
        if file_identifier.isdigit():
            file_hash = calculate_drime_hash(int(file_identifier))
            # Need to fetch file entry to get IV
            search_result = client.get_vault_file_entries(per_page=1000)
            search_entries = []
            if isinstance(search_result, dict):
                if "data" in search_result:
                    search_entries = search_result["data"]
                elif "pagination" in search_result and isinstance(
                    search_result["pagination"], dict
                ):
                    search_entries = search_result["pagination"].get("data", [])
            for entry in search_entries:
                if str(entry.get("id")) == file_identifier:
                    file_iv = entry.get("iv")
                    original_filename = entry.get("name")
                    break
        elif "/" in file_identifier:
            # Path-based resolution: Test1/subfolder/file.txt
            path_parts = file_identifier.split("/")
            file_name = path_parts[-1]
            folder_parts = path_parts[:-1]

            # Navigate through folders to find the target folder
            current_folder_hash = ""
            for folder_name in folder_parts:
                # Get entries in current folder
                search_result = client.get_vault_file_entries(
                    folder_hash=current_folder_hash, per_page=1000
                )

                # Handle response format
                search_entries = []
                if isinstance(search_result, dict):
                    if "data" in search_result:
                        search_entries = search_result["data"]
                    elif "pagination" in search_result and isinstance(
                        search_result["pagination"], dict
                    ):
                        search_entries = search_result["pagination"].get("data", [])

                # Find the folder
                folder_found = False
                for entry in search_entries:
                    if (
                        entry.get("type") == "folder"
                        and entry.get("name") == folder_name
                    ):
                        current_folder_hash = entry.get("hash", "")
                        folder_found = True
                        break

                if not folder_found:
                    out.error(f"Folder '{folder_name}' not found in vault path")
                    ctx.exit(1)
                    return

            # Now find the file in the target folder
            search_result = client.get_vault_file_entries(
                folder_hash=current_folder_hash, per_page=1000
            )

            search_entries = []
            if isinstance(search_result, dict):
                if "data" in search_result:
                    search_entries = search_result["data"]
                elif "pagination" in search_result and isinstance(
                    search_result["pagination"], dict
                ):
                    search_entries = search_result["pagination"].get("data", [])

            for entry in search_entries:
                if entry.get("type") != "folder" and entry.get("name") == file_name:
                    file_hash = entry.get("hash")
                    original_filename = entry.get("name")
                    file_iv = entry.get("iv")
                    if not out.quiet:
                        out.info(f"Resolved '{file_identifier}' to hash: {file_hash}")
                    break

            if not file_hash:
                out.error(
                    f"File '{file_name}' not found in vault path '{file_identifier}'"
                )
                ctx.exit(1)
                return
        else:
            # Could be a name or already a hash
            # Search in vault root for the file by name
            search_result = client.get_vault_file_entries(per_page=1000)

            # Handle response format
            search_entries = []
            if isinstance(search_result, dict):
                if "data" in search_result:
                    search_entries = search_result["data"]
                elif "pagination" in search_result and isinstance(
                    search_result["pagination"], dict
                ):
                    search_entries = search_result["pagination"].get("data", [])

            found = False
            original_filename = None
            for entry in search_entries:
                entry_name = entry.get("name", "")
                entry_hash = entry.get("hash", "")
                entry_type = entry.get("type", "")

                # Match by name or hash, only files (not folders)
                if entry_type != "folder" and (
                    entry_name == file_identifier or entry_hash == file_identifier
                ):
                    file_hash = entry_hash
                    original_filename = entry_name
                    file_iv = entry.get("iv")
                    found = True
                    if not out.quiet:
                        out.info(f"Resolved '{file_identifier}' to hash: {file_hash}")
                    break

            if not found:
                # Check if this looks like a hash (base64-like, no file extension)
                # Hashes are typically alphanumeric with possible = padding
                looks_like_hash = (
                    "." not in file_identifier and len(file_identifier) >= 8
                )
                if looks_like_hash:
                    # Assume it's already a valid hash
                    file_hash = file_identifier
                else:
                    out.error(
                        f"File '{file_identifier}' not found in vault root. "
                        "Use a path like 'Folder/file.txt' for files in subfolders."
                    )
                    ctx.exit(1)
                    return  # Unreachable, but helps type checker

        if not file_hash:
            out.error(f"Could not resolve file identifier: {file_identifier}")
            ctx.exit(1)
            return  # Unreachable, but helps type checker

        # Download the encrypted file to a temp location
        if not out.quiet:
            out.info("Downloading encrypted vault file...")

        # Download to a temporary file first
        import tempfile

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            temp_path = Path(tmp_file.name)

        try:
            # Download encrypted content
            client.download_vault_file(
                hash_value=file_hash,
                output_path=temp_path,
            )

            # Read encrypted content
            encrypted_content = temp_path.read_bytes()

            # Decrypt the content
            if not out.quiet:
                out.info("Decrypting file...")

            decrypted_content = decrypt_file_content(
                vault_key, encrypted_content, iv_b64=file_iv
            )

            # Determine output path
            if output:
                save_path = Path(output)
            elif original_filename:
                save_path = Path(original_filename)
            else:
                # Try to extract filename from the path identifier
                if "/" in file_identifier:
                    save_path = Path(file_identifier.split("/")[-1])
                else:
                    save_path = Path(file_identifier)

            # Write decrypted content
            save_path.write_bytes(decrypted_content)

            out.success(f"Downloaded and decrypted: {save_path}")

        finally:
            # Clean up temp file
            if temp_path.exists():
                temp_path.unlink()

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("upload")
@click.argument("file_path", type=click.Path(exists=True))
@click.option(
    "--folder",
    "-f",
    type=str,
    default=None,
    help="Target folder name, ID, or hash in vault (default: root)",
)
@click.option(
    "--password",
    "-p",
    type=str,
    default=None,
    help="Vault password (will prompt if not provided)",
)
@click.pass_context
def vault_upload(
    ctx: Any,
    file_path: str,
    folder: Optional[str],
    password: Optional[str],
) -> None:
    """Upload a file to the vault with encryption.

    Encrypts a local file and uploads it to your encrypted vault.
    You will be prompted for your vault password.

    FILE_PATH: Path to the local file to upload

    Examples:
        pydrime vault upload secret.txt                    # Upload to vault root
        pydrime vault upload document.pdf -f MyFolder      # Upload to folder
        pydrime vault upload photo.jpg -p mypassword       # With password option
    """
    from .vault_crypto import VaultPasswordError, encrypt_filename, unlock_vault

    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info for password verification
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        vault_id = vault_info.get("id")
        if not vault_id:
            out.error("Could not get vault ID.")
            ctx.exit(1)
            return

        # Get encryption parameters from vault
        salt = vault_info.get("salt")
        check = vault_info.get("check")
        iv = vault_info.get("iv")

        if not all([salt, check, iv]):
            out.error("Vault encryption parameters not found.")
            ctx.exit(1)
            return

        # Get password from: CLI option > environment variable > prompt
        if not password:
            password = get_vault_password_from_env()
        if not password:
            password = click.prompt("Vault password", hide_input=True)

        # Unlock the vault (verify password)
        if not out.quiet:
            out.info("Verifying vault password...")

        try:
            vault_key = unlock_vault(password, salt, check, iv)
        except VaultPasswordError:
            out.error("Invalid vault password")
            ctx.exit(1)
            return

        # Resolve folder if specified
        parent_id: Optional[int] = None
        if folder:
            # Check if it's a numeric ID
            if folder.isdigit():
                parent_id = int(folder)
            else:
                # Search for folder by name or hash
                search_result = client.get_vault_file_entries(per_page=1000)

                search_entries = []
                if isinstance(search_result, dict):
                    if "data" in search_result:
                        search_entries = search_result["data"]
                    elif "pagination" in search_result and isinstance(
                        search_result["pagination"], dict
                    ):
                        search_entries = search_result["pagination"].get("data", [])

                found = False
                for entry in search_entries:
                    entry_name = entry.get("name", "")
                    entry_hash = entry.get("hash", "")
                    entry_type = entry.get("type", "")

                    if entry_type == "folder" and (
                        entry_name == folder or entry_hash == folder
                    ):
                        parent_id = entry.get("id")
                        found = True
                        if not out.quiet:
                            out.info(f"Resolved folder '{folder}' to ID: {parent_id}")
                        break

                if not found:
                    out.error(f"Folder '{folder}' not found in vault.")
                    ctx.exit(1)
                    return

        # Read and encrypt the file
        local_path = Path(file_path)
        if not out.quiet:
            out.info(f"Encrypting {local_path.name}...")

        # Read file content
        file_content = local_path.read_bytes()

        # Encrypt the filename
        encrypted_name, name_iv = encrypt_filename(vault_key, local_path.name)

        # Encrypt the file content
        ciphertext, content_iv_bytes = vault_key.encrypt(file_content)

        # Convert IV to base64
        import base64

        content_iv = base64.b64encode(content_iv_bytes).decode("ascii")

        # Upload the encrypted file
        if not out.quiet:
            out.info("Uploading encrypted file...")

        result = client.upload_vault_file(
            file_path=local_path,
            encrypted_content=ciphertext,
            encrypted_name=encrypted_name,
            name_iv=name_iv,
            content_iv=content_iv,
            vault_id=vault_id,
            parent_id=parent_id,
        )

        if out.json_output:
            out.output_json(result)
            return

        file_entry = result.get("fileEntry", {})
        out.success(
            f"Uploaded and encrypted: {local_path.name} "
            f"(ID: {file_entry.get('id', 'N/A')})"
        )

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("rm")
@click.argument("file_identifier", type=str)
@click.option(
    "--no-trash",
    is_flag=True,
    default=False,
    help="Delete permanently instead of moving to trash",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Skip confirmation prompt",
)
@click.pass_context
def vault_rm(
    ctx: Any,
    file_identifier: str,
    no_trash: bool,
    yes: bool,
) -> None:
    """Delete a file or folder from the vault.

    By default, files are moved to trash. Use --no-trash to delete permanently.

    FILE_IDENTIFIER: File or folder name, ID, or hash to delete

    Examples:
        pydrime vault rm secret.txt                # Move to trash
        pydrime vault rm secret.txt --no-trash     # Delete permanently
        pydrime vault rm 34431                     # Delete by ID
        pydrime vault rm MzQ0MzF8cGFkZA            # Delete by hash
        pydrime vault rm MyFolder -y               # Skip confirmation
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        vault_id = vault_info.get("id")
        if not vault_id:
            out.error("Could not get vault ID.")
            ctx.exit(1)
            return

        # Resolve file identifier to ID
        entry_id: Optional[int] = None
        entry_name: Optional[str] = None
        entry_type: Optional[str] = None

        # Check if it's a numeric ID
        if file_identifier.isdigit():
            entry_id = int(file_identifier)
            # Fetch entry info to get name
            search_result = client.get_vault_file_entries(per_page=1000)
            search_entries = []
            if isinstance(search_result, dict):
                if "data" in search_result:
                    search_entries = search_result["data"]
                elif "pagination" in search_result and isinstance(
                    search_result["pagination"], dict
                ):
                    search_entries = search_result["pagination"].get("data", [])
            for entry in search_entries:
                if entry.get("id") == entry_id:
                    entry_name = entry.get("name")
                    entry_type = entry.get("type")
                    break
        else:
            # Search for entry by name or hash
            search_result = client.get_vault_file_entries(per_page=1000)

            search_entries = []
            if isinstance(search_result, dict):
                if "data" in search_result:
                    search_entries = search_result["data"]
                elif "pagination" in search_result and isinstance(
                    search_result["pagination"], dict
                ):
                    search_entries = search_result["pagination"].get("data", [])

            for entry in search_entries:
                e_name = entry.get("name", "")
                e_hash = entry.get("hash", "")

                if e_name == file_identifier or e_hash == file_identifier:
                    entry_id = entry.get("id")
                    entry_name = e_name
                    entry_type = entry.get("type")
                    if not out.quiet:
                        out.info(f"Resolved '{file_identifier}' to ID: {entry_id}")
                    break

        if not entry_id:
            out.error(f"Entry '{file_identifier}' not found in vault.")
            ctx.exit(1)
            return

        # Confirmation prompt
        action = "permanently delete" if no_trash else "move to trash"
        display_name = entry_name or file_identifier
        type_str = f" ({entry_type})" if entry_type else ""

        if not yes:
            confirm = click.confirm(
                f"Are you sure you want to {action} '{display_name}'{type_str}?"
            )
            if not confirm:
                out.info("Cancelled.")
                return

        # Delete the entry
        if not out.quiet:
            out.info(
                f"{'Deleting' if no_trash else 'Moving to trash'}: {display_name}..."
            )

        result = client.delete_vault_file_entries(
            entry_ids=[entry_id],
            delete_forever=no_trash,
        )

        if out.json_output:
            out.output_json(result)
            return

        if no_trash:
            out.success(f"Permanently deleted: {display_name}")
        else:
            out.success(f"Moved to trash: {display_name}")

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


@main.command()
@click.option(
    "--workspace",
    "-w",
    type=int,
    help="Workspace ID (0 for personal workspace)",
)
@click.option(
    "--folder",
    "-f",
    type=str,
    help="Folder ID or name to scan (omit for root folder)",
)
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    help="Scan recursively into subfolders",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show duplicates without deleting (default)",
)
@click.option(
    "--delete",
    is_flag=True,
    help="Actually delete duplicate files (moves to trash)",
)
@click.option(
    "--keep-newest",
    is_flag=True,
    help="Keep newest file instead of oldest (default: keep oldest)",
)
@click.pass_context
def find_duplicates(
    ctx: Any,
    workspace: Optional[int],
    folder: Optional[str],
    recursive: bool,
    dry_run: bool,
    delete: bool,
    keep_newest: bool,
) -> None:
    """Find and optionally delete duplicate files.

    Duplicates are identified by having identical filename, size, and parent folder.
    By default, the oldest file (lowest ID) is kept and newer duplicates are deleted.

    Examples:

        # Dry run (show duplicates without deleting)
        pydrime find-duplicates

        # Find duplicates in a specific folder by ID
        pydrime find-duplicates --folder 12345

        # Find duplicates in a specific folder by name
        pydrime find-duplicates --folder "My Documents"

        # Find duplicates recursively
        pydrime find-duplicates --recursive

        # Actually delete duplicates (moves to trash)
        pydrime find-duplicates --delete

        # Keep newest file instead of oldest
        pydrime find-duplicates --delete --keep-newest
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get workspace ID
        workspace_id = (
            workspace
            if workspace is not None
            else (config.get_default_workspace() or 0)
        )

        # Get current folder context
        current_folder = config.get_current_folder()

        # Parse folder ID or name
        folder_id: Optional[int] = None
        if folder:
            # Handle special cases for root
            if folder in ("0", "/"):
                folder_id = None
            else:
                # Resolve folder identifier (ID or name) to folder ID
                try:
                    folder_id = client.resolve_folder_identifier(
                        identifier=folder,
                        parent_id=current_folder,
                        workspace_id=workspace_id,
                    )
                    if not out.quiet and not folder.isdigit():
                        out.info(f"Resolved '{folder}' to folder ID: {folder_id}")
                except DrimeNotFoundError:
                    out.error(f"Folder not found: {folder}")
                    ctx.exit(1)

        # Show configuration
        if not out.quiet:
            out.info("=" * 60)
            out.info("Duplicate File Finder")
            out.info("=" * 60)
            workspace_display, _ = format_workspace_display(client, workspace_id)
            out.info(f"Workspace: {workspace_display}")

            if folder_id is not None:
                folder_display, _ = get_folder_display_name(client, folder_id)
                out.info(f"Folder: {folder_display}")
            else:
                out.info("Folder: Root")

            out.info(f"Recursive: {'Yes' if recursive else 'No'}")
            out.info(f"Mode: {'DELETE' if delete else 'DRY RUN'}")
            out.info(f"Keep: {'Newest' if keep_newest else 'Oldest'}")
            out.info("=" * 60)
            out.info("")

        # Create entries manager and duplicate finder
        entries_manager = FileEntriesManager(client, workspace_id)
        finder = DuplicateFileFinder(entries_manager, out)

        # Find duplicates
        duplicates = finder.find_duplicates(folder_id=folder_id, recursive=recursive)

        # Display duplicates
        finder.display_duplicates(duplicates)

        # Get entries to delete
        entries_to_delete = finder.get_entries_to_delete(
            duplicates, keep_oldest=not keep_newest
        )

        if not entries_to_delete:
            out.info("No duplicate files to delete.")
            return

        # Show summary
        out.info("=" * 60)
        out.info(f"Total duplicate files to delete: {len(entries_to_delete)}")
        out.info("=" * 60)

        # Delete or dry run
        if delete:
            # Confirm deletion
            if not out.quiet:
                out.warning(
                    f"\nAbout to delete {len(entries_to_delete)} duplicate file(s)."
                )
                out.warning("Files will be moved to trash (can be restored).")
                if not click.confirm("Continue?", default=False):
                    out.info("Cancelled.")
                    return

            # Delete files
            out.info("Deleting duplicate files...")
            entry_ids = [entry.id for entry in entries_to_delete]

            # Delete in batches of 100 (API limit)
            batch_size = 100
            for i in range(0, len(entry_ids), batch_size):
                batch = entry_ids[i : i + batch_size]
                client.delete_file_entries(batch, delete_forever=False)

                if not out.quiet:
                    out.info(f"Deleted {i + len(batch)}/{len(entry_ids)} files...")

            out.success(
                f"Successfully deleted {len(entries_to_delete)} duplicate files."
            )
            out.info("Files have been moved to trash and can be restored if needed.")
        else:
            # Dry run
            out.info("\nDRY RUN MODE - No files were deleted.")
            out.info("To actually delete duplicates, use: --delete")

    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)


if __name__ == "__main__":
    main()
