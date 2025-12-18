"""Upload command for pydrime CLI."""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

import click

if TYPE_CHECKING:
    pass
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from ..api import DrimeClient
from ..auth import require_api_key
from ..config import config
from ..duplicate_handler import DuplicateHandler
from ..exceptions import DrimeAPIError
from ..output import OutputFormatter
from ..upload_preview import display_upload_preview
from ..validation import validate_cloud_files, validate_single_file
from ..workspace_utils import format_workspace_display, get_folder_display_name
from .adapters import _DrimeClientAdapter, create_entries_manager_factory
from .helpers import scan_directory


@click.command()
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
    "--simple-progress",
    is_flag=True,
    help=(
        "Use simple text progress "
        "(no spinners/animations, suitable for CI/CD or piped output)"
    ),
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
@click.option(
    "--validate",
    is_flag=True,
    help="Validate cloud files after upload (check file size and users field)",
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
    simple_progress: bool,
    chunk_size: int,
    multipart_threshold: int,
    start_delay: float,
    validate: bool,
) -> None:
    """Upload a file or directory to Drime Cloud.

    PATH: Local file or directory to upload

    Ignore Files (.pydrignore):
      When uploading a directory, you can place a .pydrignore file in any
      directory to exclude files from upload. Uses gitignore-style patterns.

      Example .pydrignore:
        # Ignore all log files
        *.log
        # Ignore temp directories
        temp/
        # But include important.log
        !important.log

    Examples:
        pydrime upload ./data                  # Upload directory
        pydrime upload ./data --validate       # Upload and validate
    """
    from syncengine import SyncEngine  # type: ignore[import-not-found]
    from syncengine.modes import SyncMode  # type: ignore[import-not-found]
    from syncengine.pair import SyncPair  # type: ignore[import-not-found]

    out: OutputFormatter = ctx.obj["out"]
    source_path = Path(path)

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
    if source_path.is_file():
        _upload_single_file(
            ctx=ctx,
            client=client,
            source_path=source_path,
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
            validate=validate,
        )
        return

    # Directory upload - collect files for preview and duplicate handling
    out.info(f"Scanning directory: {source_path}")
    # Always use parent as base_path so the folder name is included in
    # relative paths
    base_path = source_path.parent
    files_to_upload = scan_directory(source_path, base_path, out)

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

        # Build skip set and rename map for sync engine
        # The DuplicateHandler uses paths like "sync/test_folder/file.txt"
        # (with folder name prefix from scan_directory using base_path=parent)
        # but SyncEngine.upload_folder scans relative to source_path, so paths
        # are like "test_folder/file.txt". We need to strip the folder prefix.
        folder_prefix = f"{source_path.name}/"
        files_to_skip = {
            p[len(folder_prefix) :] if p.startswith(folder_prefix) else p
            for p in dup_handler.files_to_skip
        }
        file_renames = {
            (k[len(folder_prefix) :] if k.startswith(folder_prefix) else k): v
            for k, v in dup_handler.rename_map.items()
        }

        # Determine if force upload is needed (for replace action)
        force_upload = (
            dup_handler.chosen_action == "replace"
            and len(dup_handler.entries_to_delete) > 0
        )

        # Determine the remote path for the sync engine
        # When remote_path is specified, include the local folder name
        # e.g., uploading "test/" with remote_path="dest" -> "dest/test/..."
        if remote_path:
            effective_remote_path = f"{remote_path}/{source_path.name}"
        else:
            effective_remote_path = source_path.name

        # Wrap client in adapter for syncengine compatibility
        adapted_client = _DrimeClientAdapter(client)

        # Create sync pair for upload
        # Try with parent_id first (syncengine >= 0.2.2)
        try:
            pair = SyncPair(
                source=source_path,
                destination=effective_remote_path,
                sync_mode=SyncMode.SOURCE_TO_DESTINATION,
                storage_id=workspace,
                parent_id=current_folder_id,
            )
        except TypeError:
            # Fallback for older syncengine versions without parent_id support
            pair = SyncPair(
                source=source_path,
                destination=effective_remote_path,
                sync_mode=SyncMode.SOURCE_TO_DESTINATION,
                storage_id=workspace,
            )
            if current_folder_id:
                out.warning(
                    "Your syncengine version doesn't support parent_id. "
                    "Please upgrade: pip install -U syncengine>=0.2.2"
                )

        # Create progress tracker if progress display is enabled
        progress_display: Any = None
        if no_progress or out.quiet:
            tracker = None
            engine_out = OutputFormatter(json_output=out.json_output, quiet=True)
        elif simple_progress:
            from ..cli_progress import SimpleTextProgressDisplay

            progress_display = SimpleTextProgressDisplay()
            tracker = progress_display.create_tracker()
            # Silence engine output when progress display is active
            engine_out = OutputFormatter(json_output=out.json_output, quiet=True)
        else:
            from ..cli_progress import SyncProgressDisplay

            progress_display = SyncProgressDisplay()
            tracker = progress_display.create_tracker()
            # Silence engine output when progress display is active
            engine_out = OutputFormatter(json_output=out.json_output, quiet=True)

        # Create sync engine
        engine = SyncEngine(
            adapted_client, create_entries_manager_factory(), output=engine_out
        )

        # Perform upload using sync_pair with progress tracking
        try:
            if tracker and not (no_progress or out.quiet):
                # Use progress display for interactive uploads
                with progress_display:
                    stats = engine.sync_pair(
                        pair,
                        dry_run=False,
                        chunk_size=chunk_size_bytes,
                        multipart_threshold=multipart_threshold_bytes,
                        batch_size=50,  # Process 50 files per batch
                        use_streaming=True,  # Enable streaming mode
                        max_workers=workers,
                        start_delay=start_delay,
                        sync_progress_tracker=tracker,
                        files_to_skip=files_to_skip,
                        file_renames=file_renames,
                        force_upload=force_upload,  # Force upload when replacing
                    )
            else:
                # No progress display
                stats = engine.sync_pair(
                    pair,
                    dry_run=False,
                    chunk_size=chunk_size_bytes,
                    multipart_threshold=multipart_threshold_bytes,
                    batch_size=50,  # Process 50 files per batch
                    use_streaming=True,  # Enable streaming mode
                    max_workers=workers,
                    start_delay=start_delay,
                    files_to_skip=files_to_skip,
                    file_renames=file_renames,
                    force_upload=force_upload,  # Force upload when replacing
                )
        except KeyboardInterrupt:
            out.warning("\nUpload cancelled by user")
            raise

        # Note: When using replace mode, files are automatically overwritten
        # by the cloud service during upload. No need to delete old entries.

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

        # Run validation if requested
        if validate:
            validation_result = validate_cloud_files(
                client=client,
                out=out,
                local_path=source_path,
                remote_path=effective_remote_path,
                workspace_id=workspace,
            )
            if out.json_output:
                out.output_json(
                    {
                        "success": stats["uploads"],
                        "failed": stats["errors"],
                        "skipped": stats["skips"],
                        "validation": validation_result,
                    }
                )
            if validation_result.get("has_issues", False):
                ctx.exit(1)

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
    source_path: Path,
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
    validate: bool = False,
) -> None:
    """Handle single file upload (not using sync engine).

    This function handles the upload of a single file, including
    dry-run preview, duplicate handling, and progress display.
    """

    files_to_upload = [(source_path, remote_path or source_path.name)]

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
            progress_callback_fn: Optional[Callable[[int, int], None]] = None
            if progress_display:
                progress_display.start()
                file_size = file_path.stat().st_size
                task_id = progress_display.add_task(
                    f"[cyan]{file_path.name}",
                    total=file_size,
                )

                def _progress_callback(bytes_uploaded: int, total_bytes: int) -> None:
                    if progress_display:
                        progress_display.update(
                            task_id, completed=bytes_uploaded, total=total_bytes
                        )

                progress_callback_fn = _progress_callback
            elif not out.quiet:
                # Show a simple message when progress bar is disabled
                file_size = file_path.stat().st_size
                size_str = out.format_size(file_size)
                out.progress_message(f"Uploading {upload_path} ({size_str})...")
            else:
                task_id = None
                file_size = file_path.stat().st_size
                size_str = out.format_size(file_size)
                out.progress_message(f"Uploading {upload_path} ({size_str})")

            result = client.upload_file(
                file_path,
                parent_id=current_folder_id,
                relative_path=upload_path,  # Use full path including filename
                workspace_id=workspace,
                progress_callback=progress_callback_fn,
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
                json_result: dict[str, Any] = {
                    "success": 1,
                    "failed": 0,
                    "skipped": 0,
                    "files": uploaded_files,
                }
                # Add validation if requested
                if validate:
                    validation_result = validate_single_file(
                        client=client,
                        out=out,
                        local_path=file_path,
                        remote_path=upload_path,
                        workspace_id=workspace,
                    )
                    json_result["validation"] = validation_result
                    if validation_result.get("has_issues", False):
                        ctx.exit(1)
                out.output_json(json_result)
            else:
                out.print_summary(
                    "Upload Complete", [("Successfully uploaded", "1 file")]
                )
                # Run validation if requested
                if validate:
                    validation_result = validate_single_file(
                        client=client,
                        out=out,
                        local_path=file_path,
                        remote_path=upload_path,
                        workspace_id=workspace,
                    )
                    if validation_result.get("has_issues", False):
                        ctx.exit(1)

        finally:
            if progress_display:
                progress_display.stop()

    except KeyboardInterrupt:
        out.warning("\nUpload cancelled by user")
        ctx.exit(130)
    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)
