"""Sync command for PyDrime CLI."""

import logging
import re
from pathlib import Path
from typing import Any, Optional

import click
from syncengine import (  # type: ignore[import-not-found]
    ComparisonMode,
    SyncConfig,
    SyncConfigError,
    SyncEngine,
    SyncMode,
    SyncPair,
    load_sync_pairs_from_json,
)

from ..api import DrimeClient
from ..auth import require_api_key
from ..config import config
from ..exceptions import DrimeAPIError
from ..output import OutputFormatter
from ..validation import validate_cloud_files
from ..workspace_utils import (
    format_workspace_display,
    resolve_workspace_identifier,
)
from .adapters import _DrimeClientAdapter, create_entries_manager_factory

logger = logging.getLogger(__name__)


def _display_sync_summary(out: OutputFormatter, stats: dict, dry_run: bool) -> None:
    """Display sync summary stats.

    This function mirrors SyncEngine._display_summary but uses the CLI's
    OutputFormatter. Used when progress display is enabled and engine
    output is suppressed.

    Args:
        out: OutputFormatter instance
        stats: Statistics dictionary from sync operation
        dry_run: Whether this was a dry run
    """
    out.print("")
    if dry_run:
        out.success("Dry run complete!")
    else:
        out.success("Sync complete!")

    # Show statistics
    total_actions = (
        stats.get("uploads", 0)
        + stats.get("downloads", 0)
        + stats.get("deletes_local", 0)
        + stats.get("deletes_remote", 0)
        + stats.get("renames_local", 0)
        + stats.get("renames_remote", 0)
    )
    errors = stats.get("errors", 0)

    if total_actions > 0 or errors > 0:
        out.info(f"Total actions: {total_actions}")
        if stats.get("uploads", 0) > 0:
            out.info(f"  Uploaded: {stats['uploads']}")
        if stats.get("downloads", 0) > 0:
            out.info(f"  Downloaded: {stats['downloads']}")
        if stats.get("renames_local", 0) > 0:
            out.info(f"  Renamed locally: {stats['renames_local']}")
        if stats.get("renames_remote", 0) > 0:
            out.info(f"  Renamed remotely: {stats['renames_remote']}")
        if stats.get("deletes_local", 0) > 0:
            out.info(f"  Deleted locally: {stats['deletes_local']}")
        if stats.get("deletes_remote", 0) > 0:
            out.info(f"  Deleted remotely: {stats['deletes_remote']}")
        if stats.get("skips", 0) > 0:
            out.info(f"  Already synced: {stats['skips']}")
        if errors > 0:
            out.error(f"  Failed: {errors}")
    else:
        out.info("No changes needed - everything is in sync!")


@click.command()
@click.argument("path", type=str, required=False, default=None)
@click.option("--remote-path", "-r", help="Remote destination path")
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses default workspace if not specified)",
)
@click.option(
    "--config",
    "-C",
    "config_file",
    type=click.Path(exists=True),
    help="JSON config file with list of sync pairs",
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
@click.option(
    "--validate",
    is_flag=True,
    help="Validate cloud files after sync (check file size and users field)",
)
@click.pass_context
def sync(
    ctx: Any,
    path: Optional[str],
    remote_path: Optional[str],
    workspace: Optional[int],
    config_file: Optional[str],
    dry_run: bool,
    no_progress: bool,
    chunk_size: int,
    multipart_threshold: int,
    batch_size: int,
    no_streaming: bool,
    workers: int,
    start_delay: float,
    validate: bool,
) -> None:
    """Sync files between local directory and Drime Cloud.

    PATH: Local directory to sync OR literal sync pair in format:
          /local/path:syncMode:/remote/path

    Sync Modes:
      - twoWay (tw): Mirror every action in both directions
      - localToCloud (std): Mirror local actions to cloud only
      - localBackup (sb): Upload to cloud, never delete
      - cloudToLocal (dts): Mirror cloud actions to local only
      - cloudBackup (db): Download from cloud, never delete

    Ignore Files (.pydrignore):
      Place a .pydrignore file in any directory to exclude files from sync.
      Uses gitignore-style patterns (similar to Kopia's .kopiaignore).

      Supported patterns:
        # Comment lines start with #
        *.log           - Ignore all .log files anywhere
        /logs           - Ignore 'logs' only at root directory
        temp/           - Ignore directories named 'temp'
        !important.log  - Un-ignore important.log (negation)
        *.db*           - Ignore files with .db in extension
        **/cache/**     - Ignore any 'cache' directory and contents
        [a-z]*.tmp      - Character ranges and wildcards
        ?tmp.db         - ? matches exactly one character

      Hierarchical: .pydrignore files in subdirectories only apply to that
      subtree and can override parent rules using negation (!).

    JSON Config Format (--config):
      A JSON file containing a list of sync pair objects:
      [
        {
          "storage": "My Team",              // optional, workspace ID
                                             // or name
                                             // (default: uses configured
                                             // default workspace)
                                             // Note: "workspace" is also
                                             // accepted as an alias
          "local": "/path/to/local",         // required
          "remote": "remote/path",           // required
          "syncMode": "twoWay",              // required
          "disableLocalTrash": false,        // optional, default: false
          "ignore": ["*.tmp"],               // optional, CLI patterns
                                             //(in addition to .pydrignore)
          "excludeDotFiles": false           // optional, default: false
        }
      ]

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
        pydrime sync ./backup:std:/CloudBackup
        pydrime sync ./local:sb:/Backup

        # JSON config file with multiple sync pairs
        pydrime sync --config sync_pairs.json
        pydrime sync -C sync_pairs.json --dry-run

        # Other options
        pydrime sync . -w 5                          # Sync in workspace 5
        pydrime sync ./data --dry-run                # Preview sync changes
        pydrime sync ./data -b 100                   # Process 100 files per batch
        pydrime sync ./data --no-streaming           # Scan all files upfront
        pydrime sync ./data --validate               # Validate files after sync
    """
    from ..cli_progress import run_sync_with_progress

    out: OutputFormatter = ctx.obj["out"]

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

    # Initialize client
    client = DrimeClient(api_key=api_key)

    # Check if using JSON config file
    if config_file is not None:
        # Load sync pairs from JSON config file
        if path is not None or remote_path is not None:
            out.error(
                "Cannot use --config with PATH or --remote-path arguments. "
                "All sync pairs must be defined in the config file."
            )
            ctx.exit(1)

        config_path = Path(config_file)
        try:
            sync_pairs_data = load_sync_pairs_from_json(config_path)
        except SyncConfigError as e:
            out.error(str(e))
            ctx.exit(1)
            return  # For type checker

        if not out.quiet:
            out.info(f"Loaded {len(sync_pairs_data)} sync pair(s) from {config_path}")
            out.info("")

        # Process each sync pair from JSON
        all_stats: list[dict[str, Any]] = []
        total_conflicts = 0

        for i, pair_data in enumerate(sync_pairs_data):
            # Support both "workspace" (pydrime terminology) and
            # "storage" (syncengine terminology)
            # If user provides "workspace", use it; otherwise fall back to "storage"
            if "workspace" in pair_data:
                pair_data["storage"] = pair_data["workspace"]

            # Resolve workspace identifier (int, str name, or None) to workspace ID
            # Use the default workspace if not specified in config
            default_ws = config.get_default_workspace() or 0
            workspace_from_json = pair_data.get("storage")
            try:
                resolved_workspace_id = resolve_workspace_identifier(
                    client, workspace_from_json, default_ws
                )
            except ValueError as e:
                out.error(f"Sync pair at index {i}: {e}")
                ctx.exit(1)
                return  # For type checker

            # Create SyncPair from dict data
            config_pair = SyncPair(
                source=Path(pair_data["local"]),
                destination=pair_data["remote"],
                sync_mode=SyncMode.from_string(pair_data["syncMode"]),
                storage_id=resolved_workspace_id,
                disable_source_trash=pair_data.get("disableLocalTrash", False),
                ignore=pair_data.get("ignore", []),
                exclude_dot_files=pair_data.get("excludeDotFiles", False),
            )

            if not out.quiet:
                out.info("=" * 60)
                out.info(f"Sync Pair {i + 1}/{len(sync_pairs_data)}")
                out.info("=" * 60)
                workspace_display, _ = format_workspace_display(
                    client, config_pair.storage_id
                )
                out.info(f"Workspace: {workspace_display}")
                out.info(f"Sync mode: {config_pair.sync_mode.value}")
                out.info(f"Source path: {config_pair.source}")
                # Display "/" for root when remote is empty (normalized from "/")
                remote_display = (
                    config_pair.destination if config_pair.destination else "/ (root)"
                )
                out.info(f"Destination path: {remote_display}")
                if config_pair.ignore:
                    out.info(f"Ignore patterns: {config_pair.ignore}")
                if config_pair.exclude_dot_files:
                    out.info("Excluding dot files")
                if config_pair.disable_source_trash:
                    out.info("Local trash disabled")
                out.info("")

            try:
                # Create output formatter for engine (respect no_progress)
                # When using progress display, set engine to quiet mode
                use_progress_display = not no_progress and not out.quiet and not dry_run
                engine_out = OutputFormatter(
                    json_output=out.json_output,
                    quiet=use_progress_display or no_progress or out.quiet,
                )

                # Wrap client in adapter for syncengine compatibility
                adapted_client = _DrimeClientAdapter(client)

                # Use SIZE_ONLY comparison mode to avoid expensive MD5 computation
                # and unreliable mtime comparison
                # (Drime doesn't preserve original mtimes)
                sync_config = SyncConfig(
                    ignore_file_name=".pydrignore",
                    comparison_mode=ComparisonMode.SIZE_ONLY,
                )

                # Create sync engine
                engine = SyncEngine(
                    adapted_client,
                    create_entries_manager_factory(),
                    output=engine_out,
                    config=sync_config,
                )

                # Execute sync - use progress display for non-dry-run
                if use_progress_display:
                    stats = run_sync_with_progress(
                        engine=engine,
                        pair=config_pair,
                        dry_run=dry_run,
                        chunk_size=chunk_size_bytes,
                        multipart_threshold=multipart_threshold_bytes,
                        batch_size=batch_size,
                        use_streaming=not no_streaming,
                        max_workers=workers,
                        start_delay=start_delay,
                        out=out,  # Pass CLI output for pre-sync status
                    )
                else:
                    stats = engine.sync_pair(
                        config_pair,
                        dry_run=dry_run,
                        chunk_size=chunk_size_bytes,
                        multipart_threshold=multipart_threshold_bytes,
                        batch_size=batch_size,
                        use_streaming=not no_streaming,
                        max_workers=workers,
                        start_delay=start_delay,
                    )

                # Display summary stats when using progress display
                # (engine output is suppressed during progress display)
                if use_progress_display and not out.quiet:
                    _display_sync_summary(out, stats, dry_run=False)

                # Add pair info to stats
                stats["pair_index"] = i
                stats["local"] = str(config_pair.source)
                stats["remote"] = config_pair.destination
                all_stats.append(stats)
                total_conflicts += stats.get("conflicts", 0)

                # Run validation if requested and not dry-run
                if validate and not dry_run:
                    validation_result = validate_cloud_files(
                        client=client,
                        out=out,
                        local_path=config_pair.source,
                        remote_path=config_pair.destination,
                        workspace_id=resolved_workspace_id,
                    )
                    stats["validation"] = validation_result
                    if validation_result.get("has_issues", False):
                        out.error(
                            f"Validation failed for pair {i + 1}: "
                            f"{validation_result.get('issues_count', 0)} issue(s) found"
                        )

            except DrimeAPIError as e:
                out.error(f"API error for pair {i + 1}: {e}")
                all_stats.append(
                    {
                        "pair_index": i,
                        "local": str(config_pair.source),
                        "remote": config_pair.destination,
                        "error": str(e),
                    }
                )
            except Exception as e:
                out.error(f"Error for pair {i + 1}: {e}")
                all_stats.append(
                    {
                        "pair_index": i,
                        "local": str(config_pair.source),
                        "remote": config_pair.destination,
                        "error": str(e),
                    }
                )

        # Output combined results
        if out.json_output:
            out.output_json({"pairs": all_stats, "total_pairs": len(sync_pairs_data)})

        # Exit with warning if there were conflicts
        if total_conflicts > 0 and not out.quiet:
            out.warning(
                f"\n{total_conflicts} conflict(s) were skipped across all pairs. "
                "Please resolve conflicts manually."
            )

        return

    # Single path mode (original behavior)
    if path is None:
        out.error("Either PATH argument or --config option is required")
        ctx.exit(1)
        return  # For type checker

    # Detect if path is a literal sync pair format
    # Format: /local:mode:/remote (3 parts) or /local:/remote (2 parts)
    # On Windows, paths like C:\Users\... have a colon after drive letter,
    # so we need to handle this case specially.
    # A literal pair must have colons that are NOT drive letter colons.
    # We detect this by checking if the path looks like a Windows drive path.

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

    # Use default workspace if none specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

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
            pair.storage_id = workspace
            source_path = pair.source

            if not out.quiet:
                out.info(f"Parsed sync pair: {pair.sync_mode.value}")
                out.info(f"  Local:  {pair.source}")
                out.info(f"  Remote: {pair.destination}")
        except ValueError as e:
            out.error(f"Invalid sync pair format: {e}")
            ctx.exit(1)
            return  # Unreachable, but helps type checker
    else:
        # Path is a simple directory path
        source_path = Path(path)

        # Validate path exists and is a directory
        if not source_path.exists():
            out.error(f"Path does not exist: {path}")
            ctx.exit(1)

        if not source_path.is_dir():
            out.error(f"Path is not a directory: {path}")
            ctx.exit(1)

        # Determine remote path
        if remote_path is None:
            # Use folder name as remote path
            remote_path = source_path.name

        # Create sync pair with TWO_WAY as default
        pair = SyncPair(
            source=source_path,
            destination=remote_path,
            sync_mode=SyncMode.TWO_WAY,
            storage_id=workspace,
        )

    if not out.quiet:
        # Show workspace information
        workspace_display, _ = format_workspace_display(client, workspace)
        out.info(f"Workspace: {workspace_display}")
        out.info(f"Sync mode: {pair.sync_mode.value}")
        out.info(f"Local path: {pair.source}")
        # Display "/" for root when remote is empty (normalized from "/")
        remote_display = pair.destination if pair.destination else "/ (root)"
        out.info(f"Remote path: {remote_display}")
        logger.debug(f"SyncPair storage_id: {pair.storage_id}")
        out.info("")  # Empty line for readability

    try:
        # Create output formatter for engine (respect no_progress)
        # When using progress display, set engine to quiet mode
        # to avoid duplicate output
        use_progress_display = not no_progress and not out.quiet and not dry_run
        engine_out = OutputFormatter(
            json_output=out.json_output,
            quiet=use_progress_display or no_progress or out.quiet,
        )

        # Create sync engine with client adapter
        client_adapter = _DrimeClientAdapter(client)

        # Use SIZE_ONLY comparison mode to avoid expensive MD5 computation
        # and unreliable mtime comparison (Drime doesn't preserve original mtimes)
        sync_config = SyncConfig(
            ignore_file_name=".pydrignore",
            comparison_mode=ComparisonMode.SIZE_ONLY,
        )

        engine = SyncEngine(
            client_adapter,
            create_entries_manager_factory(),
            output=engine_out,
            config=sync_config,
        )

        # Execute sync - use progress display for non-dry-run
        if use_progress_display:
            stats = run_sync_with_progress(
                engine=engine,
                pair=pair,
                dry_run=dry_run,
                chunk_size=chunk_size_bytes,
                multipart_threshold=multipart_threshold_bytes,
                batch_size=batch_size,
                use_streaming=not no_streaming,
                max_workers=workers,
                start_delay=start_delay,
                out=out,  # Pass CLI output for pre-sync status
            )
        else:
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

        # Display summary stats when using progress display
        # (engine output is suppressed during progress display)
        if use_progress_display and not out.quiet:
            _display_sync_summary(out, stats, dry_run=False)

        # Output results
        if out.json_output:
            out.output_json(stats)

        # Exit with warning if there were conflicts
        if stats.get("conflicts", 0) > 0 and not out.quiet:
            out.warning(
                f"\n⚠  {stats['conflicts']} conflict(s) were skipped. "
                "Please resolve conflicts manually."
            )

        # Run validation if requested and not dry-run
        if validate and not dry_run:
            validation_result = validate_cloud_files(
                client=client,
                out=out,
                local_path=pair.source,
                remote_path=pair.destination,
                workspace_id=workspace,
            )
            if out.json_output:
                stats["validation"] = validation_result
                out.output_json(stats)
            if validation_result.get("has_issues", False):
                ctx.exit(1)

    except KeyboardInterrupt:
        out.warning("\nSync cancelled by user")
        ctx.exit(130)
    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)
    except Exception as e:
        out.error(f"Error: {e}")
        ctx.exit(1)
