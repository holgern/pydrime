"""Download command for PyDrime CLI."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

import click
from syncengine import SyncEngine  # type: ignore[import-not-found]

from ..api import DrimeClient
from ..config import config
from ..download_helpers import (
    download_single_file,
    get_entry_from_hash,
    resolve_identifier_to_hash,
)
from ..exceptions import DrimeAPIError
from ..models import FileEntry
from ..output import OutputFormatter
from ..utils import is_glob_pattern
from .adapters import _DrimeClientAdapter, create_entries_manager_factory


@click.command()
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
@click.option(
    "--simple-progress",
    is_flag=True,
    help=(
        "Use simple text progress "
        "(no spinners/animations, suitable for CI/CD or piped output)"
    ),
)
@click.pass_context
def download(
    ctx: Any,
    entry_identifiers: tuple[str, ...],
    output: Optional[str],
    on_duplicate: str,
    workers: int,
    no_progress: bool,
    simple_progress: bool,
) -> None:
    """Download file(s) or folder(s) from Drime Cloud.

    ENTRY_IDENTIFIERS: One or more file/folder paths, names, glob patterns,
                       hashes, or numeric IDs

    Supports file/folder paths (e.g., folder/file.txt), names (resolved in current
    directory), glob patterns (*, ?, []), numeric IDs, and hashes. Folders are
    automatically downloaded recursively with all their contents using the sync
    engine for reliable downloads.

    Glob patterns:
        * matches any sequence of characters
        ? matches any single character
        [abc] matches any character in the set

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
        pydrime download "*.txt"                      # Download all .txt files
        pydrime download "bench*"                     # Entries starting with "bench"
        pydrime download "file?.txt"                  # Match file1.txt, file2.txt
    """
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

        # Expand glob patterns to entry names
        expanded_identifiers: list[str] = []
        for identifier in entry_identifiers:
            if is_glob_pattern(identifier):
                # Resolve glob pattern to matching entries
                matching_entries = client.resolve_entries_by_pattern(
                    pattern=identifier,
                    parent_id=current_folder,
                    workspace_id=workspace,
                )
                if matching_entries:
                    if not out.quiet:
                        out.info(
                            f"Matched {len(matching_entries)} entries "
                            f"with pattern '{identifier}'"
                        )
                    # Use entry names as identifiers
                    for entry in matching_entries:
                        expanded_identifiers.append(entry.name)
                else:
                    out.warning(f"No entries match pattern '{identifier}'")
            else:
                expanded_identifiers.append(identifier)

        if not expanded_identifiers:
            out.warning("No entries to download.")
            return

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

                # Create progress tracker if progress display is enabled
                progress_display: Any = None
                if no_progress or out.quiet:
                    tracker = None
                    engine_out = OutputFormatter(
                        json_output=out.json_output, quiet=out.quiet
                    )
                elif simple_progress:
                    from ..cli_progress import SimpleTextProgressDisplay

                    progress_display = SimpleTextProgressDisplay()
                    tracker = progress_display.create_tracker()
                    # Silence engine output when progress display is active
                    engine_out = OutputFormatter(
                        json_output=out.json_output, quiet=True
                    )
                else:
                    from ..cli_progress import SyncProgressDisplay

                    progress_display = SyncProgressDisplay()
                    tracker = progress_display.create_tracker()
                    # Silence engine output when progress display is active
                    engine_out = OutputFormatter(
                        json_output=out.json_output, quiet=True
                    )

                # Wrap client in adapter for syncengine compatibility
                adapted_client = _DrimeClientAdapter(client)
                engine = SyncEngine(
                    adapted_client, create_entries_manager_factory(), output=engine_out
                )

                # Use sync engine's download_folder method with progress tracking
                # overwrite=True uses CLOUD_BACKUP mode (download only, no delete)
                try:
                    if tracker and not (no_progress or out.quiet):
                        # Use progress display for interactive downloads
                        with progress_display:
                            stats = engine.download_folder(
                                remote_entry=entry,
                                local_path=folder_path,
                                storage_id=workspace,
                                overwrite=(on_duplicate == "overwrite"),
                                max_workers=workers,
                                sync_progress_tracker=tracker,
                            )
                    else:
                        # No progress display
                        stats = engine.download_folder(
                            remote_entry=entry,
                            local_path=folder_path,
                            storage_id=workspace,
                            overwrite=(on_duplicate == "overwrite"),
                            max_workers=workers,
                        )
                except KeyboardInterrupt:
                    out.warning("\nDownload cancelled by user")
                    raise

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
                    single_file=(len(expanded_identifiers) == 1),
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

        if workers > 1 and len(expanded_identifiers) > 1:
            # Parallel download for multiple entries
            # Note: For folders, each folder download already uses parallel workers
            # internally via SyncEngine, so we use sequential here to avoid
            # over-parallelization
            has_folders = False
            for identifier in expanded_identifiers:
                folder_check_entry = resolve_and_get_entry(identifier)
                if folder_check_entry and folder_check_entry.is_folder:
                    has_folders = True
                    break

            if has_folders:
                # Sequential for folder downloads (they parallelize internally)
                for identifier in expanded_identifiers:
                    process_identifier(identifier, output_dir if output else None)
            else:
                # Parallel for multiple file downloads
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {}
                    for identifier in expanded_identifiers:
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
            for identifier in expanded_identifiers:
                process_identifier(identifier, output_dir if output else None)

        if out.json_output:
            out.output_json({"files": downloaded_files})

        # Exit with error if all downloads failed
        if nonlocal_error_count[0] == len(expanded_identifiers):
            ctx.exit(1)

    except KeyboardInterrupt:
        out.warning("\nDownload cancelled by user")
        ctx.exit(130)  # Standard exit code for SIGINT
    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)
