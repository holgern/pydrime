"""Utility commands - status, cd, usage, validate, and find_duplicates commands."""

from pathlib import Path
from typing import Any, Optional, cast

import click

from ..api import DrimeClient
from ..config import config
from ..duplicate_finder import DuplicateFileFinder
from ..exceptions import DrimeAPIError, DrimeNotFoundError
from ..file_entries_manager import FileEntriesManager
from ..models import FileEntriesResult, FileEntry, SchemaValidationWarning, UserStatus
from ..output import OutputFormatter
from ..workspace_utils import format_workspace_display, get_folder_display_name
from .helpers import scan_directory


@click.command()
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
        out.info("Run 'pydrime init' to configure your API key")
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


@click.command()
@click.argument("folder_identifier", type=str, required=False)
@click.pass_context
def cd(ctx: Any, folder_identifier: Optional[str]) -> None:
    """Change current working directory (folder).

    FOLDER_IDENTIFIER: ID or name of the folder to navigate to
                       (omit or use 0 or / for root, use .. for parent)

    Examples:
        pydrime cd 480432024    # Navigate to folder with ID 480432024
        pydrime cd ..           # Navigate to parent folder
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

    # Handle ".." to navigate to parent folder
    if folder_identifier == "..":
        current_folder = config.get_current_folder()
        if current_folder is None:
            # Already at root, do nothing
            out.info("Already at root directory")
            return

        try:
            client = DrimeClient(api_key=api_key)
            folder_info = client.get_folder_info(current_folder)
            parent_id = folder_info.get("parent_id")

            if parent_id is None or parent_id == 0:
                # Parent is root
                config.save_current_folder(None)
                out.success("Changed to root directory")
            else:
                config.save_current_folder(parent_id)
                out.success(f"Changed to folder ID: {parent_id}")
        except DrimeAPIError as e:
            out.error(str(e))
            ctx.exit(1)
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
        out.error(str(e))
        ctx.exit(1)


@click.command()
@click.pass_context
def usage(ctx: Any) -> None:
    """Display storage space usage information.

    Shows how much storage you've used and how much is available.
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        result = client.get_space_usage()

        if out.json_output:
            out.output_json(result)
            return

        if isinstance(result, dict):
            used = result.get("used", 0)
            total = result.get("available", 0)
            available = total - used
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
@click.pass_context
def validate(
    ctx: Any, path: str, remote_path: Optional[str], workspace: Optional[int]
) -> None:  # noqa: C901
    """Validate that local files/folders are uploaded with correct size.

    PATH: Local file or directory to validate

    Checks if every file in the given path exists in Drime Cloud,
    has the same size as the local file, and has the users field set
    (indicating a complete upload).

    Files without the users field are flagged as incomplete uploads,
    which can occur due to race conditions during parallel uploads.

    Examples:
        pydrime validate drime_test              # Validate folder
        pydrime validate drime_test/test1.txt    # Validate single file
        pydrime validate . -w 5                  # Validate current dir in workspace 5
        pydrime validate /path/to/local -r remote_folder  # Validate with remote path
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]
    source_path = Path(path)

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
        if source_path.is_file():
            files_to_validate = [(source_path, remote_path or source_path.name)]
        else:
            out.info(f"Scanning directory: {source_path}")
            # Use parent as base_path so the folder name is included in relative paths
            base_path = source_path.parent if remote_path is None else source_path
            files_to_validate = scan_directory(source_path, base_path, out)

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
        elif not source_path.is_file():
            # If validating a directory without remote_path, look for matching folder
            folder_entry = file_manager.find_folder_by_name(
                source_path.name, current_folder_id
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
        incomplete_files = []

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
            elif not matching_entry.users:
                # File exists with correct size but has no users field
                # This indicates an incomplete upload (race condition during parallel)
                incomplete_files.append(
                    {
                        "path": rel_path,
                        "size": local_size,
                        "cloud_id": matching_entry.id,
                        "reason": "No users field (incomplete upload)",
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
                    "incomplete": len(incomplete_files),
                    "valid_files": valid_files,
                    "missing_files": missing_files,
                    "size_mismatch_files": size_mismatch_files,
                    "incomplete_files": incomplete_files,
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

            # Show incomplete files (no users field)
            if incomplete_files:
                out.warning(f"⚠ Incomplete: {len(incomplete_files)} file(s)")
                for f in incomplete_files:
                    file_size = cast(int, f["size"])
                    out.print(
                        f"  ⚠ {f['path']} [ID: {f['cloud_id']}] "
                        f"({out.format_size(file_size)}) - {f['reason']}"
                    )
                out.print("")

            # Summary
            total = len(files_to_validate)
            valid = len(valid_files)
            issues = (
                len(missing_files) + len(size_mismatch_files) + len(incomplete_files)
            )

            out.print("=" * 60)
            if issues == 0:
                out.success(f"All {total} file(s) validated successfully!")
            else:
                msg = f"Validation complete: {valid}/{total} valid, {issues} issue(s)"
                out.warning(msg)
            out.print("=" * 60)

        # Exit with error code if there are issues
        if missing_files or size_mismatch_files or incomplete_files:
            ctx.exit(1)

    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)


@click.command(name="find-duplicates")
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
    delete: bool,
    keep_newest: bool,
) -> None:
    """Find and optionally delete duplicate files.

    Duplicates are files with the same name but different IDs within the same folder.
    This detects files that were uploaded multiple times with the same name.
    By default, the oldest file (lowest ID) is kept and newer duplicates are deleted.

    Examples:

        # Show duplicates in current folder
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
            out.info(f"Mode: {'DELETE' if delete else 'SHOW ONLY'}")
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

        # Delete or show only
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
            # Show only mode
            out.info("\nTo delete duplicates, use: pydrime find-duplicates --delete")

    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)
