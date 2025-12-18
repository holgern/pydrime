"""List commands - ls and du commands for pydrime CLI."""

from typing import Any, Optional

import click

from ..api import DrimeClient
from ..config import config
from ..exceptions import DrimeAPIError, DrimeNotFoundError
from ..models import FileEntriesResult, FileEntry
from ..output import OutputFormatter
from ..utils import glob_match, is_glob_pattern


@click.command()
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

    PARENT_IDENTIFIER: ID, name, or glob pattern of parent folder
                       (omit to list current directory)

    Supports glob patterns (*, ?, []) to filter entries by name:
    - * matches any sequence of characters
    - ? matches any single character
    - [abc] matches any character in the set

    Similar to Unix ls command, shows file and folder names in a columnar format.
    Use 'du' command for detailed disk usage information.

    Examples:
        pydrime ls                          # List current directory
        pydrime ls 480432024                # List folder by ID
        pydrime ls test_folder              # List folder by name
        pydrime ls Documents                # List folder by name
        pydrime ls "*.txt"                  # List all .txt files
        pydrime ls "bench*"                 # List entries starting with bench
        pydrime ls "file?.txt"              # Match file1.txt, file2.txt, etc.
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

        # Display workspace and current directory info
        if not out.quiet and not out.json_output:
            from ..workspace_utils import (
                format_workspace_display,
                get_folder_display_name,
            )

            workspace_display, _ = format_workspace_display(client, workspace)
            current_folder_id = config.get_current_folder()
            folder_display, _ = get_folder_display_name(client, current_folder_id)
            out.info(f"Workspace: {workspace_display} | Directory: {folder_display}")

        # Check if parent_identifier is a glob pattern
        glob_pattern = None
        if parent_identifier is not None and is_glob_pattern(parent_identifier):
            # It's a glob pattern - will filter entries in current folder
            glob_pattern = parent_identifier
            parent_identifier = None

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
        elif not any(
            [deleted, starred, recent, shared, folder_hash, query, glob_pattern]
        ):
            # If no parent_identifier specified, use current working directory
            parent_id = config.get_current_folder()
        elif glob_pattern:
            # Use current folder for glob pattern filtering
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

        # Apply glob pattern filtering if specified
        if glob_pattern:
            filtered_entries = [
                e for e in file_entries.entries if glob_match(glob_pattern, e.name)
            ]
            file_entries.entries = filtered_entries
            if not out.quiet and filtered_entries:
                out.info(
                    f"Matched {len(filtered_entries)} entries with '{glob_pattern}'"
                )

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


@click.command()
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

        # Display workspace and current directory info
        if not out.quiet and not out.json_output:
            from ..workspace_utils import (
                format_workspace_display,
                get_folder_display_name,
            )

            workspace_display, _ = format_workspace_display(client, workspace)
            current_folder_id = config.get_current_folder()
            folder_display, _ = get_folder_display_name(client, current_folder_id)
            out.info(f"Workspace: {workspace_display} | Directory: {folder_display}")

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
            "per_page": 100,  # Request more entries per page
        }

        # Add parent_id if specified
        if parent_id is not None:
            params["parent_ids"] = [parent_id]

        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}

        # Collect all entries across pages
        all_entries: list[FileEntry] = []
        current_page = 1

        while True:
            params["page"] = current_page
            result = client.get_file_entries(**params)

            # Parse the response into our data model
            page_entries = FileEntriesResult.from_api_response(result)
            all_entries.extend(page_entries.entries)

            # Check if there are more pages
            if page_entries.pagination:
                current = page_entries.pagination.get("current_page")
                last = page_entries.pagination.get("last_page")
                if current is not None and last is not None and current < last:
                    current_page += 1
                    continue
            break

        # Create a combined result
        file_entries = FileEntriesResult(
            entries=all_entries,
            raw_data=result,  # Keep last page's raw data for reference
            pagination={"total": len(all_entries)},
        )

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
