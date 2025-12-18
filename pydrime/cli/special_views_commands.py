"""Special views commands - recent, trash, and starred commands for pydrime CLI."""

from typing import Any, Optional

import click

from ..api import DrimeClient
from ..config import config
from ..exceptions import DrimeAPIError
from ..models import FileEntriesResult
from ..output import OutputFormatter


@click.command()
@click.option("--workspace", "-w", type=int, default=None, help="Workspace ID")
@click.option("--page", "-p", type=int, default=1, help="Page number (1-based)")
@click.option(
    "--page-size", type=int, default=50, help="Number of items per page (default: 50)"
)
@click.option(
    "--order-by",
    type=click.Choice(["created_at", "updated_at", "name", "file_size"]),
    default="created_at",
    help="Field to order by (default: created_at)",
)
@click.option(
    "--order-dir",
    type=click.Choice(["asc", "desc"]),
    default="desc",
    help="Order direction (default: desc)",
)
@click.pass_context
def recent(
    ctx: Any,
    workspace: Optional[int],
    page: int,
    page_size: int,
    order_by: str,
    order_dir: str,
) -> None:
    """List recently accessed files.

    Shows files that have been recently created or modified, ordered by date.

    Examples:
        pydrime recent                          # List recent files
        pydrime recent --page 2                 # List page 2 of recent files
        pydrime recent --order-by updated_at    # Order by last update
        pydrime recent --order-dir asc          # Order ascending (oldest first)
        pydrime recent -w 1593                  # List recent files in workspace 1593
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

        # Display workspace info
        if not out.quiet and not out.json_output:
            from ..workspace_utils import format_workspace_display

            workspace_display, _ = format_workspace_display(client, workspace)
            out.info(f"Workspace: {workspace_display} | Recent files")

        # Call API with recent-specific parameters
        result = client.get_file_entries(
            page_id="recent",
            backup=0,
            recent_only=True,
            workspace_id=workspace,
            order_by=order_by,
            order_dir=order_dir,
            page=page,
            per_page=page_size,
        )

        # Parse the response into our data model
        file_entries = FileEntriesResult.from_api_response(result)

        # Output based on format
        if out.json_output:
            out.output_json(file_entries.to_dict())
            return

        if file_entries.is_empty:
            if not out.quiet:
                out.info("No recent files found")
            return

        # Text format - simple list of names (like Unix ls)
        table_data = file_entries.to_table_data()
        out.output_table(
            table_data,
            ["name"],
            {"name": "Name"},
        )

        # Display pagination info
        if not out.quiet and file_entries.pagination:
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

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@click.command()
@click.option("--workspace", "-w", type=int, default=None, help="Workspace ID")
@click.option("--page", "-p", type=int, default=1, help="Page number (1-based)")
@click.option(
    "--page-size", type=int, default=50, help="Number of items per page (default: 50)"
)
@click.option(
    "--order-by",
    type=click.Choice(["created_at", "updated_at", "name", "file_size"]),
    default="updated_at",
    help="Field to order by (default: updated_at)",
)
@click.option(
    "--order-dir",
    type=click.Choice(["asc", "desc"]),
    default="desc",
    help="Order direction (default: desc)",
)
@click.pass_context
def trash(
    ctx: Any,
    workspace: Optional[int],
    page: int,
    page_size: int,
    order_by: str,
    order_dir: str,
) -> None:
    """List deleted files and folders in trash.

    Shows files and folders that have been deleted and are in the trash.

    Examples:
        pydrime trash                          # List trashed files
        pydrime trash --page 2                 # List page 2 of trashed files
        pydrime trash --order-by name          # Order by name
        pydrime trash --order-dir asc          # Order ascending
        pydrime trash -w 1593                  # List trash in workspace 1593
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

        # Display workspace info
        if not out.quiet and not out.json_output:
            from ..workspace_utils import format_workspace_display

            workspace_display, _ = format_workspace_display(client, workspace)
            out.info(f"Workspace: {workspace_display} | Trash")

        # Call API with trash-specific parameters
        result = client.get_file_entries(
            page_id="trash",
            backup=0,
            deleted_only=True,
            workspace_id=workspace,
            order_by=order_by,
            order_dir=order_dir,
            page=page,
            per_page=page_size,
        )

        # Parse the response into our data model
        file_entries = FileEntriesResult.from_api_response(result)

        # Output based on format
        if out.json_output:
            out.output_json(file_entries.to_dict())
            return

        if file_entries.is_empty:
            if not out.quiet:
                out.info("Trash is empty")
            return

        # Text format - simple list of names (like Unix ls)
        table_data = file_entries.to_table_data()
        out.output_table(
            table_data,
            ["name"],
            {"name": "Name"},
        )

        # Display pagination info
        if not out.quiet and file_entries.pagination:
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

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@click.command()
@click.option("--workspace", "-w", type=int, default=None, help="Workspace ID")
@click.option("--page", "-p", type=int, default=1, help="Page number (1-based)")
@click.option(
    "--page-size", type=int, default=50, help="Number of items per page (default: 50)"
)
@click.option(
    "--order-by",
    type=click.Choice(["created_at", "updated_at", "name", "file_size"]),
    default="updated_at",
    help="Field to order by (default: updated_at)",
)
@click.option(
    "--order-dir",
    type=click.Choice(["asc", "desc"]),
    default="desc",
    help="Order direction (default: desc)",
)
@click.pass_context
def starred(
    ctx: Any,
    workspace: Optional[int],
    page: int,
    page_size: int,
    order_by: str,
    order_dir: str,
) -> None:
    """List starred files and folders.

    Shows files and folders that have been marked as starred/favorites.

    Examples:
        pydrime starred                          # List starred files
        pydrime starred --page 2                 # List page 2 of starred files
        pydrime starred --order-by name          # Order by name
        pydrime starred --order-dir asc          # Order ascending
        pydrime starred -w 1593                  # List starred in workspace 1593
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

        # Display workspace info
        if not out.quiet and not out.json_output:
            from ..workspace_utils import format_workspace_display

            workspace_display, _ = format_workspace_display(client, workspace)
            out.info(f"Workspace: {workspace_display} | Starred")

        # Call API with starred-specific parameters
        result = client.get_file_entries(
            page_id="starred",
            backup=0,
            starred_only=True,
            workspace_id=workspace,
            order_by=order_by,
            order_dir=order_dir,
            page=page,
            per_page=page_size,
        )

        # Parse the response into our data model
        file_entries = FileEntriesResult.from_api_response(result)

        # Output based on format
        if out.json_output:
            out.output_json(file_entries.to_dict())
            return

        if file_entries.is_empty:
            if not out.quiet:
                out.info("No starred files found")
            return

        # Text format - simple list of names (like Unix ls)
        table_data = file_entries.to_table_data()
        out.output_table(
            table_data,
            ["name"],
            {"name": "Name"},
        )

        # Display pagination info
        if not out.quiet and file_entries.pagination:
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

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)
