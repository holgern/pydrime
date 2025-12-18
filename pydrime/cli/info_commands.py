"""Info commands - stat and pwd commands for pydrime CLI."""

from typing import Any

import click

from ..api import DrimeClient
from ..config import config
from ..exceptions import DrimeAPIError, DrimeNotFoundError
from ..output import OutputFormatter
from ..utils import parse_iso_timestamp


@click.command()
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
    from ..download_helpers import get_entry_from_hash, resolve_identifier_to_hash

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
            icon = "[D]" if entry.type == "folder" else "[F]"

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


@click.command()
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
