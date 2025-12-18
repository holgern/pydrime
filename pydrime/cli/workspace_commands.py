"""Workspace-related commands."""

from typing import Any, Optional

import click

from ..api import DrimeClient
from ..config import config
from ..exceptions import DrimeAPIError
from ..output import OutputFormatter


@click.command()
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


@click.command()
@click.pass_context
def workspaces(ctx: Any) -> None:
    """List all workspaces you have access to.

    Shows workspace name, ID, your role, and owner information.
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
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


@click.command()
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
        out.info("Run 'pydrime init' to configure your API key")
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
