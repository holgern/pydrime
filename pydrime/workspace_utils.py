"""Workspace resolution and display utilities."""

from typing import Optional

from .api import DrimeClient
from .exceptions import DrimeAPIError


def get_workspace_name(client: DrimeClient, workspace_id: int) -> Optional[str]:
    """Get workspace name by ID.

    Args:
        client: Drime API client
        workspace_id: Workspace ID

    Returns:
        Workspace name if found, None otherwise
    """
    try:
        result = client.get_workspaces()
        if isinstance(result, dict) and "workspaces" in result:
            for ws in result["workspaces"]:
                if ws.get("id") == workspace_id:
                    return ws.get("name")
    except (DrimeAPIError, Exception):
        pass
    return None


def format_workspace_display(
    client: DrimeClient, workspace_id: int
) -> tuple[str, Optional[str]]:
    """Format workspace for display.

    Args:
        client: Drime API client
        workspace_id: Workspace ID

    Returns:
        Tuple of (display_string, workspace_name)
    """
    if workspace_id == 0:
        return ("Personal (0)", None)

    workspace_name = get_workspace_name(client, workspace_id)
    if workspace_name:
        return (f"{workspace_name} ({workspace_id})", workspace_name)
    return (str(workspace_id), None)


def get_folder_display_name(
    client: DrimeClient, folder_id: Optional[int]
) -> tuple[str, Optional[str]]:
    """Get folder display name for output.

    Args:
        client: Drime API client
        folder_id: Folder ID or None for root

    Returns:
        Tuple of (display_string, folder_name)
    """
    if folder_id is None:
        return ("/ (Root, ID: 0)", None)

    try:
        folder_info = client.get_folder_info(folder_id)
        folder_name = folder_info.get("name")
        return (f"/{folder_name} (ID: {folder_id})", folder_name)
    except (DrimeAPIError, Exception):
        return (f"ID {folder_id}", None)
