"""Workspace resolution and display utilities."""

from typing import Optional, Union

from .api import DrimeClient
from .exceptions import DrimeAPIError


def resolve_workspace_identifier(
    client: DrimeClient,
    identifier: Union[int, str, None],
    default_workspace: Optional[int] = None,
) -> int:
    """Resolve a workspace identifier (ID, name, or None) to an integer ID.

    Args:
        client: Drime API client
        identifier: Workspace ID (int), workspace name (str), or None
        default_workspace: Default workspace ID to use when identifier is None
            (defaults to 0 for personal workspace)

    Returns:
        Resolved workspace ID as integer

    Raises:
        ValueError: If workspace name is not found
    """
    # Handle None - use default workspace
    if identifier is None:
        return default_workspace if default_workspace is not None else 0

    # Handle integer ID directly
    if isinstance(identifier, int):
        return identifier

    # Handle string - could be numeric string or workspace name
    if identifier.isdigit():
        return int(identifier)

    # Try to resolve as workspace name (case-insensitive)
    result = client.get_workspaces()
    if isinstance(result, dict) and "workspaces" in result:
        identifier_lower = identifier.lower()
        for ws in result["workspaces"]:
            if ws.get("name", "").lower() == identifier_lower:
                return int(ws.get("id", 0))

    raise ValueError(
        f"Workspace '{identifier}' not found. "
        f"Use 'pydrime workspaces' to list available workspaces."
    )


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
                    name: Optional[str] = ws.get("name")
                    return name
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
