"""Configuration loading for sync pairs from JSON files."""

from pathlib import Path
from typing import Any

from .modes import SyncMode


class SyncConfigError(Exception):
    """Error loading or validating sync configuration."""

    pass


def load_sync_pairs_from_json(json_path: Path) -> list[dict[str, Any]]:
    """Load and validate sync pairs from a JSON file.

    The JSON file should contain a list of sync pair dictionaries with these fields:
        - workspace (int or str, optional): Workspace ID or name
          (default: uses configured default workspace)
        - local (str, required): Local directory path
        - remote (str, required): Remote path in cloud
        - syncMode (str, required): Sync mode (twoWay, localToCloud, etc.)
        - disableLocalTrash (bool, optional): Disable local trash (default: false)
        - ignore (list[str], optional): Glob patterns to ignore (default: [])
        - excludeDotFiles (bool, optional): Exclude dot files (default: false)

    Args:
        json_path: Path to the JSON configuration file

    Returns:
        List of validated sync pair dictionaries

    Raises:
        SyncConfigError: If file is invalid or contains errors

    Examples:
        >>> pairs = load_sync_pairs_from_json(Path("sync_config.json"))
        >>> for pair in pairs:
        ...     print(f"{pair['local']} -> {pair['remote']}")
    """
    import json

    try:
        with open(json_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise SyncConfigError(f"Invalid JSON in {json_path}: {e}") from None
    except OSError as e:
        raise SyncConfigError(f"Cannot read file {json_path}: {e}") from None

    if not isinstance(data, list):
        raise SyncConfigError(
            f"Invalid JSON format in {json_path}: expected a list of sync pairs"
        )

    if not data:
        raise SyncConfigError(f"No sync pairs found in {json_path}")

    # Validate each sync pair
    valid_pairs: list[dict[str, Any]] = []
    for i, pair_data in enumerate(data):
        if not isinstance(pair_data, dict):
            raise SyncConfigError(
                f"Invalid sync pair at index {i}: expected a dictionary"
            )

        # Check required fields
        required_fields = ["local", "remote", "syncMode"]
        missing = [f for f in required_fields if f not in pair_data]
        if missing:
            raise SyncConfigError(
                f"Sync pair at index {i} missing required fields: {', '.join(missing)}"
            )

        # Validate syncMode
        try:
            SyncMode.from_string(pair_data["syncMode"])
        except ValueError as e:
            raise SyncConfigError(f"Sync pair at index {i}: {e}") from None

        # Validate local path exists
        local_path = Path(pair_data["local"])
        if not local_path.exists():
            raise SyncConfigError(
                f"Sync pair at index {i}: local path does not exist: {local_path}"
            )
        if not local_path.is_dir():
            raise SyncConfigError(
                f"Sync pair at index {i}: local path is not a directory: {local_path}"
            )

        # Normalize and validate optional fields
        # Use None as default for workspace to indicate "use default workspace"
        # The workspace value can be an integer (ID) or a string (name)
        workspace_value = pair_data.get("workspace")
        normalized: dict[str, Any] = {
            "local": str(local_path),
            "remote": pair_data["remote"],
            "syncMode": pair_data["syncMode"],
            "workspace": workspace_value,  # None, int, or str (name)
            "disableLocalTrash": pair_data.get("disableLocalTrash", False),
            "ignore": pair_data.get("ignore", []),
            "excludeDotFiles": pair_data.get("excludeDotFiles", False),
        }

        # Validate workspace type (int, str, or None)
        if normalized["workspace"] is not None and not isinstance(
            normalized["workspace"], (int, str)
        ):
            raise SyncConfigError(
                f"Sync pair at index {i}: workspace must be an integer or string"
            )
        if not isinstance(normalized["disableLocalTrash"], bool):
            raise SyncConfigError(
                f"Sync pair at index {i}: disableLocalTrash must be a boolean"
            )
        if not isinstance(normalized["ignore"], list):
            raise SyncConfigError(f"Sync pair at index {i}: ignore must be a list")
        if not isinstance(normalized["excludeDotFiles"], bool):
            raise SyncConfigError(
                f"Sync pair at index {i}: excludeDotFiles must be a boolean"
            )

        valid_pairs.append(normalized)

    return valid_pairs
