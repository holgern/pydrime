"""Sync pair definition for synchronizing local and remote paths."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from .modes import SyncMode


@dataclass
class SyncPair:
    """Defines a synchronization pair between local and remote paths.

    A sync pair specifies how files should be synchronized between a local directory
    and a remote path in Drime Cloud, including the sync mode and various options.

    Examples:
        >>> pair = SyncPair(
        ...     local=Path("/home/user/Documents"),
        ...     remote="/Documents",
        ...     sync_mode=SyncMode.TWO_WAY
        ... )
        >>> pair.alias = "documents"
        >>> pair.ignore = ["*.tmp", "*.log"]
    """

    local: Path
    """Local directory path to sync"""

    remote: str
    """Remote path in Drime Cloud (e.g., "/Documents" or "Documents")"""

    sync_mode: SyncMode
    """Synchronization mode (how files are synced)"""

    alias: Optional[str] = None
    """Optional alias for easy reference in CLI"""

    disable_local_trash: bool = False
    """If True, deleted local files are permanently deleted instead of moved to trash"""

    ignore: list[str] = field(default_factory=list)
    """List of glob patterns to ignore (e.g., ["*.log", "temp/*"])"""

    exclude_dot_files: bool = False
    """If True, exclude files and folders starting with dot"""

    workspace_id: int = 0
    """Workspace ID (0 for personal workspace)"""

    @property
    def use_local_trash(self) -> bool:
        """Whether to use local trash for deleted files."""
        return not self.disable_local_trash

    def __post_init__(self) -> None:
        """Validate and normalize sync pair configuration."""
        # Ensure local is a Path object (runtime coercion when passed as str)
        # Cast to Any to allow runtime type check without mypy complaining
        local_value: Any = self.local
        if not isinstance(local_value, Path):
            object.__setattr__(self, "local", Path(local_value))

        # Ensure sync_mode is SyncMode enum
        if isinstance(self.sync_mode, str):
            self.sync_mode = SyncMode.from_string(self.sync_mode)

        # Normalize remote path (remove leading/trailing slashes for consistency)
        # When remote is "/" or empty, files sync directly to cloud root.
        # E.g., local/subdir/file.txt -> /subdir/file.txt (not /local/subdir/file.txt)
        if self.remote == "/":
            # Root directory - normalize to empty string (meaning cloud root)
            self.remote = ""
        else:
            self.remote = self.remote.strip("/")

    @classmethod
    def from_dict(cls, data: dict) -> "SyncPair":
        """Create SyncPair from dictionary (e.g., from JSON config).

        Args:
            data: Dictionary with sync pair configuration

        Returns:
            SyncPair instance

        Raises:
            ValueError: If required fields are missing or invalid

        Examples:
            >>> data = {
            ...     "local": "/home/user/Documents",
            ...     "remote": "/Documents",
            ...     "syncMode": "twoWay",
            ...     "alias": "documents"
            ... }
            >>> pair = SyncPair.from_dict(data)
        """
        required_fields = ["local", "remote", "syncMode"]
        missing = [f for f in required_fields if f not in data]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

        return cls(
            local=Path(data["local"]),
            remote=data["remote"],
            sync_mode=SyncMode.from_string(data["syncMode"]),
            alias=data.get("alias"),
            disable_local_trash=data.get("disableLocalTrash", False),
            ignore=data.get("ignore", []),
            exclude_dot_files=data.get("excludeDotFiles", False),
            workspace_id=data.get("workspaceId", 0),
        )

    def to_dict(self) -> dict:
        """Convert SyncPair to dictionary for JSON serialization.

        Returns:
            Dictionary representation of sync pair.
            Uses POSIX-style paths (forward slashes) for cross-platform consistency.
        """
        return {
            "local": self.local.as_posix(),
            "remote": self.remote,
            "syncMode": self.sync_mode.value,
            "alias": self.alias,
            "disableLocalTrash": self.disable_local_trash,
            "ignore": self.ignore,
            "excludeDotFiles": self.exclude_dot_files,
            "workspaceId": self.workspace_id,
        }

    @classmethod
    def parse_literal(
        cls, literal: str, default_mode: Optional[SyncMode] = None
    ) -> "SyncPair":
        """Parse a literal sync pair string.

        Supports various formats:
        - /local:/remote                     # Two-way (default)
        - /local:twoWay:/remote              # Explicit mode
        - /local:tw:/remote                  # Abbreviated mode
        - /local:localToCloud:/remote        # Full mode name

        On Windows, also supports:
        - C:/local:mode:/remote              # Windows path with mode
        - C:/local:/remote                   # Windows path without mode

        Args:
            literal: Literal sync pair string
            default_mode: Default sync mode if not specified (defaults to TWO_WAY)

        Returns:
            SyncPair instance

        Raises:
            ValueError: If literal format is invalid

        Examples:
            >>> pair = SyncPair.parse_literal("/home/user/docs:/Documents")
            >>> pair.sync_mode
            SyncMode.TWO_WAY
            >>> pair = SyncPair.parse_literal("/home/user/docs:ltc:/Documents")
            >>> pair.sync_mode
            SyncMode.LOCAL_TO_CLOUD
        """
        import re

        if default_mode is None:
            default_mode = SyncMode.TWO_WAY

        # Handle Windows drive letters (e.g., C:, D:)
        # If path starts with a drive letter, split only on colons after the drive
        windows_drive_match = re.match(r"^([A-Za-z]:)", literal)
        if windows_drive_match:
            drive = windows_drive_match.group(1)
            rest_of_literal = literal[len(drive) :]
            parts = rest_of_literal.split(":")
            # Prepend the drive to the first part
            if parts:
                parts[0] = drive + parts[0]
        else:
            parts = literal.split(":")

        if len(parts) == 2:
            # Format: /local:/remote (default mode)
            local, remote = parts
            sync_mode = default_mode
        elif len(parts) == 3:
            # Format: /local:mode:/remote
            local, mode_str, remote = parts
            sync_mode = SyncMode.from_string(mode_str)
        else:
            raise ValueError(
                f"Invalid sync pair literal: {literal}. "
                "Expected format: '/local:/remote' or '/local:mode:/remote'"
            )

        # Validate paths are not empty
        if not local or not remote:
            raise ValueError(
                f"Invalid sync pair literal: {literal}. "
                "Local and remote paths cannot be empty"
            )

        return cls(
            local=Path(local),
            remote=remote,
            sync_mode=sync_mode,
        )

    def __str__(self) -> str:
        """String representation of sync pair.

        Uses POSIX-style paths for cross-platform consistency.
        """
        local_str = self.local.as_posix()
        if self.alias:
            return f"{self.alias} ({local_str} ←{self.sync_mode.value}→ {self.remote})"
        return f"{local_str} ←{self.sync_mode.value}→ {self.remote}"

    def __repr__(self) -> str:
        """Detailed representation of sync pair."""
        return (
            f"SyncPair(local={self.local.as_posix()}, remote={self.remote}, "
            f"sync_mode={self.sync_mode}, alias={self.alias})"
        )
