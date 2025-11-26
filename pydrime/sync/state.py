"""State management for tracking sync history.

This module provides state tracking for bidirectional sync modes,
enabling proper detection of file deletions by remembering which
files were present in previous sync operations.
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SyncState:
    """Represents the state of a sync pair from a previous sync.

    Stores information about which files existed in both local and remote
    locations during the last successful sync, enabling deletion detection.
    """

    local_path: str
    """Local directory path that was synced"""

    remote_path: str
    """Remote path that was synced"""

    synced_files: set[str] = field(default_factory=set)
    """Set of relative paths that existed in both locations after last sync"""

    last_sync: Optional[str] = None
    """ISO timestamp of last successful sync"""

    def to_dict(self) -> dict:
        """Convert state to dictionary for JSON serialization."""
        return {
            "local_path": self.local_path,
            "remote_path": self.remote_path,
            "synced_files": sorted(self.synced_files),
            "last_sync": self.last_sync,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SyncState":
        """Create SyncState from dictionary."""
        return cls(
            local_path=data.get("local_path", ""),
            remote_path=data.get("remote_path", ""),
            synced_files=set(data.get("synced_files", [])),
            last_sync=data.get("last_sync"),
        )


class SyncStateManager:
    """Manages sync state persistence for tracking file deletions.

    The state is stored in a JSON file in the user's config directory,
    keyed by a hash of the local and remote paths to support multiple
    sync pairs.
    """

    def __init__(self, state_dir: Optional[Path] = None):
        """Initialize state manager.

        Args:
            state_dir: Directory to store state files. Defaults to
                      ~/.config/pydrime/sync_state/
        """
        if state_dir is None:
            state_dir = Path.home() / ".config" / "pydrime" / "sync_state"
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _get_state_key(self, local_path: Path, remote_path: str) -> str:
        """Generate a unique key for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path

        Returns:
            Hash-based key for the sync pair
        """
        # Use absolute path for consistency
        local_abs = str(local_path.resolve())
        combined = f"{local_abs}:{remote_path}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def _get_state_file(self, local_path: Path, remote_path: str) -> Path:
        """Get the state file path for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path

        Returns:
            Path to the state file
        """
        key = self._get_state_key(local_path, remote_path)
        return self.state_dir / f"{key}.json"

    def load_state(self, local_path: Path, remote_path: str) -> Optional[SyncState]:
        """Load sync state for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path

        Returns:
            SyncState if found, None otherwise
        """
        state_file = self._get_state_file(local_path, remote_path)

        if not state_file.exists():
            logger.debug(f"No sync state found at {state_file}")
            return None

        try:
            with open(state_file, encoding="utf-8") as f:
                data = json.load(f)
            state = SyncState.from_dict(data)
            logger.debug(
                f"Loaded sync state with {len(state.synced_files)} files "
                f"from {state.last_sync}"
            )
            return state
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to load sync state: {e}")
            return None

    def save_state(
        self,
        local_path: Path,
        remote_path: str,
        synced_files: set[str],
    ) -> None:
        """Save sync state for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path
            synced_files: Set of relative paths that exist in both locations
        """
        state = SyncState(
            local_path=str(local_path.resolve()),
            remote_path=remote_path,
            synced_files=synced_files,
            last_sync=datetime.now().isoformat(),
        )

        state_file = self._get_state_file(local_path, remote_path)

        try:
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, indent=2)
            logger.debug(
                f"Saved sync state with {len(synced_files)} files to {state_file}"
            )
        except OSError as e:
            logger.warning(f"Failed to save sync state: {e}")

    def clear_state(self, local_path: Path, remote_path: str) -> bool:
        """Clear sync state for a sync pair.

        Args:
            local_path: Local directory path
            remote_path: Remote path

        Returns:
            True if state was cleared, False if no state existed
        """
        state_file = self._get_state_file(local_path, remote_path)

        if state_file.exists():
            state_file.unlink()
            logger.debug(f"Cleared sync state at {state_file}")
            return True
        return False
