"""File comparison logic for sync operations."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from .modes import SyncMode
from .scanner import LocalFile, RemoteFile


class SyncAction(str, Enum):
    """Actions that can be taken during sync."""

    UPLOAD = "upload"
    """Upload local file to remote"""

    DOWNLOAD = "download"
    """Download remote file to local"""

    DELETE_LOCAL = "delete_local"
    """Delete local file"""

    DELETE_REMOTE = "delete_remote"
    """Delete remote file"""

    SKIP = "skip"
    """Skip file (no action needed)"""

    CONFLICT = "conflict"
    """File conflict detected"""


@dataclass
class SyncDecision:
    """Represents a decision about how to sync a file."""

    action: SyncAction
    """Action to take"""

    reason: str
    """Human-readable reason for this decision"""

    local_file: Optional[LocalFile]
    """Local file (if exists)"""

    remote_file: Optional[RemoteFile]
    """Remote file (if exists)"""

    relative_path: str
    """Relative path of the file"""


class FileComparator:
    """Compares local and remote files to determine sync actions."""

    def __init__(self, sync_mode: SyncMode):
        """Initialize file comparator.

        Args:
            sync_mode: Sync mode to use for comparison
        """
        self.sync_mode = sync_mode

    def compare_files(
        self,
        local_files: dict[str, LocalFile],
        remote_files: dict[str, RemoteFile],
    ) -> list[SyncDecision]:
        """Compare local and remote files and determine sync actions.

        Args:
            local_files: Dictionary mapping relative_path to LocalFile
            remote_files: Dictionary mapping relative_path to RemoteFile

        Returns:
            List of SyncDecision objects
        """
        decisions: list[SyncDecision] = []

        # Get all unique paths
        all_paths = set(local_files.keys()) | set(remote_files.keys())

        for path in sorted(all_paths):
            local_file = local_files.get(path)
            remote_file = remote_files.get(path)

            decision = self._compare_single_file(path, local_file, remote_file)
            decisions.append(decision)

        return decisions

    def _compare_single_file(
        self,
        path: str,
        local_file: Optional[LocalFile],
        remote_file: Optional[RemoteFile],
    ) -> SyncDecision:
        """Compare a single file and determine action.

        Args:
            path: Relative path of the file
            local_file: Local file (if exists)
            remote_file: Remote file (if exists)

        Returns:
            SyncDecision for this file
        """
        # Case 1: File exists in both locations
        if local_file and remote_file:
            return self._compare_existing_files(path, local_file, remote_file)

        # Case 2: File only exists locally
        if local_file and not remote_file:
            return self._handle_local_only(path, local_file)

        # Case 3: File only exists remotely
        if remote_file and not local_file:
            return self._handle_remote_only(path, remote_file)

        # Should never happen
        return SyncDecision(
            action=SyncAction.SKIP,
            reason="No file found",
            local_file=None,
            remote_file=None,
            relative_path=path,
        )

    def _compare_existing_files(
        self, path: str, local_file: LocalFile, remote_file: RemoteFile
    ) -> SyncDecision:
        """Compare files that exist in both locations."""
        # Check if files are identical (same size)
        if local_file.size == remote_file.size:
            # Files are likely identical - skip
            return SyncDecision(
                action=SyncAction.SKIP,
                reason="Files are identical (same size)",
                local_file=local_file,
                remote_file=remote_file,
                relative_path=path,
            )

        # Files are different - check modification times
        if remote_file.mtime is None:
            # No remote mtime - can't compare, prefer local for safety
            if self.sync_mode.allows_upload:
                return SyncDecision(
                    action=SyncAction.UPLOAD,
                    reason="Remote mtime unavailable, uploading local version",
                    local_file=local_file,
                    remote_file=remote_file,
                    relative_path=path,
                )
            else:
                return SyncDecision(
                    action=SyncAction.SKIP,
                    reason="Different sizes but cannot determine which is newer",
                    local_file=local_file,
                    remote_file=remote_file,
                    relative_path=path,
                )

        # Compare modification times
        local_mtime = local_file.mtime
        remote_mtime = remote_file.mtime

        # Allow 2 second tolerance for filesystem differences
        time_diff = abs(local_mtime - remote_mtime)
        if time_diff < 2:
            # Times are essentially the same but sizes differ - conflict
            reason = (
                f"Same timestamp but different sizes "
                f"({local_file.size} vs {remote_file.size})"
            )
            return SyncDecision(
                action=SyncAction.CONFLICT,
                reason=reason,
                local_file=local_file,
                remote_file=remote_file,
                relative_path=path,
            )

        # Determine which is newer
        if local_mtime > remote_mtime:
            # Local is newer
            if self.sync_mode.allows_upload:
                return SyncDecision(
                    action=SyncAction.UPLOAD,
                    reason="Local file is newer",
                    local_file=local_file,
                    remote_file=remote_file,
                    relative_path=path,
                )
        else:
            # Remote is newer
            if self.sync_mode.allows_download:
                return SyncDecision(
                    action=SyncAction.DOWNLOAD,
                    reason="Remote file is newer",
                    local_file=local_file,
                    remote_file=remote_file,
                    relative_path=path,
                )

        # Can't sync due to mode restrictions
        return SyncDecision(
            action=SyncAction.SKIP,
            reason=f"Files differ but sync mode {self.sync_mode.value} prevents action",
            local_file=local_file,
            remote_file=remote_file,
            relative_path=path,
        )

    def _handle_local_only(self, path: str, local_file: LocalFile) -> SyncDecision:
        """Handle file that only exists locally."""
        if self.sync_mode.allows_upload:
            return SyncDecision(
                action=SyncAction.UPLOAD,
                reason="New local file",
                local_file=local_file,
                remote_file=None,
                relative_path=path,
            )
        elif self.sync_mode.allows_local_delete:
            # File was deleted from cloud and should be deleted locally
            # (for cloudToLocal mode)
            return SyncDecision(
                action=SyncAction.DELETE_LOCAL,
                reason="File deleted from cloud",
                local_file=local_file,
                remote_file=None,
                relative_path=path,
            )
        else:
            reason = (
                f"Local-only file but sync mode {self.sync_mode.value} prevents action"
            )
            return SyncDecision(
                action=SyncAction.SKIP,
                reason=reason,
                local_file=local_file,
                remote_file=None,
                relative_path=path,
            )

    def _handle_remote_only(self, path: str, remote_file: RemoteFile) -> SyncDecision:
        """Handle file that only exists remotely."""
        if self.sync_mode.allows_download:
            return SyncDecision(
                action=SyncAction.DOWNLOAD,
                reason="New remote file",
                local_file=None,
                remote_file=remote_file,
                relative_path=path,
            )
        elif self.sync_mode.allows_remote_delete:
            # File was deleted locally and should be deleted remotely
            return SyncDecision(
                action=SyncAction.DELETE_REMOTE,
                reason="File deleted locally",
                local_file=None,
                remote_file=remote_file,
                relative_path=path,
            )
        else:
            reason = (
                f"Remote-only file but sync mode {self.sync_mode.value} prevents action"
            )
            return SyncDecision(
                action=SyncAction.SKIP,
                reason=reason,
                local_file=None,
                remote_file=remote_file,
                relative_path=path,
            )
