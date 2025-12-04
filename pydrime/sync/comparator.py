"""File comparison logic for sync operations."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from .modes import SyncMode
from .scanner import LocalFile, RemoteFile
from .state import LocalTree, RemoteTree


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

    RENAME_LOCAL = "rename_local"
    """Rename/move local file (detected via remote rename)"""

    RENAME_REMOTE = "rename_remote"
    """Rename/move remote file (detected via local rename)"""

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

    old_path: Optional[str] = None
    """For rename operations: the previous path of the file"""

    new_path: Optional[str] = None
    """For rename operations: the new path of the file"""


class FileComparator:
    """Compares local and remote files to determine sync actions.

    For bidirectional sync modes (TWO_WAY), the comparator can use previous
    sync state to determine whether a file that exists only on one side is
    a new file or was deleted from the other side.

    Rename detection uses file_id (for local files) and id (for remote files)
    to detect when a file has been moved/renamed rather than deleted+created.
    This enables efficient rename operations instead of re-uploading/downloading.
    """

    def __init__(
        self,
        sync_mode: SyncMode,
        previous_synced_files: Optional[set[str]] = None,
        previous_local_tree: Optional[LocalTree] = None,
        previous_remote_tree: Optional[RemoteTree] = None,
    ):
        """Initialize file comparator.

        Args:
            sync_mode: Sync mode to use for comparison
            previous_synced_files: Set of relative paths that were synced
                                  in the previous sync operation. Used for
                                  deletion detection in TWO_WAY mode.
            previous_local_tree: Previous local tree state for rename detection.
                                Contains file_id -> path mapping.
            previous_remote_tree: Previous remote tree state for rename detection.
                                 Contains id -> path mapping.
        """
        self.sync_mode = sync_mode
        self.previous_synced_files = previous_synced_files or set()
        self.previous_local_tree = previous_local_tree
        self.previous_remote_tree = previous_remote_tree

        # Track detected renames to avoid processing them twice
        self._local_renames: dict[int, str] = {}  # file_id -> new_path
        self._remote_renames: dict[int, str] = {}  # id -> new_path
        self._handled_rename_old_paths: set[str] = set()

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

        # First pass: detect renames by comparing with previous state
        if self.sync_mode == SyncMode.TWO_WAY:
            self._detect_renames(local_files, remote_files)

        # Get all unique paths
        all_paths = set(local_files.keys()) | set(remote_files.keys())

        for path in sorted(all_paths):
            # Skip if this path was already handled as old path of a rename
            if path in self._handled_rename_old_paths:
                continue

            local_file = local_files.get(path)
            remote_file = remote_files.get(path)

            decision = self._compare_single_file(
                path, local_file, remote_file, local_files, remote_files
            )
            decisions.append(decision)

        return decisions

    def _detect_renames(
        self,
        local_files: dict[str, LocalFile],
        remote_files: dict[str, RemoteFile],
    ) -> None:
        """Detect renames by comparing current files with previous state.

        A rename is detected when:
        - Local: A file_id exists in the previous local tree at path A,
                 but now exists at path B in current local files
        - Remote: An id exists in the previous remote tree at path A,
                  but now exists at path B in current remote files

        Args:
            local_files: Current local files
            remote_files: Current remote files
        """
        # Detect local renames (same file_id, different path)
        if self.previous_local_tree:
            # Build current file_id -> path mapping
            current_local_by_file_id = {
                f.file_id: f.relative_path for f in local_files.values()
            }

            for file_id, prev_local_state in self.previous_local_tree.file_ids.items():
                if file_id in current_local_by_file_id:
                    current_path = current_local_by_file_id[file_id]
                    prev_path = prev_local_state.path
                    if current_path != prev_path:
                        # Local rename detected!
                        self._local_renames[file_id] = current_path

        # Detect remote renames (same id, different path)
        if self.previous_remote_tree:
            # Build current id -> path mapping
            current_remote_by_id = {
                f.id: f.relative_path for f in remote_files.values()
            }

            for remote_id, prev_remote_state in self.previous_remote_tree.ids.items():
                if remote_id in current_remote_by_id:
                    current_path = current_remote_by_id[remote_id]
                    prev_path = prev_remote_state.path
                    if current_path != prev_path:
                        # Remote rename detected!
                        self._remote_renames[remote_id] = current_path

    def _compare_single_file(
        self,
        path: str,
        local_file: Optional[LocalFile],
        remote_file: Optional[RemoteFile],
        local_files: Optional[dict[str, LocalFile]] = None,
        remote_files: Optional[dict[str, RemoteFile]] = None,
    ) -> SyncDecision:
        """Compare a single file and determine action.

        Args:
            path: Relative path of the file
            local_file: Local file (if exists)
            remote_file: Remote file (if exists)
            local_files: All local files (for rename lookup)
            remote_files: All remote files (for rename lookup)

        Returns:
            SyncDecision for this file
        """
        # Case 1: File exists in both locations
        if local_file and remote_file:
            return self._compare_existing_files(path, local_file, remote_file)

        # Case 2: File only exists locally
        if local_file and not remote_file:
            return self._handle_local_only(
                path, local_file, local_files or {}, remote_files or {}
            )

        # Case 3: File only exists remotely
        if remote_file and not local_file:
            return self._handle_remote_only(
                path, remote_file, local_files or {}, remote_files or {}
            )

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
        """Compare files that exist in both locations.

        Comparison logic:
        1. First compare sizes - if different, files are definitely different
        2. If sizes match, compare hashes if available (remote has hash)
        3. If hash not available, fall back to mtime comparison
        """
        # Check if files are identical by size first (quick check)
        if local_file.size == remote_file.size:
            # Sizes match - check hash if available for content verification
            # Remote files always have a hash from the API
            if remote_file.hash:
                # For now, we skip hash verification on download as computing
                # local hash is expensive. The size check is usually sufficient.
                # TODO: Add optional hash verification flag
                pass

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

    def _handle_local_only(
        self,
        path: str,
        local_file: LocalFile,
        local_files: dict[str, LocalFile],
        remote_files: dict[str, RemoteFile],
    ) -> SyncDecision:
        """Handle file that only exists locally.

        For TWO_WAY mode with previous state:
        - If file was previously synced, it was deleted from remote -> delete local
        - If file was NOT previously synced, it's a new file -> upload
        - If file_id matches a renamed file in remote, it's a rename -> rename remote

        Args:
            path: Current path of the file
            local_file: Local file object
            local_files: All current local files
            remote_files: All current remote files
        """
        # Check if this is a local rename that needs to be propagated to remote
        if (
            self.sync_mode == SyncMode.TWO_WAY
            and local_file.file_id in self._local_renames
            and self.previous_local_tree
        ):
            prev_state = self.previous_local_tree.get_by_file_id(local_file.file_id)
            if prev_state and prev_state.path != path:
                old_path = prev_state.path
                # Check if old path still exists in remote
                if old_path in remote_files:
                    # Mark old path as handled
                    self._handled_rename_old_paths.add(old_path)
                    return SyncDecision(
                        action=SyncAction.RENAME_REMOTE,
                        reason=f"Local file renamed from '{old_path}' to '{path}'",
                        local_file=local_file,
                        remote_file=remote_files[old_path],
                        relative_path=path,
                        old_path=old_path,
                        new_path=path,
                    )

        # For TWO_WAY mode, use previous state to determine action
        if self.sync_mode == SyncMode.TWO_WAY and self.previous_synced_files:
            if path in self.previous_synced_files:
                # File was synced before but now only exists locally
                # This means it was deleted from remote -> delete local
                return SyncDecision(
                    action=SyncAction.DELETE_LOCAL,
                    reason="File deleted from cloud (was previously synced)",
                    local_file=local_file,
                    remote_file=None,
                    relative_path=path,
                )
            # else: File was not synced before, treat as new local file

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

    def _handle_remote_only(
        self,
        path: str,
        remote_file: RemoteFile,
        local_files: dict[str, LocalFile],
        remote_files: dict[str, RemoteFile],
    ) -> SyncDecision:
        """Handle file that only exists remotely.

        For TWO_WAY mode with previous state:
        - If file was previously synced, it was deleted locally -> delete remote
        - If file was NOT previously synced, it's a new file -> download
        - If id matches a renamed file locally, it's a rename -> rename local

        Args:
            path: Current path of the file
            remote_file: Remote file object
            local_files: All current local files
            remote_files: All current remote files
        """
        # Check if this is a remote rename that needs to be propagated to local
        if (
            self.sync_mode == SyncMode.TWO_WAY
            and remote_file.id in self._remote_renames
            and self.previous_remote_tree
        ):
            prev_state = self.previous_remote_tree.get_by_id(remote_file.id)
            if prev_state and prev_state.path != path:
                old_path = prev_state.path
                # Check if old path still exists in local
                if old_path in local_files:
                    # Mark old path as handled
                    self._handled_rename_old_paths.add(old_path)
                    return SyncDecision(
                        action=SyncAction.RENAME_LOCAL,
                        reason=f"Remote file renamed from '{old_path}' to '{path}'",
                        local_file=local_files[old_path],
                        remote_file=remote_file,
                        relative_path=path,
                        old_path=old_path,
                        new_path=path,
                    )

        # For TWO_WAY mode, use previous state to determine action
        if self.sync_mode == SyncMode.TWO_WAY and self.previous_synced_files:
            if path in self.previous_synced_files:
                # File was synced before but now only exists remotely
                # This means it was deleted locally -> delete remote
                return SyncDecision(
                    action=SyncAction.DELETE_REMOTE,
                    reason="File deleted locally (was previously synced)",
                    local_file=None,
                    remote_file=remote_file,
                    relative_path=path,
                )
            # else: File was not synced before, treat as new remote file

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
