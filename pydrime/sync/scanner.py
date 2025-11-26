"""Directory scanning utilities for sync operations."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ..models import FileEntry
from .ignore import IGNORE_FILE_NAME, IgnoreFileManager

logger = logging.getLogger(__name__)


@dataclass
class LocalFile:
    """Represents a local file with metadata."""

    path: Path
    """Absolute path to the file"""

    relative_path: str
    """Relative path (using forward slashes for cross-platform compatibility)"""

    size: int
    """File size in bytes"""

    mtime: float
    """Last modification time (Unix timestamp)"""

    file_id: int = 0
    """Filesystem file identifier (inode on Unix, file index on Windows).

    This value persists across renames on most filesystems, enabling
    rename detection by tracking the same file_id at a different path.
    """

    creation_time: Optional[float] = None
    """Creation time (Unix timestamp) if available"""

    @classmethod
    def from_path(cls, file_path: Path, base_path: Path) -> "LocalFile":
        """Create LocalFile from a path.

        Args:
            file_path: Absolute path to the file
            base_path: Base path for calculating relative paths

        Returns:
            LocalFile instance
        """
        stat = file_path.stat()
        # Use as_posix() to ensure forward slashes on all platforms
        relative_path = file_path.relative_to(base_path).as_posix()

        # Get creation time if available (platform-dependent)
        creation_time: Optional[float] = None
        # st_birthtime on macOS, st_ctime on Linux (though ctime is change time)
        stat_any: Any = stat  # Cast to Any to access platform-specific attributes
        if hasattr(stat_any, "st_birthtime"):
            creation_time = stat_any.st_birthtime

        return cls(
            path=file_path,
            relative_path=relative_path,
            size=stat.st_size,
            mtime=stat.st_mtime,
            file_id=stat.st_ino,
            creation_time=creation_time,
        )


@dataclass
class RemoteFile:
    """Represents a remote file with metadata."""

    entry: FileEntry
    """Remote file entry from API"""

    relative_path: str
    """Relative path in the remote filesystem"""

    @property
    def size(self) -> int:
        """File size in bytes."""
        return self.entry.file_size

    @property
    def mtime(self) -> Optional[float]:
        """Last modification time (Unix timestamp)."""
        if self.entry.updated_at:
            # Parse ISO timestamp string to datetime then to Unix timestamp
            from datetime import datetime

            try:
                # Handle various ISO formats
                timestamp_str = self.entry.updated_at
                if timestamp_str.endswith("Z"):
                    timestamp_str = timestamp_str[:-1] + "+00:00"
                dt = datetime.fromisoformat(timestamp_str)
                return dt.timestamp()
            except (ValueError, AttributeError):
                return None
        return None

    @property
    def id(self) -> int:
        """Remote file entry ID."""
        return self.entry.id

    @property
    def uuid(self) -> int:
        """Remote file entry UUID (alias for id).

        In Drime API, the entry ID serves as the unique identifier
        that persists across renames, similar to file_id for local files.
        """
        return self.entry.id

    @property
    def hash(self) -> str:
        """Remote file entry hash (MD5)."""
        return self.entry.hash


class DirectoryScanner:
    """Scans directories and builds file lists.

    Supports .pydrignore files for gitignore-style pattern matching.
    When scanning a directory, any .pydrignore file in that directory
    or its subdirectories will be loaded and applied hierarchically.

    Examples:
        >>> scanner = DirectoryScanner()
        >>> files = scanner.scan_local(Path("/sync/folder"))
        >>> # Files matching patterns in .pydrignore are excluded

        >>> # With CLI patterns
        >>> scanner = DirectoryScanner(ignore_patterns=["*.tmp", "cache/*"])
        >>> files = scanner.scan_local(Path("/sync/folder"))
    """

    def __init__(
        self,
        ignore_patterns: Optional[list[str]] = None,
        exclude_dot_files: bool = False,
        use_ignore_files: bool = True,
    ):
        """Initialize directory scanner.

        Args:
            ignore_patterns: List of glob patterns to ignore (e.g., ["*.log", "temp/*"])
            exclude_dot_files: Whether to exclude files/folders starting with dot
            use_ignore_files: Whether to load .pydrignore files from directories
        """
        self.ignore_patterns = ignore_patterns or []
        self.exclude_dot_files = exclude_dot_files
        self.use_ignore_files = use_ignore_files
        self._ignore_manager: Optional[IgnoreFileManager] = None

    def _init_ignore_manager(self, base_path: Path) -> IgnoreFileManager:
        """Initialize the ignore file manager for a scan.

        Args:
            base_path: Root directory of the scan

        Returns:
            IgnoreFileManager instance
        """
        manager = IgnoreFileManager(base_path=base_path)

        # Load CLI patterns first (they apply globally)
        if self.ignore_patterns:
            manager.load_cli_patterns(self.ignore_patterns)

        return manager

    def should_ignore(
        self,
        path: Path,
        base_path: Path,
        is_dir: bool = False,
    ) -> bool:
        """Check if a path should be ignored based on patterns.

        Args:
            path: Path to check
            base_path: Base path for relative path calculation
            is_dir: Whether the path is a directory

        Returns:
            True if path should be ignored
        """
        # Never ignore the ignore file itself from scanning perspective
        # (but it won't be synced as it starts with .)
        if path.name == IGNORE_FILE_NAME:
            return True

        # Check dot files (but allow .pydrignore to be read)
        if self.exclude_dot_files and path.name.startswith("."):
            return True

        # Check using ignore manager if available
        if self._ignore_manager is not None:
            relative_path = path.relative_to(base_path).as_posix()
            if self._ignore_manager.is_ignored(relative_path, is_dir=is_dir):
                logger.debug(f"Ignoring (from rules): {relative_path}")
                return True

        return False

    def scan_local(
        self, directory: Path, base_path: Optional[Path] = None
    ) -> list[LocalFile]:
        """Recursively scan a local directory.

        This method scans a directory tree, loading .pydrignore files from
        each directory and applying their rules hierarchically. Rules from
        parent directories apply to all descendants, while rules from
        subdirectories only apply within that subtree.

        Args:
            directory: Directory to scan
            base_path: Base path for calculating relative paths (defaults to directory)

        Returns:
            List of LocalFile objects

        Examples:
            >>> scanner = DirectoryScanner()
            >>> files = scanner.scan_local(Path("/home/user/documents"))
            >>> for f in files:
            ...     print(f.relative_path)
        """
        if base_path is None:
            base_path = directory
            # Initialize ignore manager for new scan
            if self.use_ignore_files:
                self._ignore_manager = self._init_ignore_manager(base_path)
            else:
                self._ignore_manager = None
                # Still need to apply CLI patterns
                if self.ignore_patterns:
                    self._ignore_manager = IgnoreFileManager(base_path=base_path)
                    self._ignore_manager.load_cli_patterns(self.ignore_patterns)

        files: list[LocalFile] = []

        try:
            # Load .pydrignore from this directory if it exists
            if self.use_ignore_files and self._ignore_manager is not None:
                self._ignore_manager.load_from_directory(directory)

            for item in directory.iterdir():
                # Check if should be ignored
                is_dir = item.is_dir()
                if self.should_ignore(item, base_path, is_dir=is_dir):
                    continue

                if item.is_file():
                    try:
                        local_file = LocalFile.from_path(item, base_path)
                        files.append(local_file)
                    except (OSError, PermissionError):
                        # Skip files we can't read
                        continue
                elif is_dir:
                    # Recursively scan subdirectories
                    files.extend(self.scan_local(item, base_path))
        except PermissionError:
            # Skip directories we can't read
            pass

        return files

    def scan_remote(
        self, entries_with_paths: list[tuple[FileEntry, str]]
    ) -> list[RemoteFile]:
        """Process remote file entries into RemoteFile objects.

        Args:
            entries_with_paths: List of (FileEntry, relative_path) tuples from API

        Returns:
            List of RemoteFile objects
        """
        remote_files: list[RemoteFile] = []

        for entry, rel_path in entries_with_paths:
            # Only include files, not folders
            if entry.type != "folder":
                remote_file = RemoteFile(entry=entry, relative_path=rel_path)
                remote_files.append(remote_file)

        return remote_files
