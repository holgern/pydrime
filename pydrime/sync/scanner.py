"""Directory scanning utilities for sync operations."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ..models import FileEntry


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

        return cls(
            path=file_path,
            relative_path=relative_path,
            size=stat.st_size,
            mtime=stat.st_mtime,
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
    def hash(self) -> str:
        """Remote file entry hash."""
        return self.entry.hash


class DirectoryScanner:
    """Scans directories and builds file lists."""

    def __init__(
        self,
        ignore_patterns: Optional[list[str]] = None,
        exclude_dot_files: bool = False,
    ):
        """Initialize directory scanner.

        Args:
            ignore_patterns: List of glob patterns to ignore (e.g., ["*.log", "temp/*"])
            exclude_dot_files: Whether to exclude files/folders starting with dot
        """
        self.ignore_patterns = ignore_patterns or []
        self.exclude_dot_files = exclude_dot_files

    def should_ignore(self, path: Path, base_path: Path) -> bool:
        """Check if a path should be ignored based on patterns.

        Args:
            path: Path to check
            base_path: Base path for relative path calculation

        Returns:
            True if path should be ignored
        """
        # Check dot files
        if self.exclude_dot_files and path.name.startswith("."):
            return True

        # Check ignore patterns
        if self.ignore_patterns:
            import fnmatch

            relative_path = path.relative_to(base_path).as_posix()
            for pattern in self.ignore_patterns:
                if fnmatch.fnmatch(relative_path, pattern):
                    return True
                # Also match against just the filename
                if fnmatch.fnmatch(path.name, pattern):
                    return True

        return False

    def scan_local(
        self, directory: Path, base_path: Optional[Path] = None
    ) -> list[LocalFile]:
        """Recursively scan a local directory.

        Args:
            directory: Directory to scan
            base_path: Base path for calculating relative paths (defaults to directory)

        Returns:
            List of LocalFile objects
        """
        if base_path is None:
            base_path = directory

        files: list[LocalFile] = []

        try:
            for item in directory.iterdir():
                # Check if should be ignored
                if self.should_ignore(item, base_path):
                    continue

                if item.is_file():
                    try:
                        local_file = LocalFile.from_path(item, base_path)
                        files.append(local_file)
                    except (OSError, PermissionError):
                        # Skip files we can't read
                        continue
                elif item.is_dir():
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
