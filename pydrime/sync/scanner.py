"""Directory scanning utilities for sync operations."""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from stat import S_ISREG
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

    @classmethod
    def from_dir_entry(
        cls, entry: os.DirEntry, base_path: Path, stat_result: os.stat_result
    ) -> "LocalFile":
        """Create LocalFile from an os.DirEntry with pre-fetched stat.

        This is more efficient than from_path() because os.scandir() provides
        cached stat information on most platforms, avoiding extra syscalls.

        Args:
            entry: Directory entry from os.scandir()
            base_path: Base path for calculating relative paths
            stat_result: Pre-fetched stat result (cached or explicit)

        Returns:
            LocalFile instance
        """
        file_path = Path(entry.path)
        # Use as_posix() to ensure forward slashes on all platforms
        relative_path = file_path.relative_to(base_path).as_posix()

        # Get creation time if available (platform-dependent)
        creation_time: Optional[float] = None
        stat_any: Any = stat_result
        if hasattr(stat_any, "st_birthtime"):
            creation_time = stat_any.st_birthtime

        return cls(
            path=file_path,
            relative_path=relative_path,
            size=stat_result.st_size,
            mtime=stat_result.st_mtime,
            file_id=stat_result.st_ino,
            creation_time=creation_time,
        )

    @classmethod
    def from_scandir_fast(
        cls,
        entry: os.DirEntry,
        base_path_str: str,
        base_path_len: int,
    ) -> "LocalFile":
        """Create LocalFile from os.DirEntry with minimal overhead.

        This is the fastest path - uses string operations instead of Path objects
        where possible, and uses cached stat from scandir.

        Args:
            entry: Directory entry from os.scandir()
            base_path_str: Base path as string (with trailing separator)
            base_path_len: Length of base_path_str for slicing

        Returns:
            LocalFile instance
        """
        # Get stat - cached on most platforms from scandir
        stat_result = entry.stat(follow_symlinks=False)

        # Calculate relative path using string slicing (faster than Path operations)
        # entry.path is absolute, remove base_path prefix
        rel_path = entry.path[base_path_len:]
        # Convert backslashes to forward slashes for cross-platform compatibility
        if os.sep != "/":
            rel_path = rel_path.replace(os.sep, "/")

        # Get creation time if available (platform-dependent)
        creation_time: Optional[float] = None
        stat_any: Any = stat_result
        if hasattr(stat_any, "st_birthtime"):
            creation_time = stat_any.st_birthtime

        return cls(
            path=Path(entry.path),
            relative_path=rel_path,
            size=stat_result.st_size,
            mtime=stat_result.st_mtime,
            file_id=stat_result.st_ino,
            creation_time=creation_time,
        )

    @classmethod
    def from_stat_fast(
        cls,
        abs_path_str: str,
        relative_path: str,
        stat_result: os.stat_result,
    ) -> "LocalFile":
        """Create LocalFile from pre-computed path strings and stat.

        This is the fastest path for os.walk() based scanning - all path
        computations are done externally with string operations.

        Args:
            abs_path_str: Absolute path as string
            relative_path: Pre-computed relative path (forward slashes)
            stat_result: Pre-fetched stat result

        Returns:
            LocalFile instance
        """
        # Get creation time if available (platform-dependent)
        creation_time: Optional[float] = None
        stat_any: Any = stat_result
        if hasattr(stat_any, "st_birthtime"):
            creation_time = stat_any.st_birthtime

        return cls(
            path=Path(abs_path_str),
            relative_path=relative_path,
            size=stat_result.st_size,
            mtime=stat_result.st_mtime,
            file_id=stat_result.st_ino,
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
        return self._should_ignore_name(path.name, path, base_path, is_dir)

    def _should_ignore_name(
        self,
        name: str,
        path: Path,
        base_path: Path,
        is_dir: bool = False,
    ) -> bool:
        """Check if a path should be ignored based on patterns.

        Internal method that takes name separately to avoid extra Path operations.

        Args:
            name: File or directory name
            path: Full path to check
            base_path: Base path for relative path calculation
            is_dir: Whether the path is a directory

        Returns:
            True if path should be ignored
        """
        # Never ignore the ignore file itself from scanning perspective
        # (but it won't be synced as it starts with .)
        if name == IGNORE_FILE_NAME:
            return True

        # Check dot files (but allow .pydrignore to be read)
        if self.exclude_dot_files and name.startswith("."):
            return True

        # Check using ignore manager if available
        if self._ignore_manager is not None:
            relative_path = path.relative_to(base_path).as_posix()
            if self._ignore_manager.is_ignored(relative_path, is_dir=is_dir):
                logger.debug(f"Ignoring (from rules): {relative_path}")
                return True

        return False

    def _should_ignore_fast(
        self,
        name: str,
        relative_path: str,
        is_dir: bool = False,
    ) -> bool:
        """Fast ignore check using pre-computed relative path string.

        This is the optimized version that avoids Path operations entirely.

        Args:
            name: File or directory name
            relative_path: Pre-computed relative path (forward slashes)
            is_dir: Whether the path is a directory

        Returns:
            True if path should be ignored
        """
        # Never ignore the ignore file itself from scanning perspective
        if name == IGNORE_FILE_NAME:
            return True

        # Check dot files
        if self.exclude_dot_files and name.startswith("."):
            return True

        # Check using ignore manager if available
        if self._ignore_manager is not None:
            if self._ignore_manager.is_ignored(relative_path, is_dir=is_dir):
                return True

        return False

    def scan_local(
        self, directory: Path, base_path: Optional[Path] = None
    ) -> list[LocalFile]:
        """Recursively scan a local directory.

        This method scans a directory tree using os.walk() for efficient
        traversal. It loads .pydrignore files from each directory and
        applies their rules hierarchically.

        Uses os.walk() with optimized string operations for maximum performance
        on large directory trees.

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

        # Use fast path with os.walk() and string operations
        return self._scan_local_fast(directory, base_path)

    def _scan_local_fast(self, directory: Path, base_path: Path) -> list[LocalFile]:
        """Fast local directory scan using os.walk() and string operations.

        This method minimizes object creation and uses string operations
        instead of Path objects where possible for maximum performance.

        Args:
            directory: Directory to scan
            base_path: Base path for calculating relative paths

        Returns:
            List of LocalFile objects
        """
        files: list[LocalFile] = []

        # Convert to string once and prepare for path calculations
        base_path_str = str(base_path)
        if not base_path_str.endswith(os.sep):
            base_path_str += os.sep
        base_len = len(base_path_str)

        # Use forward slash for cross-platform compatibility
        use_posix = os.sep != "/"

        # Track directories to skip (ignored directories)
        # os.walk with topdown=True allows modifying dirs in-place to skip subtrees

        try:
            for dirpath, dirnames, filenames in os.walk(
                str(directory), topdown=True, followlinks=False
            ):
                # Calculate relative path for this directory
                if len(dirpath) > base_len:
                    dir_rel_path = dirpath[base_len:]
                    if use_posix:
                        dir_rel_path = dir_rel_path.replace(os.sep, "/")
                else:
                    dir_rel_path = ""

                # Load .pydrignore from this directory if enabled
                if self.use_ignore_files and self._ignore_manager is not None:
                    ignore_file = os.path.join(dirpath, IGNORE_FILE_NAME)
                    if os.path.isfile(ignore_file):
                        self._ignore_manager.load_from_file_path(ignore_file)

                # Filter directories in-place to skip ignored ones
                # This prevents os.walk from descending into them
                dirs_to_remove = []
                for dirname in dirnames:
                    # Build relative path for this directory
                    if dir_rel_path:
                        subdir_rel = f"{dir_rel_path}/{dirname}"
                    else:
                        subdir_rel = dirname

                    if self._should_ignore_fast(dirname, subdir_rel, is_dir=True):
                        dirs_to_remove.append(dirname)

                # Remove ignored directories (modifying in place)
                for d in dirs_to_remove:
                    dirnames.remove(d)

                # Process files in this directory
                for filename in filenames:
                    # Skip .pydrignore files
                    if filename == IGNORE_FILE_NAME:
                        continue

                    # Build relative path
                    if dir_rel_path:
                        file_rel_path = f"{dir_rel_path}/{filename}"
                    else:
                        file_rel_path = filename

                    # Check if file should be ignored
                    if self._should_ignore_fast(filename, file_rel_path, is_dir=False):
                        continue

                    # Build absolute path and get stat
                    abs_path = os.path.join(dirpath, filename)
                    try:
                        stat_result = os.stat(abs_path, follow_symlinks=False)
                        # Only include regular files
                        if not S_ISREG(stat_result.st_mode):
                            continue

                        local_file = LocalFile.from_stat_fast(
                            abs_path, file_rel_path, stat_result
                        )
                        files.append(local_file)
                    except (OSError, PermissionError):
                        # Skip files we can't stat
                        continue

        except PermissionError:
            # Skip directories we can't read
            pass

        return files

    def _scan_local_recursive(
        self, directory: Path, base_path: Path
    ) -> list[LocalFile]:
        """Original recursive scan using os.scandir().

        Kept for compatibility and cases where os.walk() behavior differs.

        Args:
            directory: Directory to scan
            base_path: Base path for calculating relative paths

        Returns:
            List of LocalFile objects
        """
        files: list[LocalFile] = []

        try:
            # Load .pydrignore from this directory if it exists
            if self.use_ignore_files and self._ignore_manager is not None:
                self._ignore_manager.load_from_directory(directory)

            # Use os.scandir() for efficient directory scanning with cached stat
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        # Get cached stat info - on most platforms (Linux, Windows),
                        # this uses cached info from the directory listing itself.
                        is_dir = entry.is_dir(follow_symlinks=False)
                        is_file = entry.is_file(follow_symlinks=False)

                        # Check if should be ignored using the entry name directly
                        item_path = Path(entry.path)
                        if self._should_ignore_name(
                            entry.name, item_path, base_path, is_dir=is_dir
                        ):
                            continue

                        if is_file:
                            try:
                                # Get stat result - cached on most platforms
                                stat_result = entry.stat(follow_symlinks=False)
                                local_file = LocalFile.from_dir_entry(
                                    entry, base_path, stat_result
                                )
                                files.append(local_file)
                            except (OSError, PermissionError):
                                # Skip files we can't read
                                continue
                        elif is_dir:
                            # Recursively scan subdirectories
                            files.extend(
                                self._scan_local_recursive(item_path, base_path)
                            )
                    except (OSError, PermissionError):
                        # Skip entries we can't access
                        continue
        except PermissionError:
            # Skip directories we can't read
            pass

        return files

    def scan_local_single_level(
        self,
        directory: Path,
        base_path: Optional[Path] = None,
    ) -> tuple[list[LocalFile], list[str]]:
        """Scan a single level of a local directory (non-recursive).

        This method is optimized for incremental sync - it returns both files
        and subdirectory names without descending into subdirectories.
        This allows the caller to decide which subdirectories to traverse
        based on comparison with remote state.

        Args:
            directory: Directory to scan
            base_path: Base path for calculating relative paths (defaults to directory)

        Returns:
            Tuple of (list of LocalFile objects, list of subdirectory relative paths)

        Examples:
            >>> scanner = DirectoryScanner()
            >>> files, subdirs = scanner.scan_local_single_level(Path("/sync"))
            >>> # files contains LocalFile objects for files in /sync
            >>> # subdirs contains relative paths like "subdir1", "subdir2"
        """
        # Initialize ignore manager only on first call (when scanning root directory)
        # or when base_path changes
        if base_path is None:
            base_path = directory

        # Initialize ignore manager if not already initialized or if base_path changed
        if self._ignore_manager is None or (
            self._ignore_manager.base_path is not None
            and self._ignore_manager.base_path != base_path
        ):
            if self.use_ignore_files:
                self._ignore_manager = self._init_ignore_manager(base_path)
            elif self.ignore_patterns:
                # Still need to apply CLI patterns even without ignore files
                self._ignore_manager = IgnoreFileManager(base_path=base_path)
                self._ignore_manager.load_cli_patterns(self.ignore_patterns)

        files: list[LocalFile] = []
        subdirs: list[str] = []

        # Convert to strings for fast path operations
        base_path_str = str(base_path)
        if not base_path_str.endswith(os.sep):
            base_path_str += os.sep
        base_len = len(base_path_str)
        use_posix = os.sep != "/"

        # Calculate relative path for this directory
        dir_str = str(directory)
        if len(dir_str) > base_len:
            dir_rel_path = dir_str[base_len:]
            if use_posix:
                dir_rel_path = dir_rel_path.replace(os.sep, "/")
        else:
            dir_rel_path = ""

        try:
            # Load .pydrignore from this directory if enabled
            if self.use_ignore_files and self._ignore_manager is not None:
                ignore_file = os.path.join(dir_str, IGNORE_FILE_NAME)
                if os.path.isfile(ignore_file):
                    self._ignore_manager.load_from_file_path(ignore_file)

            # Use os.scandir() for efficient single-level scan
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        name = entry.name

                        # Skip .pydrignore files
                        if name == IGNORE_FILE_NAME:
                            continue

                        # Build relative path
                        if dir_rel_path:
                            rel_path = f"{dir_rel_path}/{name}"
                        else:
                            rel_path = name

                        is_dir = entry.is_dir(follow_symlinks=False)
                        is_file = entry.is_file(follow_symlinks=False)

                        # Check if should be ignored
                        if self._should_ignore_fast(name, rel_path, is_dir=is_dir):
                            continue

                        if is_file:
                            try:
                                stat_result = entry.stat(follow_symlinks=False)
                                if S_ISREG(stat_result.st_mode):
                                    local_file = LocalFile.from_stat_fast(
                                        entry.path, rel_path, stat_result
                                    )
                                    files.append(local_file)
                            except (OSError, PermissionError):
                                continue
                        elif is_dir:
                            # Add subdirectory relative path for caller to process
                            subdirs.append(rel_path)

                    except (OSError, PermissionError):
                        continue

        except PermissionError:
            pass

        return files, subdirs

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
