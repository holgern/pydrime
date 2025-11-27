"""Ignore file handling for sync operations (.pydrignore files).

This module implements gitignore-style pattern matching similar to Kopia's
.kopiaignore files. It supports hierarchical ignore files where .pydrignore
files in subdirectories apply only to that directory and its descendants.

Supported pattern syntax:
    # Comment lines start with #
    ! Negates a rule (un-ignores a previously ignored path)
    * Wildcard matching any characters (except /)
    ** Double wildcard matching any characters including /
    ? Matches exactly one character
    [abc] Matches one of a, b, or c
    [a-z] Matches characters in range a-z
    / at start: matches only at root of the sync directory
    / at end: matches only directories

Examples:
    *.log           # Ignore all .log files anywhere
    /logs           # Ignore 'logs' only at root directory
    temp/           # Ignore directories named 'temp'
    !important.log  # Don't ignore important.log (negation)
    *.db*           # Ignore files with .db anywhere in extension
    **/cache/**     # Ignore any 'cache' directory and its contents
"""

import fnmatch
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default ignore file name
IGNORE_FILE_NAME = ".pydrignore"

# Local trash directory name (always excluded from sync)
LOCAL_TRASH_DIR_NAME = ".pydrime.trash.local"

# Default patterns that are always ignored
DEFAULT_IGNORE_PATTERNS = [
    LOCAL_TRASH_DIR_NAME,  # Local trash directory for deleted files
]


@dataclass
class IgnoreRule:
    """Represents a single ignore rule from a .pydrignore file."""

    pattern: str
    """Original pattern from the ignore file"""

    negated: bool = False
    """If True, this rule un-ignores (whitelists) matching paths"""

    anchored: bool = False
    """If True, pattern only matches at the root (started with /)"""

    dir_only: bool = False
    """If True, pattern only matches directories (ended with /)"""

    source_path: Optional[Path] = None
    """Path to the .pydrignore file that contains this rule"""

    def __post_init__(self) -> None:
        """Process the pattern to set flags and normalize."""
        pattern = self.pattern.strip()

        # Check for negation
        if pattern.startswith("!"):
            self.negated = True
            pattern = pattern[1:]

        # Check if anchored (starts with /)
        if pattern.startswith("/"):
            self.anchored = True
            pattern = pattern[1:]

        # Check if directory-only (ends with /)
        if pattern.endswith("/"):
            self.dir_only = True
            pattern = pattern[:-1]

        # Normalize the pattern
        self.pattern = pattern

    def matches(self, relative_path: str, is_dir: bool = False) -> bool:
        """Check if this rule matches a given path.

        Args:
            relative_path: Path relative to the .pydrignore file's directory
            is_dir: Whether the path is a directory

        Returns:
            True if the rule matches the path
        """
        # Directory-only patterns don't match files
        if self.dir_only and not is_dir:
            return False

        # Normalize path separators
        path = relative_path.replace("\\", "/")

        # Get just the filename for non-anchored, non-path patterns
        filename = path.rsplit("/", 1)[-1] if "/" in path else path

        # Convert pattern to regex for matching
        return self._pattern_matches(path, filename)

    def _pattern_matches(self, full_path: str, filename: str) -> bool:
        """Check if pattern matches using gitignore-style rules.

        Args:
            full_path: Full relative path
            filename: Just the filename component

        Returns:
            True if pattern matches
        """
        pattern = self.pattern

        # Handle ** (match any path components)
        if "**" in pattern:
            # Convert ** to regex pattern
            regex = self._pattern_to_regex(pattern)
            return bool(re.match(regex, full_path))

        # If pattern contains /, it should match the full path
        if "/" in pattern:
            if self.anchored:
                # Anchored pattern: must match from start
                return self._fnmatch_path(full_path, pattern)
            else:
                # Non-anchored with /: can match anywhere in path
                # Try matching from each path component
                parts = full_path.split("/")
                for i in range(len(parts)):
                    subpath = "/".join(parts[i:])
                    if self._fnmatch_path(subpath, pattern):
                        return True
                return False
        else:
            # Simple pattern without /
            if self.anchored:
                # Anchored: only match first path component
                first_component = full_path.split("/")[0]
                return fnmatch.fnmatchcase(first_component, pattern)
            else:
                # Non-anchored: match filename anywhere in tree
                # Also try matching each path component
                parts = full_path.split("/")
                for part in parts:
                    if fnmatch.fnmatchcase(part, pattern):
                        return True
                return False

    def _fnmatch_path(self, path: str, pattern: str) -> bool:
        """Match a path against a pattern using fnmatch.

        Args:
            path: Path to match
            pattern: Pattern to match against

        Returns:
            True if path matches pattern
        """
        return fnmatch.fnmatchcase(path, pattern)

    def _pattern_to_regex(self, pattern: str) -> str:
        """Convert a gitignore-style pattern to a regex.

        Args:
            pattern: Gitignore-style pattern with ** support

        Returns:
            Regex pattern string
        """
        # Escape special regex characters except our wildcards
        result = ""
        i = 0
        while i < len(pattern):
            c = pattern[i]
            if c == "*":
                if i + 1 < len(pattern) and pattern[i + 1] == "*":
                    # ** matches any path
                    if i + 2 < len(pattern) and pattern[i + 2] == "/":
                        # **/ at start or middle
                        result += r"(?:.*/)?"
                        i += 3
                        continue
                    else:
                        # ** at end or alone
                        result += r".*"
                        i += 2
                        continue
                else:
                    # Single * matches anything except /
                    result += r"[^/]*"
            elif c == "?":
                result += r"[^/]"
            elif c == "[":
                # Character class - find the closing ]
                j = i + 1
                if j < len(pattern) and pattern[j] in "!^":
                    j += 1
                if j < len(pattern) and pattern[j] == "]":
                    j += 1
                while j < len(pattern) and pattern[j] != "]":
                    j += 1
                result += pattern[i : j + 1]
                i = j
            elif c in r"\.^$+{}()|":
                result += "\\" + c
            else:
                result += c
            i += 1

        return "^" + result + "$"


@dataclass
class IgnoreFileManager:
    """Manages .pydrignore files for a sync operation.

    This class handles loading, caching, and checking ignore rules from
    .pydrignore files. It supports hierarchical ignore files where rules
    in subdirectories only apply to that subtree.

    The manager caches ignore results to avoid rechecking the same paths
    repeatedly, which significantly improves performance for large directory
    trees. The cache is automatically invalidated when new rules are loaded.

    Examples:
        >>> manager = IgnoreFileManager()
        >>> manager.load_from_directory(Path("/sync/root"))
        >>> if manager.is_ignored("logs/debug.log"):
        ...     print("File is ignored")
    """

    base_path: Optional[Path] = None
    """Base path of the sync operation (root directory)"""

    rules: list[IgnoreRule] = field(default_factory=list)
    """List of ignore rules loaded from all .pydrignore files"""

    _loaded_files: set[Path] = field(default_factory=set)
    """Set of .pydrignore files that have been loaded"""

    # Add CLI-provided patterns
    cli_patterns: list[str] = field(default_factory=list)
    """Patterns provided via CLI --ignore option"""

    # Cache for ignore results: (path, is_dir) -> ignored
    _ignore_cache: dict[tuple[str, bool], bool] = field(default_factory=dict)
    """Cache mapping (relative_path, is_dir) to ignore result"""

    def __post_init__(self) -> None:
        """Initialize with default ignore patterns."""
        # Load default patterns that are always ignored
        for pattern in DEFAULT_IGNORE_PATTERNS:
            rule = IgnoreRule(pattern=pattern)
            self.rules.append(rule)
        # Initialize cache
        self._ignore_cache = {}

    def _invalidate_cache(self) -> None:
        """Invalidate the ignore result cache.

        Called when rules change (new patterns loaded or cleared).
        """
        self._ignore_cache.clear()

    def load_cli_patterns(self, patterns: list[str]) -> None:
        """Load ignore patterns from CLI arguments.

        Args:
            patterns: List of glob patterns from CLI --ignore option
        """
        self.cli_patterns = patterns
        for pattern in patterns:
            if pattern.strip() and not pattern.strip().startswith("#"):
                rule = IgnoreRule(pattern=pattern.strip())
                self.rules.append(rule)
        # Invalidate cache when rules change
        self._invalidate_cache()

    def load_from_directory(self, directory: Path) -> None:
        """Load .pydrignore file from a directory if it exists.

        This method loads the .pydrignore file from the specified directory
        and adds its rules to the manager. Call this for each directory
        being scanned to support hierarchical ignore files.

        Args:
            directory: Directory to check for .pydrignore file
        """
        if self.base_path is None:
            self.base_path = directory

        ignore_file = directory / IGNORE_FILE_NAME
        if ignore_file.exists() and ignore_file not in self._loaded_files:
            self._load_ignore_file(ignore_file)
            self._loaded_files.add(ignore_file)
            # Invalidate cache when new rules are loaded
            self._invalidate_cache()

    def load_from_file_path(self, filepath_str: str) -> None:
        """Load .pydrignore file from a string path if not already loaded.

        This is an optimized version that avoids Path object creation
        for the file existence check when the caller already knows
        the file exists.

        Args:
            filepath_str: String path to the .pydrignore file
        """
        filepath = Path(filepath_str)
        if filepath not in self._loaded_files:
            self._load_ignore_file(filepath)
            self._loaded_files.add(filepath)
            # Invalidate cache when new rules are loaded
            self._invalidate_cache()

    def _load_ignore_file(self, filepath: Path) -> None:
        """Load rules from a .pydrignore file.

        Args:
            filepath: Path to the .pydrignore file
        """
        try:
            content = filepath.read_text(encoding="utf-8")
            logger.debug(f"Loading ignore file: {filepath}")

            for line_num, line in enumerate(content.splitlines(), 1):
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue

                try:
                    rule = IgnoreRule(pattern=line, source_path=filepath)
                    self.rules.append(rule)
                    logger.debug(
                        f"  Rule {line_num}: '{line}' "
                        f"(negated={rule.negated}, anchored={rule.anchored})"
                    )
                except Exception as e:
                    logger.warning(
                        f"Invalid pattern at {filepath}:{line_num}: {line} ({e})"
                    )

        except OSError as e:
            logger.warning(f"Cannot read ignore file {filepath}: {e}")

    def is_ignored(
        self,
        relative_path: str,
        is_dir: bool = False,
        check_parents: bool = True,
    ) -> bool:
        """Check if a path should be ignored.

        This method evaluates all rules in order, with later rules
        (including negations) taking precedence. Results are cached
        to avoid rechecking the same paths repeatedly.

        Args:
            relative_path: Path relative to base_path
            is_dir: Whether the path is a directory
            check_parents: If True, also check if any parent directory is ignored

        Returns:
            True if the path should be ignored
        """
        # Normalize path
        path = relative_path.replace("\\", "/").strip("/")

        if not path:
            return False

        # Check cache first (only for the exact path, not parents)
        cache_key = (path, is_dir)
        if cache_key in self._ignore_cache:
            return self._ignore_cache[cache_key]

        # Check if any parent directory is ignored (unless checking a subdir rule)
        if check_parents:
            parts = path.split("/")
            for i in range(1, len(parts)):
                parent_path = "/".join(parts[:i])
                # Check parent with caching (recursively)
                if self.is_ignored(parent_path, is_dir=True, check_parents=False):
                    # Cache the result for this path too
                    self._ignore_cache[cache_key] = True
                    return True

        # Check the path itself
        result = self._check_rules(path, is_dir)

        # Cache the result
        self._ignore_cache[cache_key] = result

        return result

    def _check_rules(self, relative_path: str, is_dir: bool) -> bool:
        """Check rules against a specific path.

        Args:
            relative_path: Path to check
            is_dir: Whether path is a directory

        Returns:
            True if path is ignored
        """
        ignored = False

        for rule in self.rules:
            # Calculate the path relative to the rule's source file
            if rule.source_path and self.base_path:
                rule_dir = rule.source_path.parent
                if rule_dir != self.base_path:
                    # Rule is from a subdirectory, adjust relative path
                    try:
                        rule_rel_dir = rule_dir.relative_to(self.base_path).as_posix()
                        if not relative_path.startswith(rule_rel_dir + "/"):
                            # Path is not under this rule's directory
                            continue
                        # Adjust path to be relative to rule's directory
                        check_path = relative_path[len(rule_rel_dir) + 1 :]
                    except ValueError:
                        continue
                else:
                    check_path = relative_path
            else:
                check_path = relative_path

            if rule.matches(check_path, is_dir):
                # Later rules override earlier ones
                ignored = not rule.negated

        return ignored

    def get_effective_rules(self) -> list[IgnoreRule]:
        """Get all currently loaded rules.

        Returns:
            List of all ignore rules
        """
        return list(self.rules)

    def clear(self) -> None:
        """Clear all loaded rules and reset state.

        Note: This also clears default patterns. Call __post_init__ to reload them.
        """
        self.rules.clear()
        self._loaded_files.clear()
        self.cli_patterns.clear()
        self.base_path = None
        # Reload default patterns
        self.__post_init__()


def load_ignore_file(filepath: Path) -> list[str]:
    """Load patterns from a .pydrignore file.

    This is a convenience function for loading patterns from a single
    ignore file without using the full IgnoreFileManager.

    Args:
        filepath: Path to the .pydrignore file

    Returns:
        List of pattern strings (comments and empty lines excluded)
    """
    patterns: list[str] = []

    try:
        content = filepath.read_text(encoding="utf-8")
        for line in content.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                patterns.append(line)
    except OSError:
        pass

    return patterns
