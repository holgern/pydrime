"""Helper functions shared across CLI commands."""

from pathlib import Path
from typing import Optional

from ..output import OutputFormatter


def scan_directory(
    path: Path, base_path: Path, out: OutputFormatter
) -> list[tuple[Path, str]]:
    """Recursively scan directory and return list of (file_path, relative_path) tuples.

    Args:
        path: Directory to scan
        base_path: Base path for calculating relative paths
        out: Output formatter for warnings

    Returns:
        List of tuples containing file paths and their relative paths
        (paths use forward slashes for cross-platform compatibility)
    """
    files = []

    try:
        for item in path.iterdir():
            if item.is_file():
                # Use as_posix() to ensure forward slashes on all platforms
                relative_path = item.relative_to(base_path).as_posix()
                files.append((item, relative_path))
            elif item.is_dir():
                files.extend(scan_directory(item, base_path, out))
    except PermissionError as e:
        out.warning(f"Permission denied: {e}")

    return files


# Environment variable name for vault password (in-memory only, not stored to disk)
VAULT_PASSWORD_ENV_VAR = "PYDRIME_VAULT_PASSWORD"


def get_vault_password_from_env() -> Optional[str]:
    """Get vault password from environment variable if set."""
    import os

    return os.environ.get(VAULT_PASSWORD_ENV_VAR)


def _load_htpasswd_for_server(
    htpasswd_path: Path, out: OutputFormatter
) -> tuple[Optional[str], Optional[str]]:
    """Load username and password from .htpasswd file.

    Simplified implementation that reads the first valid entry.

    Args:
        htpasswd_path: Path to .htpasswd file
        out: Output formatter for warnings

    Returns:
        Tuple of (username, password_hash) or (None, None) if file doesn't exist
    """
    if not htpasswd_path.exists():
        out.warning(f"htpasswd file not found: {htpasswd_path}")
        return None, None

    try:
        content = htpasswd_path.read_text()
        lines = content.strip().split("\n")

        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if ":" in line:
                username, password_hash = line.split(":", 1)
                out.info(
                    "[yellow]Note: htpasswd authentication uses "
                    "simple comparison[/yellow]"
                )
                return username.strip(), password_hash.strip()

        return None, None
    except Exception as e:
        out.warning(f"Error reading .htpasswd file: {e}")
        return None, None
