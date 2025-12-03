"""WebDAV server module for Drime Cloud.

This module provides a WebDAV server that exposes Drime Cloud storage,
allowing clients to mount cloud storage as a local drive.

Example usage:
    pydrime webdav --host 0.0.0.0 --port 8080
    pydrime webdav --username user --password secret
    pydrime webdav --readonly
"""

from typing import Any

# Default values that don't require wsgidav
DEFAULT_MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB
DEFAULT_CACHE_TTL = 30.0  # 30 seconds


def __getattr__(name: str) -> Any:
    """Lazy import to avoid circular import issues with wsgidav."""
    if name in ("DrimeDAVProvider", "DrimeCollection", "DrimeResource"):
        from .provider import DrimeCollection, DrimeDAVProvider, DrimeResource

        return {
            "DrimeDAVProvider": DrimeDAVProvider,
            "DrimeCollection": DrimeCollection,
            "DrimeResource": DrimeResource,
        }[name]
    elif name in ("create_webdav_app", "run_webdav_server"):
        from .server import create_webdav_app, run_webdav_server

        return {
            "create_webdav_app": create_webdav_app,
            "run_webdav_server": run_webdav_server,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DrimeDAVProvider",
    "DrimeCollection",
    "DrimeResource",
    "create_webdav_app",
    "run_webdav_server",
    "DEFAULT_CACHE_TTL",
    "DEFAULT_MAX_FILE_SIZE",
]
