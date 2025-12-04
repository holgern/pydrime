"""REST API server module for restic backup backend.

This module provides a REST API server compatible with restic's REST backend v2
specification, allowing restic to use Drime Cloud as a backup storage.

Example usage:
    pydrime rest --host 0.0.0.0 --port 8000
    pydrime rest --username user --password secret
    pydrime rest --readonly

Restic usage:
    restic -r rest:http://localhost:8000/repo init
    restic -r rest:http://localhost:8000/repo backup /path/to/data
"""

from typing import Any


def __getattr__(name: str) -> Any:
    """Lazy import to avoid importing cheroot unless needed."""
    if name == "ResticStorageProvider":
        from .provider import ResticStorageProvider

        return ResticStorageProvider
    elif name in ("ResticRESTApp", "create_rest_app", "run_rest_server"):
        from .server import ResticRESTApp, create_rest_app, run_rest_server

        return {
            "ResticRESTApp": ResticRESTApp,
            "create_rest_app": create_rest_app,
            "run_rest_server": run_rest_server,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ResticStorageProvider",
    "ResticRESTApp",
    "create_rest_app",
    "run_rest_server",
]
