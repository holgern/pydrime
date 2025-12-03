"""WebDAV server runner using WsgiDAV and Cheroot."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..api import DrimeClient

logger = logging.getLogger(__name__)


class ContentTypeFixMiddleware:
    """Middleware to fix WsgiDAV bug with Content-Type header in LOCK responses.

    WsgiDAV 4.3.3 has a bug where LOCK responses have Content-Type: "application"
    instead of "application/xml". This causes litmus and other WebDAV clients
    to fail parsing the response.

    See: https://github.com/mar10/wsgidav/issues/XXX (to be reported)
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    def __call__(self, environ: dict, start_response: Any) -> Any:
        request_method = environ.get("REQUEST_METHOD", "")

        def fixed_start_response(
            status: str, headers: list[tuple[str, str]], exc_info: Any = None
        ) -> Any:
            # Fix Content-Type for LOCK responses
            if request_method == "LOCK":
                fixed_headers = []
                for name, value in headers:
                    if (
                        name.lower() == "content-type"
                        and value == "application; charset=utf-8"
                    ):
                        # Fix the broken Content-Type
                        fixed_headers.append(
                            ("Content-Type", "application/xml; charset=utf-8")
                        )
                    else:
                        fixed_headers.append((name, value))
                return start_response(status, fixed_headers, exc_info)
            return start_response(status, headers, exc_info)

        return self.app(environ, fixed_start_response)


# Default configuration values
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080
DEFAULT_CACHE_TTL = 30.0
DEFAULT_MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB


def create_webdav_app(
    client: DrimeClient,
    workspace_id: int = 0,
    readonly: bool = False,
    cache_ttl: float = DEFAULT_CACHE_TTL,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    username: str | None = None,
    password: str | None = None,
    verbose: int = 1,
) -> Any:
    """Create a WsgiDAV application for Drime Cloud.

    Args:
        client: The Drime API client
        workspace_id: The workspace ID to serve (0 for personal)
        readonly: Whether to allow write operations
        cache_ttl: Cache time-to-live in seconds
        max_file_size: Maximum file size for uploads/downloads in bytes
        username: WebDAV username for authentication (None for anonymous)
        password: WebDAV password for authentication
        verbose: WsgiDAV verbosity level (0-5)

    Returns:
        WsgiDAVApp instance
    """
    from wsgidav.wsgidav_app import WsgiDAVApp  # type: ignore[import-untyped]

    from .provider import DrimeDAVProvider

    # Create the provider
    provider = DrimeDAVProvider(
        client=client,
        workspace_id=workspace_id,
        readonly=readonly,
        cache_ttl=cache_ttl,
        max_file_size=max_file_size,
    )

    # Build configuration
    config: dict[str, Any] = {
        "provider_mapping": {"/": provider},
        "verbose": verbose,
        "logging": {
            "enable": verbose > 0,
            "enable_loggers": [],
        },
        # Enable directory browser for web access
        "dir_browser": {
            "enable": True,
            "response_trailer": (
                f"<p>Drime Cloud WebDAV Server | "
                f"Workspace: {workspace_id} | "
                f"{'Read-only' if readonly else 'Read-write'}</p>"
            ),
        },
        # Lock storage (required for write operations)
        "lock_storage": True,
        # Property manager
        "property_manager": True,
    }

    # Configure pydrime webdav logging based on verbosity
    if verbose >= 5:
        logging.getLogger("pydrime.webdav").setLevel(logging.DEBUG)
    elif verbose >= 3:
        logging.getLogger("pydrime.webdav").setLevel(logging.INFO)
    elif verbose >= 1:
        logging.getLogger("pydrime.webdav").setLevel(logging.WARNING)

    # Configure authentication
    if username and password:
        config["http_authenticator"] = {
            "domain_controller": None,  # Use SimpleDomainController
            "accept_basic": True,
            "accept_digest": True,
            "default_to_digest": True,
        }
        config["simple_dc"] = {
            "user_mapping": {
                "*": {
                    username: {
                        "password": password,
                    }
                }
            }
        }
    else:
        # Anonymous access
        config["http_authenticator"] = {
            "domain_controller": None,
            "accept_basic": False,
            "accept_digest": False,
        }
        config["simple_dc"] = {
            "user_mapping": {
                "*": True  # Allow anonymous access
            }
        }

    app = WsgiDAVApp(config)

    # Wrap with middleware to fix WsgiDAV Content-Type bug in LOCK responses
    return ContentTypeFixMiddleware(app)


def run_webdav_server(
    client: DrimeClient,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    workspace_id: int = 0,
    readonly: bool = False,
    cache_ttl: float = DEFAULT_CACHE_TTL,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    username: str | None = None,
    password: str | None = None,
    verbose: int = 1,
    ssl_cert: str | None = None,
    ssl_key: str | None = None,
) -> None:
    """Run the WebDAV server.

    Args:
        client: The Drime API client
        host: Host address to bind to
        port: Port number to listen on
        workspace_id: The workspace ID to serve (0 for personal)
        readonly: Whether to allow write operations
        cache_ttl: Cache time-to-live in seconds
        max_file_size: Maximum file size for uploads/downloads in bytes
        username: WebDAV username for authentication (None for anonymous)
        password: WebDAV password for authentication
        verbose: WsgiDAV verbosity level (0-5)
        ssl_cert: Path to SSL certificate file (for HTTPS)
        ssl_key: Path to SSL private key file (for HTTPS)
    """
    from cheroot import wsgi

    # Create the WSGI application
    app = create_webdav_app(
        client=client,
        workspace_id=workspace_id,
        readonly=readonly,
        cache_ttl=cache_ttl,
        max_file_size=max_file_size,
        username=username,
        password=password,
        verbose=verbose,
    )

    # Configure the server
    server_args: dict[str, Any] = {
        "bind_addr": (host, port),
        "wsgi_app": app,
    }

    # Configure SSL if certificates provided
    if ssl_cert and ssl_key:
        from cheroot.ssl.builtin import BuiltinSSLAdapter

        server_args["ssl_adapter"] = BuiltinSSLAdapter(ssl_cert, ssl_key)

    # Create and start the server
    server = wsgi.Server(**server_args)

    # Set server timeouts
    server.timeout = 300  # 5 minutes
    server.shutdown_timeout = 5

    protocol = "https" if (ssl_cert and ssl_key) else "http"
    logger.info(f"Starting WebDAV server at {protocol}://{host}:{port}/")

    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    finally:
        server.stop()
        logger.info("WebDAV server stopped.")
