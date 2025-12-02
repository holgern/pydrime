"""REST API server for restic backup backend.

This module provides a REST API server compatible with restic's REST backend v2
specification, using Drime Cloud as the storage backend.

API Specification:
    https://restic.readthedocs.io/en/latest/REST_backend.html
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import parse_qs, unquote

if TYPE_CHECKING:
    from ..api import DrimeClient

logger = logging.getLogger(__name__)

# Default configuration values
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000

# API version media types
API_V1_MEDIA_TYPE = "application/vnd.x.restic.rest.v1"
API_V2_MEDIA_TYPE = "application/vnd.x.restic.rest.v2"

# Valid blob types
VALID_TYPES = {"data", "keys", "locks", "snapshots", "index", "config"}


class ResticRESTApp:
    """WSGI application implementing the restic REST backend API.

    Supports both API v1 and v2. The version is selected based on the
    Accept header in the request.
    """

    def __init__(
        self,
        client: DrimeClient,
        workspace_id: int = 0,
        readonly: bool = False,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        """Initialize the REST API application.

        Args:
            client: The Drime API client
            workspace_id: The workspace ID to use (0 for personal)
            readonly: Whether to allow write operations
            username: Username for basic authentication (None for anonymous)
            password: Password for basic authentication
        """
        from .provider import ResticStorageProvider

        self.provider = ResticStorageProvider(
            client=client,
            workspace_id=workspace_id,
            readonly=readonly,
        )
        self.username = username
        self.password = password

    def _get_api_version(self, environ: dict[str, Any]) -> int:
        """Determine API version from Accept header.

        Args:
            environ: WSGI environment

        Returns:
            1 or 2 depending on the Accept header
        """
        accept = environ.get("HTTP_ACCEPT", "")
        logger.debug(f"Accept header: '{accept}'")
        # Check for exact match as restic-rest-server does
        if accept == API_V2_MEDIA_TYPE:
            logger.debug("Detected API v2 request")
            return 2
        logger.debug("Using API v1 (default)")
        return 1

    def _check_auth(self, environ: dict[str, Any]) -> bool:
        """Check basic authentication.

        Args:
            environ: WSGI environment

        Returns:
            True if authenticated or no auth required
        """
        if not self.username or not self.password:
            return True  # No auth required

        import base64

        auth_header = environ.get("HTTP_AUTHORIZATION", "")
        if not auth_header.startswith("Basic "):
            return False

        try:
            encoded = auth_header[6:]
            decoded = base64.b64decode(encoded).decode("utf-8")
            user, passwd = decoded.split(":", 1)
            return user == self.username and passwd == self.password
        except Exception:
            return False

    def _parse_path(self, path: str) -> tuple[str, str | None, str | None]:
        """Parse request path into repo_path, blob_type, and blob_name.

        Args:
            path: Request path like /repo/data/abc123

        Returns:
            Tuple of (repo_path, blob_type, blob_name)
            blob_type and blob_name may be None
        """
        # Remove leading/trailing slashes and decode
        path = unquote(path.strip("/"))

        if not path:
            return "", None, None

        # Split path into parts and filter out empty strings
        parts = [p for p in path.split("/") if p]

        if not parts:
            return "", None, None

        # Check if the last part is "config" (special case)
        if parts[-1] == "config":
            # Special case: config is both type and name
            return "/".join(parts[:-1]), "config", None

        # Check if the last part is a valid type (for listing)
        if parts[-1] in VALID_TYPES:
            # Just the type, no name (for listing)
            return "/".join(parts[:-1]), parts[-1], None

        # Check if second-to-last is a type (for blob operations with name)
        if len(parts) >= 2:
            potential_type = parts[-2]

            # Handle data blobs with subdirectories (data/00/abc123)
            if len(parts) >= 3 and parts[-3] == "data" and len(parts[-2]) == 2:
                # Path is like /repo/data/00/abc123
                blob_type = "data"
                blob_name = parts[-1]
                return "/".join(parts[:-3]), blob_type, blob_name

            # Handle other blob types (keys/abc123, locks/abc123, etc.)
            if potential_type in VALID_TYPES and potential_type != "config":
                blob_type = potential_type
                blob_name = parts[-1]
                return "/".join(parts[:-2]), blob_type, blob_name

        # No type found, entire path is repo_path
        return "/".join(parts), None, None

    def _send_response(
        self,
        start_response: Callable,
        status: str,
        headers: list[tuple[str, str]] | None = None,
        body: bytes | None = None,
    ) -> list[bytes]:
        """Send HTTP response.

        Args:
            start_response: WSGI start_response callable
            status: HTTP status string like "200 OK"
            headers: Optional list of headers
            body: Optional response body

        Returns:
            Response body as list of bytes
        """
        if headers is None:
            headers = []

        # Only add Content-Length if not already present
        has_content_length = any(h[0].lower() == "content-length" for h in headers)
        if not has_content_length:
            if body is not None:
                headers.append(("Content-Length", str(len(body))))
            else:
                headers.append(("Content-Length", "0"))

        start_response(status, headers)
        return [body] if body else [b""]

    def _send_json(
        self,
        start_response: Callable,
        status: str,
        data: Any,
        api_version: int = 1,
    ) -> list[bytes]:
        """Send JSON response.

        Args:
            start_response: WSGI start_response callable
            status: HTTP status string
            data: Data to serialize as JSON
            api_version: API version to indicate in Content-Type header

        Returns:
            Response body as list of bytes
        """
        body = json.dumps(data).encode("utf-8")
        if api_version == 2:
            content_type = API_V2_MEDIA_TYPE
        else:
            content_type = API_V1_MEDIA_TYPE
        logger.debug(f"Sending JSON response with Content-Type: {content_type}")
        headers = [("Content-Type", content_type)]
        return self._send_response(start_response, status, headers, body)

    def __call__(
        self, environ: dict[str, Any], start_response: Callable
    ) -> list[bytes]:
        """WSGI application entry point.

        Args:
            environ: WSGI environment
            start_response: WSGI start_response callable

        Returns:
            Response body as list of bytes
        """
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "/")
        query_string = environ.get("QUERY_STRING", "")

        logger.debug(f"Request: {method} {path}?{query_string}")

        # Check authentication
        if not self._check_auth(environ):
            headers = [("WWW-Authenticate", 'Basic realm="restic"')]
            return self._send_response(
                start_response, "401 Unauthorized", headers, b"Unauthorized"
            )

        # Parse query parameters
        params = parse_qs(query_string)

        # Parse path
        repo_path, blob_type, blob_name = self._parse_path(path)

        # Get API version
        api_version = self._get_api_version(environ)

        logger.debug(
            f"Parsed: repo={repo_path}, type={blob_type}, name={blob_name}, "
            f"api_v={api_version}"
        )

        # Route request
        try:
            if method == "POST" and "create" in params:
                # Create repository
                return self._handle_create_repo(repo_path, start_response)
            elif method == "DELETE" and blob_type is None:
                # Delete repository
                return self._handle_delete_repo(repo_path, start_response)
            elif blob_type == "config":
                # Config operations
                if method == "HEAD":
                    return self._handle_head_config(repo_path, start_response)
                elif method == "GET":
                    return self._handle_get_config(repo_path, start_response)
                elif method == "POST":
                    return self._handle_save_config(repo_path, environ, start_response)
            elif blob_type in VALID_TYPES and blob_type != "config":
                if blob_name is None:
                    # List blobs
                    if method == "GET":
                        return self._handle_list_blobs(
                            repo_path, blob_type, api_version, start_response
                        )
                else:
                    # Blob operations
                    if method == "HEAD":
                        return self._handle_head_blob(
                            repo_path, blob_type, blob_name, start_response
                        )
                    elif method == "GET":
                        return self._handle_get_blob(
                            repo_path, blob_type, blob_name, environ, start_response
                        )
                    elif method == "POST":
                        return self._handle_save_blob(
                            repo_path, blob_type, blob_name, environ, start_response
                        )
                    elif method == "DELETE":
                        return self._handle_delete_blob(
                            repo_path, blob_type, blob_name, start_response
                        )

            # Not found or not handled
            return self._send_response(
                start_response, "404 Not Found", body=b"Not Found"
            )

        except Exception as e:
            logger.exception(f"Error handling request: {e}")
            return self._send_response(
                start_response, "500 Internal Server Error", body=str(e).encode()
            )

    def _handle_create_repo(
        self, repo_path: str, start_response: Callable
    ) -> list[bytes]:
        """Handle repository creation."""
        if self.provider.is_readonly():
            return self._send_response(
                start_response, "403 Forbidden", body=b"Read-only mode"
            )

        if self.provider.repository_exists(repo_path):
            return self._send_response(start_response, "200 OK")

        if self.provider.create_repository(repo_path):
            return self._send_response(start_response, "200 OK")
        else:
            return self._send_response(
                start_response,
                "500 Internal Server Error",
                body=b"Failed to create repository",
            )

    def _handle_delete_repo(
        self, repo_path: str, start_response: Callable
    ) -> list[bytes]:
        """Handle repository deletion."""
        if self.provider.is_readonly():
            return self._send_response(
                start_response, "403 Forbidden", body=b"Read-only mode"
            )

        if self.provider.delete_repository(repo_path):
            return self._send_response(start_response, "200 OK")
        else:
            return self._send_response(start_response, "404 Not Found")

    def _handle_head_config(
        self, repo_path: str, start_response: Callable
    ) -> list[bytes]:
        """Handle HEAD request for config (check if repo exists and return size)."""
        exists, size = self.provider.config_exists(repo_path)
        if exists:
            headers = [("Content-Length", str(size))]
            return self._send_response(start_response, "200 OK", headers)
        else:
            return self._send_response(start_response, "404 Not Found")

    def _handle_get_config(
        self, repo_path: str, start_response: Callable
    ) -> list[bytes]:
        """Handle GET request for config."""
        data = self.provider.get_config(repo_path)
        if data is not None:
            headers = [("Content-Type", "application/octet-stream")]
            return self._send_response(start_response, "200 OK", headers, data)
        else:
            return self._send_response(start_response, "404 Not Found")

    def _handle_save_config(
        self, repo_path: str, environ: dict[str, Any], start_response: Callable
    ) -> list[bytes]:
        """Handle POST request to save config."""
        if self.provider.is_readonly():
            return self._send_response(
                start_response, "403 Forbidden", body=b"Read-only mode"
            )

        # Read request body
        try:
            content_length = int(environ.get("CONTENT_LENGTH", 0))
            data = environ["wsgi.input"].read(content_length)
        except Exception as e:
            logger.error(f"Error reading request body: {e}")
            return self._send_response(
                start_response, "400 Bad Request", body=b"Invalid request body"
            )

        if self.provider.save_config(repo_path, data):
            return self._send_response(start_response, "200 OK")
        else:
            return self._send_response(
                start_response,
                "500 Internal Server Error",
                body=b"Failed to save config",
            )

    def _handle_list_blobs(
        self,
        repo_path: str,
        blob_type: str,
        api_version: int,
        start_response: Callable,
    ) -> list[bytes]:
        """Handle GET request to list blobs."""
        blobs = self.provider.list_blobs(repo_path, blob_type)

        if blobs is None:
            return self._send_response(start_response, "404 Not Found")

        if api_version == 1:
            # V1: Return array of names
            names = [b["name"] for b in blobs]
            logger.debug(f"List blobs v1 response: {names}")
            return self._send_json(start_response, "200 OK", names, api_version=1)
        else:
            # V2: Return array of {name, size} objects
            logger.debug(f"List blobs v2 response: {blobs}")
            return self._send_json(start_response, "200 OK", blobs, api_version=2)

    def _handle_head_blob(
        self,
        repo_path: str,
        blob_type: str,
        blob_name: str,
        start_response: Callable,
    ) -> list[bytes]:
        """Handle HEAD request to check if blob exists."""
        exists, size = self.provider.blob_exists(repo_path, blob_type, blob_name)

        if exists:
            headers = [("Content-Length", str(size))]
            return self._send_response(start_response, "200 OK", headers)
        else:
            return self._send_response(start_response, "404 Not Found")

    def _handle_get_blob(
        self,
        repo_path: str,
        blob_type: str,
        blob_name: str,
        environ: dict[str, Any],
        start_response: Callable,
    ) -> list[bytes]:
        """Handle GET request for blob content."""
        data = self.provider.get_blob(repo_path, blob_type, blob_name)

        if data is None:
            return self._send_response(start_response, "404 Not Found")

        # Check for Range header
        range_header = environ.get("HTTP_RANGE", "")
        if range_header and range_header.startswith("bytes="):
            # Parse range (only single range supported)
            match = re.match(r"bytes=(\d*)-(\d*)", range_header)
            if match:
                start_str, end_str = match.groups()
                total_size = len(data)

                if start_str and end_str:
                    start = int(start_str)
                    end = min(int(end_str), total_size - 1)
                elif start_str:
                    start = int(start_str)
                    end = total_size - 1
                elif end_str:
                    # Last N bytes
                    suffix_length = int(end_str)
                    start = max(0, total_size - suffix_length)
                    end = total_size - 1
                else:
                    start = 0
                    end = total_size - 1

                if start > end or start >= total_size:
                    headers = [("Content-Range", f"bytes */{total_size}")]
                    return self._send_response(
                        start_response, "416 Range Not Satisfiable", headers
                    )

                partial_data = data[start : end + 1]
                headers = [
                    ("Content-Type", "application/octet-stream"),
                    ("Content-Range", f"bytes {start}-{end}/{total_size}"),
                    ("Accept-Ranges", "bytes"),
                ]
                return self._send_response(
                    start_response, "206 Partial Content", headers, partial_data
                )

        # Full content
        headers = [
            ("Content-Type", "application/octet-stream"),
            ("Accept-Ranges", "bytes"),
        ]
        return self._send_response(start_response, "200 OK", headers, data)

    def _handle_save_blob(
        self,
        repo_path: str,
        blob_type: str,
        blob_name: str,
        environ: dict[str, Any],
        start_response: Callable,
    ) -> list[bytes]:
        """Handle POST request to save blob."""
        if self.provider.is_readonly():
            return self._send_response(
                start_response, "403 Forbidden", body=b"Read-only mode"
            )

        # Read request body
        try:
            content_length = int(environ.get("CONTENT_LENGTH", 0))
            data = environ["wsgi.input"].read(content_length)
        except Exception as e:
            logger.error(f"Error reading request body: {e}")
            return self._send_response(
                start_response, "400 Bad Request", body=b"Invalid request body"
            )

        if self.provider.save_blob(repo_path, blob_type, blob_name, data):
            return self._send_response(start_response, "200 OK")
        else:
            return self._send_response(
                start_response, "500 Internal Server Error", body=b"Failed to save blob"
            )

    def _handle_delete_blob(
        self,
        repo_path: str,
        blob_type: str,
        blob_name: str,
        start_response: Callable,
    ) -> list[bytes]:
        """Handle DELETE request for blob."""
        if self.provider.is_readonly():
            return self._send_response(
                start_response, "403 Forbidden", body=b"Read-only mode"
            )

        if self.provider.delete_blob(repo_path, blob_type, blob_name):
            return self._send_response(start_response, "200 OK")
        else:
            return self._send_response(start_response, "404 Not Found")


def create_rest_app(
    client: DrimeClient,
    workspace_id: int = 0,
    readonly: bool = False,
    username: str | None = None,
    password: str | None = None,
) -> ResticRESTApp:
    """Create a restic REST API WSGI application.

    Args:
        client: The Drime API client
        workspace_id: The workspace ID to serve (0 for personal)
        readonly: Whether to allow write operations
        username: Username for basic authentication (None for anonymous)
        password: Password for basic authentication

    Returns:
        ResticRESTApp instance
    """
    return ResticRESTApp(
        client=client,
        workspace_id=workspace_id,
        readonly=readonly,
        username=username,
        password=password,
    )


def run_rest_server(
    client: DrimeClient,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    workspace_id: int = 0,
    readonly: bool = False,
    username: str | None = None,
    password: str | None = None,
    ssl_cert: str | None = None,
    ssl_key: str | None = None,
) -> None:
    """Run the restic REST API server.

    Args:
        client: The Drime API client
        host: Host address to bind to
        port: Port number to listen on
        workspace_id: The workspace ID to serve (0 for personal)
        readonly: Whether to allow write operations
        username: Username for basic authentication (None for anonymous)
        password: Password for basic authentication
        ssl_cert: Path to SSL certificate file (for HTTPS)
        ssl_key: Path to SSL private key file (for HTTPS)
    """
    from cheroot import wsgi

    # Create the WSGI application
    app = create_rest_app(
        client=client,
        workspace_id=workspace_id,
        readonly=readonly,
        username=username,
        password=password,
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
    logger.info(f"Starting restic REST server at {protocol}://{host}:{port}/")

    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    finally:
        server.stop()
        logger.info("REST server stopped.")
