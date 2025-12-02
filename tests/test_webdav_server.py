"""Tests for the WebDAV server module."""

# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestContentTypeFixMiddleware:
    """Tests for the ContentTypeFixMiddleware class."""

    def setup_method(self):
        """Set up test fixtures."""
        pass

    def _create_middleware(self, app):
        """Create a ContentTypeFixMiddleware wrapping the given app."""
        from pydrime.webdav.server import ContentTypeFixMiddleware

        return ContentTypeFixMiddleware(app)

    def test_lock_request_fixes_content_type(self):
        """Test that LOCK request Content-Type is fixed."""

        # Mock app that returns broken Content-Type
        def mock_app(environ, start_response):
            start_response("200 OK", [("Content-Type", "application; charset=utf-8")])
            return [b"<response/>"]

        middleware = self._create_middleware(mock_app)
        environ = {"REQUEST_METHOD": "LOCK"}
        response_headers = []

        def capturing_start_response(status, headers, exc_info=None):
            response_headers.extend(headers)

        list(middleware(environ, capturing_start_response))

        # Should have fixed Content-Type
        content_types = [v for k, v in response_headers if k == "Content-Type"]
        assert len(content_types) == 1
        assert content_types[0] == "application/xml; charset=utf-8"

    def test_non_lock_request_unchanged(self):
        """Test that non-LOCK requests are not modified."""

        def mock_app(environ, start_response):
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"content"]

        middleware = self._create_middleware(mock_app)
        environ = {"REQUEST_METHOD": "GET"}
        response_headers = []

        def capturing_start_response(status, headers, exc_info=None):
            response_headers.extend(headers)

        list(middleware(environ, capturing_start_response))

        content_types = [v for k, v in response_headers if k == "Content-Type"]
        assert len(content_types) == 1
        assert content_types[0] == "text/plain"

    def test_lock_with_correct_content_type_unchanged(self):
        """Test that LOCK with correct Content-Type is not modified."""

        def mock_app(environ, start_response):
            start_response(
                "200 OK", [("Content-Type", "application/xml; charset=utf-8")]
            )
            return [b"<response/>"]

        middleware = self._create_middleware(mock_app)
        environ = {"REQUEST_METHOD": "LOCK"}
        response_headers = []

        def capturing_start_response(status, headers, exc_info=None):
            response_headers.extend(headers)

        list(middleware(environ, capturing_start_response))

        content_types = [v for k, v in response_headers if k == "Content-Type"]
        assert len(content_types) == 1
        assert content_types[0] == "application/xml; charset=utf-8"


class TestCreateWebdavApp:
    """Tests for the create_webdav_app function."""

    def test_create_app_basic(self):
        """Test creating a basic WebDAV app."""
        from pydrime.webdav.server import ContentTypeFixMiddleware, create_webdav_app

        mock_client = MagicMock()
        app = create_webdav_app(client=mock_client)

        # App should be wrapped with middleware
        assert isinstance(app, ContentTypeFixMiddleware)

    def test_create_app_with_auth(self):
        """Test creating a WebDAV app with authentication."""
        from pydrime.webdav.server import create_webdav_app

        mock_client = MagicMock()
        app = create_webdav_app(
            client=mock_client,
            username="testuser",
            password="testpass",
        )

        # App should be created successfully
        assert app is not None

    def test_create_app_readonly(self):
        """Test creating a readonly WebDAV app."""
        from pydrime.webdav.server import create_webdav_app

        mock_client = MagicMock()
        app = create_webdav_app(
            client=mock_client,
            readonly=True,
        )

        # App should be created successfully
        assert app is not None

    def test_create_app_with_workspace(self):
        """Test creating a WebDAV app with workspace ID."""
        from pydrime.webdav.server import create_webdav_app

        mock_client = MagicMock()
        app = create_webdav_app(
            client=mock_client,
            workspace_id=42,
        )

        assert app is not None


class TestRunWebdavServer:
    """Tests for the run_webdav_server function."""

    @patch("cheroot.wsgi.Server")
    def test_run_server_basic(self, mock_wsgi_server):
        """Test running the WebDAV server."""
        from pydrime.webdav.server import run_webdav_server

        mock_client = MagicMock()
        mock_server = MagicMock()
        mock_wsgi_server.return_value = mock_server

        # Simulate keyboard interrupt to stop the server
        mock_server.start.side_effect = KeyboardInterrupt()

        run_webdav_server(
            client=mock_client,
            host="127.0.0.1",
            port=8080,
        )

        mock_wsgi_server.assert_called_once()
        mock_server.start.assert_called_once()
        mock_server.stop.assert_called_once()

    @patch("cheroot.wsgi.Server")
    def test_run_server_custom_port(self, mock_wsgi_server):
        """Test running the WebDAV server on custom port."""
        from pydrime.webdav.server import run_webdav_server

        mock_client = MagicMock()
        mock_server = MagicMock()
        mock_wsgi_server.return_value = mock_server
        mock_server.start.side_effect = KeyboardInterrupt()

        run_webdav_server(
            client=mock_client,
            host="0.0.0.0",
            port=9999,
        )

        # Verify server was created with correct bind address
        call_kwargs = mock_wsgi_server.call_args[1]
        assert call_kwargs["bind_addr"] == ("0.0.0.0", 9999)

    @patch("cheroot.ssl.builtin.BuiltinSSLAdapter")
    @patch("cheroot.wsgi.Server")
    def test_run_server_with_ssl(self, mock_wsgi_server, mock_ssl_adapter):
        """Test running the WebDAV server with SSL."""
        from pydrime.webdav.server import run_webdav_server

        mock_client = MagicMock()
        mock_server = MagicMock()
        mock_wsgi_server.return_value = mock_server
        mock_server.start.side_effect = KeyboardInterrupt()

        run_webdav_server(
            client=mock_client,
            ssl_cert="/path/to/cert.pem",
            ssl_key="/path/to/key.pem",
        )

        # Verify SSL adapter was created
        mock_ssl_adapter.assert_called_once_with(
            "/path/to/cert.pem", "/path/to/key.pem"
        )


class TestDefaultValues:
    """Tests for default configuration values."""

    def test_default_host(self):
        """Test default host value."""
        from pydrime.webdav.server import DEFAULT_HOST

        assert DEFAULT_HOST == "127.0.0.1"

    def test_default_port(self):
        """Test default port value."""
        from pydrime.webdav.server import DEFAULT_PORT

        assert DEFAULT_PORT == 8080

    def test_default_cache_ttl(self):
        """Test default cache TTL value."""
        from pydrime.webdav.server import DEFAULT_CACHE_TTL

        assert DEFAULT_CACHE_TTL == 30.0

    def test_default_max_file_size(self):
        """Test default max file size value."""
        from pydrime.webdav.server import DEFAULT_MAX_FILE_SIZE

        assert DEFAULT_MAX_FILE_SIZE == 500 * 1024 * 1024  # 500 MB
