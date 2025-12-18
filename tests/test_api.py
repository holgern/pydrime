"""Unit tests for the Drime API client."""

from unittest.mock import Mock, patch

import httpx
import pytest

from pydrime.api import DrimeClient
from pydrime.exceptions import (
    DrimeAPIError,
    DrimeDownloadError,
    DrimeFileNotFoundError,
    DrimeRateLimitError,
)


class TestDrimeClient:
    """Tests for DrimeClient initialization and basic functionality."""

    def test_init_with_api_key(self):
        """Test client initialization with API key."""
        client = DrimeClient(api_key="test_key")
        assert client.api_key == "test_key"
        assert client.api_url == "https://app.drime.cloud/api/v1"

    def test_init_without_api_key_raises_error(self):
        """Test that initializing without API key raises error."""
        with patch("pydrime.api.config") as mock_config:
            mock_config.api_key = None
            with pytest.raises(DrimeAPIError, match="API key not configured"):
                DrimeClient(api_key=None)

    def test_init_with_custom_api_url(self):
        """Test client initialization with custom API URL."""
        client = DrimeClient(api_key="test_key", api_url="https://custom.api")
        assert client.api_url == "https://custom.api"

    def test_session_headers_set_correctly(self):
        """Test that client headers include authorization."""
        client = DrimeClient(api_key="test_key")
        http_client = client._get_client()
        assert "authorization" in http_client.headers
        assert http_client.headers["authorization"] == "Bearer test_key"


class TestAPIRequest:
    """Tests for the _request method."""

    @patch("pydrime.api.httpx.Client.request")
    def test_successful_json_response(self, mock_request):
        """Test successful API request with JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"data": "test"}'
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        result = client._request("GET", "/test")

        assert result == {"data": "test"}
        mock_request.assert_called_once()

    @patch("pydrime.api.httpx.Client.request")
    def test_empty_response(self, mock_request):
        """Test handling of empty response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        result = client._request("GET", "/test")

        assert result == {}

    @patch("pydrime.api.httpx.Client.request")
    def test_html_response_raises_error(self, mock_request):
        """Test that HTML response raises appropriate error."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"<html>Error</html>"
        mock_response.headers = {"Content-Type": "text/html"}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        with pytest.raises(
            DrimeAPIError, match="Invalid API key. The stored API key is not valid"
        ):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_http_401_error(self, mock_request):
        """Test handling of 401 Unauthorized error."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401 Unauthorized",
            request=Mock(),
            response=mock_response,
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(
            DrimeAPIError, match="Invalid API key or unauthorized access"
        ):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_http_403_error(self, mock_request):
        """Test handling of 403 Forbidden error."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "403 Forbidden",
            request=Mock(),
            response=mock_response,
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Access forbidden"):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_http_404_error(self, mock_request):
        """Test handling of 404 Not Found error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found",
            request=Mock(),
            response=mock_response,
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Resource not found"):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_network_error(self, mock_request):
        """Test handling of network errors."""
        mock_request.side_effect = httpx.ConnectError("Connection failed")

        client = DrimeClient(api_key="test_key", max_retries=0)
        with pytest.raises(DrimeAPIError, match="Network error"):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_non_json_response_unexpected_type(self, mock_request):
        """Test handling of unexpected content type (not HTML, not JSON)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"some text data"
        mock_response.headers = {"Content-Type": "text/plain"}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Unexpected response type: text/plain"):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_invalid_json_response(self, mock_request):
        """Test handling of invalid JSON in response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"{invalid json}"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Invalid JSON response from server"):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_http_429_rate_limit_error(self, mock_request):
        """Test handling of 429 Rate Limit error."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {}  # No Retry-After header
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "429 Too Many Requests",
            request=Mock(),
            response=mock_response,
        )

        # Create client with max_retries=0 to avoid retrying
        client = DrimeClient(api_key="test_key", max_retries=0)
        with pytest.raises(DrimeRateLimitError, match="Rate limit exceeded"):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_http_error_with_json_error_message(self, mock_request):
        """Test handling of HTTP error with JSON error message in response."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.content = b'{"message": "Internal server error occurred"}'
        mock_response.json.return_value = {"message": "Internal server error occurred"}
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=Mock(),
            response=mock_response,
        )

        client = DrimeClient(api_key="test_key", max_retries=0)
        with pytest.raises(
            DrimeAPIError,
            match="API request failed with status 500: Internal server error occurred",
        ):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_http_error_with_error_field(self, mock_request):
        """Test handling of HTTP error with 'error' field in response."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b'{"error": "Bad request"}'
        mock_response.json.return_value = {"error": "Bad request"}
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "400 Bad Request",
            request=Mock(),
            response=mock_response,
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(
            DrimeAPIError, match="API request failed with status 400: Bad request"
        ):
            client._request("GET", "/test")

    @patch("pydrime.api.httpx.Client.request")
    def test_http_error_with_unparseable_response(self, mock_request):
        """Test handling of HTTP error with unparseable response."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.content = b"<html>Error</html>"
        mock_response.json.side_effect = ValueError("Not JSON")
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=Mock(),
            response=mock_response,
        )

        client = DrimeClient(api_key="test_key", max_retries=0)
        with pytest.raises(DrimeAPIError, match="API request failed with status 500"):
            client._request("GET", "/test")


class TestGetLoggedUser:
    """Tests for get_logged_user method."""

    @patch("pydrime.api.DrimeClient._request")
    def test_get_logged_user_success(self, mock_request):
        """Test successful logged user retrieval."""
        mock_request.return_value = {"user": {"email": "test@example.com"}}

        client = DrimeClient(api_key="test_key")
        result = client.get_logged_user()

        assert result == {"user": {"email": "test@example.com"}}
        mock_request.assert_called_once_with("GET", "/cli/loggedUser")

    @patch("pydrime.api.DrimeClient._request")
    def test_get_logged_user_invalid_key(self, mock_request):
        """Test logged user with invalid API key returns null user."""
        mock_request.return_value = {"user": None}

        client = DrimeClient(api_key="invalid_key")
        result = client.get_logged_user()

        assert result == {"user": None}


class TestGetSpaceUsage:
    """Tests for get_space_usage method."""

    @patch("pydrime.api.DrimeClient._request")
    def test_get_space_usage_success(self, mock_request):
        """Test successful space usage retrieval."""
        mock_request.return_value = {
            "used": 454662236403,
            "available": 13194139533312,
            "status": "success",
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_space_usage()

        assert result["used"] == 454662236403
        assert result["available"] == 13194139533312
        assert result["status"] == "success"
        mock_request.assert_called_once_with("GET", "/user/space-usage")

    @patch("pydrime.api.DrimeClient._request")
    def test_get_space_usage_zero_usage(self, mock_request):
        """Test space usage with zero usage."""
        mock_request.return_value = {
            "used": 0,
            "available": 1000000000,
            "status": "success",
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_space_usage()

        assert result["used"] == 0
        assert result["available"] == 1000000000

    @patch("pydrime.api.DrimeClient._request")
    def test_get_space_usage_full_storage(self, mock_request):
        """Test space usage with full storage."""
        mock_request.return_value = {
            "used": 1000000000,
            "available": 0,
            "status": "success",
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_space_usage()

        assert result["used"] == 1000000000
        assert result["available"] == 0


class TestListFiles:
    """Tests for list_files method."""

    @patch("pydrime.api.DrimeClient._request")
    def test_list_files_default_params(self, mock_request):
        """Test list files with default parameters."""
        mock_request.return_value = {
            "data": [{"id": 1, "name": "file1.txt", "type": "text"}]
        }

        client = DrimeClient(api_key="test_key")
        result = client.list_files()

        # list_files returns the raw API response with 'data' key
        assert "data" in result
        assert len(result["data"]) == 1
        assert result["data"][0]["name"] == "file1.txt"
        mock_request.assert_called_once()

    @patch("pydrime.api.DrimeClient._request")
    def test_list_files_with_query(self, mock_request):
        """Test list files with search query."""
        mock_request.return_value = {"data": []}

        client = DrimeClient(api_key="test_key")
        client.list_files(query="test")

        call_args = mock_request.call_args
        assert "params" in call_args.kwargs
        assert call_args.kwargs["params"]["query"] == "test"

    @patch("pydrime.api.DrimeClient._request")
    def test_list_files_with_parent_id(self, mock_request):
        """Test list files with parent folder ID."""
        mock_request.return_value = {"data": []}

        client = DrimeClient(api_key="test_key")
        client.list_files(parent_id=123)

        call_args = mock_request.call_args
        assert "params" in call_args.kwargs
        # Check that parentIds is in the params (as comma-separated string)
        assert "parentIds" in call_args.kwargs["params"]
        assert call_args.kwargs["params"]["parentIds"] == "123"


class TestCreateDirectory:
    """Tests for create_directory method."""

    @patch("pydrime.api.DrimeClient._request")
    def test_create_directory_root(self, mock_request):
        """Test creating directory in root."""
        mock_request.return_value = {"folder": {"id": 1, "name": "test_folder"}}

        client = DrimeClient(api_key="test_key")
        result = client.create_directory("test_folder")

        assert result["folder"]["name"] == "test_folder"
        call_args = mock_request.call_args
        assert call_args.kwargs["json"]["name"] == "test_folder"
        # parentId should not be in json when creating in root
        assert "parentId" not in call_args.kwargs["json"]

    @patch("pydrime.api.DrimeClient._request")
    def test_create_directory_with_parent(self, mock_request):
        """Test creating directory with parent ID."""
        mock_request.return_value = {"folder": {"id": 2, "name": "subfolder"}}

        client = DrimeClient(api_key="test_key")
        client.create_directory("subfolder", parent_id=1)

        call_args = mock_request.call_args
        assert call_args.kwargs["json"]["parentId"] == 1


class TestDeleteFileEntries:
    """Tests for delete_file_entries method."""

    @patch("pydrime.api.DrimeClient._request")
    def test_delete_files_to_trash(self, mock_request):
        """Test moving files to trash."""
        mock_request.return_value = {"status": "success"}

        client = DrimeClient(api_key="test_key")
        client.delete_file_entries([1, 2, 3], delete_forever=False)

        call_args = mock_request.call_args
        assert call_args.kwargs["json"]["entryIds"] == [1, 2, 3]
        assert call_args.kwargs["json"]["deleteForever"] is False

    @patch("pydrime.api.DrimeClient._request")
    def test_delete_files_permanently(self, mock_request):
        """Test permanently deleting files."""
        mock_request.return_value = {"status": "success"}

        client = DrimeClient(api_key="test_key")
        client.delete_file_entries([1], delete_forever=True)

        call_args = mock_request.call_args
        assert call_args.kwargs["json"]["deleteForever"] is True


class TestFolderResolution:
    """Tests for folder name resolution methods."""

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_get_folder_by_name_exact_match(self, mock_get_entries):
        """Test getting folder by exact name."""
        # Mock API response with folder data
        mock_get_entries.return_value = {
            "data": [
                {
                    "id": 123,
                    "name": "Documents",
                    "type": "folder",
                    "hash": "hash123",
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-01",
                    "public": False,
                    "description": None,
                    "users": [{"email": "test@example.com", "owns_entry": True}],
                }
            ]
        }

        client = DrimeClient(api_key="test_key")
        folder = client.get_folder_by_name("Documents")

        assert folder["id"] == 123
        assert folder["name"] == "Documents"
        assert folder["type"] == "folder"

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_get_folder_by_name_case_insensitive(self, mock_get_entries):
        """Test case-insensitive folder name match."""
        mock_get_entries.return_value = {
            "data": [
                {
                    "id": 456,
                    "name": "Documents",
                    "type": "folder",
                    "hash": "hash456",
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-01",
                    "public": False,
                    "description": None,
                    "users": [{"email": "test@example.com", "owns_entry": True}],
                }
            ]
        }

        client = DrimeClient(api_key="test_key")
        folder = client.get_folder_by_name("documents", case_sensitive=False)

        assert folder["id"] == 456
        assert folder["name"] == "Documents"

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_get_folder_by_name_not_found(self, mock_get_entries):
        """Test error when folder not found."""
        from pydrime.exceptions import DrimeNotFoundError

        mock_get_entries.return_value = {"data": []}

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeNotFoundError, match="Folder 'NotFound' not found"):
            client.get_folder_by_name("NotFound")

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_get_folder_by_name_with_parent(self, mock_get_entries):
        """Test getting folder by name in specific parent."""
        mock_get_entries.return_value = {
            "data": [
                {
                    "id": 789,
                    "name": "Subfolder",
                    "type": "folder",
                    "hash": "hash789",
                    "file_size": 0,
                    "parent_id": 123,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-01",
                    "public": False,
                    "description": None,
                    "users": [{"email": "test@example.com", "owns_entry": True}],
                }
            ]
        }

        client = DrimeClient(api_key="test_key")
        folder = client.get_folder_by_name("Subfolder", parent_id=123)

        assert folder["id"] == 789
        assert folder["parent_id"] == 123
        # Verify that parent_ids was passed to get_file_entries
        mock_get_entries.assert_called_once()
        call_kwargs = mock_get_entries.call_args.kwargs
        assert call_kwargs["parent_ids"] == [123]

    @patch("pydrime.api.DrimeClient.get_folder_by_name")
    def test_resolve_folder_identifier_numeric(self, mock_get_folder):
        """Test resolving numeric folder ID."""
        client = DrimeClient(api_key="test_key")
        folder_id = client.resolve_folder_identifier("480432024")

        # Should return the ID directly without calling get_folder_by_name
        assert folder_id == 480432024
        mock_get_folder.assert_not_called()

    @patch("pydrime.api.DrimeClient.get_folder_by_name")
    def test_resolve_folder_identifier_name(self, mock_get_folder):
        """Test resolving folder name to ID."""
        mock_get_folder.return_value = {
            "id": 999,
            "name": "MyFolder",
            "type": "folder",
        }

        client = DrimeClient(api_key="test_key")
        folder_id = client.resolve_folder_identifier("MyFolder", parent_id=123)

        assert folder_id == 999
        mock_get_folder.assert_called_once_with(
            folder_name="MyFolder", parent_id=123, case_sensitive=True, workspace_id=0
        )

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_get_folder_info(self, mock_get_entries):
        """Test getting folder information."""
        mock_get_entries.return_value = {
            "folder": {
                "id": 555,
                "name": "TestFolder",
                "type": "folder",
                "hash": "hash555",
                "file_size": 1024,
                "parent_id": 100,
                "created_at": "2024-01-01T12:00:00",
                "updated_at": "2024-01-02T12:00:00",
                "public": True,
                "description": "Test description",
                "users": [{"email": "owner@example.com", "owns_entry": True}],
            },
            "data": [],
        }

        client = DrimeClient(api_key="test_key")
        folder_info = client.get_folder_info(555)

        assert folder_info["id"] == 555
        assert folder_info["name"] == "TestFolder"
        assert folder_info["hash"] == "hash555"
        assert folder_info["parent_id"] == 100
        assert folder_info["owner"] == "owner@example.com"
        assert folder_info["public"] is True
        # Check that folder_id parameter was used (with hash)
        mock_get_entries.assert_called_once()
        call_kwargs = mock_get_entries.call_args.kwargs
        assert "folder_id" in call_kwargs

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_get_folder_info_not_found(self, mock_get_entries):
        """Test error when folder info not found."""
        from pydrime.exceptions import DrimeNotFoundError

        mock_get_entries.return_value = {"data": [], "folder": None}

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeNotFoundError, match="Folder with ID 999 not found"):
            client.get_folder_info(999)

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_get_folder_info_wrong_id_match(self, mock_get_entries):
        """Test error when query returns different folder."""
        from pydrime.exceptions import DrimeNotFoundError

        # Query for 555 but get back folder with different ID
        mock_get_entries.return_value = {
            "folder": {
                "id": 777,  # Different ID
                "name": "OtherFolder",
                "type": "folder",
                "hash": "hash777",
                "file_size": 0,
                "parent_id": None,
                "created_at": "2024-01-01",
                "updated_at": "2024-01-01",
                "public": False,
                "description": None,
                "users": [{"email": "test@example.com", "owns_entry": True}],
            },
            "data": [],
        }

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeNotFoundError, match="Folder with ID 555 not found"):
            client.get_folder_info(555)

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_entry_identifier_numeric(self, mock_get_entries):
        """Test resolving numeric entry ID."""
        client = DrimeClient(api_key="test_key")
        entry_id = client.resolve_entry_identifier("480424796")

        # Should return the ID directly without calling get_file_entries
        assert entry_id == 480424796
        mock_get_entries.assert_not_called()

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_entry_identifier_by_name(self, mock_get_entries):
        """Test resolving entry name to ID."""
        mock_get_entries.return_value = {
            "data": [
                {
                    "id": 999,
                    "name": "test.txt",
                    "type": "text",
                    "hash": "hash999",
                    "file_size": 1024,
                    "parent_id": 123,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-01",
                    "public": False,
                    "description": None,
                    "users": [{"email": "test@example.com", "owns_entry": True}],
                }
            ]
        }

        client = DrimeClient(api_key="test_key")
        entry_id = client.resolve_entry_identifier(
            "test.txt", parent_id=123, workspace_id=0
        )

        assert entry_id == 999
        mock_get_entries.assert_called_once()
        call_kwargs = mock_get_entries.call_args.kwargs
        assert call_kwargs["query"] == "test.txt"
        assert call_kwargs["parent_ids"] == [123]
        assert call_kwargs["workspace_id"] == 0

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_entry_identifier_not_found(self, mock_get_entries):
        """Test error when entry not found."""
        from pydrime.exceptions import DrimeNotFoundError

        mock_get_entries.return_value = {"data": []}

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeNotFoundError, match="Entry 'notfound.txt' not found"):
            client.resolve_entry_identifier("notfound.txt")

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_entry_identifier_case_insensitive(self, mock_get_entries):
        """Test case-insensitive entry resolution."""
        mock_get_entries.return_value = {
            "data": [
                {
                    "id": 888,
                    "name": "Test.TXT",
                    "type": "text",
                    "hash": "hash888",
                    "file_size": 512,
                    "parent_id": None,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-01",
                    "public": False,
                    "description": None,
                    "users": [{"email": "test@example.com", "owns_entry": True}],
                }
            ]
        }

        client = DrimeClient(api_key="test_key")
        entry_id = client.resolve_entry_identifier("test.txt")

        assert entry_id == 888


class TestResolvePathToId:
    """Tests for resolve_path_to_id method."""

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_path_single_file(self, mock_get_entries):
        """Test resolving a single-level path (file in root)."""

        mock_get_entries.return_value = {
            "data": [
                {
                    "id": 100,
                    "name": "test_file.txt",
                    "type": "text",
                    "hash": "hash100",
                    "file_size": 1024,
                    "parent_id": None,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-01",
                    "public": False,
                    "description": None,
                    "users": [{"email": "test@example.com", "owns_entry": True}],
                }
            ]
        }

        client = DrimeClient(api_key="test_key")
        entry_id = client.resolve_path_to_id("test_file.txt")

        assert entry_id == 100

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_path_nested(self, mock_get_entries):
        """Test resolving a nested path (folder/file.txt)."""

        def get_entries_side_effect(**kwargs):
            parent_ids = kwargs.get("parent_ids")
            if parent_ids is None:
                # Root level - return folder
                return {
                    "data": [
                        {
                            "id": 200,
                            "name": "my_folder",
                            "type": "folder",
                            "hash": "hash200",
                            "file_size": 0,
                            "parent_id": None,
                            "created_at": "2024-01-01",
                            "updated_at": "2024-01-01",
                            "public": False,
                            "description": None,
                            "users": [
                                {"email": "test@example.com", "owns_entry": True}
                            ],
                        }
                    ]
                }
            elif parent_ids == [200]:
                # Inside folder - return file
                return {
                    "data": [
                        {
                            "id": 300,
                            "name": "test_file.txt",
                            "type": "text",
                            "hash": "hash300",
                            "file_size": 1024,
                            "parent_id": 200,
                            "created_at": "2024-01-01",
                            "updated_at": "2024-01-01",
                            "public": False,
                            "description": None,
                            "users": [
                                {"email": "test@example.com", "owns_entry": True}
                            ],
                        }
                    ]
                }
            return {"data": []}

        mock_get_entries.side_effect = get_entries_side_effect

        client = DrimeClient(api_key="test_key")
        entry_id = client.resolve_path_to_id("my_folder/test_file.txt")

        assert entry_id == 300

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_path_with_leading_slash(self, mock_get_entries):
        """Test resolving an absolute path starting with /."""
        mock_get_entries.return_value = {
            "data": [
                {
                    "id": 100,
                    "name": "test_file.txt",
                    "type": "text",
                    "hash": "hash100",
                    "file_size": 1024,
                    "parent_id": None,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-01",
                    "public": False,
                    "description": None,
                    "users": [{"email": "test@example.com", "owns_entry": True}],
                }
            ]
        }

        client = DrimeClient(api_key="test_key")
        entry_id = client.resolve_path_to_id("/test_file.txt")

        assert entry_id == 100

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_path_not_found(self, mock_get_entries):
        """Test error when path not found."""
        from pydrime.exceptions import DrimeNotFoundError

        mock_get_entries.return_value = {"data": []}

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeNotFoundError, match="Path not found"):
            client.resolve_path_to_id("nonexistent/file.txt")

    @patch("pydrime.api.DrimeClient.get_file_entries")
    def test_resolve_path_intermediate_not_folder(self, mock_get_entries):
        """Test error when intermediate path component is not a folder."""
        from pydrime.exceptions import DrimeNotFoundError

        mock_get_entries.return_value = {
            "data": [
                {
                    "id": 100,
                    "name": "not_a_folder",
                    "type": "text",  # File, not folder
                    "hash": "hash100",
                    "file_size": 1024,
                    "parent_id": None,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-01",
                    "public": False,
                    "description": None,
                    "users": [{"email": "test@example.com", "owns_entry": True}],
                }
            ]
        }

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeNotFoundError, match="is not a folder"):
            client.resolve_path_to_id("not_a_folder/file.txt")


class TestFolderCount:
    """Tests for folder count method."""

    @patch("pydrime.api.DrimeClient._request")
    def test_get_folder_count_success(self, mock_request):
        """Test getting folder count."""
        mock_request.return_value = {"count": 16, "status": "success", "seo": None}

        client = DrimeClient(api_key="test_key")
        count = client.get_folder_count(481967773)

        assert count == 16
        mock_request.assert_called_once_with("GET", "/folders/481967773/count")

    @patch("pydrime.api.DrimeClient._request")
    def test_get_folder_count_empty_folder(self, mock_request):
        """Test getting count for empty folder."""
        mock_request.return_value = {"count": 0, "status": "success", "seo": None}

        client = DrimeClient(api_key="test_key")
        count = client.get_folder_count(123456)

        assert count == 0

    @patch("pydrime.api.DrimeClient._request")
    def test_get_folder_count_missing_count_key(self, mock_request):
        """Test getting count when response missing count key."""
        mock_request.return_value = {"status": "success"}

        client = DrimeClient(api_key="test_key")
        count = client.get_folder_count(123456)

        assert count == 0  # Should default to 0


class TestNotifications:
    """Tests for notification methods."""

    @patch("pydrime.api.DrimeClient._request")
    def test_get_notifications_success(self, mock_request):
        """Test getting notifications."""
        mock_request.return_value = {
            "pagination": {
                "current_page": 1,
                "data": [
                    {
                        "id": "e0d8de26-4016-489a-94f3-361bd7127b9b",
                        "type": "App\\Notifications\\FileEntrySharedNotif",
                        "notifiable_type": "App\\User",
                        "notifiable_id": 18001,
                        "data": {
                            "mainAction": {"action": ""},
                            "lines": [
                                {"content": "User shared a file with you"},
                                {
                                    "icon": "text",
                                    "content": "test_file.txt",
                                    "action": {"action": "/drive/shares"},
                                },
                            ],
                        },
                        "read_at": None,
                        "created_at": "2025-11-25T10:38:03.000000Z",
                        "updated_at": "2025-11-25T10:38:03.000000Z",
                    }
                ],
                "from": 1,
                "last_page": 1,
                "next_page": None,
                "per_page": 10,
                "prev_page": None,
                "to": 1,
                "total": 1,
            },
            "status": "success",
            "seo": None,
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_notifications()

        assert result["status"] == "success"
        assert result["pagination"]["current_page"] == 1
        assert len(result["pagination"]["data"]) == 1
        assert (
            result["pagination"]["data"][0]["id"]
            == "e0d8de26-4016-489a-94f3-361bd7127b9b"
        )
        mock_request.assert_called_once_with(
            "GET", "/notifications", params={"perPage": 10, "page": 1, "workspaceId": 0}
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_notifications_with_pagination(self, mock_request):
        """Test getting notifications with custom pagination."""
        mock_request.return_value = {
            "pagination": {
                "current_page": 2,
                "data": [],
                "from": 21,
                "last_page": 2,
                "next_page": None,
                "per_page": 20,
                "prev_page": 1,
                "to": 25,
                "total": 25,
            },
            "status": "success",
            "seo": None,
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_notifications(per_page=20, page=2)

        assert result["pagination"]["current_page"] == 2
        assert result["pagination"]["per_page"] == 20
        mock_request.assert_called_once_with(
            "GET", "/notifications", params={"perPage": 20, "page": 2, "workspaceId": 0}
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_notifications_empty(self, mock_request):
        """Test getting notifications when none exist."""
        mock_request.return_value = {
            "pagination": {
                "current_page": 1,
                "data": [],
                "from": None,
                "last_page": 1,
                "next_page": None,
                "per_page": 10,
                "prev_page": None,
                "to": None,
                "total": 0,
            },
            "status": "success",
            "seo": None,
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_notifications()

        assert result["pagination"]["total"] == 0
        assert result["pagination"]["data"] == []


class TestVault:
    """Tests for vault methods."""

    @patch("pydrime.api.DrimeClient._request")
    def test_get_vault_success(self, mock_request):
        """Test getting vault information."""
        mock_request.return_value = {
            "vault": {
                "id": 784,
                "user_id": 18001,
                "salt": "HQl1b8OV5ytpaQ2OeLVcmw==",
                "check": "pGwm7bCIZLq8zSm9YOUGqDmuUu6z98/dFa5gnQ==",
                "iv": "6tJwX855hm2TB5zG",
                "created_at": "2025-11-24T08:58:15.000000Z",
                "updated_at": "2025-11-24T08:58:15.000000Z",
            },
            "status": "success",
            "seo": None,
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_vault()

        assert result["status"] == "success"
        assert result["vault"]["id"] == 784
        assert result["vault"]["user_id"] == 18001
        assert result["vault"]["salt"] == "HQl1b8OV5ytpaQ2OeLVcmw=="
        assert result["vault"]["iv"] == "6tJwX855hm2TB5zG"
        mock_request.assert_called_once_with("GET", "/vault")

    @patch("pydrime.api.DrimeClient._request")
    def test_get_vault_no_vault(self, mock_request):
        """Test getting vault when none exists."""
        mock_request.return_value = {
            "vault": None,
            "status": "success",
            "seo": None,
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_vault()

        assert result["status"] == "success"
        assert result["vault"] is None

    @patch("pydrime.api.DrimeClient._request")
    def test_get_vault_file_entries_success(self, mock_request):
        """Test getting vault file entries."""
        mock_request.return_value = {
            "data": [
                {
                    "id": 34431,
                    "name": "encrypted_file.txt",
                    "type": "text",
                    "file_size": 1024,
                    "parent_id": 34430,
                    "is_encrypted": 1,
                    "vault_id": 784,
                    "hash": "abc123",
                    "created_at": "2025-11-25T12:00:00.000000Z",
                    "updated_at": "2025-11-25T12:00:00.000000Z",
                }
            ],
            "status": "success",
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_vault_file_entries()

        assert result["status"] == "success"
        assert len(result["data"]) == 1
        assert result["data"][0]["name"] == "encrypted_file.txt"
        assert result["data"][0]["is_encrypted"] == 1
        mock_request.assert_called_once_with(
            "GET",
            "/vault/file-entries",
            params={
                "page": 1,
                "perPage": 50,
                "orderBy": "updated_at",
                "orderDir": "desc",
                "backup": 0,
            },
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_vault_file_entries_with_folder(self, mock_request):
        """Test getting vault file entries from specific folder."""
        mock_request.return_value = {
            "data": [
                {
                    "id": 34432,
                    "name": "secret.pdf",
                    "type": "pdf",
                    "is_encrypted": 1,
                    "vault_id": 784,
                }
            ],
            "status": "success",
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_vault_file_entries(
            folder_hash="MzQ0MzB8cGFkZA",
            page=2,
            per_page=20,
            order_by="name",
            order_dir="asc",
        )

        assert len(result["data"]) == 1
        mock_request.assert_called_once_with(
            "GET",
            "/vault/file-entries",
            params={
                "folderId": "MzQ0MzB8cGFkZA",
                "pageId": "MzQ0MzB8cGFkZA",
                "page": 2,
                "perPage": 20,
                "orderBy": "name",
                "orderDir": "asc",
                "backup": 0,
            },
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_vault_file_entries_empty(self, mock_request):
        """Test getting vault file entries when empty."""
        mock_request.return_value = {
            "data": [],
            "status": "success",
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_vault_file_entries()

        assert result["data"] == []


class TestVaultDownload:
    """Tests for vault download method."""

    def test_download_vault_file_success(self, tmp_path):
        """Test downloading a vault file successfully."""
        from contextlib import contextmanager
        from unittest.mock import Mock, patch

        # Create mock response for stream context manager
        mock_response = Mock()
        mock_response.headers = {
            "Content-Disposition": 'attachment; filename="secret.txt"',
            "Content-Length": "12",
        }
        mock_response.iter_bytes.return_value = [b"vault content"]
        mock_response.raise_for_status.return_value = None

        @contextmanager
        def mock_stream(*args, **kwargs):
            yield mock_response

        with patch("pydrime.api.httpx.Client") as mock_client_class:
            mock_client = Mock()
            mock_client.stream = mock_stream
            mock_client.is_closed = False
            mock_client_class.return_value = mock_client

            client = DrimeClient(api_key="test_key")
            output_path = tmp_path / "downloaded.txt"
            result = client.download_vault_file(
                "MzQ0MzF8cGFkZA", output_path=output_path
            )

            assert result == output_path
            assert output_path.read_bytes() == b"vault content"

    def test_download_vault_file_with_progress(self, tmp_path):
        """Test vault download with progress callback."""
        from contextlib import contextmanager
        from unittest.mock import Mock, patch

        mock_response = Mock()
        mock_response.headers = {
            "Content-Disposition": 'attachment; filename="file.txt"',
            "Content-Length": "100",
        }
        mock_response.iter_bytes.return_value = [b"a" * 50, b"b" * 50]
        mock_response.raise_for_status.return_value = None

        progress_calls = []

        def progress_callback(downloaded, total):
            progress_calls.append((downloaded, total))

        @contextmanager
        def mock_stream(*args, **kwargs):
            yield mock_response

        with patch("pydrime.api.httpx.Client") as mock_client_class:
            mock_client = Mock()
            mock_client.stream = mock_stream
            mock_client.is_closed = False
            mock_client_class.return_value = mock_client

            client = DrimeClient(api_key="test_key")
            output_path = tmp_path / "file.txt"
            client.download_vault_file(
                "hash123", output_path=output_path, progress_callback=progress_callback
            )

            assert len(progress_calls) == 2
            assert progress_calls[0] == (50, 100)
            assert progress_calls[1] == (100, 100)

    def test_download_vault_file_default_filename(self, tmp_path, monkeypatch):
        """Test vault download uses default filename when none provided."""
        from contextlib import contextmanager
        from unittest.mock import Mock, patch

        # Change to tmp_path so the file is created there
        monkeypatch.chdir(tmp_path)

        mock_response = Mock()
        mock_response.headers = {"Content-Length": "5"}
        mock_response.iter_bytes.return_value = [b"hello"]
        mock_response.raise_for_status.return_value = None

        @contextmanager
        def mock_stream(*args, **kwargs):
            yield mock_response

        with patch("pydrime.api.httpx.Client") as mock_client_class:
            mock_client = Mock()
            mock_client.stream = mock_stream
            mock_client.is_closed = False
            mock_client_class.return_value = mock_client

            client = DrimeClient(api_key="test_key")
            result = client.download_vault_file("abc123")

            # Should use vault_ prefix for default filename
            assert result.name == "vault_abc123"

    def test_download_vault_file_http_error(self):
        """Test vault download handles HTTP errors."""
        from contextlib import contextmanager
        from unittest.mock import Mock, patch

        import httpx

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=Mock(), response=mock_response
        )

        @contextmanager
        def mock_stream(*args, **kwargs):
            yield mock_response

        with patch("pydrime.api.httpx.Client") as mock_client_class:
            mock_client = Mock()
            mock_client.stream = mock_stream
            mock_client.is_closed = False
            mock_client_class.return_value = mock_client

            client = DrimeClient(api_key="test_key")

            with pytest.raises(DrimeDownloadError) as exc_info:
                client.download_vault_file("invalid_hash")

            assert "Vault download failed" in str(exc_info.value)


class TestGetFileContent:
    """Tests for get_file_content method."""

    def test_get_file_content_full_file(self):
        """Test getting entire file content."""
        from contextlib import contextmanager
        from unittest.mock import Mock, patch

        mock_response = Mock()
        mock_response.read.return_value = b"Hello World!"
        mock_response.raise_for_status.return_value = None

        @contextmanager
        def mock_stream(*args, **kwargs):
            yield mock_response

        with patch("pydrime.api.httpx.Client") as mock_client_class:
            mock_client = Mock()
            mock_client.stream = mock_stream
            mock_client.is_closed = False
            mock_client_class.return_value = mock_client

            client = DrimeClient(api_key="test_key")
            content = client.get_file_content("testhash")

            assert content == b"Hello World!"

    def test_get_file_content_with_max_bytes(self):
        """Test getting file content with byte limit."""
        from contextlib import contextmanager
        from unittest.mock import Mock, patch

        mock_response = Mock()
        mock_response.iter_bytes.return_value = [b"Hello", b" World!"]
        mock_response.raise_for_status.return_value = None

        @contextmanager
        def mock_stream(*args, **kwargs):
            yield mock_response

        with patch("pydrime.api.httpx.Client") as mock_client_class:
            mock_client = Mock()
            mock_client.stream = mock_stream
            mock_client.is_closed = False
            mock_client_class.return_value = mock_client

            client = DrimeClient(api_key="test_key")
            content = client.get_file_content("testhash", max_bytes=5)

            assert content == b"Hello"

    def test_get_file_content_http_error(self):
        """Test get_file_content raises error on HTTP failure."""
        from contextlib import contextmanager
        from unittest.mock import Mock, patch

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=Mock(), response=Mock()
        )

        @contextmanager
        def mock_stream(*args, **kwargs):
            yield mock_response

        with patch("pydrime.api.httpx.Client") as mock_client_class:
            mock_client = Mock()
            mock_client.stream = mock_stream
            mock_client.is_closed = False
            mock_client_class.return_value = mock_client

            client = DrimeClient(api_key="test_key")

            with pytest.raises(DrimeDownloadError) as exc_info:
                client.get_file_content("invalid_hash")

            assert "Download failed" in str(exc_info.value)


class TestFolderPath:
    """Tests for folder path method."""

    @patch("pydrime.api.DrimeClient._request")
    def test_get_folder_path_regular_folder(self, mock_request):
        """Test getting path for a regular folder."""
        mock_request.return_value = {
            "path": [
                {
                    "id": 481003603,
                    "name": "restic",
                    "description": None,
                    "file_name": "restic",
                    "file_size": 414573494633,
                    "parent_id": None,
                    "created_at": "2025-11-21T19:21:26.000000Z",
                    "updated_at": "2025-11-25T11:21:54.000000Z",
                    "type": "folder",
                    "public": False,
                    "workspace_id": 0,
                    "is_encrypted": 0,
                    "vault_id": None,
                    "owner_id": 18001,
                    "permissions": {
                        "files.update": True,
                        "files.create": True,
                        "files.download": True,
                        "files.delete": True,
                    },
                    "hash": "NDgxMDAzNjAzfA",
                    "users": [
                        {
                            "email": "user@example.com",
                            "id": 18001,
                            "owns_entry": True,
                        }
                    ],
                    "tags": [],
                }
            ],
            "status": "success",
            "seo": None,
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_folder_path("NDgxMDAzNjAzfA")

        assert result["status"] == "success"
        assert len(result["path"]) == 1
        assert result["path"][0]["id"] == 481003603
        assert result["path"][0]["name"] == "restic"
        assert result["path"][0]["is_encrypted"] == 0
        assert result["path"][0]["vault_id"] is None
        mock_request.assert_called_once_with(
            "GET", "/folders/NDgxMDAzNjAzfA/path", params=None
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_folder_path_vault_folder(self, mock_request):
        """Test getting path for a vault folder."""
        mock_request.return_value = {
            "path": [
                {
                    "id": 34430,
                    "name": "Test1",
                    "description": None,
                    "file_name": "Test1",
                    "mime": None,
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2025-11-25T12:37:28.000000Z",
                    "updated_at": "2025-11-25T12:37:28.000000Z",
                    "deleted_at": None,
                    "path": "34430",
                    "type": "folder",
                    "public": False,
                    "is_encrypted": 1,
                    "vault_id": 784,
                    "owner_id": 18001,
                    "permissions": {
                        "files.update": True,
                        "files.create": False,
                        "files.download": True,
                        "files.delete": True,
                    },
                    "hash": "MzQ0MzB8cGFkZA",
                    "tags": [],
                }
            ],
            "status": "success",
            "seo": None,
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_folder_path("MzQ0MzB8cGFkZA", vault_id=784)

        assert result["status"] == "success"
        assert len(result["path"]) == 1
        assert result["path"][0]["id"] == 34430
        assert result["path"][0]["name"] == "Test1"
        assert result["path"][0]["is_encrypted"] == 1
        assert result["path"][0]["vault_id"] == 784
        assert result["path"][0]["hash"] == "MzQ0MzB8cGFkZA"
        mock_request.assert_called_once_with(
            "GET", "/folders/MzQ0MzB8cGFkZA/path", params={"vaultId": 784}
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_folder_path_nested(self, mock_request):
        """Test getting folder path with nested folders."""
        mock_request.return_value = {
            "path": [
                {
                    "id": 34430,
                    "name": "Root",
                    "parent_id": None,
                    "type": "folder",
                    "is_encrypted": 0,
                    "vault_id": None,
                    "hash": "hash1",
                },
                {
                    "id": 34431,
                    "name": "Child",
                    "parent_id": 34430,
                    "type": "folder",
                    "is_encrypted": 0,
                    "vault_id": None,
                    "hash": "hash2",
                },
            ],
            "status": "success",
            "seo": None,
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_folder_path("hash2")

        assert len(result["path"]) == 2
        assert result["path"][0]["name"] == "Root"
        assert result["path"][1]["name"] == "Child"
        assert result["path"][1]["parent_id"] == 34430


class TestUploadValidation:
    """Tests for upload validation methods."""

    @patch("pydrime.api.DrimeClient._request")
    def test_validate_uploads_no_duplicates(self, mock_request):
        """Test validating uploads with no duplicates."""
        mock_request.return_value = {"duplicates": []}

        client = DrimeClient(api_key="test_key")
        files = [
            {"name": "test.txt", "size": 1024, "relativePath": ""},
            {"name": "doc.pdf", "size": 2048, "relativePath": "docs/"},
        ]
        result = client.validate_uploads(files, workspace_id=0)

        assert result == {"duplicates": []}
        mock_request.assert_called_once_with(
            "POST",
            "/uploads/validate",
            json={"files": files, "workspaceId": 0},
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_validate_uploads_with_duplicates(self, mock_request):
        """Test validating uploads with duplicates detected."""
        mock_request.return_value = {"duplicates": ["test.txt", "docs"]}

        client = DrimeClient(api_key="test_key")
        files = [
            {"name": "test.txt", "size": 1024, "relativePath": ""},
            {"name": "doc.pdf", "size": 2048, "relativePath": "docs/"},
        ]
        result = client.validate_uploads(files, workspace_id=5)

        assert result["duplicates"] == ["test.txt", "docs"]
        mock_request.assert_called_once_with(
            "POST",
            "/uploads/validate",
            json={"files": files, "workspaceId": 5},
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_available_name_success(self, mock_request):
        """Test getting available name for duplicate."""
        mock_request.return_value = {"available": "document (1).pdf"}

        client = DrimeClient(api_key="test_key")
        new_name = client.get_available_name("document.pdf", workspace_id=0)

        assert new_name == "document (1).pdf"
        mock_request.assert_called_once_with(
            "POST",
            "/entry/getAvailableName",
            json={"name": "document.pdf", "workspaceId": 0},
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_available_name_with_workspace(self, mock_request):
        """Test getting available name in specific workspace."""
        mock_request.return_value = {"available": "test (2).txt"}

        client = DrimeClient(api_key="test_key")
        new_name = client.get_available_name("test.txt", workspace_id=10)

        assert new_name == "test (2).txt"
        mock_request.assert_called_once_with(
            "POST",
            "/entry/getAvailableName",
            json={"name": "test.txt", "workspaceId": 10},
        )

    @patch("pydrime.api.DrimeClient._request")
    def test_get_available_name_no_available_returned(self, mock_request):
        """Test error when no available name is returned."""
        from pydrime.exceptions import DrimeAPIError

        mock_request.return_value = {}

        client = DrimeClient(api_key="test_key")
        with pytest.raises(
            DrimeAPIError, match="Could not get available name for 'test.txt'"
        ):
            client.get_available_name("test.txt", workspace_id=0)

    @patch("pydrime.api.DrimeClient._request")
    def test_get_available_name_folder(self, mock_request):
        """Test getting available name for a folder."""
        mock_request.return_value = {"available": "Documents (1)"}

        client = DrimeClient(api_key="test_key")
        new_name = client.get_available_name("Documents", workspace_id=0)

        assert new_name == "Documents (1)"
        mock_request.assert_called_once_with(
            "POST",
            "/entry/getAvailableName",
            json={"name": "Documents", "workspaceId": 0},
        )


class TestMimeTypeDetection:
    """Tests for MIME type detection."""

    def test_detect_mime_type_text_file(self):
        """Test MIME type detection for text file."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("test content")
            test_file = Path(f.name)

        try:
            client = DrimeClient(api_key="test_key")
            mime_type = client._detect_mime_type(test_file)

            # Should detect text/plain
            assert mime_type == "text/plain"
        finally:
            test_file.unlink()

    def test_detect_mime_type_json_file(self):
        """Test MIME type detection for JSON file."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"test": "data"}')
            test_file = Path(f.name)

        try:
            client = DrimeClient(api_key="test_key")
            mime_type = client._detect_mime_type(test_file)

            # Should detect application/json
            assert mime_type == "application/json"
        finally:
            test_file.unlink()

    def test_detect_mime_type_binary_file(self):
        """Test MIME type detection for binary file."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".bin", delete=False) as f:
            f.write(b"\x00\x01\x02\x03")
            test_file = Path(f.name)

        try:
            client = DrimeClient(api_key="test_key")
            mime_type = client._detect_mime_type(test_file)

            # Should detect application/octet-stream as fallback
            assert mime_type == "application/octet-stream"
        finally:
            test_file.unlink()

    def test_detect_mime_type_unknown_extension(self):
        """Test MIME type detection for file with unknown extension."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".unknownext", delete=False
        ) as f:
            f.write("test content")
            test_file = Path(f.name)

        try:
            client = DrimeClient(api_key="test_key")
            mime_type = client._detect_mime_type(test_file)

            # python-magic (if installed) detects content as text/plain
            # mimetypes module falls back to application/octet-stream
            # Both are acceptable results
            assert mime_type in ["text/plain", "application/octet-stream"]
        finally:
            test_file.unlink()

    def test_detect_mime_type_no_extension(self):
        """Test MIME type detection for file without extension."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix="", delete=False) as f:
            f.write("test content")
            test_file = Path(f.name)

        try:
            client = DrimeClient(api_key="test_key")
            mime_type = client._detect_mime_type(test_file)

            # Should return some mime type or fallback
            assert mime_type is not None
            assert isinstance(mime_type, str)
        finally:
            test_file.unlink()

    @patch("pydrime.api.DrimeClient._detect_mime_type")
    @patch("pydrime.api.DrimeClient._get_client")
    def test_upload_file_uses_mime_detection_small_file(
        self, mock_get_client, mock_detect_mime
    ):
        """Test that upload_file uses MIME detection for small files."""
        import tempfile
        from pathlib import Path
        from unittest.mock import MagicMock

        mock_detect_mime.return_value = "text/plain"

        # Mock the HTTP client response (upload_file_simple uses client.post)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "fileEntry": {"id": 123},
        }

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_get_client.return_value = mock_client

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("small file")
            test_file = Path(f.name)

        try:
            client = DrimeClient(api_key="test_key")
            # Upload small file (below multipart threshold), skip verification
            client.upload_file(
                test_file,
                use_multipart_threshold=1024 * 1024,
                verify_upload=False,
            )

            # Verify MIME detection was called
            mock_detect_mime.assert_called_once_with(test_file)
        finally:
            test_file.unlink()

    @patch("pydrime.api.DrimeClient._detect_mime_type")
    @patch("pydrime.api.DrimeClient._request")
    @patch("pydrime.api.httpx.request")
    @patch("builtins.open", create=True)
    @patch("pathlib.Path.stat")
    @patch("pathlib.Path.exists")
    def test_upload_file_multipart_uses_mime_detection(
        self,
        mock_exists,
        mock_stat,
        mock_open,
        mock_httpx_request,
        mock_request,
        mock_detect_mime,
    ):
        """Test that upload_file_multipart uses MIME detection."""
        from pathlib import Path
        from unittest.mock import MagicMock

        # Mock file existence and size
        mock_exists.return_value = True
        mock_stat_obj = MagicMock()
        mock_stat_obj.st_size = 2 * 1024 * 1024  # 2MB
        mock_stat.return_value = mock_stat_obj

        # Mock file reading
        mock_file = MagicMock()
        mock_file.read.return_value = b"x" * (2 * 1024 * 1024)
        mock_open.return_value.__enter__.return_value = mock_file

        # Mock MIME detection and API responses
        mock_detect_mime.return_value = "application/octet-stream"
        mock_request.side_effect = [
            {"uploadId": "test-upload-id", "key": "test-key"},
            {"urls": [{"partNumber": 1, "url": "https://s3.example.com/upload"}]},
            {},  # complete response
            {"status": "success", "fileEntry": {"id": 123}},  # entry creation
        ]

        # Mock S3 request response (now using httpx.request instead of httpx.put)
        mock_s3_response = Mock()
        mock_s3_response.headers = {"ETag": "test-etag"}
        mock_httpx_request.return_value = mock_s3_response

        test_file = Path("/fake/path/test.bin")

        client = DrimeClient(api_key="test_key")
        # Upload with low threshold to trigger multipart
        # Disable verification to avoid needing additional mocked responses
        client.upload_file(
            test_file,
            use_multipart_threshold=1 * 1024 * 1024,  # 1MB
            verify_upload=False,
        )

        # Verify MIME detection was called
        mock_detect_mime.assert_called_once_with(test_file)


class TestUploadVerification:
    """Tests for upload verification and retry logic."""

    @patch("pydrime.api.DrimeClient._detect_mime_type")
    @patch("pydrime.api.DrimeClient._get_client")
    def test_upload_file_with_verification_success(
        self, mock_get_client, mock_detect_mime
    ):
        """Test upload with successful verification."""
        import tempfile
        from pathlib import Path
        from unittest.mock import MagicMock

        mock_detect_mime.return_value = "text/plain"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("test content")
            test_file = Path(f.name)

        file_size = test_file.stat().st_size

        # Mock the HTTP client response (upload_file_simple uses client.post)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "fileEntry": {
                "id": 123,
                "file_size": file_size,
                "users": [{"id": 1, "email": "test@example.com"}],
            },
        }

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_get_client.return_value = mock_client

        try:
            client = DrimeClient(api_key="test_key")
            result = client.upload_file(
                test_file,
                use_multipart_threshold=1024 * 1024,
                verify_upload=True,
            )

            assert result["status"] == "success"
            assert result["fileEntry"]["id"] == 123
        finally:
            test_file.unlink()

    @patch("pydrime.api.DrimeClient._detect_mime_type")
    @patch("pydrime.api.DrimeClient._get_client")
    @patch("pydrime.api.DrimeClient._request")
    def test_upload_file_retry_on_missing_users(
        self, mock_request, mock_get_client, mock_detect_mime
    ):
        """Test upload retries when users field is missing in response."""
        import tempfile
        from pathlib import Path
        from unittest.mock import MagicMock

        mock_detect_mime.return_value = "text/plain"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("test content")
            test_file = Path(f.name)

        file_size = test_file.stat().st_size
        messages = []

        # Mock HTTP client responses for simple upload
        # First response: missing users -> triggers retry
        # Second response: with users -> success
        mock_response1 = MagicMock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {
            "status": "success",
            "fileEntry": {"id": 123, "file_size": file_size},
        }

        mock_response2 = MagicMock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {
            "status": "success",
            "fileEntry": {
                "id": 456,
                "file_size": file_size,
                "users": [{"id": 1, "email": "test@example.com"}],
            },
        }

        mock_client = MagicMock()
        mock_client.post.side_effect = [mock_response1, mock_response2]
        mock_get_client.return_value = mock_client

        # Mock delete call (for cleaning up failed entry)
        mock_request.return_value = {"status": "success"}

        try:
            client = DrimeClient(api_key="test_key")
            result = client.upload_file(
                test_file,
                use_multipart_threshold=1024 * 1024,
                verify_upload=True,
                message_callback=lambda msg: messages.append(msg),
            )

            assert result["status"] == "success"
            assert result["fileEntry"]["id"] == 456
            # Verify message callback was called
            assert len(messages) > 0
            assert "Retrying" in messages[0]
        finally:
            test_file.unlink()

    @patch("pydrime.api.DrimeClient._detect_mime_type")
    @patch("pydrime.api.DrimeClient._get_client")
    @patch("pydrime.api.DrimeClient._request")
    def test_upload_file_fails_after_max_retries(
        self, mock_request, mock_get_client, mock_detect_mime
    ):
        """Test upload fails after maximum retries."""
        import tempfile
        from pathlib import Path
        from unittest.mock import MagicMock

        import pytest

        from pydrime.exceptions import DrimeUploadError

        mock_detect_mime.return_value = "text/plain"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("test content")
            test_file = Path(f.name)

        file_size = test_file.stat().st_size

        # All attempts return missing users -> all fail verification
        def create_response():
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "status": "success",
                "fileEntry": {"id": 123, "file_size": file_size},
            }
            return mock_response

        mock_client = MagicMock()
        mock_client.post.side_effect = [
            create_response(),
            create_response(),
            create_response(),
        ]
        mock_get_client.return_value = mock_client

        # Mock delete calls (for cleaning up failed entries)
        mock_request.return_value = {"status": "success"}

        try:
            client = DrimeClient(api_key="test_key")
            with pytest.raises(DrimeUploadError) as exc_info:
                client.upload_file(
                    test_file,
                    use_multipart_threshold=1024 * 1024,
                    verify_upload=True,
                    max_upload_retries=3,
                )

            assert "verification failed after 3 attempts" in str(exc_info.value)
        finally:
            test_file.unlink()

    @patch("pydrime.api.DrimeClient._request")
    def test_get_file_entry(self, mock_request):
        """Test get_file_entry method."""
        mock_request.return_value = {
            "fileEntry": {
                "id": 123,
                "name": "test.txt",
                "file_size": 1024,
                "users": [{"id": 1}],
            }
        }

        client = DrimeClient(api_key="test_key")
        result = client.get_file_entry(123, workspace_id=0)

        mock_request.assert_called_once_with(
            "GET", "/file-entries/123", params={"workspaceId": 0}
        )
        assert result["fileEntry"]["id"] == 123


class TestExceptions:
    """Tests for exception classes."""

    def test_drime_file_not_found_error(self):
        """Test DrimeFileNotFoundError with file path."""
        file_path = "/tmp/test.txt"
        error = DrimeFileNotFoundError(file_path)

        assert str(error) == "File not found: /tmp/test.txt"
        assert error.file_path == file_path
