"""Unit tests for the Drime API client."""

from unittest.mock import Mock, patch

import pytest
import requests

from pydrime.api import DrimeClient
from pydrime.exceptions import (
    DrimeAPIError,
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
        """Test that session headers include authorization."""
        client = DrimeClient(api_key="test_key")
        assert "Authorization" in client.session.headers
        assert client.session.headers["Authorization"] == "Bearer test_key"


class TestAPIRequest:
    """Tests for the _request method."""

    @patch("pydrime.api.requests.Session.request")
    def test_successful_json_response(self, mock_request):
        """Test successful API request with JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"data": "test"}'
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"data": "test"}
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        result = client._request("GET", "/test")

        assert result == {"data": "test"}
        mock_request.assert_called_once()

    @patch("pydrime.api.requests.Session.request")
    def test_empty_response(self, mock_request):
        """Test handling of empty response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.headers = {"Content-Type": "application/json"}
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        result = client._request("GET", "/test")

        assert result == {}

    @patch("pydrime.api.requests.Session.request")
    def test_html_response_raises_error(self, mock_request):
        """Test that HTML response raises appropriate error."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"<html>Error</html>"
        mock_response.headers = {"Content-Type": "text/html"}
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        with pytest.raises(
            DrimeAPIError, match="Invalid API key - server returned HTML"
        ):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_http_401_error(self, mock_request):
        """Test handling of 401 Unauthorized error."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response)
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(
            DrimeAPIError, match="Invalid API key or unauthorized access"
        ):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_http_403_error(self, mock_request):
        """Test handling of 403 Forbidden error."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response)
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Access forbidden"):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_http_404_error(self, mock_request):
        """Test handling of 404 Not Found error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response)
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Resource not found"):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_network_error(self, mock_request):
        """Test handling of network errors."""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Network error"):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_non_json_response_unexpected_type(self, mock_request):
        """Test handling of unexpected content type (not HTML, not JSON)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"some text data"
        mock_response.headers = {"Content-Type": "text/plain"}
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Unexpected response type: text/plain"):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_invalid_json_response(self, mock_request):
        """Test handling of invalid JSON in response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"{invalid json}"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key")
        with pytest.raises(DrimeAPIError, match="Invalid JSON response from server"):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_http_429_rate_limit_error(self, mock_request):
        """Test handling of 429 Rate Limit error."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {}  # No Retry-After header
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response)
        )

        # Create client with max_retries=0 to avoid retrying
        client = DrimeClient(api_key="test_key", max_retries=0)
        with pytest.raises(DrimeRateLimitError, match="Rate limit exceeded"):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_http_error_with_json_error_message(self, mock_request):
        """Test handling of HTTP error with JSON error message in response."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.content = b'{"message": "Internal server error occurred"}'
        mock_response.json.return_value = {"message": "Internal server error occurred"}
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response)
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(
            DrimeAPIError,
            match="API request failed with status 500: Internal server error occurred",
        ):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_http_error_with_error_field(self, mock_request):
        """Test handling of HTTP error with 'error' field in response."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b'{"error": "Bad request"}'
        mock_response.json.return_value = {"error": "Bad request"}
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response)
        )

        client = DrimeClient(api_key="test_key")
        with pytest.raises(
            DrimeAPIError, match="API request failed with status 400: Bad request"
        ):
            client._request("GET", "/test")

    @patch("pydrime.api.requests.Session.request")
    def test_http_error_with_unparseable_response(self, mock_request):
        """Test handling of HTTP error with unparseable response."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.content = b"<html>Error</html>"
        mock_response.json.side_effect = ValueError("Not JSON")
        mock_request.return_value = mock_response
        mock_request.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response)
        )

        client = DrimeClient(api_key="test_key")
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

            # Should fall back to application/octet-stream
            assert mime_type == "application/octet-stream"
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
    @patch("pydrime.api.DrimeClient._request")
    def test_upload_file_uses_mime_detection_small_file(
        self, mock_request, mock_detect_mime
    ):
        """Test that upload_file uses MIME detection for small files."""
        import tempfile
        from pathlib import Path

        mock_detect_mime.return_value = "text/plain"
        mock_request.return_value = {"status": "success", "fileEntry": {"id": 123}}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("small file")
            test_file = Path(f.name)

        try:
            client = DrimeClient(api_key="test_key")
            # Upload small file (below multipart threshold)
            client.upload_file(test_file, use_multipart_threshold=1024 * 1024)

            # Verify MIME detection was called
            mock_detect_mime.assert_called_once_with(test_file)
        finally:
            test_file.unlink()

    @patch("pydrime.api.DrimeClient._detect_mime_type")
    @patch("pydrime.api.DrimeClient._request")
    def test_upload_file_multipart_uses_mime_detection(
        self, mock_request, mock_detect_mime
    ):
        """Test that upload_file_multipart uses MIME detection."""
        import tempfile
        from pathlib import Path

        mock_detect_mime.return_value = "application/octet-stream"
        mock_request.side_effect = [
            {"uploadId": "test-upload-id", "key": "test-key"},
            {"urls": [{"partNumber": 1, "url": "https://s3.example.com/upload"}]},
            {},  # complete response
            {"status": "success", "fileEntry": {"id": 123}},  # entry creation
        ]

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".bin", delete=False) as f:
            # Create a file larger than 1MB to trigger multipart
            f.write(b"x" * (2 * 1024 * 1024))
            test_file = Path(f.name)

        try:
            with patch("pydrime.api.requests.put") as mock_put:
                mock_put_response = Mock()
                mock_put_response.headers = {"ETag": "test-etag"}
                mock_put.return_value = mock_put_response

                client = DrimeClient(api_key="test_key")
                # Upload with low threshold to trigger multipart
                client.upload_file(
                    test_file,
                    use_multipart_threshold=1 * 1024 * 1024,  # 1MB
                )

                # Verify MIME detection was called
                mock_detect_mime.assert_called_once_with(test_file)
        finally:
            test_file.unlink()


class TestExceptions:
    """Tests for exception classes."""

    def test_drime_file_not_found_error(self):
        """Test DrimeFileNotFoundError with file path."""
        file_path = "/tmp/test.txt"
        error = DrimeFileNotFoundError(file_path)

        assert str(error) == "File not found: /tmp/test.txt"
        assert error.file_path == file_path
