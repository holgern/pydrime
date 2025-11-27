"""Tests for workspace utilities."""

from unittest.mock import MagicMock

import pytest

from pydrime.exceptions import DrimeAPIError
from pydrime.workspace_utils import (
    format_workspace_display,
    get_folder_display_name,
    get_workspace_name,
    resolve_workspace_identifier,
)


class TestGetWorkspaceName:
    """Tests for get_workspace_name function."""

    def test_returns_workspace_name_when_found(self):
        """Test returns workspace name when found."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 1, "name": "Team Alpha"},
                {"id": 2, "name": "Team Beta"},
            ]
        }

        result = get_workspace_name(mock_client, 1)

        assert result == "Team Alpha"

    def test_returns_none_when_workspace_not_found(self):
        """Test returns None when workspace not found."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 1, "name": "Team Alpha"},
            ]
        }

        result = get_workspace_name(mock_client, 999)

        assert result is None

    def test_returns_none_on_api_error(self):
        """Test returns None on API error."""
        mock_client = MagicMock()
        mock_client.get_workspaces.side_effect = DrimeAPIError("API Error")

        result = get_workspace_name(mock_client, 1)

        assert result is None

    def test_returns_none_when_no_workspaces_key(self):
        """Test returns None when response has no workspaces key."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {}

        result = get_workspace_name(mock_client, 1)

        assert result is None

    def test_returns_none_when_response_not_dict(self):
        """Test returns None when response is not a dict."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = []

        result = get_workspace_name(mock_client, 1)

        assert result is None


class TestFormatWorkspaceDisplay:
    """Tests for format_workspace_display function."""

    def test_personal_workspace_formatting(self):
        """Test formatting of personal workspace (ID 0)."""
        mock_client = MagicMock()

        display, name = format_workspace_display(mock_client, 0)

        assert display == "Personal (0)"
        assert name is None
        mock_client.get_workspaces.assert_not_called()

    def test_workspace_with_name(self):
        """Test formatting workspace with name."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [{"id": 5, "name": "Project Team"}]
        }

        display, name = format_workspace_display(mock_client, 5)

        assert display == "Project Team (5)"
        assert name == "Project Team"

    def test_workspace_without_name(self):
        """Test formatting workspace when name cannot be retrieved."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {"workspaces": []}

        display, name = format_workspace_display(mock_client, 5)

        assert display == "5"
        assert name is None

    def test_workspace_on_api_error(self):
        """Test formatting workspace on API error."""
        mock_client = MagicMock()
        mock_client.get_workspaces.side_effect = DrimeAPIError("Error")

        display, name = format_workspace_display(mock_client, 5)

        assert display == "5"
        assert name is None


class TestGetFolderDisplayName:
    """Tests for get_folder_display_name function."""

    def test_root_folder(self):
        """Test formatting for root folder (None)."""
        mock_client = MagicMock()

        display, name = get_folder_display_name(mock_client, None)

        assert display == "/ (Root, ID: 0)"
        assert name is None
        mock_client.get_folder_info.assert_not_called()

    def test_folder_with_name(self):
        """Test formatting folder with name."""
        mock_client = MagicMock()
        mock_client.get_folder_info.return_value = {"name": "Documents"}

        display, name = get_folder_display_name(mock_client, 123)

        assert display == "/Documents (ID: 123)"
        assert name == "Documents"
        mock_client.get_folder_info.assert_called_once_with(123)

    def test_folder_without_name(self):
        """Test formatting folder when name cannot be retrieved."""
        mock_client = MagicMock()
        mock_client.get_folder_info.side_effect = DrimeAPIError("Error")

        display, name = get_folder_display_name(mock_client, 123)

        assert display == "ID 123"
        assert name is None

    def test_folder_info_returns_empty_dict(self):
        """Test when folder_info returns empty dict."""
        mock_client = MagicMock()
        mock_client.get_folder_info.return_value = {}

        display, name = get_folder_display_name(mock_client, 123)

        # Should handle missing 'name' key gracefully (name will be None)
        assert display == "/None (ID: 123)"
        assert name is None

    def test_folder_with_special_characters_in_name(self):
        """Test folder with special characters in name."""
        mock_client = MagicMock()
        mock_client.get_folder_info.return_value = {"name": "My Folder (2023)"}

        display, name = get_folder_display_name(mock_client, 456)

        assert display == "/My Folder (2023) (ID: 456)"
        assert name == "My Folder (2023)"


class TestResolveWorkspaceIdentifier:
    """Tests for resolve_workspace_identifier function."""

    def test_none_identifier_uses_default(self):
        """Test that None returns default workspace."""
        mock_client = MagicMock()

        result = resolve_workspace_identifier(mock_client, None, 5)

        assert result == 5
        mock_client.get_workspaces.assert_not_called()

    def test_none_identifier_defaults_to_zero(self):
        """Test that None with no default returns 0."""
        mock_client = MagicMock()

        result = resolve_workspace_identifier(mock_client, None)

        assert result == 0

    def test_integer_identifier_returns_directly(self):
        """Test that integer ID is returned directly."""
        mock_client = MagicMock()

        result = resolve_workspace_identifier(mock_client, 5)

        assert result == 5
        mock_client.get_workspaces.assert_not_called()

    def test_numeric_string_is_converted(self):
        """Test that numeric string is converted to int."""
        mock_client = MagicMock()

        result = resolve_workspace_identifier(mock_client, "123")

        assert result == 123
        mock_client.get_workspaces.assert_not_called()

    def test_workspace_name_is_resolved(self):
        """Test that workspace name is resolved to ID."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [
                {"id": 1, "name": "Team Alpha"},
                {"id": 5, "name": "My Team"},
            ]
        }

        result = resolve_workspace_identifier(mock_client, "My Team")

        assert result == 5

    def test_workspace_name_is_case_insensitive(self):
        """Test that workspace name resolution is case-insensitive."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [{"id": 5, "name": "My Team"}]
        }

        result = resolve_workspace_identifier(mock_client, "MY TEAM")

        assert result == 5

    def test_unknown_workspace_name_raises_error(self):
        """Test that unknown workspace name raises ValueError."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {
            "workspaces": [{"id": 1, "name": "Team Alpha"}]
        }

        with pytest.raises(ValueError, match="Workspace 'Unknown' not found"):
            resolve_workspace_identifier(mock_client, "Unknown")

    def test_empty_workspaces_raises_error(self):
        """Test that empty workspaces list raises ValueError for name lookup."""
        mock_client = MagicMock()
        mock_client.get_workspaces.return_value = {"workspaces": []}

        with pytest.raises(ValueError, match="not found"):
            resolve_workspace_identifier(mock_client, "Unknown")
