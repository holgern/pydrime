"""Tests for data models and schema validation."""

from pydrime.models import (
    FileEntriesResult,
    FileEntry,
    FileEntryPermissions,
    FileEntryTag,
    FileEntryUser,
    Permission,
    Product,
    Role,
    SchemaValidationWarning,
    Subscription,
    User,
    UserStatus,
)


class TestSchemaValidation:
    """Tests for schema validation functionality."""

    def setup_method(self):
        """Clear warnings before each test."""
        SchemaValidationWarning.clear_warnings()
        SchemaValidationWarning.disable()

    def test_validation_disabled_by_default(self):
        """Test that validation is disabled by default."""
        assert not SchemaValidationWarning.is_enabled()

    def test_enable_disable_validation(self):
        """Test enabling and disabling validation."""
        SchemaValidationWarning.enable()
        assert SchemaValidationWarning.is_enabled()

        SchemaValidationWarning.disable()
        assert not SchemaValidationWarning.is_enabled()

    def test_no_warnings_when_disabled(self):
        """Test that no warnings are added when validation is disabled."""
        SchemaValidationWarning.disable()

        # Create a Role with missing required field
        data = {"name": "test", "default": True, "guests": False}
        Role.from_dict(data)

        assert not SchemaValidationWarning.has_warnings()

    def test_missing_required_field(self):
        """Test warning when required field is missing."""
        SchemaValidationWarning.enable()

        # Create a Role with missing required 'id' field
        data = {"name": "test", "default": True, "guests": False}
        Role.from_dict(data)

        warnings = SchemaValidationWarning.get_warnings()
        assert len(warnings) > 0
        assert any("Required field is missing" in w for w in warnings)

    def test_type_mismatch(self):
        """Test warning when field type doesn't match."""
        SchemaValidationWarning.enable()

        # Create a Role with wrong type for 'id'
        data = {"id": "not_an_int", "name": "test", "default": True, "guests": False}
        Role.from_dict(data)

        warnings = SchemaValidationWarning.get_warnings()
        assert len(warnings) > 0
        assert any("Type mismatch" in w for w in warnings)

    def test_unexpected_fields(self):
        """Test warning when unexpected fields are present."""
        SchemaValidationWarning.enable()

        # Create a Role with extra unexpected field
        data = {
            "id": 1,
            "name": "test",
            "default": True,
            "guests": False,
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
            "new_field_from_api": "unexpected",
        }
        Role.from_dict(data)

        warnings = SchemaValidationWarning.get_warnings()
        assert len(warnings) > 0
        assert any("New fields detected" in w for w in warnings)
        assert any("new_field_from_api" in w for w in warnings)

    def test_clear_warnings(self):
        """Test clearing warnings."""
        SchemaValidationWarning.enable()

        data = {"name": "test", "default": True, "guests": False}
        Role.from_dict(data)

        assert SchemaValidationWarning.has_warnings()

        SchemaValidationWarning.clear_warnings()
        assert not SchemaValidationWarning.has_warnings()

    def test_optional_field_with_none_value(self):
        """Test that None values for optional fields don't trigger warnings."""
        SchemaValidationWarning.enable()

        # Create a User with None value for optional field
        data = {
            "id": 1,
            "email": "test@example.com",
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
            "first_name": None,  # Optional field with None value
        }
        User.from_dict(data)

        # Should not have warnings about None values for optional fields
        warnings = SchemaValidationWarning.get_warnings()
        assert not any("first_name" in w and "Type mismatch" in w for w in warnings)

    def test_field_type_validation(self):
        """Test that field type validation works correctly."""
        SchemaValidationWarning.enable()

        # Create data with wrong type for email (required field)
        data = {
            "id": 1,
            "email": 123,  # Should be str, not int
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
        }
        User.from_dict(data)

        warnings = SchemaValidationWarning.get_warnings()
        assert len(warnings) > 0
        assert any("email" in w and "Type mismatch" in w for w in warnings)


class TestUserStatus:
    """Tests for UserStatus model."""

    def test_from_api_response(self):
        """Test creating UserStatus from API response."""
        api_response = {
            "user": {
                "id": 123,
                "email": "test@example.com",
                "created_at": "2024-01-01",
                "updated_at": "2024-01-01",
                "first_name": "Test",
                "last_name": "User",
                "roles": [],
                "subscriptions": [],
                "permissions": [],
            }
        }

        user_status = UserStatus.from_api_response(api_response)

        assert user_status.user.id == 123
        assert user_status.user.email == "test@example.com"
        assert user_status.user.full_name == "Test User"

    def test_text_summary(self):
        """Test generating text summary."""
        api_response = {
            "user": {
                "id": 123,
                "email": "test@example.com",
                "created_at": "2024-01-01",
                "updated_at": "2024-01-01",
                "display_name": "Test User",
                "roles": [],
                "subscriptions": [],
                "permissions": [],
            }
        }

        user_status = UserStatus.from_api_response(api_response)
        summary = user_status.to_text_summary()

        assert "Test User" in summary
        assert "test@example.com" in summary
        assert "123" in summary

    def test_table_data(self):
        """Test generating table data."""
        api_response = {
            "user": {
                "id": 123,
                "email": "test@example.com",
                "created_at": "2024-01-01",
                "updated_at": "2024-01-01",
                "timezone": "UTC",
                "country": "us",
                "roles": [],
                "subscriptions": [],
                "permissions": [],
            }
        }

        user_status = UserStatus.from_api_response(api_response)
        table_data = user_status.to_table_data()

        assert len(table_data) > 0
        assert any(row["field"] == "Email" for row in table_data)
        assert any(row["field"] == "Timezone" for row in table_data)


class TestSubscription:
    """Tests for Subscription model."""

    def test_status_text_active(self):
        """Test status text for active subscription."""
        sub = Subscription(
            id=1,
            user_id=1,
            price_id=1,
            gateway_name="stripe",
            quantity=1,
            created_at="2024-01-01",
            updated_at="2024-01-01",
            product_id=1,
            on_grace_period=False,
            on_trial=False,
            valid=True,
            active=True,
            cancelled=False,
        )

        assert sub.status_text == "Active"

    def test_status_text_trial(self):
        """Test status text for trial subscription."""
        sub = Subscription(
            id=1,
            user_id=1,
            price_id=1,
            gateway_name="stripe",
            quantity=1,
            created_at="2024-01-01",
            updated_at="2024-01-01",
            product_id=1,
            on_grace_period=False,
            on_trial=True,
            valid=True,
            active=True,
            cancelled=False,
        )

        assert sub.status_text == "Active (Trial)"

    def test_plan_name_with_product(self):
        """Test plan name with product."""
        product = Product(
            id=1,
            name="Premium Plan",
            uuid="test-uuid",
            available_space=1073741824,  # 1 GB
            created_at="2024-01-01",
            updated_at="2024-01-01",
        )

        sub = Subscription(
            id=1,
            user_id=1,
            price_id=1,
            gateway_name="stripe",
            quantity=1,
            created_at="2024-01-01",
            updated_at="2024-01-01",
            product_id=1,
            on_grace_period=False,
            on_trial=False,
            valid=True,
            active=True,
            cancelled=False,
            product=product,
        )

        assert sub.plan_name == "Premium Plan"

    def test_from_dict(self):
        """Test creating Subscription from dictionary."""
        data = {
            "id": 1,
            "user_id": 1,
            "price_id": 1,
            "gateway_name": "stripe",
            "quantity": 1,
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
            "product_id": 1,
            "on_grace_period": False,
            "on_trial": False,
            "valid": True,
            "active": True,
            "cancelled": False,
        }
        sub = Subscription.from_dict(data)
        assert sub.id == 1
        assert sub.gateway_name == "stripe"
        assert sub.active is True


class TestProduct:
    """Tests for Product model."""

    def test_format_available_space(self):
        """Test formatting available space."""
        product = Product(
            id=1,
            name="Test Plan",
            uuid="test-uuid",
            available_space=1073741824,  # 1 GB
            created_at="2024-01-01",
            updated_at="2024-01-01",
        )
        formatted = product.format_available_space()
        assert "GB" in formatted or "GiB" in formatted

    def test_from_dict(self):
        """Test creating Product from dictionary."""
        data = {
            "id": 1,
            "name": "Premium",
            "uuid": "test-uuid-123",
            "available_space": 5368709120,  # 5 GB
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
            "description": "Premium plan",
            "feature_list": ["Feature 1", "Feature 2"],
            "position": 1,
            "recommended": True,
            "free": False,
            "hidden": False,
        }
        product = Product.from_dict(data)
        assert product.id == 1
        assert product.name == "Premium"
        assert product.uuid == "test-uuid-123"
        assert product.available_space == 5368709120
        assert product.recommended is True


class TestUser:
    """Tests for User model."""

    def test_full_name_display_name(self):
        """Test full name uses display_name if available."""
        user = User(
            id=1,
            email="test@example.com",
            created_at="2024-01-01",
            updated_at="2024-01-01",
            display_name="Display Name",
            first_name="First",
            last_name="Last",
        )

        assert user.full_name == "Display Name"

    def test_full_name_first_last(self):
        """Test full name uses first and last name if no display name."""
        user = User(
            id=1,
            email="test@example.com",
            created_at="2024-01-01",
            updated_at="2024-01-01",
            first_name="First",
            last_name="Last",
        )

        assert user.full_name == "First Last"

    def test_has_permission(self):
        """Test checking for permissions."""
        perm = Permission(id=1, name="api.access", restrictions=[])
        user = User(
            id=1,
            email="test@example.com",
            created_at="2024-01-01",
            updated_at="2024-01-01",
            permissions=[perm],
        )

        assert user.has_permission("api.access")
        assert not user.has_permission("admin.access")

    def test_is_verified(self):
        """Test email verification check."""
        user_verified = User(
            id=1,
            email="test@example.com",
            created_at="2024-01-01",
            updated_at="2024-01-01",
            email_verified_at="2024-01-01",
        )

        user_unverified = User(
            id=2,
            email="test2@example.com",
            created_at="2024-01-01",
            updated_at="2024-01-01",
        )

        assert user_verified.is_verified
        assert not user_unverified.is_verified


class TestFileEntry:
    """Tests for FileEntry model."""

    def test_from_dict_file(self):
        """Test creating FileEntry from API response for a file."""
        data = {
            "id": 480542512,
            "name": "test1.txt",
            "file_name": "9dda5267-7d4a-4187-9338-9e0912a657c9",
            "mime": "application/octet-stream",
            "file_size": 9,
            "parent_id": 480432025,
            "created_at": "2025-11-19T20:48:22.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "NDgwNTQyNTEyfA",
            "url": "api/v1/file-entries/480542512",
            "users": [],
            "tags": [],
            "permissions": {
                "view": True,
                "edit": False,
                "download": True,
                "delete": False,
            },
        }

        entry = FileEntry.from_dict(data)

        assert entry.id == 480542512
        assert entry.name == "test1.txt"
        assert entry.file_size == 9
        assert entry.type == "file"
        assert entry.extension == "txt"
        assert entry.is_file
        assert not entry.is_folder
        assert entry.permissions is not None
        assert entry.permissions.view is True
        assert entry.permissions.edit is False

    def test_from_dict_folder(self):
        """Test creating FileEntry from API response for a folder."""
        data = {
            "id": 12345,
            "name": "My Folder",
            "file_name": "",
            "mime": "",
            "file_size": 0,
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "folder",
            "extension": None,
            "hash": "MTIzNDV8",
            "url": "api/v1/file-entries/12345",
            "users": [],
            "tags": [],
        }

        entry = FileEntry.from_dict(data)

        assert entry.id == 12345
        assert entry.name == "My Folder"
        assert entry.type == "folder"
        assert entry.is_folder
        assert not entry.is_file

    def test_from_dict_with_users(self):
        """Test creating FileEntry with users."""
        data = {
            "id": 1,
            "name": "test.txt",
            "file_name": "test",
            "mime": "text/plain",
            "file_size": 100,
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "abc123",
            "url": "api/v1/file-entries/1",
            "users": [
                {
                    "id": 1,
                    "email": "owner@example.com",
                    "owns_entry": True,
                    "display_name": "Owner",
                }
            ],
            "tags": [],
        }

        entry = FileEntry.from_dict(data)

        assert len(entry.users) == 1
        assert entry.users[0].email == "owner@example.com"
        assert entry.users[0].owns_entry is True
        assert entry.owner is not None
        assert entry.owner.email == "owner@example.com"

    def test_from_dict_with_tags(self):
        """Test creating FileEntry with tags."""
        data = {
            "id": 1,
            "name": "test.txt",
            "file_name": "test",
            "mime": "text/plain",
            "file_size": 100,
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "abc123",
            "url": "api/v1/file-entries/1",
            "users": [],
            "tags": [
                {"id": 1, "name": "important", "display_name": "Important"},
                {"id": 2, "name": "work", "display_name": "Work"},
            ],
        }

        entry = FileEntry.from_dict(data)

        assert len(entry.tags) == 2
        assert entry.has_tag("important")
        assert entry.has_tag("work")
        assert not entry.has_tag("personal")

    def test_is_deleted(self):
        """Test checking if file is deleted."""
        data_not_deleted = {
            "id": 1,
            "name": "test.txt",
            "file_name": "test",
            "mime": "text/plain",
            "file_size": 100,
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "abc123",
            "url": "api/v1/file-entries/1",
            "users": [],
            "tags": [],
        }

        data_deleted = {**data_not_deleted, "deleted_at": "2025-11-19T22:00:00.000000Z"}

        entry_not_deleted = FileEntry.from_dict(data_not_deleted)
        entry_deleted = FileEntry.from_dict(data_deleted)

        assert not entry_not_deleted.is_deleted
        assert entry_deleted.is_deleted

    def test_is_public(self):
        """Test checking if file is public."""
        data = {
            "id": 1,
            "name": "test.txt",
            "file_name": "test",
            "mime": "text/plain",
            "file_size": 100,
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "abc123",
            "url": "api/v1/file-entries/1",
            "users": [],
            "tags": [],
            "public": True,
        }

        entry = FileEntry.from_dict(data)

        assert entry.is_public

    def test_format_size(self):
        """Test file size formatting."""
        data = {
            "id": 1,
            "name": "test.txt",
            "file_name": "test",
            "mime": "text/plain",
            "file_size": 1073741824,  # 1 GB
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "abc123",
            "url": "api/v1/file-entries/1",
            "users": [],
            "tags": [],
        }

        entry = FileEntry.from_dict(data)
        formatted = entry.format_size()

        assert "GB" in formatted or formatted == "1.0 GB"

    def test_format_size_with_formatted_field(self):
        """Test file size formatting with pre-formatted field."""
        data = {
            "id": 1,
            "name": "test.txt",
            "file_name": "test",
            "mime": "text/plain",
            "file_size": 1073741824,
            "file_size_formatted": "1.0 GB",
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "abc123",
            "url": "api/v1/file-entries/1",
            "users": [],
            "tags": [],
        }

        entry = FileEntry.from_dict(data)

        assert entry.format_size() == "1.0 GB"

    def test_get_full_path(self):
        """Test getting full path."""
        data_with_path = {
            "id": 1,
            "name": "test.txt",
            "file_name": "test",
            "mime": "text/plain",
            "file_size": 100,
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "abc123",
            "url": "api/v1/file-entries/1",
            "users": [],
            "tags": [],
            "path": "/my/folder",
        }

        data_without_path = {**data_with_path}
        data_without_path.pop("path")

        entry_with_path = FileEntry.from_dict(data_with_path)
        entry_without_path = FileEntry.from_dict(data_without_path)

        assert entry_with_path.get_full_path() == "/my/folder/test.txt"
        assert entry_without_path.get_full_path() == "test.txt"

    def test_display_name(self):
        """Test display name property."""
        file_data = {
            "id": 1,
            "name": "test.txt",
            "file_name": "test",
            "mime": "text/plain",
            "file_size": 100,
            "parent_id": None,
            "created_at": "2025-11-19T20:00:00.000000Z",
            "type": "file",
            "extension": "txt",
            "hash": "abc123",
            "url": "api/v1/file-entries/1",
            "users": [],
            "tags": [],
        }

        folder_data = {**file_data, "type": "folder"}

        file_entry = FileEntry.from_dict(file_data)
        folder_entry = FileEntry.from_dict(folder_data)

        assert file_entry.display_name == "test.txt"
        assert folder_entry.display_name == "[D] test.txt"


class TestFileEntryUser:
    """Tests for FileEntryUser model."""

    def test_from_dict(self):
        """Test creating FileEntryUser from dictionary."""
        data = {
            "id": 1,
            "email": "user@example.com",
            "owns_entry": True,
            "entry_permissions": "owner",
            "display_name": "Test User",
        }

        user = FileEntryUser.from_dict(data)

        assert user.id == 1
        assert user.email == "user@example.com"
        assert user.owns_entry is True
        assert user.display_name == "Test User"


class TestFileEntryTag:
    """Tests for FileEntryTag model."""

    def test_from_dict(self):
        """Test creating FileEntryTag from dictionary."""
        data = {
            "id": 1,
            "name": "important",
            "display_name": "Important",
            "type": "label",
        }

        tag = FileEntryTag.from_dict(data)

        assert tag.id == 1
        assert tag.name == "important"
        assert tag.display_name == "Important"
        assert tag.type == "label"


class TestFileEntryPermissions:
    """Tests for FileEntryPermissions model."""

    def test_from_dict(self):
        """Test creating FileEntryPermissions from dictionary."""
        data = {"view": True, "edit": False, "download": True, "delete": False}

        perms = FileEntryPermissions.from_dict(data)

        assert perms.view is True
        assert perms.edit is False
        assert perms.download is True
        assert perms.delete is False

    def test_from_dict_defaults(self):
        """Test default values for FileEntryPermissions."""
        data = {}

        perms = FileEntryPermissions.from_dict(data)

        assert perms.view is True
        assert perms.edit is False
        assert perms.download is True
        assert perms.delete is False


class TestFileEntriesResult:
    """Tests for FileEntriesResult model."""

    def test_from_api_response(self):
        """Test creating FileEntriesResult from API response."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "test1.txt",
                    "file_name": "test1",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 2,
                    "name": "folder1",
                    "file_name": "",
                    "mime": "",
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2025-11-19T19:00:00.000000Z",
                    "type": "folder",
                    "extension": None,
                    "hash": "def456",
                    "url": "api/v1/file-entries/2",
                    "users": [],
                    "tags": [],
                },
            ],
            "current_page": 1,
            "last_page": 1,
            "per_page": 50,
            "total": 2,
        }

        result = FileEntriesResult.from_api_response(data)

        assert len(result.entries) == 2
        assert result.entries[0].name == "test1.txt"
        assert result.entries[1].name == "folder1"
        assert result.pagination is not None
        assert result.pagination["total"] == 2

    def test_is_empty(self):
        """Test is_empty property."""
        empty_result = FileEntriesResult.from_api_response({"data": []})
        non_empty_result = FileEntriesResult.from_api_response(
            {
                "data": [
                    {
                        "id": 1,
                        "name": "test.txt",
                        "file_name": "test",
                        "mime": "text/plain",
                        "file_size": 100,
                        "parent_id": None,
                        "created_at": "2025-11-19T20:00:00.000000Z",
                        "type": "file",
                        "extension": "txt",
                        "hash": "abc123",
                        "url": "api/v1/file-entries/1",
                        "users": [],
                        "tags": [],
                    }
                ]
            }
        )

        assert empty_result.is_empty
        assert not non_empty_result.is_empty

    def test_file_and_folder_counts(self):
        """Test file and folder count properties."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "file1.txt",
                    "file_name": "file1",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 2,
                    "name": "folder1",
                    "file_name": "",
                    "mime": "",
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2025-11-19T19:00:00.000000Z",
                    "type": "folder",
                    "extension": None,
                    "hash": "def456",
                    "url": "api/v1/file-entries/2",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 3,
                    "name": "file2.txt",
                    "file_name": "file2",
                    "mime": "text/plain",
                    "file_size": 200,
                    "parent_id": None,
                    "created_at": "2025-11-19T18:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "ghi789",
                    "url": "api/v1/file-entries/3",
                    "users": [],
                    "tags": [],
                },
            ]
        }

        result = FileEntriesResult.from_api_response(data)

        assert result.file_count == 2
        assert result.folder_count == 1
        assert result.total_size == 300

    def test_to_table_data(self):
        """Test generating table data."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "test.txt",
                    "file_name": "test",
                    "mime": "text/plain",
                    "file_size": 1024,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                }
            ]
        }

        result = FileEntriesResult.from_api_response(data)
        table_data = result.to_table_data()

        assert len(table_data) == 1
        assert table_data[0]["id"] == "1"
        assert table_data[0]["name"] == "test.txt"
        assert table_data[0]["type"] == "file"
        assert table_data[0]["hash"] == "abc123"

    def test_to_text_summary(self):
        """Test generating text summary."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "file1.txt",
                    "file_name": "file1",
                    "mime": "text/plain",
                    "file_size": 1024,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 2,
                    "name": "folder1",
                    "file_name": "",
                    "mime": "",
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2025-11-19T19:00:00.000000Z",
                    "type": "folder",
                    "extension": None,
                    "hash": "def456",
                    "url": "api/v1/file-entries/2",
                    "users": [],
                    "tags": [],
                },
            ],
            "total": 2,
        }

        result = FileEntriesResult.from_api_response(data)
        summary = result.to_text_summary()

        assert "folder" in summary
        assert "file" in summary

    def test_to_compact_list(self):
        """Test generating compact list."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "file.txt",
                    "file_name": "file",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 2,
                    "name": "folder",
                    "file_name": "",
                    "mime": "",
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2025-11-19T19:00:00.000000Z",
                    "type": "folder",
                    "extension": None,
                    "hash": "def456",
                    "url": "api/v1/file-entries/2",
                    "users": [],
                    "tags": [],
                },
            ]
        }

        result = FileEntriesResult.from_api_response(data)
        compact_list = result.to_compact_list()

        assert len(compact_list) == 2
        assert compact_list[0] == "file.txt"
        assert "[D]" in compact_list[1]

    def test_filter_by_type(self):
        """Test filtering by type."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "file.txt",
                    "file_name": "file",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 2,
                    "name": "folder",
                    "file_name": "",
                    "mime": "",
                    "file_size": 0,
                    "parent_id": None,
                    "created_at": "2025-11-19T19:00:00.000000Z",
                    "type": "folder",
                    "extension": None,
                    "hash": "def456",
                    "url": "api/v1/file-entries/2",
                    "users": [],
                    "tags": [],
                },
            ]
        }

        result = FileEntriesResult.from_api_response(data)
        files_only = result.filter_by_type("file")
        folders_only = result.filter_by_type("folder")

        assert len(files_only.entries) == 1
        assert files_only.entries[0].name == "file.txt"
        assert len(folders_only.entries) == 1
        assert folders_only.entries[0].name == "folder"

    def test_sort_by_name(self):
        """Test sorting by name."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "z_file.txt",
                    "file_name": "z_file",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 2,
                    "name": "a_file.txt",
                    "file_name": "a_file",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T19:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "def456",
                    "url": "api/v1/file-entries/2",
                    "users": [],
                    "tags": [],
                },
            ]
        }

        result = FileEntriesResult.from_api_response(data)
        sorted_result = result.sort_by("name")

        assert sorted_result.entries[0].name == "a_file.txt"
        assert sorted_result.entries[1].name == "z_file.txt"

    def test_sort_by_size(self):
        """Test sorting by size."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "small.txt",
                    "file_name": "small",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                },
                {
                    "id": 2,
                    "name": "large.txt",
                    "file_name": "large",
                    "mime": "text/plain",
                    "file_size": 1000,
                    "parent_id": None,
                    "created_at": "2025-11-19T19:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "def456",
                    "url": "api/v1/file-entries/2",
                    "users": [],
                    "tags": [],
                },
            ]
        }

        result = FileEntriesResult.from_api_response(data)
        sorted_result = result.sort_by("size", reverse=True)

        assert sorted_result.entries[0].name == "large.txt"
        assert sorted_result.entries[1].name == "small.txt"

    def test_get_by_id(self):
        """Test getting entry by ID."""
        data = {
            "data": [
                {
                    "id": 123,
                    "name": "test.txt",
                    "file_name": "test",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/123",
                    "users": [],
                    "tags": [],
                }
            ]
        }

        result = FileEntriesResult.from_api_response(data)
        entry = result.get_by_id(123)

        assert entry is not None
        assert entry.name == "test.txt"
        assert result.get_by_id(999) is None

    def test_get_by_name(self):
        """Test getting entry by name."""
        data = {
            "data": [
                {
                    "id": 1,
                    "name": "test.txt",
                    "file_name": "test",
                    "mime": "text/plain",
                    "file_size": 100,
                    "parent_id": None,
                    "created_at": "2025-11-19T20:00:00.000000Z",
                    "type": "file",
                    "extension": "txt",
                    "hash": "abc123",
                    "url": "api/v1/file-entries/1",
                    "users": [],
                    "tags": [],
                }
            ]
        }

        result = FileEntriesResult.from_api_response(data)
        entry = result.get_by_name("test.txt")

        assert entry is not None
        assert entry.id == 1
        assert result.get_by_name("nonexistent.txt") is None


class TestAdditionalModels:
    """Additional tests for remaining model coverage."""

    def test_role_from_dict(self):
        """Test creating Role from dictionary."""
        data = {
            "id": 1,
            "name": "admin",
            "default": True,
            "guests": False,
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
        }
        role = Role.from_dict(data)
        assert role.id == 1
        assert role.name == "admin"
        assert role.default is True
        assert role.guests is False

    def test_permission_from_dict(self):
        """Test creating Permission from dictionary."""
        data = {
            "id": 1,
            "name": "files.download",
            "restrictions": [],
        }
        perm = Permission.from_dict(data)
        assert perm.id == 1
        assert perm.name == "files.download"
        assert perm.restrictions == []

    def test_subscription_status_text_cancelled(self):
        """Test status text for cancelled subscription."""
        sub = Subscription(
            id=1,
            user_id=1,
            price_id=1,
            gateway_name="stripe",
            quantity=1,
            created_at="2024-01-01",
            updated_at="2024-01-01",
            product_id=1,
            on_grace_period=False,
            on_trial=False,
            valid=False,
            active=False,
            cancelled=True,
        )
        assert sub.status_text == "Cancelled"

    def test_subscription_status_text_grace_period(self):
        """Test status text for subscription on grace period."""
        sub = Subscription(
            id=1,
            user_id=1,
            price_id=1,
            gateway_name="stripe",
            quantity=1,
            created_at="2024-01-01",
            updated_at="2024-01-01",
            product_id=1,
            on_grace_period=True,
            on_trial=False,
            valid=True,
            active=False,
            cancelled=False,
        )
        assert sub.status_text == "Grace Period"

    def test_user_from_dict_with_all_fields(self):
        """Test creating User from dictionary with all optional fields."""
        data = {
            "id": 1,
            "email": "test@example.com",
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
            "first_name": "John",
            "last_name": "Doe",
            "display_name": "John D.",
            "avatar": "https://example.com/avatar.png",
            "timezone": "America/New_York",
            "country": "US",
            "language": "en",
            "email_verified_at": "2024-01-01",
            "roles": [],
            "subscriptions": [],
            "permissions": [],
        }
        user = User.from_dict(data)
        assert user.id == 1
        assert user.first_name == "John"
        assert user.last_name == "Doe"
        assert user.display_name == "John D."
        assert user.avatar == "https://example.com/avatar.png"
        assert user.timezone == "America/New_York"
        assert user.country == "US"

    def test_user_full_name_email_fallback(self):
        """Test that full_name uses email username when no names provided."""
        user = User(
            id=1,
            email="test@example.com",
            created_at="2024-01-01",
            updated_at="2024-01-01",
        )
        assert user.full_name == "test"

    def test_subscription_plan_name_without_product(self):
        """Test plan name when product is not provided."""
        sub = Subscription(
            id=1,
            user_id=1,
            price_id=1,
            gateway_name="stripe",
            quantity=1,
            created_at="2024-01-01",
            updated_at="2024-01-01",
            product_id=1,
            on_grace_period=False,
            on_trial=False,
            valid=True,
            active=True,
            cancelled=False,
        )
        assert sub.plan_name == "Unknown"
