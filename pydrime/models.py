"""Data models for Drime API responses."""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from .utils import format_size as _format_size
from .utils import parse_iso_timestamp as _parse_iso_timestamp

# Set up logging
logger = logging.getLogger(__name__)


class SchemaValidationWarning:
    """Track schema validation warnings."""

    warnings: list[str] = []
    enabled: bool = False  # Validation is disabled by default

    @classmethod
    def enable(cls) -> None:
        """Enable schema validation warnings."""
        cls.enabled = True

    @classmethod
    def disable(cls) -> None:
        """Disable schema validation warnings."""
        cls.enabled = False

    @classmethod
    def is_enabled(cls) -> bool:
        """Check if validation is enabled."""
        return cls.enabled

    @classmethod
    def add_warning(cls, context: str, field_name: str, issue: str) -> None:
        """Add a schema validation warning (only if enabled)."""
        if not cls.enabled:
            return

        warning = f"{context}.{field_name}: {issue}"
        if warning not in cls.warnings:
            cls.warnings.append(warning)
            logger.warning("API Schema Change Detected: %s", warning)

    @classmethod
    def get_warnings(cls) -> list[str]:
        """Get all warnings."""
        return cls.warnings.copy()

    @classmethod
    def clear_warnings(cls) -> None:
        """Clear all warnings."""
        cls.warnings.clear()

    @classmethod
    def has_warnings(cls) -> bool:
        """Check if there are any warnings."""
        return len(cls.warnings) > 0


def _format_timestamp(timestamp_str: Optional[str]) -> str:
    """Format a timestamp string for display without trailing zeros.

    Args:
        timestamp_str: ISO format timestamp string

    Returns:
        Formatted timestamp string without microseconds or "-" if parsing fails
    """
    dt = _parse_iso_timestamp(timestamp_str)
    if dt is None:
        return "-"

    # Format as YYYY-MM-DD HH:MM:SS without microseconds
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _validate_field(
    data: dict[str, Any],
    field_name: str,
    expected_type: type,
    context: str,
    required: bool = True,
) -> bool:
    """Validate that a field exists and has the expected type.

    Args:
        data: Dictionary containing the field
        field_name: Name of the field to validate
        expected_type: Expected Python type
        context: Context for error messages (e.g., "User", "Subscription")
        required: Whether the field is required

    Returns:
        True if validation passes, False otherwise
    """
    if field_name not in data:
        if required:
            SchemaValidationWarning.add_warning(
                context, field_name, "Required field is missing"
            )
            return False
        return True

    value = data[field_name]
    if value is None:
        return True  # None is allowed for optional fields

    if not isinstance(value, expected_type):
        actual_type = type(value).__name__
        expected_type_name = expected_type.__name__
        SchemaValidationWarning.add_warning(
            context,
            field_name,
            f"Type mismatch: expected {expected_type_name}, got {actual_type}",
        )
        return False

    return True


def _check_unexpected_fields(
    data: dict[str, Any], expected_fields: set[str], context: str
) -> None:
    """Check for unexpected fields in the API response.

    Args:
        data: Dictionary to check
        expected_fields: Set of expected field names
        context: Context for warning messages
    """
    unexpected = set(data.keys()) - expected_fields
    if unexpected:
        SchemaValidationWarning.add_warning(
            context,
            "unexpected_fields",
            f"New fields detected: {', '.join(sorted(unexpected))}",
        )


@dataclass
class Role:
    """User role information."""

    id: int
    name: str
    default: bool
    guests: bool
    created_at: str
    updated_at: str
    description: Optional[str] = None
    type: Optional[str] = None
    internal: bool = False
    workspace_id: Optional[int] = None

    # Expected fields for validation
    _EXPECTED_FIELDS = {
        "id",
        "name",
        "default",
        "guests",
        "created_at",
        "updated_at",
        "description",
        "type",
        "internal",
        "workspace_id",
    }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Role":
        """Create a Role from a dictionary with validation."""
        # Validate schema
        _validate_field(data, "id", int, "Role", required=True)
        _validate_field(data, "name", str, "Role", required=True)
        _validate_field(data, "default", bool, "Role", required=True)
        _validate_field(data, "guests", bool, "Role", required=True)
        _check_unexpected_fields(data, cls._EXPECTED_FIELDS, "Role")

        return cls(
            id=data.get("id", 0),
            name=data.get("name", ""),
            default=data.get("default", False),
            guests=data.get("guests", False),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            description=data.get("description"),
            type=data.get("type"),
            internal=data.get("internal", False),
            workspace_id=data.get("workspace_id"),
        )


@dataclass
class Product:
    """Subscription product information."""

    id: int
    name: str
    uuid: str
    available_space: int
    created_at: str
    updated_at: str
    description: Optional[str] = None
    feature_list: list[str] = field(default_factory=list)
    position: int = 0
    recommended: bool = False
    free: bool = False
    hidden: bool = False

    _EXPECTED_FIELDS = {
        "id",
        "name",
        "uuid",
        "available_space",
        "created_at",
        "updated_at",
        "description",
        "feature_list",
        "position",
        "recommended",
        "free",
        "hidden",
    }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Product":
        """Create a Product from a dictionary with validation."""
        _validate_field(data, "id", int, "Product", required=True)
        _validate_field(data, "name", str, "Product", required=True)
        _validate_field(data, "uuid", str, "Product", required=True)
        _validate_field(data, "available_space", int, "Product", required=True)
        _check_unexpected_fields(data, cls._EXPECTED_FIELDS, "Product")

        return cls(
            id=data.get("id", 0),
            name=data.get("name", ""),
            uuid=data.get("uuid", ""),
            available_space=data.get("available_space", 0),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            description=data.get("description"),
            feature_list=data.get("feature_list", []),
            position=data.get("position", 0),
            recommended=data.get("recommended", False),
            free=data.get("free", False),
            hidden=data.get("hidden", False),
        )

    def format_available_space(self) -> str:
        """Format the available space in a human-readable format."""
        return _format_size(self.available_space)


@dataclass
class Subscription:
    """User subscription information."""

    id: int
    user_id: int
    price_id: int
    gateway_name: str
    quantity: int
    created_at: str
    updated_at: str
    product_id: int
    on_grace_period: bool
    on_trial: bool
    valid: bool
    active: bool
    cancelled: bool
    gateway_id: Optional[str] = None
    description: Optional[str] = None
    trial_ends_at: Optional[str] = None
    ends_at: Optional[str] = None
    renews_at: Optional[str] = None
    product: Optional[Product] = None

    _EXPECTED_FIELDS = {
        "id",
        "user_id",
        "price_id",
        "gateway_name",
        "quantity",
        "created_at",
        "updated_at",
        "product_id",
        "on_grace_period",
        "on_trial",
        "valid",
        "active",
        "cancelled",
        "gateway_id",
        "description",
        "trial_ends_at",
        "ends_at",
        "renews_at",
        "product",
    }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Subscription":
        """Create a Subscription from a dictionary with validation."""
        _validate_field(data, "id", int, "Subscription", required=True)
        _validate_field(data, "active", bool, "Subscription", required=True)
        _check_unexpected_fields(data, cls._EXPECTED_FIELDS, "Subscription")

        product = None
        if "product" in data and data["product"]:
            product = Product.from_dict(data["product"])

        return cls(
            id=data.get("id", 0),
            user_id=data.get("user_id", 0),
            price_id=data.get("price_id", 0),
            gateway_name=data.get("gateway_name", ""),
            quantity=data.get("quantity", 1),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            product_id=data.get("product_id", 0),
            on_grace_period=data.get("on_grace_period", False),
            on_trial=data.get("on_trial", False),
            valid=data.get("valid", False),
            active=data.get("active", False),
            cancelled=data.get("cancelled", False),
            gateway_id=data.get("gateway_id"),
            description=data.get("description"),
            trial_ends_at=data.get("trial_ends_at"),
            ends_at=data.get("ends_at"),
            renews_at=data.get("renews_at"),
            product=product,
        )

    @property
    def status_text(self) -> str:
        """Get a human-readable status."""
        if self.active:
            if self.on_trial:
                return "Active (Trial)"
            return "Active"
        if self.cancelled:
            return "Cancelled"
        if self.on_grace_period:
            return "Grace Period"
        return "Inactive"

    @property
    def plan_name(self) -> str:
        """Get the plan name from the product."""
        if self.product:
            return self.product.name
        return "Unknown"


@dataclass
class PermissionRestriction:
    """Permission restriction details."""

    name: str
    value: Any

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PermissionRestriction":
        """Create a PermissionRestriction from a dictionary."""
        return cls(name=data.get("name", ""), value=data.get("value"))


@dataclass
class Permission:
    """User permission information."""

    id: int
    name: str
    restrictions: list[PermissionRestriction] = field(default_factory=list)

    _EXPECTED_FIELDS = {"id", "name", "restrictions"}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Permission":
        """Create a Permission from a dictionary with validation."""
        _validate_field(data, "id", int, "Permission", required=True)
        _validate_field(data, "name", str, "Permission", required=True)
        _check_unexpected_fields(data, cls._EXPECTED_FIELDS, "Permission")

        restrictions = [
            PermissionRestriction.from_dict(r) for r in data.get("restrictions", [])
        ]
        return cls(
            id=data.get("id", 0), name=data.get("name", ""), restrictions=restrictions
        )


@dataclass
class User:
    """User information from Drime API."""

    id: int
    email: str
    created_at: str
    updated_at: str
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    display_name: str = ""
    language: str = "en"
    country: str = ""
    timezone: str = ""
    avatar: str = ""
    onboarding: int = 0
    news: int = 0
    has_password: bool = True
    model_type: str = "user"
    space_available: Optional[int] = None
    available_space: Optional[int] = None
    stripe_id: Optional[str] = None
    card_brand: Optional[str] = None
    card_last_four: Optional[str] = None
    card_expires: Optional[str] = None
    paypal_id: Optional[str] = None
    email_verified_at: Optional[str] = None
    banned_at: Optional[str] = None
    roles: list[Role] = field(default_factory=list)
    subscriptions: list[Subscription] = field(default_factory=list)
    permissions: list[Permission] = field(default_factory=list)

    _EXPECTED_FIELDS = {
        "id",
        "email",
        "created_at",
        "updated_at",
        "username",
        "first_name",
        "last_name",
        "display_name",
        "language",
        "country",
        "timezone",
        "avatar",
        "onboarding",
        "news",
        "has_password",
        "model_type",
        "space_available",
        "available_space",
        "stripe_id",
        "card_brand",
        "card_last_four",
        "card_expires",
        "paypal_id",
        "email_verified_at",
        "banned_at",
        "roles",
        "subscriptions",
        "permissions",
    }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "User":
        """Create a User from a dictionary with validation."""
        _validate_field(data, "id", int, "User", required=True)
        _validate_field(data, "email", str, "User", required=True)
        _check_unexpected_fields(data, cls._EXPECTED_FIELDS, "User")

        roles = [Role.from_dict(r) for r in data.get("roles", [])]
        subscriptions = [
            Subscription.from_dict(s) for s in data.get("subscriptions", [])
        ]
        permissions = [Permission.from_dict(p) for p in data.get("permissions", [])]

        return cls(
            id=data.get("id", 0),
            email=data.get("email", ""),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            username=data.get("username", ""),
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", ""),
            display_name=data.get("display_name", ""),
            language=data.get("language", "en"),
            country=data.get("country", ""),
            timezone=data.get("timezone", ""),
            avatar=data.get("avatar", ""),
            onboarding=data.get("onboarding", 0),
            news=data.get("news", 0),
            has_password=data.get("has_password", True),
            model_type=data.get("model_type", "user"),
            space_available=data.get("space_available"),
            available_space=data.get("available_space"),
            stripe_id=data.get("stripe_id"),
            card_brand=data.get("card_brand"),
            card_last_four=data.get("card_last_four"),
            card_expires=data.get("card_expires"),
            paypal_id=data.get("paypal_id"),
            email_verified_at=data.get("email_verified_at"),
            banned_at=data.get("banned_at"),
            roles=roles,
            subscriptions=subscriptions,
            permissions=permissions,
        )

    @property
    def full_name(self) -> str:
        """Get the full name of the user."""
        if self.display_name:
            return self.display_name
        if self.first_name or self.last_name:
            return f"{self.first_name} {self.last_name}".strip()
        return self.username or self.email.split("@")[0]

    @property
    def active_subscription(self) -> Optional[Subscription]:
        """Get the active subscription, if any."""
        for sub in self.subscriptions:
            if sub.active:
                return sub
        return None

    @property
    def available_storage(self) -> int:
        """Get total available storage in bytes."""
        if self.active_subscription and self.active_subscription.product:
            return self.active_subscription.product.available_space
        return self.space_available or self.available_space or 0

    def format_storage(self) -> str:
        """Format the available storage in a human-readable format."""
        return _format_size(self.available_storage)

    @property
    def is_verified(self) -> bool:
        """Check if the user's email is verified."""
        return self.email_verified_at is not None

    @property
    def is_banned(self) -> bool:
        """Check if the user is banned."""
        return self.banned_at is not None

    def has_permission(self, permission_name: str) -> bool:
        """Check if the user has a specific permission."""
        return any(p.name == permission_name for p in self.permissions)

    def get_permission_restrictions(self, permission_name: str) -> list[dict[str, Any]]:
        """Get restrictions for a specific permission."""
        for perm in self.permissions:
            if perm.name == permission_name:
                return [{"name": r.name, "value": r.value} for r in perm.restrictions]
        return []


@dataclass
class UserStatus:
    """Complete user status information from the API."""

    user: User
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "UserStatus":
        """Create a UserStatus from the API response."""
        user_data = data.get("user", {})
        user = User.from_dict(user_data)
        return cls(user=user, raw_data=data)

    def to_dict(self) -> dict[str, Any]:
        """Convert back to dictionary format."""
        return self.raw_data

    def to_text_summary(self) -> str:
        """Generate a one-line text summary."""
        parts = []

        if self.user.full_name != self.user.email.split("@")[0]:
            parts.append(f"Name: {self.user.full_name}")

        parts.append(f"Email: {self.user.email}")
        parts.append(f"ID: {self.user.id}")

        if self.user.available_storage > 0:
            parts.append(f"Storage: {self.user.format_storage()}")

        if self.user.active_subscription:
            sub = self.user.active_subscription
            parts.append(f"Plan: {sub.plan_name}")
            parts.append(f"Status: {sub.status_text}")

        return " | ".join(parts)

    def to_table_data(self) -> list[dict[str, str]]:
        """Generate table data for display."""
        table_data = []

        if self.user.full_name != self.user.email.split("@")[0]:
            table_data.append({"field": "Name", "value": self.user.full_name})

        table_data.append({"field": "Email", "value": self.user.email})
        table_data.append({"field": "User ID", "value": str(self.user.id)})

        if self.user.created_at:
            table_data.append(
                {"field": "Account Created", "value": self.user.created_at}
            )

        if self.user.available_storage > 0:
            table_data.append(
                {"field": "Total Storage", "value": self.user.format_storage()}
            )

        if self.user.timezone:
            table_data.append({"field": "Timezone", "value": self.user.timezone})

        if self.user.country:
            table_data.append({"field": "Country", "value": self.user.country.upper()})

        # Subscription info
        if self.user.active_subscription:
            sub = self.user.active_subscription
            table_data.append({"field": "Plan", "value": sub.plan_name})
            table_data.append(
                {"field": "Subscription Status", "value": sub.status_text}
            )

            if sub.renews_at:
                table_data.append({"field": "Renews At", "value": sub.renews_at})
            elif sub.ends_at:
                table_data.append({"field": "Ends At", "value": sub.ends_at})

        return table_data

    def to_long_format_items(self) -> list[tuple[str, str]]:
        """Generate items for long format display."""
        items = []

        if self.user.full_name != self.user.email.split("@")[0]:
            items.append(("Name", self.user.full_name))

        items.append(("Email", self.user.email))
        items.append(("User ID", str(self.user.id)))

        if self.user.username:
            items.append(("Username", self.user.username))

        if self.user.created_at:
            items.append(("Account Created", self.user.created_at))

        if self.user.email_verified_at:
            items.append(("Email Verified", "Yes"))
        else:
            items.append(("Email Verified", "No"))

        if self.user.available_storage > 0:
            items.append(("Total Storage", self.user.format_storage()))

        if self.user.timezone:
            items.append(("Timezone", self.user.timezone))

        if self.user.country:
            items.append(("Country", self.user.country.upper()))

        if self.user.language:
            items.append(("Language", self.user.language.upper()))

        # Subscription info
        if self.user.active_subscription:
            sub = self.user.active_subscription
            items.append(("Plan", sub.plan_name))
            items.append(("Subscription Status", sub.status_text))
            items.append(("Gateway", sub.gateway_name))

            if sub.renews_at:
                items.append(("Renews At", sub.renews_at))
            elif sub.ends_at:
                items.append(("Ends At", sub.ends_at))

            if sub.trial_ends_at:
                items.append(("Trial Ends", sub.trial_ends_at))

        # Role info
        if self.user.roles:
            role_names = ", ".join(r.name for r in self.user.roles)
            items.append(("Roles", role_names))

        # Permission count
        items.append(("Permissions", str(len(self.user.permissions))))

        return items


@dataclass
class FileEntryUser:
    """User information associated with a file entry."""

    id: int
    email: str
    owns_entry: bool
    entry_permissions: Optional[str] = None
    display_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    avatar: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FileEntryUser":
        """Create a FileEntryUser from a dictionary."""
        return cls(
            id=data.get("id", 0),
            email=data.get("email", ""),
            owns_entry=data.get("owns_entry", False),
            entry_permissions=data.get("entry_permissions"),
            display_name=data.get("display_name"),
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
            avatar=data.get("avatar"),
        )


@dataclass
class FileEntryTag:
    """Tag information for a file entry."""

    id: int
    name: str
    display_name: Optional[str] = None
    type: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FileEntryTag":
        """Create a FileEntryTag from a dictionary."""
        return cls(
            id=data.get("id", 0),
            name=data.get("name", ""),
            display_name=data.get("display_name"),
            type=data.get("type"),
        )


@dataclass
class FileEntryPermissions:
    """Permissions for a file entry."""

    view: bool = True
    edit: bool = False
    download: bool = True
    delete: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FileEntryPermissions":
        """Create FileEntryPermissions from a dictionary."""
        return cls(
            view=data.get("view", True),
            edit=data.get("edit", False),
            download=data.get("download", True),
            delete=data.get("delete", False),
        )


@dataclass
class FileEntry:
    """File entry information from Drime API."""

    id: int
    name: str
    file_name: str
    mime: str
    file_size: int
    parent_id: Optional[int]
    created_at: str
    type: str
    extension: Optional[str]
    hash: str
    url: str
    updated_at: Optional[str] = None
    deleted_at: Optional[str] = None
    description: Optional[str] = None
    password: Optional[str] = None
    public: bool = False
    thumbnail: bool = False
    workspace_id: Optional[int] = None
    owner_id: Optional[int] = None
    path: Optional[str] = None
    users: list[FileEntryUser] = field(default_factory=list)
    tags: list[FileEntryTag] = field(default_factory=list)
    permissions: Optional[FileEntryPermissions] = None

    # Additional fields
    file_size_formatted: Optional[str] = None

    _EXPECTED_FIELDS = {
        "id",
        "name",
        "file_name",
        "mime",
        "file_size",
        "parent_id",
        "created_at",
        "updated_at",
        "deleted_at",
        "type",
        "extension",
        "hash",
        "url",
        "description",
        "password",
        "public",
        "thumbnail",
        "workspace_id",
        "owner_id",
        "path",
        "users",
        "tags",
        "permissions",
        "file_size_formatted",
    }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FileEntry":
        """Create a FileEntry from a dictionary with validation."""
        _validate_field(data, "id", int, "FileEntry", required=True)
        _validate_field(data, "name", str, "FileEntry", required=True)
        _validate_field(data, "type", str, "FileEntry", required=True)
        _check_unexpected_fields(data, cls._EXPECTED_FIELDS, "FileEntry")

        # Parse nested objects
        users = [FileEntryUser.from_dict(u) for u in data.get("users", [])]
        tags = [FileEntryTag.from_dict(t) for t in data.get("tags", [])]
        permissions = None
        if "permissions" in data and data["permissions"]:
            permissions = FileEntryPermissions.from_dict(data["permissions"])

        return cls(
            id=data.get("id", 0),
            name=data.get("name", ""),
            file_name=data.get("file_name", ""),
            mime=data.get("mime", ""),
            file_size=data.get("file_size", 0),
            parent_id=data.get("parent_id"),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at"),
            deleted_at=data.get("deleted_at"),
            type=data.get("type", ""),
            extension=data.get("extension"),
            hash=data.get("hash", ""),
            url=data.get("url", ""),
            description=data.get("description"),
            password=data.get("password"),
            public=data.get("public", False),
            thumbnail=data.get("thumbnail", False),
            workspace_id=data.get("workspace_id"),
            owner_id=data.get("owner_id"),
            path=data.get("path"),
            users=users,
            tags=tags,
            permissions=permissions,
            file_size_formatted=data.get("file_size_formatted"),
        )

    @property
    def is_folder(self) -> bool:
        """Check if this entry is a folder."""
        return self.type == "folder"

    @property
    def is_file(self) -> bool:
        """Check if this entry is a file."""
        return self.type == "file"

    @property
    def is_deleted(self) -> bool:
        """Check if this entry is deleted."""
        return self.deleted_at is not None

    @property
    def is_public(self) -> bool:
        """Check if this entry is public."""
        return self.public

    def format_size(self) -> str:
        """Format the file size in a human-readable format."""
        if self.file_size_formatted:
            return self.file_size_formatted
        return _format_size(self.file_size)

    @property
    def owner(self) -> Optional[FileEntryUser]:
        """Get the owner of this file entry."""
        for user in self.users:
            if user.owns_entry:
                return user
        return None

    def has_tag(self, tag_name: str) -> bool:
        """Check if the file has a specific tag."""
        return any(t.name == tag_name for t in self.tags)

    def get_full_path(self) -> str:
        """Get the full path of the file."""
        if self.path:
            return f"{self.path}/{self.name}"
        return self.name

    @property
    def display_name(self) -> str:
        """Get a display-friendly name."""
        if self.is_folder:
            return f"[D] {self.name}"
        return self.name


@dataclass
class FileEntriesResult:
    """Wrapper for file entries list with formatting methods."""

    entries: list[FileEntry]
    raw_data: dict[str, Any] = field(default_factory=dict)
    pagination: Optional[dict[str, Any]] = None

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "FileEntriesResult":
        """Create FileEntriesResult from API response.

        Args:
            data: API response containing file entries

        Returns:
            FileEntriesResult instance
        """
        # Extract entries from paginated response
        entries_data = data.get("data", []) if isinstance(data, dict) else data
        if not isinstance(entries_data, list):
            entries_data = []

        # Parse entries using FileEntry model
        entries = [FileEntry.from_dict(entry) for entry in entries_data]

        # Extract pagination info if available
        pagination = None
        if isinstance(data, dict):
            pagination = {
                "current_page": data.get("current_page"),
                "last_page": data.get("last_page"),
                "per_page": data.get("per_page"),
                "total": data.get("total"),
                "from": data.get("from"),
                "to": data.get("to"),
                "next_page": data.get("next_page"),
                "prev_page": data.get("prev_page"),
            }

        return cls(entries=entries, raw_data=data, pagination=pagination)

    def to_dict(self) -> dict[str, Any]:
        """Convert back to dictionary format."""
        return self.raw_data

    def to_table_data(self) -> list[dict[str, str]]:
        """Generate table data for display.

        Returns:
            List of dictionaries suitable for table output
        """
        table_data = []
        for entry in self.entries:
            table_data.append(
                {
                    "id": str(entry.id),
                    "name": entry.name,
                    "type": entry.type,
                    "size": entry.format_size() if entry.file_size else "-",
                    "hash": entry.hash,
                    "parent_id": str(entry.parent_id)
                    if entry.parent_id is not None
                    else "-",
                    "created": _format_timestamp(entry.created_at),
                }
            )
        return table_data

    def to_long_format_items(self) -> list[tuple[str, str, dict[str, Any]]]:
        """Generate items for long format display.

        Returns:
            List of tuples (name, type indicator, details dict)
        """
        items = []
        for entry in self.entries:
            type_icon = "[D]" if entry.is_folder else "[F]"
            details = {
                "id": entry.id,
                "hash": entry.hash,
                "size": entry.format_size(),
                "parent_id": entry.parent_id,
                "created": _format_timestamp(entry.created_at),
                "owner": entry.owner.email if entry.owner else None,
                "public": entry.is_public,
                "deleted": entry.is_deleted,
            }
            items.append((entry.name, type_icon, details))
        return items

    def to_text_summary(self) -> str:
        """Generate a one-line text summary.

        Returns:
            Summary string
        """
        if not self.entries:
            return "No files found"

        # Calculate total size including both files and folders
        # (folders already have their total size from the API)
        total_size = sum(e.file_size for e in self.entries)
        folder_count = sum(1 for e in self.entries if e.is_folder)
        file_count = len(self.entries) - folder_count

        parts = []
        if folder_count > 0:
            parts.append(f"{folder_count} folder(s)")
        if file_count > 0:
            parts.append(f"{file_count} file(s)")

        if total_size > 0:
            parts.append(f"Total size: {_format_size(total_size)}")

        if self.pagination and self.pagination.get("total"):
            parts.append(f"Total items: {self.pagination['total']}")

        return " | ".join(parts)

    def to_compact_list(self) -> list[str]:
        """Generate a compact list of entries (name only).

        Returns:
            List of entry names with type indicators
        """
        return [entry.display_name for entry in self.entries]

    def filter_by_type(self, entry_type: str) -> "FileEntriesResult":
        """Filter entries by type.

        Args:
            entry_type: Type to filter by ('file' or 'folder')

        Returns:
            New FileEntriesResult with filtered entries
        """
        filtered = [e for e in self.entries if e.type == entry_type]
        return FileEntriesResult(
            entries=filtered, raw_data=self.raw_data, pagination=self.pagination
        )

    def filter_deleted(self, include_deleted: bool = True) -> "FileEntriesResult":
        """Filter entries by deleted status.

        Args:
            include_deleted: If True, only return deleted entries.
                           If False, only return non-deleted entries.

        Returns:
            New FileEntriesResult with filtered entries
        """
        if include_deleted:
            filtered = [e for e in self.entries if e.is_deleted]
        else:
            filtered = [e for e in self.entries if not e.is_deleted]

        return FileEntriesResult(
            entries=filtered, raw_data=self.raw_data, pagination=self.pagination
        )

    def sort_by(self, key: str = "name", reverse: bool = False) -> "FileEntriesResult":
        """Sort entries by a specified key.

        Args:
            key: Sort key ('name', 'size', 'created', 'type')
            reverse: If True, sort in descending order

        Returns:
            New FileEntriesResult with sorted entries
        """
        if key == "name":
            sorted_entries = sorted(
                self.entries, key=lambda e: e.name.lower(), reverse=reverse
            )
        elif key == "size":
            sorted_entries = sorted(
                self.entries, key=lambda e: e.file_size, reverse=reverse
            )
        elif key == "created":
            sorted_entries = sorted(
                self.entries, key=lambda e: e.created_at or "", reverse=reverse
            )
        elif key == "type":
            sorted_entries = sorted(self.entries, key=lambda e: e.type, reverse=reverse)
        else:
            sorted_entries = self.entries

        return FileEntriesResult(
            entries=sorted_entries, raw_data=self.raw_data, pagination=self.pagination
        )

    @property
    def total_size(self) -> int:
        """Get total size of all files (excluding folders)."""
        return sum(e.file_size for e in self.entries if not e.is_folder)

    @property
    def folder_count(self) -> int:
        """Get number of folders."""
        return sum(1 for e in self.entries if e.is_folder)

    @property
    def file_count(self) -> int:
        """Get number of files."""
        return sum(1 for e in self.entries if e.is_file)

    @property
    def is_empty(self) -> bool:
        """Check if result is empty."""
        return len(self.entries) == 0

    def get_by_id(self, entry_id: int) -> Optional[FileEntry]:
        """Get entry by ID.

        Args:
            entry_id: Entry ID to find

        Returns:
            FileEntry if found, None otherwise
        """
        for entry in self.entries:
            if entry.id == entry_id:
                return entry
        return None

    def get_by_name(self, name: str) -> Optional[FileEntry]:
        """Get entry by name.

        Args:
            name: Entry name to find

        Returns:
            FileEntry if found, None otherwise
        """
        for entry in self.entries:
            if entry.name == name:
                return entry
        return None
