"""Sync modes for different synchronization strategies."""

from enum import Enum


class SyncMode(str, Enum):
    """Synchronization modes for local and cloud sync.

    Each mode has different behavior for file changes, deletions, conflicts.
    """

    # Full sync modes
    TWO_WAY = "twoWay"
    """Mirror every action in both directions.
    Renaming, deleting & moving is applied to both sides."""

    LOCAL_TO_CLOUD = "localToCloud"
    """Mirror every action done locally to the cloud but never act on cloud changes.
    Renaming, deleting & moving is only transferred to the cloud."""

    LOCAL_BACKUP = "localBackup"
    """Only upload data to the cloud, never delete anything or act on cloud changes.
    Renaming & moving is transferred to the cloud, but not local deletions."""

    CLOUD_TO_LOCAL = "cloudToLocal"
    """Mirror every action done in the cloud locally but never act on local changes.
    Renaming, deleting & moving is only transferred to the local side."""

    CLOUD_BACKUP = "cloudBackup"
    """Only download data from the cloud, never delete anything or act on local changes.
    Renaming & moving is transferred to the local side, but not cloud deletions."""

    @classmethod
    def from_string(cls, value: str) -> "SyncMode":
        """Parse sync mode from string, supporting abbreviations.

        Args:
            value: Mode string (full name or abbreviation)

        Returns:
            SyncMode enum value

        Raises:
            ValueError: If mode string is not recognized

        Examples:
            >>> SyncMode.from_string("twoWay")
            SyncMode.TWO_WAY
            >>> SyncMode.from_string("tw")
            SyncMode.TWO_WAY
            >>> SyncMode.from_string("ltc")
            SyncMode.LOCAL_TO_CLOUD
        """
        # Map of abbreviations to full names
        abbreviations = {
            "tw": "twoWay",
            "ltc": "localToCloud",
            "lb": "localBackup",
            "ctl": "cloudToLocal",
            "cb": "cloudBackup",
        }

        # Normalize input
        normalized = value.lower()

        # Check if it's an abbreviation
        if normalized in abbreviations:
            value = abbreviations[normalized]

        # Try to find matching enum value (case-insensitive)
        for mode in cls:
            if mode.value.lower() == value.lower():
                return mode

        # If no match found, raise error with helpful message
        valid_values = [m.value for m in cls] + list(abbreviations.keys())
        raise ValueError(
            f"Invalid sync mode: {value}. Valid values are: {', '.join(valid_values)}"
        )

    @property
    def allows_upload(self) -> bool:
        """Check if this mode allows uploading files."""
        return self in {
            SyncMode.TWO_WAY,
            SyncMode.LOCAL_TO_CLOUD,
            SyncMode.LOCAL_BACKUP,
        }

    @property
    def allows_download(self) -> bool:
        """Check if this mode allows downloading files."""
        return self in {
            SyncMode.TWO_WAY,
            SyncMode.CLOUD_TO_LOCAL,
            SyncMode.CLOUD_BACKUP,
        }

    @property
    def allows_local_delete(self) -> bool:
        """Check if this mode allows deleting local files."""
        return self in {SyncMode.TWO_WAY, SyncMode.CLOUD_TO_LOCAL}

    @property
    def allows_remote_delete(self) -> bool:
        """Check if this mode allows deleting remote files."""
        return self in {SyncMode.TWO_WAY, SyncMode.LOCAL_TO_CLOUD}

    @property
    def is_bidirectional(self) -> bool:
        """Check if this mode syncs in both directions."""
        return self == SyncMode.TWO_WAY

    @property
    def requires_local_scan(self) -> bool:
        """Check if this mode requires scanning local files.

        We need to scan local files if we might upload, delete locally,
        or download (to compare against existing local files for idempotency).
        """
        return self.allows_upload or self.allows_local_delete or self.allows_download

    @property
    def requires_remote_scan(self) -> bool:
        """Check if this mode requires scanning remote files.

        We need to scan remote files if we might download, delete remotely,
        or upload (to compare against existing remote files for idempotency).
        """
        return self.allows_download or self.allows_remote_delete or self.allows_upload

    def __str__(self) -> str:
        """Return string representation."""
        return self.value
