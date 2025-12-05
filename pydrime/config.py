"""Configuration management for Drime Cloud API."""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


class Config:
    """Configuration manager for Drime Cloud credentials and settings."""

    CONFIG_DIR = Path.home() / ".config" / "pydrime"
    CONFIG_FILE = CONFIG_DIR / "config"
    LOG_DIR = CONFIG_DIR / "logs"
    DEFAULT_LOG_FILE = LOG_DIR / "pydrime.log"

    def __init__(self) -> None:
        """Initialize configuration by loading from multiple sources."""
        # Priority order:
        # 1. Environment variables (highest priority)
        # 2. ~/.config/pydrime/config file
        # 3. Local .env file (lowest priority)

        # Initialize attributes
        self._config_api_key: Optional[str] = None
        self._config_current_folder_id: Optional[int] = None
        self._config_default_workspace_id: Optional[int] = None

        # Load local .env file if it exists
        env_path = Path.cwd() / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        # Load config from ~/.config/pydrime/config
        self._load_from_config_file()

        # Environment variables take precedence
        self.api_key: Optional[str] = os.getenv("DRIME_API_KEY") or self._config_api_key
        self.api_url: str = os.getenv("DRIME_API_URL", "https://app.drime.cloud/api/v1")
        self.current_folder_id: Optional[int] = self._config_current_folder_id
        self.default_workspace_id: Optional[int] = self._config_default_workspace_id

    def _load_from_config_file(self) -> None:
        """Load configuration from ~/.config/pydrime/config file."""
        if self.CONFIG_FILE.exists():
            try:
                with open(self.CONFIG_FILE) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            if "=" in line:
                                key, value = line.split("=", 1)
                                key = key.strip()
                                value = value.strip().strip('"').strip("'")

                                if key == "DRIME_API_KEY":
                                    self._config_api_key = value
                                elif key == "CURRENT_FOLDER_ID":
                                    try:
                                        self._config_current_folder_id = (
                                            int(value) if value else None
                                        )
                                    except ValueError:
                                        pass
                                elif key == "DEFAULT_WORKSPACE_ID":
                                    try:
                                        self._config_default_workspace_id = (
                                            int(value) if value else None
                                        )
                                    except ValueError:
                                        pass
            except Exception:
                # Silently ignore errors reading config file
                pass

    def is_configured(self) -> bool:
        """Check if required configuration is present."""
        return bool(self.api_key)

    def get_headers(self) -> dict[str, str]:
        """Get authentication headers for API requests."""
        if not self.api_key:
            raise ValueError("API key not configured")

        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def save_api_key(self, api_key: str) -> None:
        """Save API key to ~/.config/pydrime/config file.

        Args:
            api_key: The API key to save
        """
        # Create config directory if it doesn't exist
        self.CONFIG_DIR.mkdir(parents=True, exist_ok=True)

        # Read existing config
        config_lines = []
        if self.CONFIG_FILE.exists():
            with open(self.CONFIG_FILE) as f:
                config_lines = [
                    line for line in f if not line.strip().startswith("DRIME_API_KEY=")
                ]

        # Add new API key
        config_lines.append(f"DRIME_API_KEY={api_key}\n")

        # Write config file
        with open(self.CONFIG_FILE, "w") as f:
            f.writelines(config_lines)

        # Set secure permissions (owner read/write only)
        self.CONFIG_FILE.chmod(0o600)

        # Update current instance
        self.api_key = api_key
        self._config_api_key = api_key

    def get_config_path(self) -> Path:
        """Get the path to the config file.

        Returns:
            Path to the config file
        """
        return self.CONFIG_FILE

    def save_current_folder(self, folder_id: Optional[int]) -> None:
        """Save current folder ID to config file.

        Args:
            folder_id: The folder ID to save as current working directory
                (None for root)
        """
        # Create config directory if it doesn't exist
        self.CONFIG_DIR.mkdir(parents=True, exist_ok=True)

        # Read existing config
        config_lines = []
        if self.CONFIG_FILE.exists():
            with open(self.CONFIG_FILE) as f:
                config_lines = [
                    line
                    for line in f
                    if not line.strip().startswith("CURRENT_FOLDER_ID=")
                ]

        # Add new current folder
        if folder_id is not None:
            config_lines.append(f"CURRENT_FOLDER_ID={folder_id}\n")
        else:
            config_lines.append("CURRENT_FOLDER_ID=\n")

        # Write config file
        with open(self.CONFIG_FILE, "w") as f:
            f.writelines(config_lines)

        # Set secure permissions (owner read/write only)
        self.CONFIG_FILE.chmod(0o600)

        # Update current instance
        self.current_folder_id = folder_id
        self._config_current_folder_id = folder_id

    def get_current_folder(self) -> Optional[int]:
        """Get the current working folder ID.

        Returns:
            Current folder ID or None for root
        """
        return self.current_folder_id

    def save_default_workspace(self, workspace_id: Optional[int]) -> None:
        """Save default workspace ID to config file.

        Args:
            workspace_id: The workspace ID to save as default (None for personal)
        """
        # Create config directory if it doesn't exist
        self.CONFIG_DIR.mkdir(parents=True, exist_ok=True)

        # Read existing config
        config_lines = []
        if self.CONFIG_FILE.exists():
            with open(self.CONFIG_FILE) as f:
                config_lines = [
                    line
                    for line in f
                    if not line.strip().startswith("DEFAULT_WORKSPACE_ID=")
                ]

        # Add new default workspace
        if workspace_id is not None:
            config_lines.append(f"DEFAULT_WORKSPACE_ID={workspace_id}\n")
        else:
            config_lines.append("DEFAULT_WORKSPACE_ID=\n")

        # Write config file
        with open(self.CONFIG_FILE, "w") as f:
            f.writelines(config_lines)

        # Set secure permissions (owner read/write only)
        self.CONFIG_FILE.chmod(0o600)

        # Update current instance
        self.default_workspace_id = workspace_id
        self._config_default_workspace_id = workspace_id

    def get_default_workspace(self) -> Optional[int]:
        """Get the default workspace ID.

        Returns:
            Default workspace ID or None for personal workspace (0)
        """
        return self.default_workspace_id


# Global config instance
config = Config()
