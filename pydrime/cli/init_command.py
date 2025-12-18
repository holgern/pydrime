"""Init command for configuring Drime Cloud API key."""

from typing import Any

import click

from ..api import DrimeClient
from ..config import config
from ..exceptions import DrimeAPIError
from ..output import OutputFormatter


@click.command()
@click.option(
    "--api-key",
    "-k",
    prompt="Enter your Drime Cloud API key",
    help="Drime Cloud API key",
)
@click.pass_context
def init(ctx: Any, api_key: str) -> None:
    """Initialize Drime Cloud configuration.

    Stores your API key in ~/.config/pydrime/config for future use.
    """
    out: OutputFormatter = ctx.obj["out"]

    try:
        # Validate API key by attempting to use it
        out.info("Validating API key...")
        client = DrimeClient(api_key=api_key)

        # Try to make a simple API call to validate the key
        try:
            user_info = client.get_logged_user()
            # Check if user is null (invalid API key)
            if not user_info or not user_info.get("user"):
                out.error("API key validation failed: Invalid API key")
                if not click.confirm("Save API key anyway?", default=False):
                    out.warning("Configuration cancelled.")
                    ctx.exit(1)
            else:
                out.success("✓ API key is valid")
        except DrimeAPIError as e:
            out.error(f"API key validation failed: {e}")
            if not click.confirm("Save API key anyway?", default=False):
                out.warning("Configuration cancelled.")
                ctx.exit(1)

        # Save the API key
        config.save_api_key(api_key)
        config_path = config.get_config_path()

        out.print_summary(
            "Initialization Complete",
            [
                ("Status", "✓ Configuration saved successfully"),
                ("Config file", str(config_path)),
                ("Note", "You can now use drime commands without specifying --api-key"),
            ],
        )

    except Exception as e:
        out.error(f"Initialization failed: {e}")
        ctx.exit(1)
