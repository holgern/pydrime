"""Authentication and API key validation helpers."""

from typing import Any, Optional

from .config import config
from .output import OutputFormatter


def require_api_key(ctx: Any, out: OutputFormatter) -> Optional[str]:
    """Check if API key is configured and exit with error if not.

    Args:
        ctx: Click context
        out: Output formatter

    Returns:
        API key if available

    Raises:
        SystemExit: If API key is not configured
    """
    api_key: Optional[str] = ctx.obj.get("api_key")

    # Check if API key is not configured (None or empty string)
    if not config.is_configured() and (not api_key or api_key == ""):
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key, or:")
        out.info("• Set the DRIME_API_KEY environment variable")
        out.info("• Use --api-key option")
        out.info("• Create a .env file with DRIME_API_KEY=your_key_here")
        ctx.exit(1)

    return api_key


def require_api_key_simple(ctx: Any, out: OutputFormatter) -> Optional[str]:
    """Check if API key is configured (simple error message).

    Args:
        ctx: Click context
        out: Output formatter

    Returns:
        API key if available

    Raises:
        SystemExit: If API key is not configured
    """
    api_key: Optional[str] = ctx.obj.get("api_key")

    # Check if API key is not configured (None or empty string)
    if not config.is_configured() and (not api_key or api_key == ""):
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    return api_key
