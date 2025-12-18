"""PyDrime CLI - Modular command-line interface for Drime Cloud."""

from typing import Any, Optional

import click

from ..config import config
from ..logging import setup_logging
from ..models import SchemaValidationWarning
from ..output import OutputFormatter

# Import command modules
from .download_command import download
from .file_management_commands import mkdir, rename, rm, share
from .info_commands import pwd, stat
from .init_command import init
from .list_commands import du, ls
from .read_commands import cat, head, tail
from .server_commands import server
from .special_views_commands import recent, starred, trash
from .sync_command import sync
from .upload_command import upload
from .utility_commands import cd, find_duplicates, status, usage, validate
from .vault_commands import vault
from .workspace_commands import folders, workspace, workspaces


@click.group()
@click.option("--api-key", "-k", envvar="DRIME_API_KEY", help="Drime Cloud API key")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-essential output")
@click.option("--json", is_flag=True, help="Output in JSON format")
@click.option(
    "--validate-schema",
    is_flag=True,
    help="Enable API schema validation warnings (for debugging)",
)
@click.option(
    "--log-level",
    type=click.Choice(["error", "warning", "info", "debug", "api"]),
    envvar="PYDRIME_LOG_LEVEL",
    help="Log level (enables logging to console, or file if --log-file is set)",
)
@click.option(
    "--log-file",
    type=click.Path(),
    envvar="PYDRIME_LOG_FILE",
    help="Log to file instead of console (default: ~/.config/pydrime/logs/pydrime.log)",
)
@click.version_option()
@click.pass_context
def main(
    ctx: Any,
    api_key: Optional[str],
    quiet: bool,
    json: bool,
    validate_schema: bool,
    log_level: Optional[str],
    log_file: Optional[str],
) -> None:
    """PyDrime - Upload & Download files and directories to Drime Cloud."""
    # Store settings in context for subcommands to access
    ctx.ensure_object(dict)
    ctx.obj["api_key"] = api_key
    ctx.obj["out"] = OutputFormatter(json_output=json, quiet=quiet)
    ctx.obj["validate_schema"] = validate_schema
    ctx.obj["log_level"] = log_level

    # Configure logging based on log-level and log-file flags
    setup_logging(log_level=log_level, log_file=log_file)

    # Enable schema validation if flag is set
    if validate_schema:
        SchemaValidationWarning.enable()
        SchemaValidationWarning.clear_warnings()  # Clear any previous warnings


# Register commands
# Core commands
main.add_command(init)
main.add_command(status)
main.add_command(usage)

# Navigation commands
main.add_command(cd)
main.add_command(pwd)

# List commands
main.add_command(ls)
main.add_command(du)

# File reading commands
main.add_command(cat)
main.add_command(head)
main.add_command(tail)

# Info commands
main.add_command(stat)

# Special views
main.add_command(recent)
main.add_command(trash)
main.add_command(starred)

# Workspace commands
main.add_command(workspace)
main.add_command(workspaces)
main.add_command(folders)

# File management commands
main.add_command(mkdir)
main.add_command(rename)
main.add_command(rm)
main.add_command(share)

# Upload/download/sync commands
main.add_command(upload)
main.add_command(download)
main.add_command(sync)

# Validation and duplicate commands
main.add_command(validate)
main.add_command(find_duplicates)

# Command groups
main.add_command(vault)
main.add_command(server)


if __name__ == "__main__":
    main()
