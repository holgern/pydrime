"""Read commands - cat, head, and tail commands for pydrime CLI."""

from typing import Any, Optional

import click

from ..api import DrimeClient
from ..config import config
from ..exceptions import DrimeAPIError
from ..output import OutputFormatter


def _get_file_content_lines(
    client: DrimeClient,
    identifier: str,
    current_folder: Optional[int],
    workspace: int,
    out: OutputFormatter,
    ctx: Any,
    max_bytes: Optional[int] = None,
) -> tuple[Optional[list[str]], Optional[str]]:
    """Get file content as lines.

    Args:
        client: Drime API client
        identifier: File path, name, hash, or numeric ID
        current_folder: Current folder ID
        workspace: Workspace ID
        out: Output formatter
        ctx: Click context
        max_bytes: Maximum bytes to read (None for entire file)

    Returns:
        Tuple of (lines, filename) or (None, None) if error
    """
    from ..download_helpers import get_entry_from_hash, resolve_identifier_to_hash

    # Resolve identifier to hash
    hash_value = resolve_identifier_to_hash(
        client, identifier, current_folder, workspace, out
    )

    if not hash_value:
        out.error(f"File not found: {identifier}")
        return None, None

    # Get entry details to check it's a file
    entry = get_entry_from_hash(client, hash_value, identifier, out)

    if not entry:
        return None, None

    if entry.type == "folder":
        out.error(f"Cannot display folder contents: {identifier}")
        return None, None

    # Get file content
    content = client.get_file_content(hash_value, max_bytes=max_bytes)

    # Try to decode as text
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = content.decode("latin-1")
        except UnicodeDecodeError:
            out.error(f"File appears to be binary: {identifier}")
            return None, None

    lines = text.splitlines()
    return lines, entry.name


@click.command()
@click.argument("identifier", type=str)
@click.option(
    "-n",
    "--number",
    is_flag=True,
    help="Number all output lines",
)
@click.pass_context
def cat(ctx: Any, identifier: str, number: bool) -> None:
    """Print file contents to standard output.

    IDENTIFIER: File path, name, hash, or numeric ID

    Displays the entire contents of a cloud file. For binary files, use
    the download command instead.

    Examples:
        pydrime cat readme.txt              # By name
        pydrime cat folder/config.json      # By path
        pydrime cat 480424796               # By numeric ID
        pydrime cat NDgwNDI0Nzk2fA          # By hash
        pydrime cat readme.txt -n           # With line numbers
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        lines, filename = _get_file_content_lines(
            client, identifier, current_folder, workspace, out, ctx
        )

        if lines is None:
            ctx.exit(1)
            return

        if out.json_output:
            out.output_json({"filename": filename, "lines": lines})
        else:
            if number:
                for i, line in enumerate(lines, 1):
                    click.echo(f"{i:6d}  {line}")
            else:
                for line in lines:
                    click.echo(line)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@click.command()
@click.argument("identifier", type=str)
@click.option(
    "-n",
    "--lines",
    "num_lines",
    type=int,
    default=10,
    help="Number of lines to show (default: 10)",
)
@click.option(
    "-c",
    "--bytes",
    "num_bytes",
    type=int,
    default=None,
    help="Number of bytes to show (overrides -n)",
)
@click.pass_context
def head(ctx: Any, identifier: str, num_lines: int, num_bytes: Optional[int]) -> None:
    """Print first lines of a file.

    IDENTIFIER: File path, name, hash, or numeric ID

    Displays the first N lines (default: 10) or bytes of a cloud file.

    Examples:
        pydrime head readme.txt             # First 10 lines
        pydrime head readme.txt -n 20       # First 20 lines
        pydrime head config.json -c 100     # First 100 bytes
        pydrime head folder/file.txt        # By path
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        if num_bytes is not None:
            # Byte mode - read exact bytes
            from ..download_helpers import (
                get_entry_from_hash,
                resolve_identifier_to_hash,
            )

            hash_value = resolve_identifier_to_hash(
                client, identifier, current_folder, workspace, out
            )
            if not hash_value:
                out.error(f"File not found: {identifier}")
                ctx.exit(1)
                return

            entry = get_entry_from_hash(client, hash_value, identifier, out)
            if not entry:
                ctx.exit(1)
                return

            if entry.type == "folder":
                out.error(f"Cannot display folder contents: {identifier}")
                ctx.exit(1)
                return

            content = client.get_file_content(hash_value, max_bytes=num_bytes)

            if out.json_output:
                try:
                    text = content.decode("utf-8")
                    out.output_json({"filename": entry.name, "content": text})
                except UnicodeDecodeError:
                    import base64

                    out.output_json(
                        {
                            "filename": entry.name,
                            "content_base64": base64.b64encode(content).decode("ascii"),
                        }
                    )
            else:
                try:
                    click.echo(content.decode("utf-8"), nl=False)
                except UnicodeDecodeError:
                    out.error(
                        "File appears to be binary. Use -c with --json for base64."
                    )
                    ctx.exit(1)
        else:
            # Line mode
            # Read extra bytes to ensure we get enough lines
            # Estimate ~100 bytes per line on average
            max_bytes_estimate = num_lines * 200

            lines, filename = _get_file_content_lines(
                client,
                identifier,
                current_folder,
                workspace,
                out,
                ctx,
                max_bytes=max_bytes_estimate,
            )

            if lines is None:
                ctx.exit(1)
                return

            output_lines = lines[:num_lines]

            if out.json_output:
                out.output_json({"filename": filename, "lines": output_lines})
            else:
                for line in output_lines:
                    click.echo(line)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@click.command()
@click.argument("identifier", type=str)
@click.option(
    "-n",
    "--lines",
    "num_lines",
    type=int,
    default=10,
    help="Number of lines to show (default: 10)",
)
@click.option(
    "-c",
    "--bytes",
    "num_bytes",
    type=int,
    default=None,
    help="Number of bytes to show (overrides -n)",
)
@click.pass_context
def tail(ctx: Any, identifier: str, num_lines: int, num_bytes: Optional[int]) -> None:
    """Print last lines of a file.

    IDENTIFIER: File path, name, hash, or numeric ID

    Displays the last N lines (default: 10) or bytes of a cloud file.

    Examples:
        pydrime tail readme.txt             # Last 10 lines
        pydrime tail readme.txt -n 20       # Last 20 lines
        pydrime tail logfile.log -c 500     # Last 500 bytes
        pydrime tail folder/file.txt        # By path
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()
        workspace = config.get_default_workspace() or 0

        if num_bytes is not None:
            # Byte mode - need to read entire file and take last bytes
            from ..download_helpers import (
                get_entry_from_hash,
                resolve_identifier_to_hash,
            )

            hash_value = resolve_identifier_to_hash(
                client, identifier, current_folder, workspace, out
            )
            if not hash_value:
                out.error(f"File not found: {identifier}")
                ctx.exit(1)
                return

            entry = get_entry_from_hash(client, hash_value, identifier, out)
            if not entry:
                ctx.exit(1)
                return

            if entry.type == "folder":
                out.error(f"Cannot display folder contents: {identifier}")
                ctx.exit(1)
                return

            # For tail -c, we need to read the whole file
            content = client.get_file_content(hash_value)
            content = content[-num_bytes:] if len(content) > num_bytes else content

            if out.json_output:
                try:
                    text = content.decode("utf-8")
                    out.output_json({"filename": entry.name, "content": text})
                except UnicodeDecodeError:
                    import base64

                    out.output_json(
                        {
                            "filename": entry.name,
                            "content_base64": base64.b64encode(content).decode("ascii"),
                        }
                    )
            else:
                try:
                    click.echo(content.decode("utf-8"), nl=False)
                except UnicodeDecodeError:
                    out.error(
                        "File appears to be binary. Use -c with --json for base64."
                    )
                    ctx.exit(1)
        else:
            # Line mode - need to read entire file
            lines, filename = _get_file_content_lines(
                client, identifier, current_folder, workspace, out, ctx
            )

            if lines is None:
                ctx.exit(1)
                return

            output_lines = lines[-num_lines:] if len(lines) > num_lines else lines

            if out.json_output:
                out.output_json({"filename": filename, "lines": output_lines})
            else:
                for line in output_lines:
                    click.echo(line)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)
