"""File management commands (mkdir, rename, rm, share)."""

from typing import Any, Optional

import click

from ..api import DrimeClient
from ..config import config
from ..exceptions import DrimeAPIError, DrimeNotFoundError
from ..output import OutputFormatter
from ..utils import is_glob_pattern


@click.command()
@click.argument("name")
@click.option("--parent-id", "-p", type=int, help="Parent folder ID (omit for root)")
@click.pass_context
def mkdir(ctx: Any, name: str, parent_id: Optional[int]) -> None:
    """Create a directory in Drime Cloud.

    NAME: Name of the directory to create
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)
        result = client.create_directory(name=name, parent_id=parent_id)

        if out.json_output:
            out.output_json(result)
        else:
            out.success(f"Directory created: {name}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@click.command()
@click.argument("entry_identifier", type=str)
@click.argument("new_name", type=str)
@click.option("--description", "-d", help="New description for the entry")
@click.pass_context
def rename(
    ctx: Any, entry_identifier: str, new_name: str, description: Optional[str]
) -> None:
    """Rename a file or folder entry.

    ENTRY_IDENTIFIER: ID or name of the entry to rename
    NEW_NAME: New name for the entry

    Supports both numeric IDs and file/folder names. Names are resolved
    in the current working directory.

    Examples:
        pydrime rename 480424796 newfile.txt         # Rename by ID
        pydrime rename test1.txt newfile.txt         # Rename by name
        pydrime rename drime_test my_folder          # Rename folder by name
        pydrime rename test.txt file.txt -d "Desc"   # Rename with description
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

        # Resolve identifier to entry ID
        try:
            entry_id = int(entry_identifier)
        except ValueError:
            # Not a numeric ID, resolve by name
            entry_id = client.resolve_entry_identifier(
                entry_identifier, current_folder, workspace
            )

        result = client.update_file_entry(
            entry_id, name=new_name, description=description
        )

        if out.json_output:
            out.output_json(result)
        else:
            out.success(f"✓ Entry renamed to: {new_name}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@click.command()
@click.argument("entry_identifiers", nargs=-1, type=str, required=True)
@click.option("--no-trash", is_flag=True, help="Delete permanently (cannot be undone)")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses default workspace if not specified)",
)
@click.pass_context
def rm(
    ctx: Any,
    entry_identifiers: tuple[str, ...],
    no_trash: bool,
    yes: bool,
    workspace: Optional[int],
) -> None:
    """Delete one or more file or folder entries.

    ENTRY_IDENTIFIERS: One or more entry IDs, names, or glob patterns to delete

    Supports numeric IDs, file/folder names, and glob patterns (*, ?, []).
    Names and patterns are resolved in the current working directory.

    Examples:
        pydrime rm 480424796                    # Delete by ID
        pydrime rm test1.txt                    # Delete by name
        pydrime rm drime_test                   # Delete folder by name
        pydrime rm test1.txt test2.txt          # Delete multiple files
        pydrime rm 480424796 drime_test         # Mix IDs and names
        pydrime rm "*.log"                      # Delete all .log files
        pydrime rm "temp*"                      # Delete entries starting with temp
        pydrime rm --no-trash test1.txt         # Permanent deletion
        pydrime rm -w 5 test.txt                # Delete in workspace 5
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    # Use default workspace if none specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

    try:
        client = DrimeClient(api_key=api_key)
        current_folder = config.get_current_folder()

        # Resolve all identifiers to entry IDs
        entry_ids: list[int] = []
        for identifier in entry_identifiers:
            try:
                # Check if identifier is a glob pattern
                if is_glob_pattern(identifier):
                    # Resolve glob pattern to matching entries
                    matching_entries = client.resolve_entries_by_pattern(
                        pattern=identifier,
                        parent_id=current_folder,
                        workspace_id=workspace,
                    )
                    if not matching_entries:
                        out.warning(f"No entries match pattern '{identifier}'")
                        continue
                    if not out.quiet:
                        out.info(
                            f"Pattern '{identifier}' matched {len(matching_entries)} "
                            f"entries: {', '.join(e.name for e in matching_entries)}"
                        )
                    entry_ids.extend(e.id for e in matching_entries)
                # Check if identifier is a path (contains /)
                elif "/" in identifier:
                    entry_id = client.resolve_path_to_id(
                        path=identifier,
                        workspace_id=workspace,
                    )
                    if not out.quiet:
                        out.info(f"Resolved '{identifier}' to entry ID: {entry_id}")
                    entry_ids.append(entry_id)
                else:
                    entry_id = client.resolve_entry_identifier(
                        identifier=identifier,
                        parent_id=current_folder,
                        workspace_id=workspace,
                    )
                    if not out.quiet and not identifier.isdigit():
                        out.info(f"Resolved '{identifier}' to entry ID: {entry_id}")
                    entry_ids.append(entry_id)
            except DrimeNotFoundError as e:
                out.error(str(e))
                ctx.exit(1)

        if not entry_ids:
            out.warning("No entries to delete.")
            return

        # Confirm deletion
        action = "permanently delete" if no_trash else "move to trash"
        if (
            not yes
            and not out.quiet
            and not click.confirm(
                f"Are you sure you want to {action} {len(entry_ids)} item(s)?"
            )
        ):
            out.warning("Deletion cancelled.")
            return

        result = client.delete_file_entries(
            entry_ids, delete_forever=no_trash, workspace_id=workspace
        )

        if out.json_output:
            out.output_json(result)
        else:
            if no_trash:
                out.success(f"✓ Permanently deleted {len(entry_ids)} item(s)")
            else:
                out.success(f"✓ Moved {len(entry_ids)} item(s) to trash")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@click.command()
@click.argument("entry_identifiers", nargs=-1, required=True)
@click.option("--password", "-p", help="Optional password for the link")
@click.option(
    "--expires", "-e", help="Expiration date (format: 2025-12-31T23:59:59.000000Z)"
)
@click.option("--allow-edit", is_flag=True, help="Allow editing through the link")
@click.option(
    "--allow-download",
    is_flag=True,
    default=True,
    help="Allow downloading through the link",
)
@click.pass_context
def share(
    ctx: Any,
    entry_identifiers: tuple[str, ...],
    password: Optional[str],
    expires: Optional[str],
    allow_edit: bool,
    allow_download: bool,
) -> None:
    """Create a shareable link for file(s) or folder(s).

    ENTRY_IDENTIFIERS: One or more IDs, names, or glob patterns of entries to share

    Supports numeric IDs, file/folder names, and glob patterns (*, ?, []).
    Names are resolved in the current working directory.

    Glob patterns:
        * matches any sequence of characters
        ? matches any single character
        [abc] matches any character in the set

    Examples:
        pydrime share 480424796                   # Share by ID
        pydrime share test1.txt                   # Share by name
        pydrime share drime_test                  # Share folder by name
        pydrime share test.txt -p mypass123       # Share with password
        pydrime share test.txt -e 2025-12-31      # Share with expiration
        pydrime share test.txt --allow-edit       # Allow editing
        pydrime share "*.txt"                     # Share all .txt files
        pydrime share "doc*"                      # Share entries starting with doc
        pydrime share file1.txt file2.txt         # Share multiple files
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

        # Expand glob patterns to entry names
        expanded_identifiers: list[str] = []
        for identifier in entry_identifiers:
            if is_glob_pattern(identifier):
                # Resolve glob pattern to matching entries
                matching_entries = client.resolve_entries_by_pattern(
                    pattern=identifier,
                    parent_id=current_folder,
                    workspace_id=workspace,
                )
                if matching_entries:
                    if not out.quiet:
                        out.info(
                            f"Matched {len(matching_entries)} entries "
                            f"with pattern '{identifier}'"
                        )
                    for entry in matching_entries:
                        expanded_identifiers.append(entry.name)
                else:
                    out.warning(f"No entries match pattern '{identifier}'")
            else:
                expanded_identifiers.append(identifier)

        if not expanded_identifiers:
            out.warning("No entries to share.")
            return

        # Process each identifier
        results: list[dict] = []
        errors: list[str] = []

        for identifier in expanded_identifiers:
            # Resolve identifier to entry ID
            try:
                entry_id = int(identifier)
            except ValueError:
                # Not a numeric ID, resolve by name
                try:
                    entry_id = client.resolve_entry_identifier(
                        identifier, current_folder, workspace
                    )
                except DrimeNotFoundError:
                    errors.append(f"Entry not found: {identifier}")
                    continue

            try:
                result = client.create_shareable_link(
                    entry_id=entry_id,
                    password=password,
                    expires_at=expires,
                    allow_edit=allow_edit,
                    allow_download=allow_download,
                )

                if isinstance(result, dict) and "link" in result:
                    link_hash = result["link"].get("hash", "")
                    link_url = f"https://dri.me/{link_hash}"
                    results.append(
                        {"identifier": identifier, "url": link_url, "result": result}
                    )
                    if not out.quiet and not out.json_output:
                        out.success(f"✓ {identifier}: {link_url}")
                else:
                    results.append({"identifier": identifier, "result": result})
                    if not out.quiet and not out.json_output:
                        out.warning(
                            f"Link created for {identifier} (format unexpected)"
                        )
            except DrimeAPIError as e:
                errors.append(f"{identifier}: {e}")

        if out.json_output:
            out.output_json({"links": results, "errors": errors})
        else:
            if results and not out.quiet:
                if len(results) > 1:
                    out.success(f"\n✓ Created {len(results)} shareable link(s)")
            if errors:
                for error in errors:
                    out.error(error)

        if errors and not results:
            ctx.exit(1)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)
