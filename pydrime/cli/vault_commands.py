"""Vault commands for encrypted storage management."""

import base64
import glob as glob_module
import re
import tempfile
from pathlib import Path
from typing import Any, Literal, Optional, cast

import click

from ..api import DrimeClient
from ..config import config
from ..exceptions import DrimeAPIError
from ..output import OutputFormatter
from ..utils import (
    calculate_drime_hash,
    glob_match,
    is_glob_pattern,
    parse_iso_timestamp,
)
from ..vault_crypto import (
    VaultPasswordError,
    decrypt_file_content,
    decrypt_filename,
    encrypt_filename,
    unlock_vault,
)

try:
    from .helpers import VAULT_PASSWORD_ENV_VAR, get_vault_password_from_env
except ImportError:
    # Fallback if helpers module not available yet
    VAULT_PASSWORD_ENV_VAR = "PYDRIME_VAULT_PASSWORD"

    def get_vault_password_from_env() -> Optional[str]:
        import os

        return os.environ.get(VAULT_PASSWORD_ENV_VAR)


@click.group()
@click.pass_context
def vault(ctx: Any) -> None:
    """Manage encrypted vault storage.

    Commands for working with your encrypted vault.

    Examples:
        pydrime vault show                # Show vault info
        pydrime vault ls                  # List vault root
        pydrime vault ls Test1            # List folder by name
    """
    pass


@vault.command("unlock")
@click.pass_context
def vault_unlock(ctx: Any) -> None:
    """Unlock the vault for the current shell session.

    Prompts for your vault password and outputs shell commands to set
    an environment variable. The password is stored in memory only
    and never written to disk.

    Usage (bash/zsh):
        eval $(pydrime vault unlock)

    Usage (fish):
        pydrime vault unlock | source

    After unlocking, vault commands won't prompt for password.
    Use 'pydrime vault lock' to clear the password from your session.
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info for password verification
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        # Get encryption parameters from vault
        salt = vault_info.get("salt")
        check = vault_info.get("check")
        iv = vault_info.get("iv")

        if not all([salt, check, iv]):
            out.error("Vault encryption parameters not found.")
            ctx.exit(1)
            return

        # Prompt for password
        password = click.prompt("Vault password", hide_input=True, err=True)

        # Verify the password
        try:
            unlock_vault(password, salt, check, iv)
        except VaultPasswordError:
            out.error("Invalid vault password.")
            ctx.exit(1)
            return

        # Output shell command to set environment variable
        # Using click.echo to bypass OutputFormatter and write to stdout
        click.echo(f"export {VAULT_PASSWORD_ENV_VAR}='{password}'")

        # Print success message to stderr so it doesn't interfere with eval
        click.echo("Vault unlocked. Password stored in shell session.", err=True)

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("lock")
@click.pass_context
def vault_lock(ctx: Any) -> None:
    """Lock the vault and clear password from shell session.

    Outputs shell commands to unset the vault password environment variable.

    Usage (bash/zsh):
        eval $(pydrime vault lock)

    Usage (fish):
        pydrime vault lock | source
    """
    # Output shell command to unset environment variable
    click.echo(f"unset {VAULT_PASSWORD_ENV_VAR}")

    # Print message to stderr so it doesn't interfere with eval
    click.echo("Vault locked. Password cleared from shell session.", err=True)


@vault.command("show")
@click.pass_context
def vault_show(ctx: Any) -> None:
    """Show vault information.

    Displays metadata about your encrypted vault including ID and timestamps.

    Examples:
        pydrime vault show                # Show vault info
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info
        vault_result = client.get_vault()

        if out.json_output:
            out.output_json(vault_result)
            return

        vault_info = vault_result.get("vault")
        if not vault_info:
            out.warning("No vault found. You may need to set up a vault first.")
            return

        # Display vault info
        out.print(f"ID: {vault_info.get('id')}")
        out.print(f"User ID: {vault_info.get('user_id')}")
        out.print(f"Created: {vault_info.get('created_at', 'N/A')}")
        out.print(f"Updated: {vault_info.get('updated_at', 'N/A')}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("ls")
@click.argument("folder_identifier", type=str, default="", required=False)
@click.option(
    "--page",
    "-p",
    type=int,
    default=1,
    help="Page number (default: 1)",
)
@click.option(
    "--page-size",
    type=int,
    default=50,
    help="Number of items per page (default: 50)",
)
@click.option(
    "--order-by",
    type=click.Choice(["updated_at", "created_at", "name", "file_size"]),
    default="updated_at",
    help="Field to order by (default: updated_at)",
)
@click.option(
    "--order",
    type=click.Choice(["asc", "desc"]),
    default="desc",
    help="Order direction (default: desc)",
)
@click.option(
    "--decrypt",
    "-d",
    is_flag=True,
    default=False,
    help="Decrypt file names (will prompt for password)",
)
@click.option(
    "--password",
    type=str,
    default=None,
    help="Vault password for decryption (will prompt if not provided)",
)
@click.pass_context
def vault_ls(
    ctx: Any,
    folder_identifier: str,
    page: int,
    page_size: int,
    order_by: str,
    order: str,
    decrypt: bool,
    password: Optional[str],
) -> None:
    """List files and folders in the vault.

    Lists encrypted files and folders stored in your vault.
    Use --decrypt to show decrypted file names (requires password).

    FOLDER_IDENTIFIER: Folder name, ID, or hash to list (default: root)

    Examples:
        pydrime vault ls                  # List root vault folder
        pydrime vault ls Test1            # List folder by name
        pydrime vault ls 34430            # List folder by ID
        pydrime vault ls MzQ0MzB8cGFkZA   # List folder by hash
        pydrime vault ls --page 2         # Show page 2 of results
        pydrime vault ls --order-by name  # Sort by name
        pydrime vault ls --decrypt        # Show decrypted file names
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info to get vault_id
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")
        vault_id: Optional[int] = vault_info.get("id") if vault_info else None

        # Resolve folder identifier to folder hash
        folder_hash: str = ""
        resolved_folder_name: Optional[str] = None

        if folder_identifier:
            # Check if it's a numeric ID - convert to hash
            if folder_identifier.isdigit():
                folder_hash = calculate_drime_hash(int(folder_identifier))
            else:
                # Could be a name or already a hash
                # First check if it looks like a hash (base64-like)
                # Try to find folder by name in vault root
                search_result = client.get_vault_file_entries(per_page=1000)

                # Handle response format
                search_entries = []
                if isinstance(search_result, dict):
                    if "data" in search_result:
                        search_entries = search_result["data"]
                    elif "pagination" in search_result and isinstance(
                        search_result["pagination"], dict
                    ):
                        search_entries = search_result["pagination"].get("data", [])

                found = False
                for entry in search_entries:
                    entry_name = entry.get("name", "")
                    entry_hash = entry.get("hash", "")
                    entry_type = entry.get("type", "")

                    # Match by name or hash, only folders
                    if entry_type == "folder" and (
                        entry_name == folder_identifier
                        or entry_hash == folder_identifier
                    ):
                        folder_hash = entry_hash
                        resolved_folder_name = entry_name
                        found = True
                        if not out.quiet:
                            out.info(
                                f"Resolved '{folder_identifier}' to folder "
                                f"hash: {folder_hash}"
                            )
                        break

                if not found:
                    # Maybe it's already a valid hash, try using it directly
                    folder_hash = folder_identifier

        # Show current path if we're in a subfolder
        if folder_hash and vault_id and not out.quiet:
            try:
                path_result = client.get_folder_path(folder_hash, vault_id=vault_id)
                if isinstance(path_result, dict) and "path" in path_result:
                    path_parts = [f.get("name", "?") for f in path_result["path"]]
                    current_path = "/" + "/".join(path_parts)
                    out.info(f"Path: {current_path}")
                    out.info("")
            except DrimeAPIError:
                # Silently ignore path errors, just don't show path
                pass

        # Cast order to Literal type for type checker
        order_dir = cast(Literal["asc", "desc"], order)

        # Get vault file entries
        result = client.get_vault_file_entries(
            folder_hash=folder_hash,
            page=page,
            per_page=page_size,
            order_by=order_by,
            order_dir=order_dir,
        )

        if out.json_output:
            out.output_json(result)
            return

        # Handle different response formats:
        # - {"data": [...]} - data at top level
        # - {"pagination": {"data": [...], ...}} - data nested in pagination
        entries = None
        pagination = None

        if isinstance(result, dict):
            if "data" in result:
                entries = result["data"]
                pagination = result.get("pagination") or result.get("meta")
            elif "pagination" in result and isinstance(result["pagination"], dict):
                pagination = result["pagination"]
                entries = pagination.get("data", [])

        if entries is None:
            out.warning("Unexpected response format")
            out.output_json(result)
            return

        if not entries:
            if not folder_hash:
                out.info("Vault is empty")
            else:
                folder_display = (
                    resolved_folder_name or folder_identifier or folder_hash
                )
                out.info(f"No files in vault folder '{folder_display}'")
            return

        # Handle decryption if requested
        vault_key = None
        if decrypt:
            # Get encryption parameters from vault
            salt = vault_info.get("salt") if vault_info else None
            check = vault_info.get("check") if vault_info else None
            iv = vault_info.get("iv") if vault_info else None

            if not all([salt, check, iv]):
                out.error("Vault encryption parameters not found.")
                ctx.exit(1)
                return

            # Get password from: CLI option > environment variable > prompt
            if not password:
                password = get_vault_password_from_env()
            if not password:
                password = click.prompt("Vault password", hide_input=True)

            # Verify password and get vault key
            # Type assertions - we already checked these are not None above
            assert password is not None
            assert salt is not None
            assert check is not None
            assert iv is not None
            try:
                vault_key = unlock_vault(password, salt, check, iv)
            except VaultPasswordError:
                out.error("Invalid vault password")
                ctx.exit(1)
                return

        # Display files in table format (same as regular ls)
        table_data = []
        for entry in entries:
            entry_type = entry.get("type", "file")
            name = entry.get("name", "Unknown")

            # Decrypt file name if requested and vault key is available
            if vault_key and name != "Unknown":
                name_iv = entry.get("name_iv")
                if name_iv:
                    try:
                        decrypted_name = decrypt_filename(vault_key, name, name_iv)
                        name = decrypted_name
                    except Exception as e:
                        # If decryption fails, keep encrypted name and add indicator
                        if not out.quiet:
                            entry_id = entry.get("id")
                            out.warning(
                                f"Failed to decrypt name for entry {entry_id}: {e}"
                            )
                        name = f"{name} [encrypted]"

            # Format created timestamp
            created_at = entry.get("created_at", "")
            created_str = ""
            if created_at:
                created_dt = parse_iso_timestamp(created_at)
                created_str = (
                    created_dt.strftime("%Y-%m-%d %H:%M:%S") if created_dt else ""
                )

            table_data.append(
                {
                    "id": str(entry.get("id", "")),
                    "name": name,
                    "type": entry_type,
                    "size": out.format_size(entry.get("file_size", 0)),
                    "hash": entry.get("hash", ""),
                    "parent_id": str(entry.get("parent_id", ""))
                    if entry.get("parent_id")
                    else "-",
                    "created": created_str,
                }
            )

        out.output_table(
            table_data,
            ["id", "name", "type", "size", "hash", "parent_id", "created"],
            {
                "id": "ID",
                "name": "Name",
                "type": "Type",
                "size": "Size",
                "hash": "Hash",
                "parent_id": "Parent ID",
                "created": "Created",
            },
        )

        # Show pagination info if available
        if pagination:
            current = pagination.get("current_page", page)
            total_pages = pagination.get("last_page")
            total_items = pagination.get("total")
            if total_items is not None and total_pages is not None:
                out.info("")
                out.info(f"Page {current} of {total_pages} ({total_items} total)")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("download")
@click.argument("file_identifiers", type=str, nargs=-1, required=True)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output directory (default: current directory)",
)
@click.option(
    "--password",
    "-p",
    type=str,
    default=None,
    help="Vault password (will prompt if not provided)",
)
@click.pass_context
def vault_download(
    ctx: Any,
    file_identifiers: tuple[str, ...],
    output: Optional[str],
    password: Optional[str],
) -> None:
    """Download files or folders from the vault.

    Downloads encrypted files from your vault and decrypts them locally.
    Supports glob patterns (* ? []) to match multiple files.
    Folders are downloaded recursively with their directory structure preserved.
    You will be prompted for your vault password.

    FILE_IDENTIFIERS: File/folder paths, names, IDs, hashes, or glob patterns

    Examples:
        pydrime vault download document.pdf              # Download file from root
        pydrime vault download Test1                     # Download entire folder
        pydrime vault download Test1/document.pdf        # Download from subfolder
        pydrime vault download 34431                     # Download by ID
        pydrime vault download doc.pdf -o ./output      # Download to directory
        pydrime vault download "*.txt"                   # Download all .txt files
        pydrime vault download "doc*" "*.pdf"            # Multiple patterns
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info for password verification
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        # Get encryption parameters from vault
        salt = vault_info.get("salt")
        check = vault_info.get("check")
        iv = vault_info.get("iv")

        if not all([salt, check, iv]):
            out.error("Vault encryption parameters not found.")
            ctx.exit(1)
            return

        # Get password from: CLI option > environment variable > prompt
        if not password:
            password = get_vault_password_from_env()
        if not password:
            password = click.prompt("Vault password", hide_input=True)

        # Unlock the vault (verify password)
        if not out.quiet:
            out.info("Verifying vault password...")

        # password is guaranteed to be non-None at this point
        assert password is not None
        try:
            vault_key = unlock_vault(password, salt, check, iv)
        except VaultPasswordError:
            out.error("Invalid vault password")
            ctx.exit(1)
            return

        # Helper function to get vault entries from a folder
        def get_vault_entries(folder_hash: str = "") -> list[dict]:
            """Get vault file entries from a folder."""
            result = client.get_vault_file_entries(
                folder_hash=folder_hash, per_page=1000
            )
            entries = []
            if isinstance(result, dict):
                if "data" in result:
                    entries = result["data"]
                elif "pagination" in result and isinstance(result["pagination"], dict):
                    entries = result["pagination"].get("data", [])
            return entries

        # Helper to recursively get all files in a vault folder
        def get_folder_files_recursive(
            folder_hash: str, base_path: str = ""
        ) -> list[tuple[str, str, Optional[str], str]]:
            """Get all files in a folder recursively.

            Returns list of (hash, filename, iv, relative_path) tuples.
            """
            all_files: list[tuple[str, str, Optional[str], str]] = []
            entries = get_vault_entries(folder_hash)

            for entry in entries:
                entry_name = entry.get("name", "")
                entry_type = entry.get("type", "")
                entry_hash = entry.get("hash", "")

                if entry_type == "folder":
                    # Recurse into subfolder
                    subfolder_path = (
                        f"{base_path}/{entry_name}" if base_path else entry_name
                    )
                    all_files.extend(
                        get_folder_files_recursive(entry_hash, subfolder_path)
                    )
                else:
                    # It's a file
                    rel_path = f"{base_path}/{entry_name}" if base_path else entry_name
                    all_files.append(
                        (entry_hash, entry_name, entry.get("iv"), rel_path)
                    )

            return all_files

        # Helper function to resolve a single file identifier
        # Returns: (results, folders, has_error)
        # results: list of (hash, filename, iv) for direct files
        # folders: list of (folder_name, folder_hash) for folder downloads
        def resolve_file_identifier(
            file_identifier: str,
        ) -> tuple[
            list[tuple[str, str, Optional[str]]],
            list[tuple[str, str]],
            bool,
        ]:
            """Resolve file identifier to files and folders.

            Returns:
                Tuple of (file_results, folder_results, has_error) where:
                - file_results: list of (hash, filename, iv) for files
                - folder_results: list of (folder_name, folder_hash) for folders
                - has_error: indicates a critical error that should stop processing
            """
            results: list[tuple[str, str, Optional[str]]] = []
            folders: list[tuple[str, str]] = []

            # Check if it's a numeric ID - convert to hash
            if file_identifier.isdigit():
                file_hash = calculate_drime_hash(int(file_identifier))
                search_entries = get_vault_entries()
                for entry in search_entries:
                    if str(entry.get("id")) == file_identifier:
                        if entry.get("type") == "folder":
                            folders.append(
                                (entry.get("name", ""), entry.get("hash", ""))
                            )
                        else:
                            results.append(
                                (file_hash, entry.get("name", ""), entry.get("iv"))
                            )
                        break
                if not results and not folders:
                    # ID not found in entries, but try using computed hash
                    results.append((file_hash, file_identifier, None))
                return results, folders, False

            # Check if it's a path with folders
            if "/" in file_identifier:
                path_parts = file_identifier.split("/")
                file_pattern = path_parts[-1]
                folder_parts = path_parts[:-1]

                # Navigate through folders
                current_folder_hash = ""
                for folder_name in folder_parts:
                    search_entries = get_vault_entries(current_folder_hash)
                    folder_found = False
                    for entry in search_entries:
                        if (
                            entry.get("type") == "folder"
                            and entry.get("name") == folder_name
                        ):
                            current_folder_hash = entry.get("hash", "")
                            folder_found = True
                            break
                    if not folder_found:
                        out.error(f"Folder '{folder_name}' not found in vault path")
                        return [], [], True  # Critical error

                # Get entries in target folder
                search_entries = get_vault_entries(current_folder_hash)

                # Check for glob pattern
                if is_glob_pattern(file_pattern):
                    for entry in search_entries:
                        entry_name = entry.get("name", "")
                        if glob_match(file_pattern, entry_name):
                            if entry.get("type") == "folder":
                                folders.append((entry_name, entry.get("hash", "")))
                            else:
                                results.append(
                                    (
                                        entry.get("hash", ""),
                                        entry_name,
                                        entry.get("iv"),
                                    )
                                )
                    if (results or folders) and not out.quiet:
                        total = len(results) + len(folders)
                        out.info(
                            f"Matched {total} entries with pattern '{file_identifier}'"
                        )
                else:
                    # Exact match - check both files and folders
                    for entry in search_entries:
                        if entry.get("name") == file_pattern:
                            if entry.get("type") == "folder":
                                folders.append(
                                    (entry.get("name", ""), entry.get("hash", ""))
                                )
                                if not out.quiet:
                                    out.info(f"Resolved '{file_identifier}' to folder")
                            else:
                                results.append(
                                    (
                                        entry.get("hash", ""),
                                        entry.get("name", ""),
                                        entry.get("iv"),
                                    )
                                )
                                if not out.quiet:
                                    out.info(
                                        f"Resolved '{file_identifier}' to hash: "
                                        f"{entry.get('hash')}"
                                    )
                            break
                    if not results and not folders:
                        out.error(
                            f"'{file_pattern}' not found in "
                            f"vault path '{file_identifier}'"
                        )
                        return [], [], True  # Critical error
                return results, folders, False

            # Root level - could be name, hash, or glob pattern
            search_entries = get_vault_entries()

            # Check for glob pattern
            if is_glob_pattern(file_identifier):
                for entry in search_entries:
                    entry_name = entry.get("name", "")
                    if glob_match(file_identifier, entry_name):
                        if entry.get("type") == "folder":
                            folders.append((entry_name, entry.get("hash", "")))
                        else:
                            results.append(
                                (
                                    entry.get("hash", ""),
                                    entry_name,
                                    entry.get("iv"),
                                )
                            )
                if (results or folders) and not out.quiet:
                    total = len(results) + len(folders)
                    out.info(
                        f"Matched {total} entries with pattern '{file_identifier}'"
                    )
                elif not results and not folders:
                    out.warning(f"No entries match pattern '{file_identifier}'")
                return results, folders, False

            # Exact match by name or hash - check both files and folders
            for entry in search_entries:
                entry_name = entry.get("name", "")
                entry_hash = entry.get("hash", "")
                entry_type = entry.get("type", "")

                if entry_name == file_identifier or entry_hash == file_identifier:
                    if entry_type == "folder":
                        folders.append((entry_name, entry_hash))
                        if not out.quiet:
                            out.info(f"Resolved '{file_identifier}' to folder")
                    else:
                        results.append((entry_hash, entry_name, entry.get("iv")))
                        if not out.quiet:
                            out.info(
                                f"Resolved '{file_identifier}' to hash: {entry_hash}"
                            )
                    return results, folders, False

            # Check if this looks like a hash
            looks_like_hash = "." not in file_identifier and len(file_identifier) >= 8
            if looks_like_hash:
                results.append((file_identifier, file_identifier, None))
            else:
                out.error(
                    f"'{file_identifier}' not found in vault root. "
                    "Use a path like 'Folder/file.txt' for files in subfolders."
                )
                return [], [], True  # Critical error
            return results, folders, False

        # Helper function to download and decrypt a single file
        def download_single_file(
            file_hash: str,
            original_filename: str,
            file_iv: Optional[str],
            output_dir: Path,
        ) -> bool:
            """Download and decrypt a single vault file. Returns True on success."""
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                temp_path = Path(tmp_file.name)

            try:
                if not out.quiet:
                    out.info(f"Downloading encrypted vault file: {original_filename}")

                client.download_vault_file(
                    hash_value=file_hash,
                    output_path=temp_path,
                )

                encrypted_content = temp_path.read_bytes()

                if not out.quiet:
                    out.info(f"Decrypting file: {original_filename}")

                decrypted_content = decrypt_file_content(
                    vault_key, encrypted_content, iv_b64=file_iv
                )

                save_path = output_dir / original_filename
                save_path.write_bytes(decrypted_content)

                out.success(f"Downloaded and decrypted: {save_path}")
                return True

            except Exception as e:
                out.error(f"Failed to download {original_filename}: {e}")
                return False

            finally:
                if temp_path.exists():
                    temp_path.unlink()

        # Determine output directory
        output_dir = Path(output) if output else Path.cwd()
        if output and not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        # Expand all identifiers (including glob patterns)
        # files_to_download: list of (hash, filename, iv, relative_path)
        files_to_download: list[tuple[str, str, Optional[str], str]] = []
        has_critical_error = False
        for identifier in file_identifiers:
            resolved_files, resolved_folders, has_error = resolve_file_identifier(
                identifier
            )
            if has_error:
                has_critical_error = True
            # Add direct files (relative_path = filename)
            for file_hash, filename, iv in resolved_files:
                files_to_download.append((file_hash, filename, iv, filename))
            # Add all files from folders recursively
            for folder_name, folder_hash in resolved_folders:
                folder_files = get_folder_files_recursive(folder_hash, folder_name)
                if folder_files:
                    if not out.quiet:
                        out.info(
                            f"Found {len(folder_files)} files in folder '{folder_name}'"
                        )
                    files_to_download.extend(folder_files)
                else:
                    out.warning(f"Folder '{folder_name}' is empty")

        if has_critical_error:
            ctx.exit(1)
            return

        if not files_to_download:
            out.warning("No files to download.")
            return

        # Download all files
        success_count = 0
        for file_hash, filename, file_iv, rel_path in files_to_download:
            # Determine the save path based on relative path
            save_dir = output_dir / Path(rel_path).parent
            if save_dir != output_dir:
                save_dir.mkdir(parents=True, exist_ok=True)
            if download_single_file(file_hash, filename, file_iv, save_dir):
                success_count += 1

        if len(files_to_download) > 1:
            out.info(
                f"Downloaded {success_count}/{len(files_to_download)} files "
                f"successfully"
            )

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("upload")
@click.argument("file_paths", type=str, nargs=-1, required=True)
@click.option(
    "--folder",
    "-f",
    type=str,
    default=None,
    help="Target folder name, ID, or hash in vault (default: root)",
)
@click.option(
    "--password",
    "-p",
    type=str,
    default=None,
    help="Vault password (will prompt if not provided)",
)
@click.pass_context
def vault_upload(
    ctx: Any,
    file_paths: tuple[str, ...],
    folder: Optional[str],
    password: Optional[str],
) -> None:
    """Upload files or folders to the vault with encryption.

    Encrypts local files and uploads them to your encrypted vault.
    Supports glob patterns (* ? []) to match multiple files.
    Folders are uploaded recursively with their directory structure preserved.
    You will be prompted for your vault password.

    FILE_PATHS: Paths to local files, folders, or glob patterns to upload

    Examples:
        pydrime vault upload secret.txt                    # Upload file to vault root
        pydrime vault upload mydir                         # Upload entire folder
        pydrime vault upload document.pdf -f MyFolder      # Upload to folder
        pydrime vault upload photo.jpg -p mypassword       # With password option
        pydrime vault upload "*.txt"                       # Upload all .txt files
        pydrime vault upload "doc*" "*.pdf"                # Multiple patterns
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info for password verification
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        vault_id = vault_info.get("id")
        if not vault_id:
            out.error("Could not get vault ID.")
            ctx.exit(1)
            return

        # Get encryption parameters from vault
        salt = vault_info.get("salt")
        check = vault_info.get("check")
        iv = vault_info.get("iv")

        if not all([salt, check, iv]):
            out.error("Vault encryption parameters not found.")
            ctx.exit(1)
            return

        # Get password from: CLI option > environment variable > prompt
        if not password:
            password = get_vault_password_from_env()
        if not password:
            password = click.prompt("Vault password", hide_input=True)

        # Unlock the vault (verify password)
        if not out.quiet:
            out.info("Verifying vault password...")

        # password is guaranteed to be non-None at this point
        assert password is not None
        try:
            vault_key = unlock_vault(password, salt, check, iv)
        except VaultPasswordError:
            out.error("Invalid vault password")
            ctx.exit(1)
            return

        # Resolve folder if specified
        parent_id: Optional[int] = None
        if folder:
            # Check if it's a numeric ID
            if folder.isdigit():
                parent_id = int(folder)
            else:
                # Search for folder by name or hash
                search_result = client.get_vault_file_entries(per_page=1000)

                search_entries = []
                if isinstance(search_result, dict):
                    if "data" in search_result:
                        search_entries = search_result["data"]
                    elif "pagination" in search_result and isinstance(
                        search_result["pagination"], dict
                    ):
                        search_entries = search_result["pagination"].get("data", [])

                found = False
                for entry in search_entries:
                    entry_name = entry.get("name", "")
                    entry_hash = entry.get("hash", "")
                    entry_type = entry.get("type", "")

                    if entry_type == "folder" and (
                        entry_name == folder or entry_hash == folder
                    ):
                        parent_id = entry.get("id")
                        found = True
                        if not out.quiet:
                            out.info(f"Resolved folder '{folder}' to ID: {parent_id}")
                        break

                if not found:
                    out.error(f"Folder '{folder}' not found in vault.")
                    ctx.exit(1)
                    return

        # Expand glob patterns and collect files with their relative paths
        # Each entry is (source_path, relative_path_in_vault)
        files_to_upload: list[tuple[Path, str]] = []

        def collect_directory_files(dir_path: Path, base_name: str) -> None:
            """Recursively collect all files from a directory."""
            for item in dir_path.iterdir():
                rel_path = f"{base_name}/{item.name}"
                if item.is_file():
                    files_to_upload.append((item, rel_path))
                elif item.is_dir():
                    collect_directory_files(item, rel_path)

        for file_path in file_paths:
            # Use glob to expand patterns
            matches = glob_module.glob(file_path)
            if matches:
                for match in matches:
                    match_path = Path(match)
                    if match_path.is_file():
                        # Single file - relative path is just the filename
                        files_to_upload.append((match_path, match_path.name))
                    elif match_path.is_dir():
                        # Directory - collect all files with relative paths
                        if not out.quiet:
                            out.info(f"Scanning directory: {match_path.name}")
                        collect_directory_files(match_path, match_path.name)
                if len(matches) > 1 and not out.quiet:
                    out.info(
                        f"Matched {len(matches)} entries with pattern '{file_path}'"
                    )
            else:
                # No glob match - check if it's a literal file or directory
                literal_path = Path(file_path)
                if literal_path.exists():
                    if literal_path.is_file():
                        files_to_upload.append((literal_path, literal_path.name))
                    elif literal_path.is_dir():
                        if not out.quiet:
                            out.info(f"Scanning directory: {literal_path.name}")
                        collect_directory_files(literal_path, literal_path.name)
                else:
                    out.warning(f"Path not found: '{file_path}'")

        if not files_to_upload:
            out.warning("No files to upload.")
            return

        if not out.quiet:
            out.info(f"Found {len(files_to_upload)} files to upload")

        # Collect all unique folder paths that need to be created
        folders_to_create: set[str] = set()
        for _, rel_path in files_to_upload:
            # Get all parent folders for this file
            parts = rel_path.split("/")
            for i in range(1, len(parts)):
                folder_path = "/".join(parts[:i])
                folders_to_create.add(folder_path)

        # Sort folders by depth (shorter paths first) to create parents before children
        sorted_folders = sorted(folders_to_create, key=lambda x: x.count("/"))

        # Map from folder path to vault folder ID
        folder_id_cache: dict[str, int] = {}

        def get_or_create_vault_folder(folder_path: str) -> Optional[int]:
            """Get or create a vault folder, returning its ID."""
            if folder_path in folder_id_cache:
                return folder_id_cache[folder_path]

            parts = folder_path.split("/")
            folder_name = parts[-1]

            # Determine parent ID
            if len(parts) == 1:
                # Top-level folder, parent is the target folder (or root)
                folder_parent_id = parent_id
            else:
                # Nested folder, parent is the previous folder in path
                parent_path = "/".join(parts[:-1])
                folder_parent_id = folder_id_cache.get(parent_path)

            # Encrypt the folder name
            encrypted_name, _ = encrypt_filename(vault_key, folder_name)

            if not out.quiet:
                out.info(f"Creating vault folder: {folder_path}")

            try:
                result = client.create_vault_folder(
                    name=encrypted_name,
                    vault_id=vault_id,
                    parent_id=folder_parent_id,
                )
                folder_info = result.get("folder", {})
                folder_id: Optional[int] = folder_info.get("id")
                if folder_id is not None:
                    folder_id_cache[folder_path] = folder_id
                    return folder_id
            except Exception as e:
                out.error(f"Failed to create folder '{folder_path}': {e}")

            return None

        # Create all necessary folders
        for folder_path in sorted_folders:
            get_or_create_vault_folder(folder_path)

        # Helper function to upload a single file
        def upload_single_file(source_path: Path, rel_path: str) -> bool:
            """Upload and encrypt a single file. Returns True on success."""
            try:
                # Determine the parent folder ID for this file
                parts = rel_path.split("/")
                if len(parts) > 1:
                    # File is in a subfolder
                    file_parent_path = "/".join(parts[:-1])
                    file_parent_id = folder_id_cache.get(file_parent_path, parent_id)
                else:
                    # File is at root level
                    file_parent_id = parent_id

                if not out.quiet:
                    out.info(f"Encrypting {rel_path}...")

                # Read file content
                file_content = source_path.read_bytes()

                # Encrypt the filename
                encrypted_name, name_iv = encrypt_filename(vault_key, source_path.name)

                # Encrypt the file content
                ciphertext, content_iv_bytes = vault_key.encrypt(file_content)

                # Convert IV to base64
                content_iv = base64.b64encode(content_iv_bytes).decode("ascii")

                # Upload the encrypted file
                if not out.quiet:
                    out.info(f"Uploading: {rel_path}")

                result = client.upload_vault_file(
                    file_path=source_path,
                    encrypted_content=ciphertext,
                    encrypted_name=encrypted_name,
                    name_iv=name_iv,
                    content_iv=content_iv,
                    vault_id=vault_id,
                    parent_id=file_parent_id,
                )

                if out.json_output:
                    out.output_json(result)
                else:
                    file_entry = result.get("fileEntry", {})
                    out.success(
                        f"Uploaded: {rel_path} (ID: {file_entry.get('id', 'N/A')})"
                    )
                return True

            except Exception as e:
                out.error(f"Failed to upload {rel_path}: {e}")
                return False

        # Upload all files
        success_count = 0
        for source_path, rel_path in files_to_upload:
            if upload_single_file(source_path, rel_path):
                success_count += 1

        if len(files_to_upload) > 1:
            out.info(
                f"Uploaded {success_count}/{len(files_to_upload)} files successfully"
            )

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)


@vault.command("sync")
@click.argument("path", type=str, required=False, default=None)
@click.option("--remote-path", "-r", help="Remote destination path in vault")
@click.option(
    "--folder",
    "-f",
    type=str,
    default=None,
    help="Target folder name, ID, or hash in vault (default: root)",
)
@click.option(
    "--password",
    "-p",
    type=str,
    default=None,
    help="Vault password (will prompt if not provided)",
)
@click.option(
    "--config",
    "-C",
    "config_file",
    type=click.Path(exists=True),
    help="JSON config file with list of sync pairs",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be synced without syncing"
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Disable progress bars",
)
@click.option(
    "--chunk-size",
    "-c",
    type=int,
    default=25,
    help="Chunk size in MB for multipart uploads (default: 25MB)",
)
@click.option(
    "--multipart-threshold",
    "-m",
    type=int,
    default=30,
    help="File size threshold in MB for using multipart upload (default: 30MB)",
)
@click.option(
    "--batch-size",
    "-b",
    type=int,
    default=50,
    help="Number of remote files to process per batch in streaming mode (default: 50)",
)
@click.option(
    "--no-streaming",
    is_flag=True,
    help="Disable streaming mode (scan all files upfront instead of batch processing)",
)
@click.option(
    "--workers",
    type=int,
    default=1,
    help="Number of parallel workers for uploads/downloads (default: 1)",
)
@click.option(
    "--start-delay",
    type=float,
    default=0.0,
    help="Delay in seconds between starting each parallel operation (default: 0.0)",
)
@click.pass_context
def vault_sync(
    ctx: Any,
    path: Optional[str],
    remote_path: Optional[str],
    folder: Optional[str],
    password: Optional[str],
    config_file: Optional[str],
    dry_run: bool,
    no_progress: bool,
    chunk_size: int,
    multipart_threshold: int,
    batch_size: int,
    no_streaming: bool,
    workers: int,
    start_delay: float,
) -> None:
    """Sync files between local directory and encrypted vault.

    PATH: Local directory to sync OR literal sync pair in format:
          /local/path:syncMode:/remote/path

    Sync Modes:
      - twoWay (tw): Mirror every action in both directions
      - localToCloud (std): Mirror local actions to vault only
      - localBackup (sb): Upload to vault, never delete
      - cloudToLocal (dts): Mirror vault actions to local only
      - cloudBackup (db): Download from vault, never delete

    Ignore Files (.pydrignore):
      Place a .pydrignore file in any directory to exclude files from sync.
      Uses gitignore-style patterns (similar to Kopia's .kopiaignore).

      Supported patterns:
        # Comment lines start with #
        *.log           - Ignore all .log files anywhere
        /logs           - Ignore 'logs' only at root directory
        temp/           - Ignore directories named 'temp'
        !important.log  - Un-ignore important.log (negation)
        *.db*           - Ignore files with .db in extension
        **/cache/**     - Ignore any 'cache' directory and contents
        [a-z]*.tmp      - Character ranges and wildcards
        ?tmp.db         - ? matches exactly one character

      Hierarchical: .pydrignore files in subdirectories only apply to that
      subtree and can override parent rules using negation (!).

    Examples:
        # Directory path with default two-way sync
        pydrime vault sync ./my_folder
        pydrime vault sync ./docs -r remote_docs

        # Literal sync pairs with explicit modes
        pydrime vault sync /home/user/docs:twoWay:/Documents
        pydrime vault sync /home/user/pics:localToCloud:/Pictures
        pydrime vault sync ./local:localBackup:/Backup
        pydrime vault sync ./data:cloudToLocal:/CloudData
        pydrime vault sync ./archive:cloudBackup:/Archive

        # With abbreviations
        pydrime vault sync /home/user/pics:tw:/Pictures
        pydrime vault sync ./backup:std:/CloudBackup
        pydrime vault sync ./local:sb:/Backup

        # Other options
        pydrime vault sync . -f MyFolder            # Sync to vault folder
        pydrime vault sync ./data --dry-run         # Preview sync changes
        pydrime vault sync ./data -b 100            # Process 100 files per batch
        pydrime vault sync ./data --no-streaming    # Scan all files upfront
    """
    from pathlib import Path as PathLib

    from syncengine import (  # type: ignore[import-not-found]
        ComparisonMode,
        InitialSyncPreference,
        SyncConfig,
        SyncEngine,
        SyncMode,
        SyncPair,
    )

    from ..cli_progress import run_sync_with_progress
    from .vault_adapters import (  # type: ignore[import-not-found]
        _VaultClientAdapter,
        create_vault_entries_manager_factory,
    )

    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    # Validate and convert MB to bytes
    if chunk_size < 5:
        out.error("Chunk size must be at least 5MB")
        ctx.exit(1)
    if chunk_size > 100:
        out.error("Chunk size cannot exceed 100MB")
        ctx.exit(1)
    if multipart_threshold < 1:
        out.error("Multipart threshold must be at least 1MB")
        ctx.exit(1)
    if chunk_size >= multipart_threshold:
        out.error("Chunk size must be smaller than multipart threshold")
        ctx.exit(1)
    if batch_size < 1:
        out.error("Batch size must be at least 1")
        ctx.exit(1)
    if batch_size > 1000:
        out.error("Batch size cannot exceed 1000")
        ctx.exit(1)

    chunk_size_bytes = chunk_size * 1024 * 1024
    multipart_threshold_bytes = multipart_threshold * 1024 * 1024

    # Single path mode (config file support can be added later)
    if path is None:
        out.error("PATH argument is required")
        ctx.exit(1)
        return

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info for password verification
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        vault_id = vault_info.get("id")
        if not vault_id:
            out.error("Could not get vault ID.")
            ctx.exit(1)
            return

        # Get encryption parameters from vault
        salt = vault_info.get("salt")
        check = vault_info.get("check")
        iv = vault_info.get("iv")

        if not all([salt, check, iv]):
            out.error("Vault encryption parameters not found.")
            ctx.exit(1)
            return

        # Get password from: CLI option > environment variable > prompt
        if not password:
            password = get_vault_password_from_env()
        if not password:
            password = click.prompt("Vault password", hide_input=True)

        # Unlock the vault (verify password)
        if not out.quiet:
            out.info("Verifying vault password...")

        assert password is not None
        try:
            vault_key = unlock_vault(password, salt, check, iv)
        except VaultPasswordError:
            out.error("Invalid vault password")
            ctx.exit(1)
            return

        # Resolve folder if specified
        parent_id: Optional[int] = None
        if folder:
            # Check if it's a numeric ID
            if folder.isdigit():
                parent_id = int(folder)
            else:
                # Search for folder by name or hash
                search_result = client.get_vault_file_entries(per_page=1000)

                search_entries = []
                if isinstance(search_result, dict):
                    if "data" in search_result:
                        search_entries = search_result["data"]
                    elif "pagination" in search_result and isinstance(
                        search_result["pagination"], dict
                    ):
                        search_entries = search_result["pagination"].get("data", [])

                found = False
                for entry in search_entries:
                    entry_name = entry.get("name", "")
                    entry_hash = entry.get("hash", "")
                    entry_type = entry.get("type", "")

                    if entry_type == "folder" and (
                        entry_name == folder or entry_hash == folder
                    ):
                        parent_id = entry.get("id")
                        found = True
                        if not out.quiet:
                            out.info(f"Resolved folder '{folder}' to ID: {parent_id}")
                        break

                if not found:
                    out.error(f"Folder '{folder}' not found in vault.")
                    ctx.exit(1)
                    return

        # Parse path - check if it's a literal sync pair or simple path
        windows_drive_match = re.match(r"^([A-Za-z]:)", path)
        if windows_drive_match:
            drive = windows_drive_match.group(1)
            rest_of_path = path[len(drive) :]
            parts = rest_of_path.split(":")
            if parts:
                parts[0] = drive + parts[0]
        else:
            parts = path.split(":")

        is_literal_pair = len(parts) >= 2 and len(parts) <= 3

        # Create sync pair
        pair: SyncPair
        if is_literal_pair:
            if remote_path is not None:
                out.error(
                    "Cannot use --remote-path with literal sync pair format. "
                    "Use '/local:mode:/remote' format instead."
                )
                ctx.exit(1)

            try:
                pair = SyncPair.parse_literal(path)
                # Use vault_id as storage_id (will be ignored by adapter)
                pair.storage_id = vault_id
                source_path = pair.source

                if not out.quiet:
                    out.info(f"Parsed sync pair: {pair.sync_mode.value}")
                    out.info(f"  Local:  {pair.source}")
                    out.info(f"  Remote: {pair.destination}")
            except ValueError as e:
                out.error(f"Invalid sync pair format: {e}")
                ctx.exit(1)
                return
        else:
            # Simple directory path
            source_path = PathLib(path)

            if not source_path.exists():
                out.error(f"Path does not exist: {path}")
                ctx.exit(1)

            if not source_path.is_dir():
                out.error(f"Path is not a directory: {path}")
                ctx.exit(1)

            # Determine remote path
            if remote_path is None:
                remote_path = source_path.name

            # Create sync pair with TWO_WAY as default
            pair = SyncPair(
                source=source_path,
                destination=remote_path,
                sync_mode=SyncMode.TWO_WAY,
                storage_id=vault_id,
            )

        # If destination folder is specified, find or create it and use as parent
        sync_parent_id = parent_id
        if pair.destination and pair.destination != "/":
            # Create a temporary adapter to search for the folder
            temp_adapter = _VaultClientAdapter(client, vault_id, vault_key, parent_id)
            temp_manager_factory = create_vault_entries_manager_factory(
                vault_id, vault_key, parent_id
            )
            temp_manager = temp_manager_factory(client, vault_id)

            # Try to find the destination folder
            dest_folder = temp_manager.find_folder_by_name(
                pair.destination, parent_id=0 if parent_id is None else parent_id
            )

            if dest_folder:
                sync_parent_id = dest_folder.id
                if not out.quiet:
                    out.info(
                        f"Found remote folder '{pair.destination}' "
                        f"(id: {sync_parent_id})"
                    )
            elif not dry_run:
                # Create the destination folder if it doesn't exist
                if not out.quiet:
                    out.info(f"Creating remote folder '{pair.destination}'...")
                try:
                    dest_folder = temp_adapter.create_folder(
                        name=pair.destination,
                        parent_id=parent_id,
                        storage_id=vault_id,
                    )
                    sync_parent_id = dest_folder.id
                    if not out.quiet:
                        out.info(
                            f"Created remote folder '{pair.destination}' "
                            f"(id: {sync_parent_id})"
                        )
                except Exception as e:
                    # If folder creation fails (e.g., already exists),
                    # try to find it again
                    if "422" in str(e):
                        dest_folder = temp_manager.find_folder_by_name(
                            pair.destination,
                            parent_id=0 if parent_id is None else parent_id,
                        )
                        if dest_folder:
                            sync_parent_id = dest_folder.id
                            if not out.quiet:
                                out.info(
                                    f"Found existing folder '{pair.destination}' "
                                    f"(id: {sync_parent_id})"
                                )
                        else:
                            raise
                    else:
                        raise

            # Update the sync pair to sync INTO the destination folder
            # by setting parent_id and clearing destination (root of that folder)
            if not out.quiet:
                out.info(
                    f"Updating sync pair: parent_id={sync_parent_id}, "
                    f"destination=/ (was: {pair.destination})"
                )
            pair = SyncPair(
                source=pair.source,
                destination="/",  # Sync into root of the destination folder
                sync_mode=pair.sync_mode,
                storage_id=vault_id,
                parent_id=sync_parent_id,  # Set the folder as parent
                ignore=pair.ignore,
                exclude_dot_files=pair.exclude_dot_files,
                disable_source_trash=pair.disable_source_trash,
            )

        if not out.quiet:
            out.info(f"Vault ID: {vault_id}")
            out.info(f"Sync mode: {pair.sync_mode.value}")
            out.info(f"Local path: {pair.source}")
            remote_display = pair.destination if pair.destination else "/ (root)"
            out.info(f"Remote path: {remote_display}")
            if sync_parent_id:
                out.info(f"Sync folder ID: {sync_parent_id}")
            out.info("")

        # Create output formatter for engine
        use_progress_display = not no_progress and not out.quiet and not dry_run
        engine_out = OutputFormatter(
            json_output=out.json_output,
            quiet=use_progress_display or no_progress or out.quiet,
        )

        # Create vault adapter and sync engine with the resolved parent folder
        vault_adapter = _VaultClientAdapter(client, vault_id, vault_key, sync_parent_id)
        vault_manager_factory = create_vault_entries_manager_factory(
            vault_id, vault_key, sync_parent_id
        )

        # Use default comparison mode (HASH_THEN_MTIME) for vault sync
        # Use SIZE_ONLY comparison mode to avoid expensive MD5 computation
        # and unreliable mtime comparison (vault doesn't preserve original mtimes)
        sync_config = SyncConfig(
            ignore_file_name=".pydrignore",
            comparison_mode=ComparisonMode.SIZE_ONLY,
        )

        engine = SyncEngine(
            vault_adapter,
            vault_manager_factory,
            output=engine_out,
            config=sync_config,
        )

        # Execute sync
        if use_progress_display:
            stats = run_sync_with_progress(
                engine=engine,
                pair=pair,
                dry_run=dry_run,
                chunk_size=chunk_size_bytes,
                multipart_threshold=multipart_threshold_bytes,
                batch_size=batch_size,
                use_streaming=not no_streaming,
                max_workers=workers,
                start_delay=start_delay,
                out=out,
                initial_sync_preference=InitialSyncPreference.MERGE,
            )
        else:
            stats = engine.sync_pair(
                pair,
                dry_run=dry_run,
                chunk_size=chunk_size_bytes,
                multipart_threshold=multipart_threshold_bytes,
                batch_size=batch_size,
                use_streaming=not no_streaming,
                max_workers=workers,
                start_delay=start_delay,
                initial_sync_preference=InitialSyncPreference.MERGE,
            )

        # Display summary
        if use_progress_display and not out.quiet:
            from .sync_command import _display_sync_summary

            _display_sync_summary(out, stats, dry_run=dry_run)

        # Output results
        if out.json_output:
            out.output_json(stats)

        # Exit with warning if there were conflicts
        if stats.get("conflicts", 0) > 0 and not out.quiet:
            out.warning(
                f"\n⚠  {stats['conflicts']} conflict(s) were skipped. "
                "Please resolve conflicts manually."
            )

    except KeyboardInterrupt:
        out.warning("\nSync cancelled by user")
        ctx.exit(130)
    except DrimeAPIError as e:
        out.error(f"API error: {e}")
        ctx.exit(1)
    except Exception as e:
        out.error(f"Error: {e}")
        import traceback

        if not out.quiet:
            traceback.print_exc()
        ctx.exit(1)


@vault.command("rm")
@click.argument("file_identifier", type=str)
@click.option(
    "--no-trash",
    is_flag=True,
    default=False,
    help="Delete permanently instead of moving to trash",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Skip confirmation prompt",
)
@click.pass_context
def vault_rm(
    ctx: Any,
    file_identifier: str,
    no_trash: bool,
    yes: bool,
) -> None:
    """Delete a file or folder from the vault.

    By default, files are moved to trash. Use --no-trash to delete permanently.

    FILE_IDENTIFIER: File or folder name, ID, or hash to delete

    Examples:
        pydrime vault rm secret.txt                # Move to trash
        pydrime vault rm secret.txt --no-trash     # Delete permanently
        pydrime vault rm 34431                     # Delete by ID
        pydrime vault rm MzQ0MzF8cGFkZA            # Delete by hash
        pydrime vault rm MyFolder -y               # Skip confirmation
    """
    api_key = ctx.obj.get("api_key")
    out: OutputFormatter = ctx.obj["out"]

    if not config.is_configured() and not api_key:
        out.error("API key not configured.")
        out.info("Run 'pydrime init' to configure your API key")
        ctx.exit(1)

    try:
        client = DrimeClient(api_key=api_key)

        # Get vault info
        vault_result = client.get_vault()
        vault_info = vault_result.get("vault")

        if not vault_info:
            out.error("No vault found. You may need to set up a vault first.")
            ctx.exit(1)
            return

        vault_id = vault_info.get("id")
        if not vault_id:
            out.error("Could not get vault ID.")
            ctx.exit(1)
            return

        # Resolve file identifier to ID
        entry_id: Optional[int] = None
        entry_name: Optional[str] = None
        entry_type: Optional[str] = None

        # Check if it's a numeric ID
        if file_identifier.isdigit():
            entry_id = int(file_identifier)
            # Fetch entry info to get name
            search_result = client.get_vault_file_entries(per_page=1000)
            search_entries = []
            if isinstance(search_result, dict):
                if "data" in search_result:
                    search_entries = search_result["data"]
                elif "pagination" in search_result and isinstance(
                    search_result["pagination"], dict
                ):
                    search_entries = search_result["pagination"].get("data", [])
            for entry in search_entries:
                if entry.get("id") == entry_id:
                    entry_name = entry.get("name")
                    entry_type = entry.get("type")
                    break
        else:
            # Search for entry by name or hash
            search_result = client.get_vault_file_entries(per_page=1000)

            search_entries = []
            if isinstance(search_result, dict):
                if "data" in search_result:
                    search_entries = search_result["data"]
                elif "pagination" in search_result and isinstance(
                    search_result["pagination"], dict
                ):
                    search_entries = search_result["pagination"].get("data", [])

            for entry in search_entries:
                e_name = entry.get("name", "")
                e_hash = entry.get("hash", "")

                if e_name == file_identifier or e_hash == file_identifier:
                    entry_id = entry.get("id")
                    entry_name = e_name
                    entry_type = entry.get("type")
                    if not out.quiet:
                        out.info(f"Resolved '{file_identifier}' to ID: {entry_id}")
                    break

        if not entry_id:
            out.error(f"Entry '{file_identifier}' not found in vault.")
            ctx.exit(1)
            return

        # Confirmation prompt
        action = "permanently delete" if no_trash else "move to trash"
        display_name = entry_name or file_identifier
        type_str = f" ({entry_type})" if entry_type else ""

        if not yes:
            confirm = click.confirm(
                f"Are you sure you want to {action} '{display_name}'{type_str}?"
            )
            if not confirm:
                out.info("Cancelled.")
                return

        # Delete the entry
        if not out.quiet:
            out.info(
                f"{'Deleting' if no_trash else 'Moving to trash'}: {display_name}..."
            )

        result = client.delete_vault_file_entries(
            entry_ids=[entry_id],
            delete_forever=no_trash,
        )

        if out.json_output:
            out.output_json(result)
            return

        if no_trash:
            out.success(f"Permanently deleted: {display_name}")
        else:
            out.success(f"Moved to trash: {display_name}")

    except DrimeAPIError as e:
        out.error(str(e))
        ctx.exit(1)
