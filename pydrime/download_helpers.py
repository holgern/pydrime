"""Download helper functions for CLI."""

from pathlib import Path
from typing import Optional

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .api import DrimeClient
from .exceptions import DrimeAPIError, DrimeNotFoundError
from .models import FileEntriesResult, FileEntry
from .output import OutputFormatter
from .utils import is_file_id, normalize_to_hash


def resolve_path_to_hash(
    client: DrimeClient,
    path: str,
    workspace: int,
    out: OutputFormatter,
) -> Optional[str]:
    """Resolve a path (e.g., folder/subfolder/file.txt) to entry hash.

    Args:
        client: Drime API client
        path: Path to resolve (e.g., "folder/file.txt")
        workspace: Workspace ID
        out: Output formatter

    Returns:
        Hash value of the final entry, or None if path not found
    """
    parts = path.split("/")
    current_parent: Optional[int] = None

    # Navigate through each path component
    for i, part in enumerate(parts):
        is_last = i == len(parts) - 1

        # Search for the entry in the current folder
        parent_ids = [current_parent] if current_parent is not None else None
        result = client.get_file_entries(
            workspace_id=workspace,
            parent_ids=parent_ids,
        )

        if not result or not result.get("data"):
            if not out.quiet:
                out.error(f"Path not found: {'/'.join(parts[: i + 1])}")
            return None

        file_entries = FileEntriesResult.from_api_response(result)

        # Find the matching entry
        matching = [e for e in file_entries.entries if e.name == part]
        if not matching:
            # Try case-insensitive
            matching = [
                e for e in file_entries.entries if e.name.lower() == part.lower()
            ]

        if not matching:
            if not out.quiet:
                out.error(f"Path not found: {'/'.join(parts[: i + 1])}")
            return None

        entry = matching[0]

        if is_last:
            # Found the target entry
            if not out.quiet:
                out.info(f"Resolved '{path}' to hash: {entry.hash}")
            return entry.hash
        else:
            # This should be a folder, continue navigating
            if not entry.is_folder:
                if not out.quiet:
                    out.error(f"'{part}' is not a folder in path: {path}")
                return None
            current_parent = entry.id

    return None


def resolve_identifier_to_hash(
    client: DrimeClient,
    identifier: str,
    current_folder: Optional[int],
    workspace: int,
    out: OutputFormatter,
) -> Optional[str]:
    """Resolve identifier (name/ID/hash/path) to hash value.

    Args:
        client: Drime API client
        identifier: Entry identifier (name, ID, hash, or path like folder/file.txt)
        current_folder: Current folder ID
        workspace: Workspace ID
        out: Output formatter

    Returns:
        Hash value or None if not found
    """
    # Check if identifier is a path (contains /)
    if "/" in identifier:
        return resolve_path_to_hash(client, identifier, workspace, out)

    try:
        # Try resolving as entry identifier (supports names, IDs, hashes)
        entry_id = client.resolve_entry_identifier(
            identifier=identifier,
            parent_id=current_folder,
            workspace_id=workspace,
        )
        if not out.quiet and not identifier.isdigit() and not is_file_id(identifier):
            out.info(f"Resolved '{identifier}' to entry ID: {entry_id}")
        return normalize_to_hash(str(entry_id))
    except DrimeNotFoundError:
        # Not found by name, try as hash or ID directly
        if is_file_id(identifier):
            hash_value = normalize_to_hash(identifier)
            if not out.quiet:
                out.info(f"Converting ID {identifier} to hash {hash_value}")
            return hash_value
        return identifier  # Already a hash


def get_entry_from_hash(
    client: DrimeClient, hash_value: str, identifier: str, out: OutputFormatter
) -> Optional[FileEntry]:
    """Get entry object from hash value.

    Args:
        client: Drime API client
        hash_value: Entry hash value
        identifier: Original identifier for error messages
        out: Output formatter

    Returns:
        FileEntry object or None if not found
    """
    # Try searching by query first (works for files)
    result = client.get_file_entries(query=hash_value)
    if result and result.get("data"):
        file_entries = FileEntriesResult.from_api_response(result)
        if not file_entries.is_empty:
            return file_entries.entries[0]

    # Try using folder_id (works for folders)
    result = client.get_file_entries(folder_id=hash_value)
    if result and result.get("folder"):
        folder_data = result["folder"]
        return FileEntriesResult.from_api_response({"data": [folder_data]}).entries[0]

    out.error(f"Entry not found: {identifier}")
    return None


def get_unique_filename(base_path: Path) -> Path:
    """Generate a unique filename if the file already exists.

    Args:
        base_path: Base path to check

    Returns:
        Unique path that doesn't exist
    """
    if not base_path.exists():
        return base_path

    # Split name and extension
    stem = base_path.stem
    suffix = base_path.suffix
    parent = base_path.parent
    counter = 1

    # Find a unique name
    while True:
        new_name = f"{stem} ({counter}){suffix}"
        new_path = parent / new_name
        if not new_path.exists():
            return new_path
        counter += 1


def download_file_with_progress(
    client: DrimeClient,
    hash_value: str,
    output_path: Path,
    entry_name: str,
    show_progress: bool,
    no_progress: bool,
    out: OutputFormatter,
) -> Path:
    """Download a file with optional progress display.

    Args:
        client: Drime API client
        hash_value: File hash
        output_path: Output file path
        entry_name: Entry name for display
        show_progress: Whether to show progress
        no_progress: Whether to disable progress bars
        out: Output formatter

    Returns:
        Path to downloaded file

    Raises:
        DrimeAPIError: If download fails
    """
    if show_progress and not no_progress:
        # Create progress bar using rich.Progress
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            # Update 10 times per second for smoother speed calculation
            refresh_per_second=10,
        ) as progress:
            task = progress.add_task(f"[cyan]{entry_name}", total=None)

            def progress_callback(bytes_downloaded: int, total_bytes: int) -> None:
                # Update with both completed and total to ensure speed calculation works
                progress.update(task, completed=bytes_downloaded, total=total_bytes)

            saved_path = client.download_file(
                hash_value, output_path, progress_callback=progress_callback
            )
            # Don't print "Downloaded:" message when using progress bar
            # as it breaks the terminal output
            return saved_path
    else:
        if not no_progress:
            out.progress_message(f"Downloading {entry_name}...")
        saved_path = client.download_file(hash_value, output_path)
        if not out.quiet:
            out.success(f"Downloaded: {saved_path}")
        return saved_path


def download_folder_recursive(
    client: DrimeClient,
    entry: FileEntry,
    folder_path: Path,
    identifier: str,
    downloaded_files: list[dict],
    out: OutputFormatter,
    on_duplicate: str,
    no_progress: bool,
    entry_obj: Optional[FileEntry] = None,
) -> None:
    """Download folder and its contents recursively.

    Args:
        client: Drime API client
        entry: Folder entry
        folder_path: Local folder path
        identifier: Original identifier
        downloaded_files: List to append downloaded file info
        out: Output formatter
        on_duplicate: Duplicate handling strategy
        no_progress: Whether to disable progress bars
        entry_obj: Optional pre-fetched entry object
    """
    # Check if a file exists with the folder name
    if folder_path.exists() and folder_path.is_file():
        out.error(
            f"Cannot download folder '{entry.name}': "
            f"a file with this name already exists at {folder_path}"
        )
        return

    folder_path.mkdir(parents=True, exist_ok=True)
    if not out.quiet:
        out.info(f"Downloading folder: {entry.name}")

    try:
        folder_result = client.get_file_entries(parent_ids=[entry.id])
        folder_entries = FileEntriesResult.from_api_response(folder_result)

        for sub_entry in folder_entries.entries:
            if sub_entry.is_folder:
                # Recursively download subfolder
                sub_folder_path = folder_path / sub_entry.name
                download_folder_recursive(
                    client,
                    sub_entry,
                    sub_folder_path,
                    identifier if not entry_obj else entry.hash,
                    downloaded_files,
                    out,
                    on_duplicate,
                    no_progress,
                    entry_obj=sub_entry,
                )
            else:
                # Download file
                download_single_file(
                    client,
                    sub_entry.hash,
                    identifier if not entry_obj else entry.hash,
                    folder_path,
                    sub_entry.name,
                    downloaded_files,
                    out,
                    on_duplicate,
                    no_progress,
                    show_progress=True,
                )
    except DrimeAPIError as e:
        out.error(f"Error downloading folder contents: {e}")


def download_single_file(
    client: DrimeClient,
    hash_value: str,
    identifier: str,
    dest_path: Optional[Path],
    entry_name: str,
    downloaded_files: list[dict],
    out: OutputFormatter,
    on_duplicate: str,
    no_progress: bool,
    output_override: Optional[str] = None,
    single_file: bool = False,
    show_progress: bool = True,
) -> None:
    """Download a single file.

    Args:
        client: Drime API client
        hash_value: File hash
        identifier: Original identifier
        dest_path: Destination directory path
        entry_name: Entry name
        downloaded_files: List to append downloaded file info
        out: Output formatter
        on_duplicate: Duplicate handling strategy
        no_progress: Whether to disable progress bars
        output_override: Override output path
        single_file: Whether this is a single file download
        show_progress: Whether to show progress
    """
    # Determine output path
    if dest_path:
        # If dest_path is a directory, join it with the filename
        if dest_path.is_dir():
            output_path = dest_path / entry_name
        else:
            output_path = dest_path
    elif output_override and single_file:
        output_path = Path(output_override)
    else:
        output_path = Path(entry_name)

    # Check for duplicate (only files, not directories)
    if output_path.exists():
        # If a directory exists with this name, we need to rename the file
        # (can't write a file where a directory exists)
        if output_path.is_dir():
            output_path = get_unique_filename(output_path)
            out.info(
                f"Directory exists with same name, renaming file to: {output_path.name}"
            )
        # If a file exists, apply the duplicate strategy
        elif on_duplicate == "skip":
            out.info(f"Skipped (already exists): {output_path}")
            downloaded_files.append(
                {
                    "hash": hash_value,
                    "path": str(output_path),
                    "input": identifier,
                    "skipped": True,
                }
            )
            return
        elif on_duplicate == "rename":
            output_path = get_unique_filename(output_path)
            out.info(f"Renaming to avoid duplicate: {output_path.name}")

    try:
        saved_path = download_file_with_progress(
            client,
            hash_value,
            output_path,
            entry_name,
            show_progress,
            no_progress,
            out,
        )

        downloaded_files.append(
            {"hash": hash_value, "path": str(saved_path), "input": identifier}
        )
    except DrimeAPIError as e:
        out.error(f"Error downloading file: {e}")
