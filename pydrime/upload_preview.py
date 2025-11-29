"""Upload preview and dry-run display utilities."""

from pathlib import Path, PurePosixPath
from typing import Optional

from .api import DrimeClient
from .output import OutputFormatter
from .workspace_utils import format_workspace_display


def display_upload_preview(
    out: OutputFormatter,
    client: DrimeClient,
    files_to_upload: list[tuple[Path, str]],
    workspace: int,
    current_folder_id: Optional[int],
    current_folder_name: Optional[str],
    is_dry_run: bool = False,
) -> None:
    """Display upload preview showing structure and files.

    Args:
        out: Output formatter
        client: Drime API client
        files_to_upload: List of (file_path, relative_path) tuples
        workspace: Workspace ID
        current_folder_id: Current folder ID
        current_folder_name: Current folder name
        is_dry_run: Whether this is a dry run preview
    """
    if out.quiet:
        return

    # Header
    out.print("\n" + "=" * 70)
    if is_dry_run:
        out.print("DRY RUN - Upload Preview")
    else:
        out.print("Upload Preview")
    out.print("=" * 70 + "\n")

    # Show destination
    out.print("Destination:")

    # Workspace display
    workspace_display, _ = format_workspace_display(client, workspace)
    out.print(f"  Workspace: {workspace_display}")

    # Build the full destination path
    if current_folder_id is None:
        base_location = "/"
    else:
        if current_folder_name:
            base_location = f"/{current_folder_name}"
        else:
            base_location = f"/Folder_{current_folder_id}"

    out.print(f"  Base location: {base_location}")

    # Show where files will actually land
    if files_to_upload:
        # Get the first file to show the structure
        first_rel_path = files_to_upload[0][1]
        first_parts = PurePosixPath(first_rel_path).parts
        if len(first_parts) > 1:
            top_folder = first_parts[0]
            if base_location == "/":
                out.print(f"  Files will be uploaded to: /{top_folder}/...")
            else:
                out.print(
                    f"  Files will be uploaded to: {base_location}/{top_folder}/..."
                )
        else:
            out.print(f"  Files will be uploaded to: {base_location}")

    out.print("")

    # Extract and show folders that will be created
    folders_to_create = set()
    for _, rel_path in files_to_upload:
        # Strip leading slashes to avoid including root "/" as a folder
        normalized_path = rel_path.lstrip("/")
        path_parts = PurePosixPath(normalized_path).parts
        # Build all parent folder paths
        for i in range(len(path_parts) - 1):  # Exclude the filename
            folder_path = "/" + str(PurePosixPath(*path_parts[: i + 1]))
            folders_to_create.add(folder_path)

    if folders_to_create:
        sorted_folders = sorted(folders_to_create)
        out.print(f"Folders to create: {len(sorted_folders)}")
        for folder in sorted_folders:
            out.print(f"  [D] {folder}/")
        out.print("")

    # Show files to upload with structure
    out.print(f"Files to upload: {len(files_to_upload)}")

    # Group files by directory for better visualization
    files_by_dir: dict[str, list[tuple[str, int]]] = {}
    for file_path, rel_path in files_to_upload:
        posix_path = PurePosixPath(rel_path)
        dir_path = str(posix_path.parent)
        if dir_path == ".":
            dir_path = "(root)"
        if dir_path not in files_by_dir:
            files_by_dir[dir_path] = []
        file_size = file_path.stat().st_size
        files_by_dir[dir_path].append((posix_path.name, file_size))

    # Display files grouped by directory
    for dir_path in sorted(files_by_dir.keys()):
        if dir_path == "(root)":
            out.print("\n  In root:")
        else:
            out.print(f"\n  In {dir_path}/:")
        for filename, size in sorted(files_by_dir[dir_path]):
            out.print(f"    [F] {filename} ({out.format_size(size)})")

    # Calculate total size
    total_size = sum(f[0].stat().st_size for f in files_to_upload)

    # Summary
    out.print("\n" + "=" * 70)
    out.print(f"Total: {len(files_to_upload)} files, {out.format_size(total_size)}")
    out.print("=" * 70 + "\n")
