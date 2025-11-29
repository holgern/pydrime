"""Validation utilities for verifying cloud uploads.

This module provides functions to validate that files uploaded to Drime Cloud
are complete and match the local files in terms of size and metadata.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from .api import DrimeClient
from .file_entries_manager import FileEntriesManager
from .models import FileEntry
from .output import OutputFormatter


def scan_directory_for_validation(
    path: Path, base_path: Path, out: OutputFormatter
) -> list[tuple[Path, str]]:
    """Recursively scan directory and return list of (file_path, relative_path) tuples.

    Args:
        path: Directory to scan
        base_path: Base path for calculating relative paths
        out: Output formatter for warnings

    Returns:
        List of tuples containing file paths and their relative paths
        (paths use forward slashes for cross-platform compatibility)
    """
    files = []

    try:
        for item in path.iterdir():
            if item.is_file():
                # Use as_posix() to ensure forward slashes on all platforms
                relative_path = item.relative_to(base_path).as_posix()
                files.append((item, relative_path))
            elif item.is_dir():
                files.extend(scan_directory_for_validation(item, base_path, out))
    except PermissionError as e:
        out.warning(f"Permission denied: {e}")

    return files


def _find_remote_folder_id(
    file_manager: FileEntriesManager,
    remote_path: str,
    local_path: Path,
    out: OutputFormatter,
) -> int | None:
    """Find the remote folder ID for a given path.

    Args:
        file_manager: FileEntriesManager instance
        remote_path: Remote path to find
        local_path: Local directory path (used as fallback)
        out: Output formatter for warnings

    Returns:
        Folder ID if found, None otherwise
    """
    if remote_path:
        path_parts = remote_path.split("/")
        folder_id = None

        for part in path_parts:
            if part:  # Skip empty parts
                folder_entry = file_manager.find_folder_by_name(part, folder_id)
                if folder_entry:
                    folder_id = folder_entry.id
                else:
                    out.warning(f"Remote path '{remote_path}' not found, using root")
                    return None

        return folder_id
    else:
        # Look for matching folder by local folder name
        folder_entry = file_manager.find_folder_by_name(local_path.name, None)
        return folder_entry.id if folder_entry else None


def _validate_file_entry(
    rel_path: str,
    local_size: int,
    remote_file_map: dict[str, FileEntry],
) -> dict[str, Any]:
    """Validate a single file entry against the remote file map.

    Args:
        rel_path: Relative path of the file
        local_size: Local file size in bytes
        remote_file_map: Map of remote paths to FileEntry objects

    Returns:
        Dictionary with validation result containing status and details
    """
    # Look up the file in the remote map
    matching_entry = remote_file_map.get(rel_path)

    if not matching_entry:
        # Also try looking up just the filename if full path doesn't match
        file_name = Path(rel_path).name
        for path, entry in remote_file_map.items():
            if Path(path).name == file_name:
                matching_entry = entry
                break

    if not matching_entry:
        return {
            "status": "missing",
            "path": rel_path,
            "local_size": local_size,
            "reason": "Not found in cloud",
        }

    # Check size
    cloud_size = matching_entry.file_size or 0
    if cloud_size != local_size:
        return {
            "status": "size_mismatch",
            "path": rel_path,
            "local_size": local_size,
            "cloud_size": cloud_size,
            "cloud_id": matching_entry.id,
        }

    if not matching_entry.users:
        # File exists with correct size but has no users field
        # This indicates an incomplete upload (race condition during parallel)
        return {
            "status": "incomplete",
            "path": rel_path,
            "size": local_size,
            "cloud_id": matching_entry.id,
            "reason": "No users field (incomplete upload)",
        }

    return {
        "status": "valid",
        "path": rel_path,
        "size": local_size,
        "cloud_id": matching_entry.id,
    }


def _output_validation_results(
    out: OutputFormatter,
    total: int,
    valid_files: list[dict[str, Any]],
    missing_files: list[dict[str, Any]],
    size_mismatch_files: list[dict[str, Any]],
    incomplete_files: list[dict[str, Any]],
) -> None:
    """Output validation results to the console.

    Args:
        out: OutputFormatter instance
        total: Total number of files validated
        valid_files: List of valid file results
        missing_files: List of missing file results
        size_mismatch_files: List of size mismatch file results
        incomplete_files: List of incomplete file results
    """
    out.print("\n" + "-" * 60)
    out.print("Validation Results")
    out.print("-" * 60 + "\n")

    # Show valid files
    if valid_files:
        out.success(f"Valid: {len(valid_files)} file(s)")

    # Show missing files
    if missing_files:
        out.error(f"Missing: {len(missing_files)} file(s)")
        for f in missing_files:
            local_size_val = cast(int, f["local_size"])
            out.print(
                f"  {f['path']} ({out.format_size(local_size_val)}) - {f['reason']}"
            )
        out.print("")

    # Show size mismatches
    if size_mismatch_files:
        out.warning(f"Size mismatch: {len(size_mismatch_files)} file(s)")
        for f in size_mismatch_files:
            local_size_val = cast(int, f["local_size"])
            cloud_size_val = cast(int, f["cloud_size"])
            out.print(
                f"  {f['path']} [ID: {f['cloud_id']}]\n"
                f"    Local:  {out.format_size(local_size_val)}\n"
                f"    Cloud:  {out.format_size(cloud_size_val)}"
            )
        out.print("")

    # Show incomplete files (no users field)
    if incomplete_files:
        out.warning(f"Incomplete: {len(incomplete_files)} file(s)")
        for f in incomplete_files:
            file_size = cast(int, f["size"])
            out.print(
                f"  {f['path']} [ID: {f['cloud_id']}] "
                f"({out.format_size(file_size)}) - {f['reason']}"
            )
        out.print("")

    # Summary
    valid = len(valid_files)
    issues = len(missing_files) + len(size_mismatch_files) + len(incomplete_files)

    out.print("-" * 60)
    if issues == 0:
        out.success(f"All {total} file(s) validated successfully!")
    else:
        out.warning(f"Validation complete: {valid}/{total} valid, {issues} issue(s)")
    out.print("-" * 60)


def _get_empty_validation_result() -> dict[str, Any]:
    """Return an empty validation result dictionary."""
    return {
        "total": 0,
        "valid": 0,
        "missing": 0,
        "size_mismatch": 0,
        "incomplete": 0,
        "has_issues": False,
    }


def _build_remote_file_map(
    file_manager: FileEntriesManager,
    remote_folder_id: int | None,
) -> dict[str, FileEntry]:
    """Build a map of remote files from the file manager.

    Args:
        file_manager: FileEntriesManager instance
        remote_folder_id: Remote folder ID to start from

    Returns:
        Dictionary mapping relative paths to FileEntry objects
    """
    remote_files_with_paths = file_manager.get_all_recursive(
        folder_id=remote_folder_id, path_prefix=""
    )
    return {entry_path: entry for entry, entry_path in remote_files_with_paths}


def _categorize_validation_results(
    results: list[dict[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Categorize validation results by status.

    Args:
        results: List of validation result dictionaries

    Returns:
        Tuple of (valid_files, missing_files, size_mismatch_files, incomplete_files)
    """
    valid_files: list[dict[str, Any]] = []
    missing_files: list[dict[str, Any]] = []
    size_mismatch_files: list[dict[str, Any]] = []
    incomplete_files: list[dict[str, Any]] = []

    for result in results:
        status = result.get("status")
        if status == "valid":
            valid_files.append(result)
        elif status == "missing":
            missing_files.append(result)
        elif status == "size_mismatch":
            size_mismatch_files.append(result)
        elif status == "incomplete":
            incomplete_files.append(result)

    return valid_files, missing_files, size_mismatch_files, incomplete_files


def validate_cloud_files(
    client: DrimeClient,
    out: OutputFormatter,
    local_path: Path,
    remote_path: str,
    workspace_id: int,
    show_header: bool = True,
) -> dict[str, Any]:
    """Validate cloud files after sync operation.

    Checks that every file in the local directory exists in the cloud
    with the correct file size and has the users field set (indicating
    a complete upload).

    Args:
        client: DrimeClient instance
        out: OutputFormatter instance
        local_path: Local directory path that was synced
        remote_path: Remote path where files were synced
        workspace_id: Workspace ID
        show_header: Whether to show the validation header (default: True)

    Returns:
        Dictionary with validation results containing:
        - total: Total number of files validated
        - valid: Number of valid files
        - missing: Number of missing files
        - size_mismatch: Number of files with size mismatches
        - incomplete: Number of incomplete uploads
        - has_issues: Boolean indicating if any issues were found
    """
    if show_header:
        out.print("")
        out.info("=" * 60)
        out.info("Validation")
        out.info("=" * 60)
        out.info("")

    # Collect files to validate
    if not local_path.is_dir():
        out.warning("Local path is not a directory, skipping validation.")
        return _get_empty_validation_result()

    # Scan local directory for files
    files_to_validate = scan_directory_for_validation(local_path, local_path, out)

    if not files_to_validate:
        out.info("No files found to validate.")
        return _get_empty_validation_result()

    out.info(f"Validating {len(files_to_validate)} file(s)...\n")

    # Use FileEntriesManager to fetch all remote files once
    file_manager = FileEntriesManager(client, workspace_id)

    # Find the remote folder by path
    remote_folder_id = _find_remote_folder_id(
        file_manager, remote_path, local_path, out
    )

    out.progress_message("Fetching remote files...")

    # Build a map of remote files: {path: FileEntry}
    remote_file_map = _build_remote_file_map(file_manager, remote_folder_id)

    if not out.quiet:
        out.info(f"Found {len(remote_file_map)} remote file(s)\n")

    # Validate each file and collect results
    validation_results: list[dict[str, Any]] = []
    for idx, (file_path, rel_path) in enumerate(files_to_validate, 1):
        local_size = file_path.stat().st_size
        out.progress_message(f"Validating [{idx}/{len(files_to_validate)}]: {rel_path}")
        result = _validate_file_entry(rel_path, local_size, remote_file_map)
        validation_results.append(result)

    # Categorize results
    valid_files, missing_files, size_mismatch_files, incomplete_files = (
        _categorize_validation_results(validation_results)
    )

    # Output results
    if out.json_output:
        return {
            "total": len(files_to_validate),
            "valid": len(valid_files),
            "missing": len(missing_files),
            "size_mismatch": len(size_mismatch_files),
            "incomplete": len(incomplete_files),
            "valid_files": valid_files,
            "missing_files": missing_files,
            "size_mismatch_files": size_mismatch_files,
            "incomplete_files": incomplete_files,
            "has_issues": bool(
                missing_files or size_mismatch_files or incomplete_files
            ),
        }

    _output_validation_results(
        out,
        len(files_to_validate),
        valid_files,
        missing_files,
        size_mismatch_files,
        incomplete_files,
    )

    return {
        "total": len(files_to_validate),
        "valid": len(valid_files),
        "missing": len(missing_files),
        "size_mismatch": len(size_mismatch_files),
        "incomplete": len(incomplete_files),
        "has_issues": bool(missing_files or size_mismatch_files or incomplete_files),
    }


def validate_single_file(
    client: DrimeClient,
    out: OutputFormatter,
    local_path: Path,
    remote_path: str,
    workspace_id: int,
    show_header: bool = True,
) -> dict[str, Any]:
    """Validate a single file after upload.

    Checks that a single uploaded file exists in the cloud with the correct
    file size and has the users field set (indicating a complete upload).

    Args:
        client: DrimeClient instance
        out: OutputFormatter instance
        local_path: Local file path that was uploaded
        remote_path: Remote path where file was uploaded
        workspace_id: Workspace ID
        show_header: Whether to show the validation header (default: True)

    Returns:
        Dictionary with validation results containing:
        - valid: Boolean indicating if the file is valid
        - has_issues: Boolean indicating if any issues were found
        - path: The file path
        - local_size: Local file size
        - cloud_size: Cloud file size (if found)
        - reason: Reason for failure (if any)
    """
    if show_header:
        out.print("")
        out.info("=" * 60)
        out.info("Validation")
        out.info("=" * 60)
        out.info("")

    local_size = local_path.stat().st_size

    out.progress_message(f"Validating: {remote_path}")

    # Use FileEntriesManager to find the file
    file_manager = FileEntriesManager(client, workspace_id)

    # Navigate to the parent folder and find the file
    path_parts = remote_path.split("/")
    file_name = path_parts[-1]
    folder_parts = path_parts[:-1]

    folder_id = None
    for part in folder_parts:
        if part:
            folder_entry = file_manager.find_folder_by_name(part, folder_id)
            if folder_entry:
                folder_id = folder_entry.id
            else:
                out.error(f"Parent folder '{part}' not found in cloud")
                return {
                    "valid": False,
                    "has_issues": True,
                    "path": remote_path,
                    "local_size": local_size,
                    "reason": f"Parent folder '{part}' not found",
                }

    # Find the file in the folder by listing folder contents and filtering
    folder_entries = file_manager.get_all_in_folder(folder_id)
    matching_entry = None
    for entry in folder_entries:
        if not entry.is_folder and entry.name == file_name:
            matching_entry = entry
            break

    if not matching_entry:
        out.print("\n" + "-" * 60)
        out.print("Validation Results")
        out.print("-" * 60 + "\n")
        out.error(f"File not found in cloud: {remote_path}")
        out.print("-" * 60)
        return {
            "valid": False,
            "has_issues": True,
            "path": remote_path,
            "local_size": local_size,
            "reason": "Not found in cloud",
        }

    cloud_size = matching_entry.file_size or 0

    # Check size
    if cloud_size != local_size:
        out.print("\n" + "-" * 60)
        out.print("Validation Results")
        out.print("-" * 60 + "\n")
        out.warning(f"Size mismatch: {remote_path}")
        out.print(f"  Local:  {out.format_size(local_size)}")
        out.print(f"  Cloud:  {out.format_size(cloud_size)}")
        out.print("-" * 60)
        return {
            "valid": False,
            "has_issues": True,
            "path": remote_path,
            "local_size": local_size,
            "cloud_size": cloud_size,
            "cloud_id": matching_entry.id,
            "reason": "Size mismatch",
        }

    # Check users field
    if not matching_entry.users:
        out.print("\n" + "-" * 60)
        out.print("Validation Results")
        out.print("-" * 60 + "\n")
        out.warning(f"Incomplete upload: {remote_path}")
        out.print("  File exists but has no users field (incomplete upload)")
        out.print("-" * 60)
        return {
            "valid": False,
            "has_issues": True,
            "path": remote_path,
            "local_size": local_size,
            "cloud_size": cloud_size,
            "cloud_id": matching_entry.id,
            "reason": "No users field (incomplete upload)",
        }

    # File is valid
    out.print("\n" + "-" * 60)
    out.print("Validation Results")
    out.print("-" * 60 + "\n")
    out.success(f"File validated successfully: {remote_path}")
    out.print(f"  Size: {out.format_size(local_size)}")
    out.print("-" * 60)

    return {
        "valid": True,
        "has_issues": False,
        "path": remote_path,
        "local_size": local_size,
        "cloud_size": cloud_size,
        "cloud_id": matching_entry.id,
    }
