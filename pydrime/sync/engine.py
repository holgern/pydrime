"""Core sync engine for executing sync operations."""

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional

from rich.progress import Progress, SpinnerColumn, TextColumn

from ..api import DrimeClient
from ..file_entries_manager import FileEntriesManager
from ..models import FileEntry
from ..output import OutputFormatter
from ..utils import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MULTIPART_THRESHOLD,
    DEFAULT_RETRY_DELAY,
    format_size,
)
from .comparator import FileComparator, SyncAction, SyncDecision
from .concurrency import ConcurrencyLimits, SyncPauseController
from .modes import SyncMode
from .operations import SyncOperations
from .pair import SyncPair
from .progress import SyncProgressTracker
from .scanner import DirectoryScanner, LocalFile, RemoteFile
from .state import (
    SyncStateManager,
    build_local_tree_from_files,
    build_remote_tree_from_files,
)

logger = logging.getLogger(__name__)

# Timeout for future.result() to allow Ctrl+C handling on Windows
# This is needed because Python's ThreadPoolExecutor doesn't handle
# KeyboardInterrupt well on Windows without periodic timeouts
_FUTURE_RESULT_TIMEOUT = 0.5  # seconds


class SyncEngine:
    """Core sync engine that orchestrates file synchronization.

    The engine supports:
    - Multiple sync modes (TWO_WAY, LOCAL_BACKUP, CLOUD_BACKUP, etc.)
    - Parallel uploads/downloads with semaphore-based concurrency control
    - Pause/resume/cancel operations
    - Rename/move detection
    - Ignore patterns
    - State tracking for incremental sync

    Attributes:
        client: Drime API client
        output: Output formatter for displaying progress/status
        operations: Sync operations handler
        state_manager: State manager for tracking sync history
        pause_controller: Controller for pause/resume/cancel operations
        concurrency_limits: Concurrency limits for transfers and operations
    """

    def __init__(
        self,
        client: DrimeClient,
        output: Optional[OutputFormatter] = None,
        state_manager: Optional[SyncStateManager] = None,
        pause_controller: Optional[SyncPauseController] = None,
        concurrency_limits: Optional[ConcurrencyLimits] = None,
    ):
        """Initialize sync engine.

        Args:
            client: Drime API client
            output: Output formatter for displaying progress/status
            state_manager: Optional state manager for tracking sync history.
                          If None, a default one will be created for TWO_WAY mode.
            pause_controller: Optional controller for pause/resume/cancel.
                             If None, a default one will be created.
            concurrency_limits: Optional concurrency limits for operations.
                               Defaults to 10 transfers, 20 normal operations.
        """
        self.client = client
        self.output = output or OutputFormatter()
        self.operations = SyncOperations(client)
        self.state_manager = state_manager or SyncStateManager()
        self.pause_controller = pause_controller or SyncPauseController()
        self.concurrency_limits = concurrency_limits or ConcurrencyLimits()

    def pause(self) -> None:
        """Pause sync operations.

        Workers will wait at the next checkpoint until resumed.
        This does not abort current file transfers.
        """
        self.pause_controller.pause()
        if not self.output.quiet:
            self.output.info("Sync paused")

    def resume(self) -> None:
        """Resume paused sync operations.

        Workers waiting at checkpoints will continue.
        """
        self.pause_controller.resume()
        if not self.output.quiet:
            self.output.info("Sync resumed")

    def cancel(self) -> None:
        """Cancel sync operations.

        This sets removed=True and unpauses to allow workers to exit gracefully.
        Current file transfers may complete before workers exit.
        """
        self.pause_controller.cancel()
        if not self.output.quiet:
            self.output.info("Sync cancelled")

    @property
    def paused(self) -> bool:
        """Check if sync is currently paused."""
        return self.pause_controller.paused

    @property
    def cancelled(self) -> bool:
        """Check if sync has been cancelled."""
        return self.pause_controller.removed

    def reset(self) -> None:
        """Reset the sync engine for a new sync operation.

        This resets the pause controller state.
        """
        self.pause_controller.reset()

    def sync_pair(
        self,
        pair: SyncPair,
        dry_run: bool = False,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        batch_size: int = 50,
        use_streaming: bool = True,
        max_workers: int = 1,
        start_delay: float = 0.0,
        sync_progress_tracker: Optional[SyncProgressTracker] = None,
    ) -> dict:
        """Sync a single sync pair.

        Args:
            pair: Sync pair to synchronize
            dry_run: If True, only show what would be done without actually syncing
            chunk_size: Chunk size for multipart uploads (bytes)
            multipart_threshold: Threshold for using multipart upload (bytes)
            progress_callback: Optional callback for progress updates (bytes)
            batch_size: Number of files to process per batch (for streaming mode)
            use_streaming: If True, use streaming mode to process files in batches
                          If False, scan all files upfront (original behavior)
            max_workers: Number of parallel workers for uploads/downloads (default: 1)
            start_delay: Delay in seconds between starting each parallel operation
                        (default: 0.0, useful for preventing server overload)
            sync_progress_tracker: Optional progress tracker for detailed progress
                                  events (directory scans, file uploads, etc.)

        Returns:
            Dictionary with sync statistics

        Examples:
            >>> engine = SyncEngine(client)
            >>> pair = SyncPair(Path("/local"), "/remote", SyncMode.TWO_WAY)
            >>> stats = engine.sync_pair(pair, dry_run=True)
            >>> print(f"Would upload {stats['uploads']} files")
        """
        # Validate local directory exists
        if not pair.local.exists():
            raise ValueError(f"Local directory does not exist: {pair.local}")
        if not pair.local.is_dir():
            raise ValueError(f"Local path is not a directory: {pair.local}")

        if not self.output.quiet:
            self.output.info(f"Syncing: {pair.local} <-> {pair.remote}")
            self.output.info(f"Mode: {pair.sync_mode.value}")
            if dry_run:
                self.output.info("Dry run: No changes will be made")
            if use_streaming and not dry_run:
                self.output.info(
                    f"Streaming mode: Processing in batches of {batch_size}"
                )
            self.output.print("")

        # Choose between streaming and traditional mode
        # For LOCAL_TO_CLOUD and LOCAL_BACKUP, use incremental mode (even for dry-run)
        # to avoid scanning huge directories upfront
        if pair.sync_mode in (SyncMode.LOCAL_TO_CLOUD, SyncMode.LOCAL_BACKUP):
            # Use incremental mode for local-to-cloud (works for both dry-run and real)
            return self._sync_pair_incremental(
                pair,
                dry_run,
                chunk_size,
                multipart_threshold,
                progress_callback,
                batch_size,
                max_workers,
                start_delay,
                sync_progress_tracker,
            )
        elif use_streaming and not dry_run and pair.sync_mode.requires_remote_scan:
            # Use streaming mode for other modes (not dry-run)
            return self._sync_pair_streaming(
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                batch_size,
                max_workers,
                start_delay,
                sync_progress_tracker,
            )
        else:
            # Use traditional mode (scan all files upfront)
            return self._sync_pair_traditional(
                pair,
                dry_run,
                chunk_size,
                multipart_threshold,
                progress_callback,
                max_workers,
                start_delay,
                sync_progress_tracker,
            )

    def _sync_pair_traditional(
        self,
        pair: SyncPair,
        dry_run: bool,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float = 0.0,
        sync_progress_tracker: Optional[SyncProgressTracker] = None,
    ) -> dict:
        """Traditional sync: scan all files upfront, then process.

        Args:
            pair: Sync pair to synchronize
            dry_run: If True, only show what would be done
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation
            sync_progress_tracker: Optional progress tracker (disables spinners)

        Returns:
            Dictionary with sync statistics
        """
        # Load previous sync state for TWO_WAY mode (for deletion and rename detection)
        previous_synced_files: set[str] = set()
        previous_local_tree = None
        previous_remote_tree = None
        if pair.sync_mode == SyncMode.TWO_WAY:
            state = self.state_manager.load_state(pair.local, pair.remote)
            if state:
                previous_synced_files = state.synced_files
                previous_local_tree = state.local_tree
                previous_remote_tree = state.remote_tree
                logger.debug(
                    f"Loaded previous sync state with "
                    f"{len(previous_synced_files)} files, "
                    f"local_tree: {state.local_tree.size}, "
                    f"remote_tree: {state.remote_tree.size}"
                )

        # Step 1: Scan files
        # Skip internal Progress spinner if external tracker is managing display
        if sync_progress_tracker:
            # Scan without Rich progress (external display is active)
            local_files = []
            remote_files = []

            if pair.sync_mode.requires_local_scan:
                scanner = DirectoryScanner(
                    ignore_patterns=pair.ignore,
                    exclude_dot_files=pair.exclude_dot_files,
                )
                local_files = scanner.scan_local(pair.local)
                logger.debug(f"Found {len(local_files)} local file(s)")

            if pair.sync_mode.requires_remote_scan:
                remote_files = self._scan_remote(pair)
                logger.debug(f"Found {len(remote_files)} remote file(s)")
        else:
            # Use Rich progress spinner for scanning
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True,
            ) as progress:
                # Scan local files
                if pair.sync_mode.requires_local_scan:
                    task = progress.add_task("Scanning local directory...", total=None)
                    scanner = DirectoryScanner(
                        ignore_patterns=pair.ignore,
                        exclude_dot_files=pair.exclude_dot_files,
                    )
                    local_files = scanner.scan_local(pair.local)
                    progress.update(
                        task, description=f"Found {len(local_files)} local file(s)"
                    )
                else:
                    local_files = []

                # Scan remote files
                if pair.sync_mode.requires_remote_scan:
                    task = progress.add_task("Scanning remote directory...", total=None)
                    remote_files = self._scan_remote(pair)
                    progress.update(
                        task, description=f"Found {len(remote_files)} remote file(s)"
                    )
                else:
                    remote_files = []

        # Build dictionaries for comparison
        local_file_map = {f.relative_path: f for f in local_files}
        remote_file_map = {f.relative_path: f for f in remote_files}

        # Step 2: Compare files and determine actions (with rename detection)
        comparator = FileComparator(
            pair.sync_mode,
            previous_synced_files,
            previous_local_tree,
            previous_remote_tree,
        )
        decisions = comparator.compare_files(local_file_map, remote_file_map)

        # Step 3: Display plan
        stats = self._categorize_decisions(decisions)
        self._display_sync_plan(stats, decisions, dry_run)

        # Step 4: Handle conflicts if any
        if stats["conflicts"] > 0 and not dry_run:
            decisions = self._handle_conflicts(decisions)
            # Recalculate stats after conflict resolution
            stats = self._categorize_decisions(decisions)

        # Step 5: Execute actions
        if not dry_run:
            self._execute_decisions(
                decisions,
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                max_workers,
                start_delay,
            )

            # Save sync state after successful sync (for TWO_WAY mode)
            if pair.sync_mode == SyncMode.TWO_WAY:
                # After sync, the synced files are those that exist in both
                # locations. This includes: files that already existed, newly
                # uploaded, newly downloaded.
                # But excludes: deleted files (both local and remote deletions)
                current_synced_files: set[str] = set()
                for decision in decisions:
                    if decision.action == SyncAction.SKIP:
                        # Files that were already in sync
                        current_synced_files.add(decision.relative_path)
                    elif decision.action == SyncAction.UPLOAD:
                        # Files uploaded to remote (now exist in both)
                        current_synced_files.add(decision.relative_path)
                    elif decision.action == SyncAction.DOWNLOAD:
                        # Files downloaded to local (now exist in both)
                        current_synced_files.add(decision.relative_path)
                    # DELETE_LOCAL/DELETE_REMOTE are NOT added (no longer exist)

                # Build full tree state for v2 format
                # Filter to only include files that are now synced
                synced_local_files = [
                    f for f in local_files if f.relative_path in current_synced_files
                ]
                synced_remote_files = [
                    f for f in remote_files if f.relative_path in current_synced_files
                ]

                local_tree = build_local_tree_from_files(synced_local_files)
                remote_tree = build_remote_tree_from_files(synced_remote_files)

                self.state_manager.save_state(
                    pair.local,
                    pair.remote,
                    synced_files=current_synced_files,
                    local_tree=local_tree,
                    remote_tree=remote_tree,
                )
                logger.debug(
                    f"Saved sync state with {len(current_synced_files)} files "
                    f"(local_tree: {local_tree.size}, remote_tree: {remote_tree.size})"
                )

        # Step 6: Display summary
        if not self.output.quiet:
            self._display_summary(stats, dry_run)

        return stats

    def _sync_pair_streaming(
        self,
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        batch_size: int,
        max_workers: int,
        start_delay: float = 0.0,
        sync_progress_tracker: Optional[SyncProgressTracker] = None,
    ) -> dict:
        """Streaming sync: process files in batches as they're discovered.

        This mode processes remote files in batches, executing actions immediately
        without waiting for all files to be scanned first. This provides better
        performance and user experience for large cloud directories.

        Args:
            pair: Sync pair to synchronize
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates
            batch_size: Number of files per batch
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation
            sync_progress_tracker: Optional progress tracker (disables spinners)

        Returns:
            Dictionary with sync statistics
        """
        start_time = time.time()
        logger.debug(f"Starting streaming sync at {start_time:.2f}")

        # Load previous sync state for TWO_WAY mode (for deletion detection)
        # NOTE: Rename detection using previous trees is not yet implemented
        # for streaming mode - only traditional mode supports it
        previous_synced_files: set[str] = set()
        if pair.sync_mode == SyncMode.TWO_WAY:
            state = self.state_manager.load_state(pair.local, pair.remote)
            if state:
                previous_synced_files = state.synced_files
                logger.debug(
                    f"Loaded previous sync state with "
                    f"{len(previous_synced_files)} files, "
                    f"local_tree: {state.local_tree.size}, "
                    f"remote_tree: {state.remote_tree.size}"
                )

        # Step 1: Scan local files if needed
        local_files = self._scan_local_files_streaming(
            pair, use_progress=sync_progress_tracker is None
        )

        # Build dictionary for comparison
        local_file_map = {f.relative_path: f for f in local_files}

        # Track statistics
        stats = self._create_empty_stats()

        # Track which remote files we've seen (for delete detection)
        seen_remote_paths: set[str] = set()

        # Track successfully synced files for state saving
        synced_files: set[str] = set()

        # Track synced file objects for building trees
        synced_local_file_map: dict[str, LocalFile] = {}
        synced_remote_file_map: dict[str, RemoteFile] = {}

        # Step 2: Process remote files in batches
        if pair.sync_mode.requires_remote_scan:
            self._process_remote_batches_streaming(
                pair=pair,
                local_file_map=local_file_map,
                seen_remote_paths=seen_remote_paths,
                stats=stats,
                chunk_size=chunk_size,
                multipart_threshold=multipart_threshold,
                progress_callback=progress_callback,
                batch_size=batch_size,
                max_workers=max_workers,
                start_delay=start_delay,
                previous_synced_files=previous_synced_files,
                synced_files=synced_files,
                synced_local_file_map=synced_local_file_map,
                synced_remote_file_map=synced_remote_file_map,
            )

        # Step 3: Handle local-only files (files that don't exist remotely)
        if pair.sync_mode.requires_local_scan:
            self._process_local_only_files_streaming(
                pair=pair,
                local_files=local_files,
                seen_remote_paths=seen_remote_paths,
                stats=stats,
                chunk_size=chunk_size,
                multipart_threshold=multipart_threshold,
                progress_callback=progress_callback,
                max_workers=max_workers,
                start_delay=start_delay,
                previous_synced_files=previous_synced_files,
                synced_files=synced_files,
                synced_local_file_map=synced_local_file_map,
            )

        # Step 4: Save sync state after successful sync (for TWO_WAY mode)
        if pair.sync_mode == SyncMode.TWO_WAY:
            # Build full tree state from tracked file objects
            local_tree = build_local_tree_from_files(
                list(synced_local_file_map.values())
            )
            remote_tree = build_remote_tree_from_files(
                list(synced_remote_file_map.values())
            )

            self.state_manager.save_state(
                pair.local,
                pair.remote,
                synced_files=synced_files,
                local_tree=local_tree,
                remote_tree=remote_tree,
            )
            logger.debug(
                f"Saved sync state with {len(synced_files)} files "
                f"(local_tree: {local_tree.size}, remote_tree: {remote_tree.size})"
            )

        # Step 5: Display summary
        if not self.output.quiet:
            self._display_summary(stats, dry_run=False)

        return stats

    def _sync_pair_incremental(
        self,
        pair: SyncPair,
        dry_run: bool,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        batch_size: int,
        max_workers: int,
        start_delay: float = 0.0,
        sync_progress_tracker: Optional[SyncProgressTracker] = None,
    ) -> dict:
        """Incremental sync: process directories level by level.

        This mode is optimized for LOCAL_TO_CLOUD and LOCAL_BACKUP sync modes
        with huge local directories. Instead of scanning the entire local tree
        upfront, it:
        1. Scans one directory level at a time
        2. Compares files with remote (if needed)
        3. Uploads files immediately (or counts them for dry-run)
        4. Then descends into subdirectories

        This provides immediate feedback and avoids memory/time overhead of
        scanning millions of files before starting any uploads.

        Args:
            pair: Sync pair to synchronize
            dry_run: If True, only show what would be done without uploading
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates (bytes)
            batch_size: Number of files per batch (for parallel uploads)
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation
            sync_progress_tracker: Optional progress tracker for detailed events

        Returns:
            Dictionary with sync statistics
        """
        start_time = time.time()
        logger.debug(f"Starting incremental sync at {start_time:.2f}")

        if not self.output.quiet:
            mode_str = "dry-run " if dry_run else ""
            self.output.info(
                f"Using {mode_str}incremental sync mode (level-by-level)..."
            )

        # Initialize components
        scanner = DirectoryScanner(
            ignore_patterns=pair.ignore,
            exclude_dot_files=pair.exclude_dot_files,
        )
        manager = FileEntriesManager(self.client, pair.workspace_id)
        logger.debug(
            f"FileEntriesManager created with workspace_id={pair.workspace_id}"
        )
        stats = self._create_empty_stats()

        # Setup remote folder and get existing files
        # When remote is "/" or empty, files sync directly to cloud root (folder_id=0)
        remote_folder_id = self._setup_incremental_remote(pair, manager, dry_run)
        logger.debug(
            f"Remote folder setup: remote='{pair.remote}', "
            f"folder_id={remote_folder_id}, dry_run={dry_run}"
        )

        # Determine effective remote folder name for display
        syncing_to_root = not pair.remote or pair.remote == "/"
        effective_remote_name = "/ (root)" if syncing_to_root else pair.remote

        # Show remote folder status to user
        if not self.output.quiet:
            if syncing_to_root:
                # Syncing to root - folder_id is 0
                self.output.success("Syncing directly to cloud root")
            elif remote_folder_id is not None:
                self.output.success(
                    f"Remote folder '{effective_remote_name}' exists "
                    f"(id: {remote_folder_id})"
                )
            else:
                if dry_run:
                    self.output.info(
                        f"Remote folder '{effective_remote_name}' does not exist yet "
                        "(will be created on actual sync)"
                    )
                else:
                    self.output.info(
                        f"Remote folder '{effective_remote_name}' will be created"
                    )

        remote_file_set, remote_file_ids, remote_file_sizes = self._get_remote_file_set(
            pair, manager, remote_folder_id
        )
        logger.debug(
            f"Remote file set: {len(remote_file_set)} files found in workspace "
            f"{pair.workspace_id}, folder_id={remote_folder_id}"
        )

        # Show remote file count to user
        if not self.output.quiet:
            if len(remote_file_set) > 0:
                total_remote_size = sum(remote_file_sizes.values())
                size_str = format_size(total_remote_size)
                self.output.info(
                    f"Found {len(remote_file_set)} file(s) already on remote "
                    f"({size_str})"
                )
            else:
                self.output.info("No files found on remote yet")
            self.output.print("")  # Empty line for readability

        # Track all local files seen (for deletion detection)
        local_file_set: set[str] = set()

        # Process directories using BFS
        dirs_to_process = [pair.local]
        total_files_processed = 0
        total_dirs_processed = 0

        while dirs_to_process:
            if self.pause_controller.removed:
                logger.debug("Sync cancelled")
                break

            current_dir = dirs_to_process.pop(0)
            total_dirs_processed += 1

            # Process single directory
            dir_result = self._process_incremental_directory(
                current_dir=current_dir,
                pair=pair,
                scanner=scanner,
                remote_file_set=remote_file_set,
                remote_file_sizes=remote_file_sizes,
                stats=stats,
                dry_run=dry_run,
                chunk_size=chunk_size,
                multipart_threshold=multipart_threshold,
                progress_callback=progress_callback,
                max_workers=max_workers,
                start_delay=start_delay,
                sync_progress_tracker=sync_progress_tracker,
            )

            # Track local files seen
            local_file_set.update(dir_result.get("local_files", []))

            # Add subdirectories to queue
            dirs_to_process.extend(dir_result["subdirs"])
            total_files_processed += dir_result["files_count"]

            # Log progress periodically
            if total_dirs_processed % 100 == 0:
                elapsed = time.time() - start_time
                logger.debug(
                    f"Progress: {total_dirs_processed} dirs, "
                    f"{total_files_processed} files, "
                    f"{stats['uploads']} uploads in {elapsed:.1f}s"
                )

        # Handle remote deletions for LOCAL_TO_CLOUD mode
        # (files that exist remotely but not locally should be deleted)
        if pair.sync_mode == SyncMode.LOCAL_TO_CLOUD:
            files_to_delete = remote_file_set - local_file_set
            if files_to_delete:
                self._delete_remote_files_incremental(
                    files_to_delete=files_to_delete,
                    remote_file_ids=remote_file_ids,
                    stats=stats,
                    dry_run=dry_run,
                )

        # Display summary
        elapsed = time.time() - start_time
        logger.debug(
            f"Incremental sync completed: {total_dirs_processed} dirs, "
            f"{total_files_processed} files in {elapsed:.1f}s"
        )

        if not self.output.quiet:
            self._display_summary(stats, dry_run=dry_run)

        return stats

    def _setup_incremental_remote(
        self, pair: SyncPair, manager: FileEntriesManager, dry_run: bool
    ) -> Optional[int]:
        """Setup remote folder for incremental sync.

        When remote is "/" or empty, files are synced directly to the cloud root.
        For example, if local is "/path/to/my_folder" with files "a/file.txt",
        and remote is "/", files will be synced to remote "/a/file.txt".

        Args:
            pair: Sync pair configuration
            manager: File entries manager
            dry_run: If True, skip folder creation

        Returns:
            Remote folder ID if found/created, 0 for root, None if folder
            doesn't exist and couldn't be created
        """
        # When remote is "/" or empty, sync directly to root (folder_id=0)
        syncing_to_root = not pair.remote or pair.remote == "/"
        if syncing_to_root:
            logger.debug("Syncing directly to cloud root (folder_id=0)")
            return 0  # 0 means root folder

        # For non-root remote paths, find or create the folder
        effective_folder_name = pair.remote.lstrip("/")

        # Try to find existing folder
        remote_folder_id = None
        try:
            if "/" in effective_folder_name:
                # Nested path - use path resolution
                remote_folder_id = self.client.resolve_path_to_id(
                    effective_folder_name, workspace_id=pair.workspace_id
                )
            else:
                # Simple folder name - search in root
                folder_entry = manager.find_folder_by_name(
                    effective_folder_name, parent_id=0
                )
                if folder_entry:
                    remote_folder_id = folder_entry.id
                    logger.debug(
                        f"Found remote folder '{effective_folder_name}' with id="
                        f"{remote_folder_id}"
                    )
        except Exception as e:
            logger.debug(f"Folder lookup failed for '{effective_folder_name}': {e}")

        # Create remote folder if needed (not for dry-run)
        if not dry_run and remote_folder_id is None:
            try:
                result = self.client.create_folder(
                    name=effective_folder_name, parent_id=None
                )
                if result.get("status") == "success":
                    remote_folder_id = result.get("id")
                    logger.debug(f"Created remote folder: {effective_folder_name}")
            except Exception as e:
                logger.debug(f"Could not create remote folder: {e}")

        return remote_folder_id

    def _get_remote_file_set(
        self,
        pair: SyncPair,
        manager: FileEntriesManager,
        remote_folder_id: Optional[int],
    ) -> tuple[set[str], dict[str, int], dict[str, int]]:
        """Get set of existing remote file paths for idempotency checking.

        This is used by LOCAL_BACKUP and LOCAL_TO_CLOUD modes to skip files
        that already exist remotely (basic idempotency).

        Args:
            pair: Sync pair configuration
            manager: File entries manager
            remote_folder_id: Remote folder ID (None if folder doesn't exist yet)

        Returns:
            Tuple of (set of remote file paths, dict mapping paths to entry IDs,
            dict mapping paths to file sizes)
        """
        remote_file_set: set[str] = set()
        remote_file_ids: dict[str, int] = {}
        remote_file_sizes: dict[str, int] = {}

        # Skip if remote folder doesn't exist yet (nothing to compare against)
        if remote_folder_id is None:
            logger.debug("Remote folder not found, skipping remote file scan")
            return remote_file_set, remote_file_ids, remote_file_sizes

        # Both LOCAL_BACKUP and LOCAL_TO_CLOUD need to check remote files
        # for idempotency (to avoid re-uploading existing files)
        if pair.sync_mode in (SyncMode.LOCAL_BACKUP, SyncMode.LOCAL_TO_CLOUD):
            try:
                entries_with_paths = manager.get_all_recursive(
                    folder_id=remote_folder_id,
                    path_prefix="",
                )
                for entry, rel_path in entries_with_paths:
                    if entry.type != "folder":
                        remote_file_set.add(rel_path)
                        # Store entry ID for potential deletion
                        remote_file_ids[rel_path] = entry.id
                        # Store file size for change detection
                        if entry.file_size is not None:
                            remote_file_sizes[rel_path] = entry.file_size
                logger.debug(f"Found {len(remote_file_set)} existing remote files")
            except Exception as e:
                logger.debug(f"Could not get remote files: {e}")

        return remote_file_set, remote_file_ids, remote_file_sizes

    def _process_incremental_directory(
        self,
        current_dir: Path,
        pair: SyncPair,
        scanner: DirectoryScanner,
        remote_file_set: set[str],
        remote_file_sizes: dict[str, int],
        stats: dict,
        dry_run: bool,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float,
        sync_progress_tracker: Optional[SyncProgressTracker],
    ) -> dict:
        """Process a single directory in incremental sync.

        Args:
            current_dir: Directory to process
            pair: Sync pair configuration
            scanner: Directory scanner
            remote_file_set: Set of existing remote files
            remote_file_sizes: Dict mapping remote paths to file sizes
            stats: Statistics dictionary (modified in place)
            dry_run: If True, only count files
            chunk_size: Chunk size for uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional byte-level progress callback
            max_workers: Number of parallel workers
            start_delay: Delay between parallel operations
            sync_progress_tracker: Optional progress tracker

        Returns:
            Dictionary with 'subdirs' (list of Path), 'files_count' (int),
            and 'local_files' (list of relative paths)
        """
        result: dict = {"subdirs": [], "files_count": 0, "local_files": []}

        # Calculate relative directory path
        rel_dir = (
            str(current_dir.relative_to(pair.local))
            if current_dir != pair.local
            else "."
        )

        # Notify progress tracker about scan start
        if sync_progress_tracker:
            sync_progress_tracker.on_scan_dir_start(rel_dir)

        # Scan directory
        try:
            files, subdirs = scanner.scan_local_single_level(
                current_dir, base_path=pair.local
            )
        except (OSError, PermissionError) as e:
            logger.debug(f"Cannot scan {current_dir}: {e}")
            return result

        # Notify progress tracker about scan complete
        if sync_progress_tracker:
            sync_progress_tracker.on_scan_dir_complete(
                rel_dir, len(files), len(subdirs)
            )

        # Add subdirectories to result
        for subdir_rel in subdirs:
            result["subdirs"].append(pair.local / subdir_rel)

        # Show directory info if only subdirs
        if not self.output.quiet and not sync_progress_tracker:
            if subdirs and not files:
                self.output.info(
                    f"Scanning {rel_dir}: {len(subdirs)} subdirectory(ies)"
                )

        if not files:
            return result

        result["files_count"] = len(files)

        # Track all local files for deletion detection
        result["local_files"] = [f.relative_path for f in files]

        # Filter and process files
        upload_files, skipped_count = self._filter_files_for_upload(
            files, remote_file_set, remote_file_sizes, stats
        )

        # Notify tracker about skipped files
        if sync_progress_tracker and skipped_count > 0:
            sync_progress_tracker.on_files_skipped(skipped_count)

        if not upload_files:
            if not self.output.quiet and not sync_progress_tracker and files:
                total_size = sum(f.size for f in files)
                self.output.info(
                    f"Processing {rel_dir}: {len(files)} file(s) already synced "
                    f"({format_size(total_size)})"
                )
            return result

        # Show upload info with size details
        if not self.output.quiet and not sync_progress_tracker:
            upload_size = sum(f.size for f in upload_files)
            if skipped_count > 0:
                self.output.info(
                    f"Processing {rel_dir}: {len(upload_files)} to upload "
                    f"({format_size(upload_size)}), {skipped_count} already synced"
                )
            else:
                self.output.info(
                    f"Processing {rel_dir}: {len(upload_files)} to upload "
                    f"({format_size(upload_size)})"
                )

        # Execute uploads for this directory
        self._execute_incremental_uploads(
            upload_files=upload_files,
            rel_dir=rel_dir,
            pair=pair,
            stats=stats,
            dry_run=dry_run,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            progress_callback=progress_callback,
            max_workers=max_workers,
            start_delay=start_delay,
            sync_progress_tracker=sync_progress_tracker,
        )

        return result

    def _filter_files_for_upload(
        self,
        files: list[LocalFile],
        remote_file_set: set[str],
        remote_file_sizes: dict[str, int],
        stats: dict,
    ) -> tuple[list[LocalFile], int]:
        """Filter files that need to be uploaded.

        Args:
            files: List of local files
            remote_file_set: Set of existing remote files
            remote_file_sizes: Dict mapping remote paths to file sizes
            stats: Statistics dictionary (modified in place)

        Returns:
            Tuple of (files to upload, skipped count)
        """
        upload_files = []
        skipped_count = 0

        for local_file in files:
            if local_file.relative_path in remote_file_set:
                # File exists remotely - check if size changed
                remote_size = remote_file_sizes.get(local_file.relative_path)
                if remote_size is not None and local_file.size != remote_size:
                    # Size differs - file was modified, needs re-upload
                    upload_files.append(local_file)
                else:
                    # Same size - skip
                    stats["skips"] += 1
                    skipped_count += 1
            else:
                # New file - needs upload
                upload_files.append(local_file)

        return upload_files, skipped_count

    def _execute_incremental_uploads(
        self,
        upload_files: list[LocalFile],
        rel_dir: str,
        pair: SyncPair,
        stats: dict,
        dry_run: bool,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float,
        sync_progress_tracker: Optional[SyncProgressTracker],
    ) -> None:
        """Execute uploads for a batch of files in incremental sync.

        Args:
            upload_files: Files to upload
            rel_dir: Relative directory path for display
            pair: Sync pair configuration
            stats: Statistics dictionary (modified in place)
            dry_run: If True, only count files
            chunk_size: Chunk size for uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional byte-level progress callback
            max_workers: Number of parallel workers
            start_delay: Delay between parallel operations
            sync_progress_tracker: Optional progress tracker
        """
        # Create upload decisions
        upload_decisions = [
            SyncDecision(
                action=SyncAction.UPLOAD,
                reason="New local file",
                local_file=local_file,
                remote_file=None,
                relative_path=local_file.relative_path,
            )
            for local_file in upload_files
        ]

        batch_total_bytes = sum(f.size for f in upload_files)

        # Note: Upload info is already shown in _process_incremental_directory
        # Only show dry-run specific message here
        if not self.output.quiet and not sync_progress_tracker and dry_run:
            self.output.info(
                f"  Would upload {len(upload_decisions)} file(s) "
                f"({format_size(batch_total_bytes)})"
            )

        if dry_run:
            stats["uploads"] += len(upload_decisions)
            return

        # Notify tracker about batch start
        if sync_progress_tracker:
            sync_progress_tracker.on_upload_batch_start(
                rel_dir, len(upload_decisions), batch_total_bytes
            )

        # Execute uploads
        batch_stats = self._execute_batch_decisions_incremental(
            batch_decisions=upload_decisions,
            pair=pair,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            progress_callback=progress_callback,
            max_workers=max_workers,
            start_delay=start_delay,
            sync_progress_tracker=sync_progress_tracker,
        )

        uploaded_count = batch_stats.get("uploads", 0)
        stats["uploads"] += uploaded_count
        stats["errors"] = stats.get("errors", 0) + batch_stats.get("errors", 0)

        # Notify tracker about batch complete
        if sync_progress_tracker:
            sync_progress_tracker.on_upload_batch_complete(rel_dir, uploaded_count)

    def _delete_remote_files_incremental(
        self,
        files_to_delete: set[str],
        remote_file_ids: dict[str, int],
        stats: dict,
        dry_run: bool,
    ) -> None:
        """Delete remote files that no longer exist locally.

        Args:
            files_to_delete: Set of remote file paths to delete
            remote_file_ids: Dict mapping paths to entry IDs
            stats: Statistics dictionary (modified in place)
            dry_run: If True, only count deletions
        """
        if not files_to_delete:
            return

        if not self.output.quiet:
            action = "would delete" if dry_run else "deleting"
            self.output.info(
                f"\n{action.capitalize()} {len(files_to_delete)} "
                "remote file(s) not in local..."
            )

        if dry_run:
            stats["deletes_remote"] += len(files_to_delete)
            return

        # Delete each file
        deleted_count = 0
        for rel_path in files_to_delete:
            entry_id = remote_file_ids.get(rel_path)
            if entry_id:
                try:
                    self.client.delete_file_entries([entry_id])
                    deleted_count += 1
                    logger.debug(f"Deleted remote file: {rel_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete {rel_path}: {e}")
            else:
                logger.warning(f"No entry ID found for {rel_path}, cannot delete")

        stats["deletes_remote"] = deleted_count

        if not self.output.quiet and deleted_count > 0:
            self.output.info(f"  Deleted {deleted_count} remote file(s)")

    def _execute_batch_decisions_incremental(
        self,
        batch_decisions: list[SyncDecision],
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float = 0.0,
        sync_progress_tracker: Optional[SyncProgressTracker] = None,
    ) -> dict:
        """Execute a batch of sync decisions with progress tracking.

        This version supports the SyncProgressTracker for detailed progress events.

        Args:
            batch_decisions: List of sync decisions
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates (bytes)
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation
            sync_progress_tracker: Optional progress tracker for detailed events

        Returns:
            Dictionary with batch statistics
        """
        stats = self._create_empty_stats()

        actionable_decisions = [
            d
            for d in batch_decisions
            if d.action not in [SyncAction.SKIP, SyncAction.CONFLICT]
        ]

        if not actionable_decisions:
            return stats

        # Determine the effective progress callback
        # If we have a sync_progress_tracker, create per-file callbacks
        if sync_progress_tracker:
            # Execute with per-file progress tracking
            if max_workers > 1 and len(actionable_decisions) > 1:
                # Pre-create remote folders before parallel uploads
                upload_count = sum(
                    1 for d in actionable_decisions if d.action == SyncAction.UPLOAD
                )
                if upload_count > 1:
                    self._ensure_remote_folders_exist(actionable_decisions, pair)

                # Parallel execution with progress tracking
                batch_stats = self._execute_decisions_parallel_tracked(
                    actionable_decisions,
                    pair,
                    chunk_size,
                    multipart_threshold,
                    max_workers,
                    start_delay,
                    sync_progress_tracker,
                )
                stats["uploads"] = batch_stats["uploads"]
                stats["downloads"] = batch_stats["downloads"]
                stats["errors"] = batch_stats.get("errors", 0)
            else:
                # Sequential execution with progress tracking
                for decision in actionable_decisions:
                    if decision.action == SyncAction.UPLOAD and decision.local_file:
                        file_path = decision.relative_path
                        file_size = decision.local_file.size

                        # Notify file start
                        sync_progress_tracker.on_upload_file_start(file_path, file_size)

                        # Create per-file callback
                        file_callback = (
                            sync_progress_tracker.create_file_progress_callback(
                                file_path
                            )
                        )

                        try:
                            self._execute_single_decision(
                                decision,
                                pair,
                                chunk_size,
                                multipart_threshold,
                                file_callback,
                            )
                            stats["uploads"] += 1
                            sync_progress_tracker.on_upload_file_complete(file_path)
                        except Exception as e:
                            stats["errors"] = stats.get("errors", 0) + 1
                            sync_progress_tracker.on_upload_file_error(
                                file_path, str(e)
                            )
                            if not self.output.quiet:
                                self.output.error(
                                    f"Failed to sync {decision.relative_path}: {e}"
                                )
                    else:
                        # Non-upload decision
                        try:
                            self._execute_single_decision(
                                decision,
                                pair,
                                chunk_size,
                                multipart_threshold,
                                progress_callback,
                            )
                            if decision.action == SyncAction.DOWNLOAD:
                                stats["downloads"] += 1
                            elif decision.action == SyncAction.DELETE_LOCAL:
                                stats["deletes_local"] += 1
                            elif decision.action == SyncAction.DELETE_REMOTE:
                                stats["deletes_remote"] += 1
                        except Exception as e:
                            stats["errors"] = stats.get("errors", 0) + 1
                            if not self.output.quiet:
                                self.output.error(
                                    f"Failed to sync {decision.relative_path}: {e}"
                                )
        else:
            # No tracker - use original batch execution
            return self._execute_batch_decisions(
                batch_decisions=batch_decisions,
                pair=pair,
                chunk_size=chunk_size,
                multipart_threshold=multipart_threshold,
                progress_callback=progress_callback,
                max_workers=max_workers,
                start_delay=start_delay,
            )

        return stats

    def _execute_decisions_parallel_tracked(
        self,
        decisions: list[SyncDecision],
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        max_workers: int,
        start_delay: float,
        sync_progress_tracker: SyncProgressTracker,
    ) -> dict:
        """Execute sync decisions in parallel with progress tracking.

        This method handles Ctrl+C (KeyboardInterrupt) properly on Windows by using
        timeouts on future.result() calls to periodically check for interrupts.

        Args:
            decisions: List of sync decisions to execute
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation
            sync_progress_tracker: Progress tracker for detailed events

        Returns:
            Dictionary with stats of successful operations
        """
        logger.debug(f"Executing {len(decisions)} actions with {max_workers} workers")

        stats = {
            "uploads": 0,
            "downloads": 0,
            "deletes_local": 0,
            "deletes_remote": 0,
            "errors": 0,
        }

        def execute_with_tracking(
            decision: SyncDecision,
            worker_delay: float,
        ) -> tuple[str, float, bool, SyncAction]:
            """Execute a single decision with progress tracking."""
            if self.pause_controller.removed:
                return decision.relative_path, 0.0, False, decision.action

            if worker_delay > 0:
                time.sleep(worker_delay)

            is_transfer = decision.action in [SyncAction.UPLOAD, SyncAction.DOWNLOAD]
            semaphore = self.concurrency_limits.get_semaphore_for_operation(is_transfer)

            file_path = decision.relative_path
            file_size = decision.local_file.size if decision.local_file else 0

            # Notify file start for uploads
            if decision.action == SyncAction.UPLOAD:
                sync_progress_tracker.on_upload_file_start(file_path, file_size)

            # Create per-file callback for uploads
            file_callback = None
            if decision.action == SyncAction.UPLOAD:
                file_callback = sync_progress_tracker.create_file_progress_callback(
                    file_path
                )

            with semaphore:
                start = time.time()
                success = True
                try:
                    self._execute_single_decision(
                        decision,
                        pair,
                        chunk_size,
                        multipart_threshold,
                        file_callback,
                    )
                    # Notify file complete for uploads
                    if decision.action == SyncAction.UPLOAD:
                        sync_progress_tracker.on_upload_file_complete(file_path)
                except Exception as e:
                    if decision.action == SyncAction.UPLOAD:
                        sync_progress_tracker.on_upload_file_error(file_path, str(e))
                    if not self.output.quiet:
                        self.output.error(
                            f"Error syncing {decision.relative_path}: {e}"
                        )
                    success = False
                elapsed = time.time() - start
                return decision.relative_path, elapsed, success, decision.action

        executor = ThreadPoolExecutor(max_workers=max_workers)
        try:
            futures: dict[Future, SyncDecision] = {
                executor.submit(
                    execute_with_tracking, decision, i * start_delay
                ): decision
                for i, decision in enumerate(decisions)
            }

            # Process completed futures with timeout to allow Ctrl+C on Windows
            pending = set(futures.keys())
            while pending:
                # Check for cancellation
                if self.pause_controller.removed:
                    break

                # Wait for any future with timeout to allow interrupt handling
                done, pending = self._wait_for_futures_with_timeout(
                    pending, timeout=_FUTURE_RESULT_TIMEOUT
                )

                for future in done:
                    try:
                        path, elapsed, success, action = future.result(timeout=0)
                        if success:
                            if action == SyncAction.UPLOAD:
                                stats["uploads"] += 1
                            elif action == SyncAction.DOWNLOAD:
                                stats["downloads"] += 1
                            elif action == SyncAction.DELETE_LOCAL:
                                stats["deletes_local"] += 1
                            elif action == SyncAction.DELETE_REMOTE:
                                stats["deletes_remote"] += 1
                        else:
                            stats["errors"] += 1
                    except Exception as e:
                        stats["errors"] += 1
                        if not self.output.quiet:
                            self.output.error(
                                f"Unexpected error in parallel execution: {e}"
                            )

        except KeyboardInterrupt:
            logger.debug("KeyboardInterrupt received, cancelling parallel execution")
            self.pause_controller.cancel()
            raise
        finally:
            # Shutdown executor and cancel pending futures
            executor.shutdown(wait=False, cancel_futures=True)

        return stats

    def _scan_local_files_streaming(
        self, pair: SyncPair, use_progress: bool = True
    ) -> list:
        """Scan local files for streaming sync mode.

        Args:
            pair: Sync pair configuration
            use_progress: If True, show Rich progress spinner (default: True)

        Returns:
            List of local files
        """
        if not pair.sync_mode.requires_local_scan:
            return []

        scanner = DirectoryScanner(
            ignore_patterns=pair.ignore,
            exclude_dot_files=pair.exclude_dot_files,
        )

        if use_progress:
            # Use Rich progress spinner
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True,
            ) as progress:
                scan_start = time.time()
                task = progress.add_task("Scanning local directory...", total=None)
                local_files = scanner.scan_local(pair.local)
                scan_elapsed = time.time() - scan_start
                progress.update(
                    task, description=f"Found {len(local_files)} local file(s)"
                )
                logger.debug(
                    f"Local scan took {scan_elapsed:.2f}s for {len(local_files)} files"
                )
                return local_files
        else:
            # Scan without Rich progress (external display is active)
            scan_start = time.time()
            local_files = scanner.scan_local(pair.local)
            scan_elapsed = time.time() - scan_start
            logger.debug(
                f"Local scan took {scan_elapsed:.2f}s for {len(local_files)} files"
            )
            return local_files

    def _create_empty_stats(self) -> dict:
        """Create an empty statistics dictionary.

        Returns:
            Dictionary with zero counts for all stat categories
        """
        return {
            "uploads": 0,
            "downloads": 0,
            "deletes_local": 0,
            "deletes_remote": 0,
            "renames_local": 0,
            "renames_remote": 0,
            "skips": 0,
            "conflicts": 0,
            "errors": 0,
        }

    def _resolve_remote_folder_id(
        self, pair: SyncPair, manager: FileEntriesManager
    ) -> Optional[int]:
        """Resolve remote path to folder ID.

        Args:
            pair: Sync pair configuration
            manager: File entries manager

        Returns:
            Folder ID if found, None otherwise
        """
        resolve_start = time.time()
        remote_folder_id = None

        if pair.remote and pair.remote != "/":
            try:
                # Strip leading slash for folder lookup
                folder_path = pair.remote.lstrip("/")
                logger.debug("Resolving folder path: %s", folder_path)

                # Check if this is a nested path (contains /)
                if "/" in folder_path:
                    # Use the API's resolve_path_to_id for nested paths
                    try:
                        remote_folder_id = self.client.resolve_path_to_id(
                            folder_path, workspace_id=pair.workspace_id
                        )
                        logger.debug(
                            "Found remote folder '%s' via path resolution with id=%s",
                            folder_path,
                            remote_folder_id,
                        )
                    except Exception as e:
                        logger.debug(
                            "Path resolution failed for '%s': %s", folder_path, e
                        )
                else:
                    # Simple single folder name - search in root
                    folder_entry = manager.find_folder_by_name(folder_path, parent_id=0)
                    if folder_entry:
                        remote_folder_id = folder_entry.id
                        logger.debug(
                            "Found remote folder '%s' with id=%s",
                            folder_path,
                            remote_folder_id,
                        )
                    else:
                        logger.debug(
                            "Remote folder '%s' not found - will be created or "
                            "files will be uploaded to root",
                            folder_path,
                        )
            except Exception as e:
                # Remote folder doesn't exist yet
                logger.debug("Folder resolution failed: %s", e)

        resolve_elapsed = time.time() - resolve_start
        logger.debug(
            "Remote folder resolution took %.2fs (folder_id=%s)",
            resolve_elapsed,
            remote_folder_id,
        )
        return remote_folder_id

    def _process_remote_batches_streaming(
        self,
        pair: SyncPair,
        local_file_map: dict,
        seen_remote_paths: set[str],
        stats: dict,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        batch_size: int,
        max_workers: int,
        start_delay: float = 0.0,
        previous_synced_files: Optional[set[str]] = None,
        synced_files: Optional[set[str]] = None,
        synced_local_file_map: Optional[dict[str, LocalFile]] = None,
        synced_remote_file_map: Optional[dict[str, RemoteFile]] = None,
    ) -> None:
        """Process remote files in batches for streaming sync.

        Args:
            pair: Sync pair configuration
            local_file_map: Dictionary of local files by relative path
            seen_remote_paths: Set to track seen remote paths (modified in place)
            stats: Statistics dictionary (modified in place)
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates
            batch_size: Number of files per batch
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation
            previous_synced_files: Set of previously synced files (for TWO_WAY mode)
            synced_files: Set to track successfully synced files (modified in place)
            synced_local_file_map: Dict to track synced local files (in place)
            synced_remote_file_map: Dict to track synced remote files (in place)
        """
        if not self.output.quiet:
            self.output.info("Processing remote files in batches...")

        manager = FileEntriesManager(self.client, pair.workspace_id)
        scanner = DirectoryScanner(
            ignore_patterns=pair.ignore,
            exclude_dot_files=pair.exclude_dot_files,
        )

        remote_folder_id = self._resolve_remote_folder_id(pair, manager)

        # If we're syncing to a specific remote folder and it doesn't exist yet,
        # skip remote scan (no files to download/compare yet)
        if pair.remote and pair.remote != "/" and remote_folder_id is None:
            logger.debug("Remote folder not found, skipping remote scan")
            return

        # Process batches
        batch_num = 0
        total_processed = 0

        try:
            # When syncing to a specific folder, don't use it as
            # path_prefix. The files inside should be compared with
            # local files without the folder prefix.
            path_prefix = ""

            for entries_batch in manager.iter_all_recursive(
                folder_id=remote_folder_id,
                path_prefix=path_prefix,
                batch_size=batch_size,
            ):
                batch_num += 1
                (
                    batch_stats,
                    batch_synced,
                    batch_local_files,
                    batch_remote_files,
                ) = self._process_single_remote_batch(
                    entries_batch=entries_batch,
                    batch_num=batch_num,
                    scanner=scanner,
                    pair=pair,
                    local_file_map=local_file_map,
                    seen_remote_paths=seen_remote_paths,
                    chunk_size=chunk_size,
                    multipart_threshold=multipart_threshold,
                    progress_callback=progress_callback,
                    max_workers=max_workers,
                    start_delay=start_delay,
                    previous_synced_files=previous_synced_files,
                )

                # Update stats
                for key in ["uploads", "downloads", "deletes_local", "deletes_remote"]:
                    stats[key] += batch_stats.get(key, 0)
                stats["skips"] += batch_stats.get("skips", 0)
                stats["conflicts"] += batch_stats.get("conflicts", 0)

                # Track synced files
                if synced_files is not None:
                    synced_files.update(batch_synced)

                # Track synced file objects for tree building
                if synced_local_file_map is not None:
                    synced_local_file_map.update(batch_local_files)
                if synced_remote_file_map is not None:
                    synced_remote_file_map.update(batch_remote_files)

                if not self.output.quiet:
                    total_processed += batch_stats.get("processed", 0)
                    self.output.info(
                        f"Batch {batch_num}: Processing "
                        f"{batch_stats.get('processed', 0)} file(s) "
                        f"(total: {total_processed})"
                    )

        except KeyboardInterrupt:
            if not self.output.quiet:
                self.output.warning("\nSync cancelled by user")
            raise

    def _process_single_remote_batch(
        self,
        entries_batch: list,
        batch_num: int,
        scanner: DirectoryScanner,
        pair: SyncPair,
        local_file_map: dict,
        seen_remote_paths: set[str],
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float = 0.0,
        previous_synced_files: Optional[set[str]] = None,
    ) -> tuple[dict, set[str], dict[str, LocalFile], dict[str, RemoteFile]]:
        """Process a single batch of remote entries.

        Args:
            entries_batch: Batch of remote entries
            batch_num: Batch number for logging
            scanner: Directory scanner instance
            pair: Sync pair configuration
            local_file_map: Dictionary of local files by relative path
            seen_remote_paths: Set to track seen remote paths (modified in place)
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation
            previous_synced_files: Set of previously synced files (for TWO_WAY mode)

        Returns:
            Tuple of (batch stats, synced paths, synced local files, synced remotes)
        """
        logger.debug(
            "Received batch %d with %d entries",
            batch_num,
            len(entries_batch),
        )

        # Convert to RemoteFile objects
        remote_files = scanner.scan_remote(entries_batch)

        sample_paths = [f.relative_path for f in remote_files[:3]]
        logger.debug("Sample remote paths: %s", sample_paths)
        if len(entries_batch) != len(remote_files):
            filtered = len(entries_batch) - len(remote_files)
            logger.debug("Filtered out %d folders from batch", filtered)

        # Compare batch with local files
        comparator = FileComparator(pair.sync_mode, previous_synced_files)
        batch_decisions = []

        for remote_file in remote_files:
            seen_remote_paths.add(remote_file.relative_path)
            local_file = local_file_map.get(remote_file.relative_path)

            # Compare this single file
            decision = comparator._compare_single_file(
                remote_file.relative_path, local_file, remote_file
            )
            batch_decisions.append(decision)

        # Execute batch decisions
        batch_stats = self._execute_batch_decisions(
            batch_decisions=batch_decisions,
            pair=pair,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            progress_callback=progress_callback,
            max_workers=max_workers,
            start_delay=start_delay,
        )

        batch_stats["processed"] = len(remote_files)

        # Track successfully synced files and their objects
        synced_in_batch: set[str] = set()
        synced_local_in_batch: dict[str, LocalFile] = {}
        synced_remote_in_batch: dict[str, RemoteFile] = {}

        # Build remote file map for this batch
        remote_file_map = {f.relative_path: f for f in remote_files}

        for decision in batch_decisions:
            if decision.action == SyncAction.SKIP:
                # Files that were already in sync
                synced_in_batch.add(decision.relative_path)
                if decision.local_file:
                    synced_local_in_batch[decision.relative_path] = decision.local_file
                if decision.relative_path in remote_file_map:
                    synced_remote_in_batch[decision.relative_path] = remote_file_map[
                        decision.relative_path
                    ]
            elif decision.action == SyncAction.UPLOAD:
                # Files uploaded to remote (now exist in both)
                synced_in_batch.add(decision.relative_path)
                if decision.local_file:
                    synced_local_in_batch[decision.relative_path] = decision.local_file
                # Note: remote file doesn't exist yet in this batch context
            elif decision.action == SyncAction.DOWNLOAD:
                # Files downloaded to local (now exist in both)
                synced_in_batch.add(decision.relative_path)
                if decision.relative_path in remote_file_map:
                    synced_remote_in_batch[decision.relative_path] = remote_file_map[
                        decision.relative_path
                    ]
                # Note: local file exists in local_file_map if it's an update
                if decision.relative_path in local_file_map:
                    synced_local_in_batch[decision.relative_path] = local_file_map[
                        decision.relative_path
                    ]
            # DELETE_LOCAL and DELETE_REMOTE are NOT added (they no longer exist)

        # Debug: print when batch is complete
        logger.debug("Batch %d complete, waiting for next batch...", batch_num)

        return (
            batch_stats,
            synced_in_batch,
            synced_local_in_batch,
            synced_remote_in_batch,
        )

    def _execute_batch_decisions(
        self,
        batch_decisions: list[SyncDecision],
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float = 0.0,
    ) -> dict:
        """Execute a batch of sync decisions.

        Args:
            batch_decisions: List of sync decisions
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation

        Returns:
            Dictionary with batch statistics
        """
        stats = self._create_empty_stats()

        actionable_decisions = [
            d
            for d in batch_decisions
            if d.action not in [SyncAction.SKIP, SyncAction.CONFLICT]
        ]

        if max_workers > 1 and len(actionable_decisions) > 1:
            # Pre-create remote folders before parallel uploads to avoid race conditions
            upload_count = sum(
                1 for d in actionable_decisions if d.action == SyncAction.UPLOAD
            )
            if upload_count > 1:
                self._ensure_remote_folders_exist(actionable_decisions, pair)

            # Parallel execution - returns success count per action type
            batch_stats = self._execute_decisions_parallel(
                actionable_decisions,
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                max_workers,
                start_delay,
            )
            # Update stats with actual successes
            stats["uploads"] = batch_stats["uploads"]
            stats["downloads"] = batch_stats["downloads"]
            stats["deletes_local"] = batch_stats["deletes_local"]
            stats["deletes_remote"] = batch_stats["deletes_remote"]
        else:
            # Sequential execution
            for decision in actionable_decisions:
                self._execute_decision_with_stats(
                    decision=decision,
                    pair=pair,
                    chunk_size=chunk_size,
                    multipart_threshold=multipart_threshold,
                    progress_callback=progress_callback,
                    stats=stats,
                )

        # Count skips and conflicts
        for decision in batch_decisions:
            if decision.action == SyncAction.CONFLICT:
                stats["conflicts"] += 1
            elif decision.action == SyncAction.SKIP:
                stats["skips"] += 1

        return stats

    def _execute_decision_with_stats(
        self,
        decision: SyncDecision,
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        stats: dict,
    ) -> None:
        """Execute a single decision and update stats.

        Args:
            decision: Sync decision to execute
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates
            stats: Statistics dictionary (modified in place)
        """
        try:
            self._execute_single_decision(
                decision,
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
            )
            # Only increment stats on success
            if decision.action == SyncAction.UPLOAD:
                stats["uploads"] += 1
            elif decision.action == SyncAction.DOWNLOAD:
                stats["downloads"] += 1
            elif decision.action == SyncAction.DELETE_LOCAL:
                stats["deletes_local"] += 1
            elif decision.action == SyncAction.DELETE_REMOTE:
                stats["deletes_remote"] += 1
            elif decision.action == SyncAction.RENAME_LOCAL:
                stats["renames_local"] = stats.get("renames_local", 0) + 1
            elif decision.action == SyncAction.RENAME_REMOTE:
                stats["renames_remote"] = stats.get("renames_remote", 0) + 1
        except Exception as e:
            if not self.output.quiet:
                self.output.error(f"Failed to sync {decision.relative_path}: {e}")

    def _process_local_only_files_streaming(
        self,
        pair: SyncPair,
        local_files: list,
        seen_remote_paths: set[str],
        stats: dict,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float = 0.0,
        previous_synced_files: Optional[set[str]] = None,
        synced_files: Optional[set[str]] = None,
        synced_local_file_map: Optional[dict[str, LocalFile]] = None,
    ) -> None:
        """Process local-only files for streaming sync.

        Args:
            pair: Sync pair configuration
            local_files: List of local files
            seen_remote_paths: Set of remote paths already seen
            stats: Statistics dictionary (modified in place)
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart upload
            progress_callback: Optional callback for progress updates
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation
            previous_synced_files: Set of previously synced files (for TWO_WAY mode)
            synced_files: Set to track successfully synced files (modified in place)
            synced_local_file_map: Dict to track synced local files (modified in place)
        """
        local_only_files = [
            f for f in local_files if f.relative_path not in seen_remote_paths
        ]

        if not local_only_files:
            return

        if not self.output.quiet:
            self.output.info(
                f"\nProcessing {len(local_only_files)} local-only file(s)..."
            )

        comparator = FileComparator(pair.sync_mode, previous_synced_files)
        local_decisions = []

        for local_file in local_only_files:
            # Handle local-only file
            decision = comparator._compare_single_file(
                local_file.relative_path, local_file, None
            )

            # Count skips
            if decision.action == SyncAction.SKIP:
                stats["skips"] += 1

            if decision.action not in [SyncAction.SKIP, SyncAction.CONFLICT]:
                local_decisions.append(decision)

        # Execute local-only file actions - parallel if max_workers > 1
        if max_workers > 1 and len(local_decisions) > 1:
            # Pre-create remote folders before parallel uploads to avoid race conditions
            upload_count = sum(
                1 for d in local_decisions if d.action == SyncAction.UPLOAD
            )
            if upload_count > 1:
                self._ensure_remote_folders_exist(local_decisions, pair)

            local_stats = self._execute_decisions_parallel(
                local_decisions,
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                max_workers,
                start_delay,
            )
            # Update stats with actual successes
            stats["uploads"] += local_stats["uploads"]
            stats["deletes_local"] += local_stats["deletes_local"]

            # Track synced files (uploaded files)
            if synced_files is not None:
                for decision in local_decisions:
                    if decision.action == SyncAction.UPLOAD:
                        synced_files.add(decision.relative_path)
                        if synced_local_file_map is not None and decision.local_file:
                            synced_local_file_map[decision.relative_path] = (
                                decision.local_file
                            )
        else:
            for decision in local_decisions:
                self._execute_decision_with_stats(
                    decision=decision,
                    pair=pair,
                    chunk_size=chunk_size,
                    multipart_threshold=multipart_threshold,
                    progress_callback=progress_callback,
                    stats=stats,
                )
                # Track synced files (uploaded files)
                if synced_files is not None and decision.action == SyncAction.UPLOAD:
                    synced_files.add(decision.relative_path)
                    if synced_local_file_map is not None and decision.local_file:
                        synced_local_file_map[decision.relative_path] = (
                            decision.local_file
                        )

    def _scan_remote(self, pair: SyncPair) -> list[RemoteFile]:
        """Scan remote directory for files.

        Args:
            pair: Sync pair configuration

        Returns:
            List of remote files
        """
        manager = FileEntriesManager(self.client, pair.workspace_id)
        scanner = DirectoryScanner(
            ignore_patterns=pair.ignore,
            exclude_dot_files=pair.exclude_dot_files,
        )

        # Resolve remote path to folder ID
        remote_folder_id = None
        if pair.remote and pair.remote != "/":
            try:
                # Strip leading slash for folder lookup (consistent with streaming mode)
                folder_name = pair.remote.lstrip("/")
                folder_entry = manager.find_folder_by_name(folder_name)
                if folder_entry:
                    remote_folder_id = folder_entry.id
            except Exception:
                # Remote folder doesn't exist yet
                return []

        # Get all files recursively
        entries_with_paths = manager.get_all_recursive(
            folder_id=remote_folder_id,
            path_prefix="",
        )

        # Convert to RemoteFile objects
        remote_files = scanner.scan_remote(entries_with_paths)
        return remote_files

    def _categorize_decisions(self, decisions: list[SyncDecision]) -> dict:
        """Categorize decisions into statistics.

        Args:
            decisions: List of sync decisions

        Returns:
            Dictionary with statistics
        """
        stats = {
            "uploads": 0,
            "downloads": 0,
            "deletes_local": 0,
            "deletes_remote": 0,
            "renames_local": 0,
            "renames_remote": 0,
            "skips": 0,
            "conflicts": 0,
        }

        for decision in decisions:
            if decision.action == SyncAction.UPLOAD:
                stats["uploads"] += 1
            elif decision.action == SyncAction.DOWNLOAD:
                stats["downloads"] += 1
            elif decision.action == SyncAction.DELETE_LOCAL:
                stats["deletes_local"] += 1
            elif decision.action == SyncAction.DELETE_REMOTE:
                stats["deletes_remote"] += 1
            elif decision.action == SyncAction.RENAME_LOCAL:
                stats["renames_local"] += 1
            elif decision.action == SyncAction.RENAME_REMOTE:
                stats["renames_remote"] += 1
            elif decision.action == SyncAction.CONFLICT:
                stats["conflicts"] += 1
            elif decision.action == SyncAction.SKIP:
                stats["skips"] += 1

        return stats

    def _display_sync_plan(
        self,
        stats: dict,
        decisions: list[SyncDecision],
        dry_run: bool,
    ) -> None:
        """Display sync plan to user.

        Args:
            stats: Statistics dictionary
            decisions: List of sync decisions
            dry_run: Whether this is a dry run
        """
        if self.output.quiet:
            return

        self.output.info("Sync plan:")
        if stats["uploads"] > 0:
            self.output.info(f"  ↑ Upload: {stats['uploads']} file(s)")
        if stats["downloads"] > 0:
            self.output.info(f"  ↓ Download: {stats['downloads']} file(s)")
        if stats.get("renames_local", 0) > 0:
            self.output.info(f"  → Rename local: {stats['renames_local']} file(s)")
        if stats.get("renames_remote", 0) > 0:
            self.output.info(f"  → Rename remote: {stats['renames_remote']} file(s)")
        if stats["deletes_local"] > 0:
            self.output.info(f"  ✗ Delete local: {stats['deletes_local']} file(s)")
        if stats["deletes_remote"] > 0:
            self.output.info(f"  ✗ Delete remote: {stats['deletes_remote']} file(s)")
        if stats["skips"] > 0:
            self.output.info(f"  = Skip: {stats['skips']} file(s)")
        if stats["conflicts"] > 0:
            self.output.warning(f"  ⚠ Conflicts: {stats['conflicts']} file(s)")

        # Show conflicts if any
        if stats["conflicts"] > 0:
            self.output.print("")
            self.output.warning("Conflict details:")
            for decision in decisions:
                if decision.action == SyncAction.CONFLICT:
                    self.output.warning(
                        f"  {decision.relative_path}: {decision.reason}"
                    )

        self.output.print("")

    def _handle_conflicts(self, decisions: list[SyncDecision]) -> list[SyncDecision]:
        """Handle conflicts interactively.

        Args:
            decisions: List of sync decisions

        Returns:
            Updated list of decisions with conflicts resolved
        """
        # For now, skip conflicts (will implement interactive resolution later)
        updated_decisions = []
        for decision in decisions:
            if decision.action == SyncAction.CONFLICT:
                # Convert conflict to skip
                updated_decision = SyncDecision(
                    relative_path=decision.relative_path,
                    action=SyncAction.SKIP,
                    reason="Conflict - skipping",
                    local_file=decision.local_file,
                    remote_file=decision.remote_file,
                )
                updated_decisions.append(updated_decision)
            else:
                updated_decisions.append(decision)

        return updated_decisions

    def _execute_decisions(
        self,
        decisions: list[SyncDecision],
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int = 1,
        start_delay: float = 0.0,
    ) -> None:
        """Execute sync decisions in optimal order.

        Execution order (based on filen-sync best practices):
        1. Local renames/moves - do first to preserve file identity
        2. Remote renames/moves - do second to preserve file identity
        3. Local deletions - clean up before creating new files
        4. Remote deletions - clean up before uploading new files
        5. File uploads (can be parallel)
        6. File downloads (can be parallel)

        Args:
            decisions: List of sync decisions
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            progress_callback: Optional progress callback
            max_workers: Number of parallel workers (default: 1 for sequential)
            start_delay: Delay in seconds between starting each parallel operation
        """
        actionable = [
            d
            for d in decisions
            if d.action not in [SyncAction.SKIP, SyncAction.CONFLICT]
        ]

        if not actionable:
            return

        # Sort decisions by execution order priority
        # Order: renames (local then remote) -> deletes -> uploads -> downloads
        def get_action_priority(decision: SyncDecision) -> int:
            priorities = {
                SyncAction.RENAME_LOCAL: 0,  # Local renames first
                SyncAction.RENAME_REMOTE: 1,  # Remote renames second
                SyncAction.DELETE_LOCAL: 2,  # Local deletes third
                SyncAction.DELETE_REMOTE: 3,  # Remote deletes fourth
                SyncAction.UPLOAD: 4,  # Uploads fifth
                SyncAction.DOWNLOAD: 5,  # Downloads sixth
            }
            return priorities.get(decision.action, 99)

        ordered_decisions = sorted(actionable, key=get_action_priority)

        # Group decisions by action type for execution
        rename_local = [
            d for d in ordered_decisions if d.action == SyncAction.RENAME_LOCAL
        ]
        rename_remote = [
            d for d in ordered_decisions if d.action == SyncAction.RENAME_REMOTE
        ]
        delete_local = [
            d for d in ordered_decisions if d.action == SyncAction.DELETE_LOCAL
        ]
        delete_remote = [
            d for d in ordered_decisions if d.action == SyncAction.DELETE_REMOTE
        ]
        uploads = [d for d in ordered_decisions if d.action == SyncAction.UPLOAD]
        downloads = [d for d in ordered_decisions if d.action == SyncAction.DOWNLOAD]

        # Pre-create remote folders before parallel uploads to avoid race conditions
        # when multiple uploads try to create the same parent folder simultaneously
        if max_workers > 1 and len(uploads) > 1:
            self._ensure_remote_folders_exist(uploads, pair)

        # Execute in order (sequential for renames and deletes, parallel for transfers)
        if self.output.quiet:
            self._execute_ordered_quiet(
                rename_local,
                rename_remote,
                delete_local,
                delete_remote,
                uploads,
                downloads,
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                max_workers,
                start_delay,
            )
        else:
            self._execute_ordered_with_progress(
                rename_local,
                rename_remote,
                delete_local,
                delete_remote,
                uploads,
                downloads,
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                max_workers,
                start_delay,
            )

    def _execute_ordered_quiet(
        self,
        rename_local: list[SyncDecision],
        rename_remote: list[SyncDecision],
        delete_local: list[SyncDecision],
        delete_remote: list[SyncDecision],
        uploads: list[SyncDecision],
        downloads: list[SyncDecision],
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float,
    ) -> None:
        """Execute decisions in order without progress display.

        Args:
            rename_local: Local rename decisions
            rename_remote: Remote rename decisions
            delete_local: Local delete decisions
            delete_remote: Remote delete decisions
            uploads: Upload decisions
            downloads: Download decisions
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            progress_callback: Optional progress callback
            max_workers: Number of parallel workers
            start_delay: Delay between parallel operations
        """
        # 1. Execute local renames (sequential)
        for decision in rename_local:
            try:
                self._execute_single_decision(
                    decision, pair, chunk_size, multipart_threshold, None
                )
            except Exception as e:
                if not self.output.quiet:
                    self.output.error(f"Failed to sync {decision.relative_path}: {e}")

        # 2. Execute remote renames (sequential)
        for decision in rename_remote:
            try:
                self._execute_single_decision(
                    decision, pair, chunk_size, multipart_threshold, None
                )
            except Exception as e:
                if not self.output.quiet:
                    self.output.error(f"Failed to sync {decision.relative_path}: {e}")

        # 3. Execute local deletes (sequential)
        for decision in delete_local:
            try:
                self._execute_single_decision(
                    decision, pair, chunk_size, multipart_threshold, None
                )
            except Exception as e:
                if not self.output.quiet:
                    self.output.error(f"Failed to sync {decision.relative_path}: {e}")

        # 4. Execute remote deletes (sequential)
        for decision in delete_remote:
            try:
                self._execute_single_decision(
                    decision, pair, chunk_size, multipart_threshold, None
                )
            except Exception as e:
                if not self.output.quiet:
                    self.output.error(f"Failed to sync {decision.relative_path}: {e}")

        # 5. Execute uploads (can be parallel)
        if max_workers > 1 and len(uploads) > 1:
            self._execute_decisions_parallel(
                uploads,
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                max_workers,
                start_delay,
            )
        else:
            for decision in uploads:
                try:
                    self._execute_single_decision(
                        decision,
                        pair,
                        chunk_size,
                        multipart_threshold,
                        progress_callback,
                    )
                except Exception as e:
                    if not self.output.quiet:
                        self.output.error(
                            f"Failed to sync {decision.relative_path}: {e}"
                        )

        # 6. Execute downloads (can be parallel)
        if max_workers > 1 and len(downloads) > 1:
            self._execute_decisions_parallel(
                downloads,
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                max_workers,
                start_delay,
            )
        else:
            for decision in downloads:
                try:
                    self._execute_single_decision(
                        decision,
                        pair,
                        chunk_size,
                        multipart_threshold,
                        progress_callback,
                    )
                except Exception as e:
                    if not self.output.quiet:
                        self.output.error(
                            f"Failed to sync {decision.relative_path}: {e}"
                        )

    def _execute_ordered_with_progress(
        self,
        rename_local: list[SyncDecision],
        rename_remote: list[SyncDecision],
        delete_local: list[SyncDecision],
        delete_remote: list[SyncDecision],
        uploads: list[SyncDecision],
        downloads: list[SyncDecision],
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float,
    ) -> None:
        """Execute decisions in order with progress display.

        Args:
            rename_local: Local rename decisions
            rename_remote: Remote rename decisions
            delete_local: Local delete decisions
            delete_remote: Remote delete decisions
            uploads: Upload decisions
            downloads: Download decisions
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            progress_callback: Optional progress callback
            max_workers: Number of parallel workers
            start_delay: Delay between parallel operations
        """
        total = (
            len(rename_local)
            + len(rename_remote)
            + len(delete_local)
            + len(delete_remote)
            + len(uploads)
            + len(downloads)
        )

        with Progress() as progress:
            task = progress.add_task("Syncing files...", total=total)

            # 1. Execute local renames (sequential)
            for decision in rename_local:
                try:
                    self._execute_single_decision(
                        decision, pair, chunk_size, multipart_threshold, None
                    )
                except Exception as e:
                    if not self.output.quiet:
                        self.output.error(
                            f"Failed to sync {decision.relative_path}: {e}"
                        )
                progress.update(task, advance=1)

            # 2. Execute remote renames (sequential)
            for decision in rename_remote:
                try:
                    self._execute_single_decision(
                        decision, pair, chunk_size, multipart_threshold, None
                    )
                except Exception as e:
                    if not self.output.quiet:
                        self.output.error(
                            f"Failed to sync {decision.relative_path}: {e}"
                        )
                progress.update(task, advance=1)

            # 3. Execute local deletes (sequential)
            for decision in delete_local:
                try:
                    self._execute_single_decision(
                        decision, pair, chunk_size, multipart_threshold, None
                    )
                except Exception as e:
                    if not self.output.quiet:
                        self.output.error(
                            f"Failed to sync {decision.relative_path}: {e}"
                        )
                progress.update(task, advance=1)

            # 4. Execute remote deletes (sequential)
            for decision in delete_remote:
                try:
                    self._execute_single_decision(
                        decision, pair, chunk_size, multipart_threshold, None
                    )
                except Exception as e:
                    if not self.output.quiet:
                        self.output.error(
                            f"Failed to sync {decision.relative_path}: {e}"
                        )
                progress.update(task, advance=1)

            # 5. Execute uploads (can be parallel)
            if max_workers > 1 and len(uploads) > 1:
                self._execute_decisions_parallel(
                    uploads,
                    pair,
                    chunk_size,
                    multipart_threshold,
                    progress_callback,
                    max_workers,
                    start_delay,
                )
                progress.update(task, advance=len(uploads))
            else:
                for decision in uploads:
                    try:
                        self._execute_single_decision(
                            decision,
                            pair,
                            chunk_size,
                            multipart_threshold,
                            progress_callback,
                        )
                    except Exception as e:
                        if not self.output.quiet:
                            self.output.error(
                                f"Failed to sync {decision.relative_path}: {e}"
                            )
                    progress.update(task, advance=1)

            # 6. Execute downloads (can be parallel)
            if max_workers > 1 and len(downloads) > 1:
                self._execute_decisions_parallel(
                    downloads,
                    pair,
                    chunk_size,
                    multipart_threshold,
                    progress_callback,
                    max_workers,
                    start_delay,
                )
                progress.update(task, advance=len(downloads))
            else:
                for decision in downloads:
                    try:
                        self._execute_single_decision(
                            decision,
                            pair,
                            chunk_size,
                            multipart_threshold,
                            progress_callback,
                        )
                    except Exception as e:
                        if not self.output.quiet:
                            self.output.error(
                                f"Failed to sync {decision.relative_path}: {e}"
                            )
                    progress.update(task, advance=1)

    def _execute_upload(
        self,
        decision: SyncDecision,
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
    ) -> None:
        """Execute an upload operation with race condition handling."""
        local_file = decision.local_file
        if not local_file:
            return

        # Check if local file still exists (race condition handling)
        if not local_file.path.exists():
            logger.debug(
                f"Local file {decision.relative_path} no longer exists, skipping upload"
            )
            return

        logger.debug(f"Uploading {decision.relative_path}...")
        action_start = time.time()

        # Construct full remote path including the remote folder
        if pair.remote:
            full_remote_path = f"{pair.remote}/{decision.relative_path}"
        else:
            full_remote_path = decision.relative_path

        self.operations.upload_file(
            local_file=local_file,
            remote_path=full_remote_path,
            workspace_id=pair.workspace_id,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            progress_callback=progress_callback,
        )
        action_elapsed = time.time() - action_start
        logger.debug(f"Upload of {decision.relative_path} took {action_elapsed:.2f}s")

    def _execute_download(
        self,
        decision: SyncDecision,
        pair: SyncPair,
        progress_callback: Optional[Callable[[int, int], None]],
    ) -> None:
        """Execute a download operation with retry logic."""
        remote_file = decision.remote_file
        if not remote_file:
            return

        logger.debug(f"Downloading {decision.relative_path}...")
        action_start = time.time()
        local_path = pair.local / decision.relative_path

        # Retry download for transient errors
        max_retries = DEFAULT_MAX_RETRIES
        retry_delay = DEFAULT_RETRY_DELAY

        for attempt in range(max_retries):
            # Check for pause/cancel between retry attempts
            if not self.pause_controller.wait_if_paused():
                logger.debug(
                    f"Sync cancelled during download of {decision.relative_path}"
                )
                return

            try:
                self.operations.download_file(
                    remote_file=remote_file,
                    local_path=local_path,
                    progress_callback=progress_callback,
                )
                break  # Success, exit retry loop
            except Exception as e:
                error_str = str(e)
                is_retryable = any(
                    code in error_str for code in ["429", "500", "502", "503", "504"]
                )
                if is_retryable and attempt < max_retries - 1:
                    logger.debug(
                        f"Download failed (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {retry_delay:.1f}s: {e}"
                    )
                    if not self.output.quiet:
                        self.output.warning(
                            f"Retrying {decision.relative_path} "
                            f"({attempt + 1}/{max_retries})..."
                        )
                    time.sleep(retry_delay)
                    retry_delay *= 2.0  # Exponential backoff
                else:
                    raise

        action_elapsed = time.time() - action_start
        logger.debug(
            "Download of %s took %.2fs", decision.relative_path, action_elapsed
        )

    def _execute_delete_local(self, decision: SyncDecision, pair: SyncPair) -> None:
        """Execute a local delete operation with race condition handling."""
        local_file = decision.local_file
        if not local_file:
            return

        # Race condition handling: check if file still exists
        if not local_file.path.exists():
            logger.debug(
                f"Local file {decision.relative_path} already deleted, skipping"
            )
            return

        try:
            self.operations.delete_local(
                local_file=local_file,
                use_trash=pair.use_local_trash,
                sync_root=pair.local,
            )
        except FileNotFoundError:
            logger.debug(
                f"Local file {decision.relative_path} was deleted during "
                "operation, continuing"
            )

    def _execute_delete_remote(self, decision: SyncDecision) -> None:
        """Execute a remote delete operation with race condition handling."""
        remote_file = decision.remote_file
        if not remote_file:
            return

        try:
            self.operations.delete_remote(remote_file=remote_file, permanent=False)
        except Exception as e:
            # Race condition handling: ignore if file doesn't exist
            error_str = str(e).lower()
            if "not found" in error_str or "404" in str(e):
                logger.debug(f"Remote file {decision.relative_path} already deleted")
            else:
                raise

    def _execute_rename_local(self, decision: SyncDecision, pair: SyncPair) -> None:
        """Execute a local rename operation with race condition handling."""
        local_file = decision.local_file
        if not local_file or not decision.new_path:
            return

        # Race condition handling: check if source still exists
        if not local_file.path.exists():
            logger.debug(
                f"Local file {decision.old_path} no longer exists, skipping rename"
            )
            return

        logger.debug(f"Renaming local {decision.old_path} -> {decision.new_path}")
        action_start = time.time()

        try:
            self.operations.rename_local(
                local_file=local_file,
                new_relative_path=decision.new_path,
                sync_root=pair.local,
            )
            action_elapsed = time.time() - action_start
            logger.debug(f"Local rename took {action_elapsed:.2f}s")
        except FileNotFoundError:
            logger.debug(f"Local file {decision.old_path} was deleted during rename")

    def _execute_rename_remote(self, decision: SyncDecision) -> None:
        """Execute a remote rename operation with race condition handling."""
        remote_file = decision.remote_file
        if not remote_file or not decision.new_path:
            return

        logger.debug(f"Renaming remote {decision.old_path} -> {decision.new_path}")
        action_start = time.time()

        try:
            # Extract just the filename from the new path
            new_name = decision.new_path.rsplit("/", 1)[-1]
            self.operations.rename_remote(
                remote_file=remote_file,
                new_name=new_name,
                new_parent_id=None,  # Same folder for now
            )
            action_elapsed = time.time() - action_start
            logger.debug(f"Remote rename took {action_elapsed:.2f}s")
        except Exception as e:
            # Race condition handling: ignore if file doesn't exist
            error_str = str(e).lower()
            if "not found" in error_str or "404" in str(e):
                logger.debug(f"Remote file {decision.old_path} no longer exists")
            else:
                raise

    def _execute_single_decision(
        self,
        decision: SyncDecision,
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
    ) -> None:
        """Execute a single sync decision.

        Dispatches to appropriate handler method based on action type.
        Includes pause checkpoint before execution.

        Args:
            decision: Sync decision to execute
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            progress_callback: Optional progress callback

        Raises:
            Exception: If the operation fails and file still exists
        """
        # Check for pause/cancel before each operation
        if not self.pause_controller.wait_if_paused():
            logger.debug(f"Sync cancelled, skipping {decision.relative_path}")
            return

        try:
            if decision.action == SyncAction.UPLOAD:
                self._execute_upload(
                    decision, pair, chunk_size, multipart_threshold, progress_callback
                )
            elif decision.action == SyncAction.DOWNLOAD:
                self._execute_download(decision, pair, progress_callback)
            elif decision.action == SyncAction.DELETE_LOCAL:
                self._execute_delete_local(decision, pair)
            elif decision.action == SyncAction.DELETE_REMOTE:
                self._execute_delete_remote(decision)
            elif decision.action == SyncAction.RENAME_LOCAL:
                self._execute_rename_local(decision, pair)
            elif decision.action == SyncAction.RENAME_REMOTE:
                self._execute_rename_remote(decision)

        except Exception:
            # Re-raise for caller to handle error logging and tracking
            # Callers are responsible for logging errors to avoid duplication
            raise

    def _ensure_remote_folders_exist(
        self,
        decisions: list[SyncDecision],
        pair: SyncPair,
    ) -> None:
        """Pre-create remote folders before parallel uploads.

        This prevents race conditions when multiple parallel uploads try to
        create the same parent folder simultaneously, which can result in
        only one file being properly associated with the folder.

        Args:
            decisions: List of sync decisions (only UPLOAD actions are considered)
            pair: Sync pair configuration
        """
        # Collect unique parent folder paths from upload decisions
        upload_decisions = [d for d in decisions if d.action == SyncAction.UPLOAD]
        if not upload_decisions:
            return

        # Build set of unique folder paths that need to exist
        folders_to_create: set[str] = set()
        for decision in upload_decisions:
            # Construct full remote path
            if pair.remote:
                full_path = f"{pair.remote}/{decision.relative_path}"
            else:
                full_path = decision.relative_path

            # Get parent folder path (everything except the filename)
            parent_path = "/".join(full_path.split("/")[:-1])
            if parent_path:
                folders_to_create.add(parent_path)

        if not folders_to_create:
            return

        logger.debug(
            f"Pre-creating {len(folders_to_create)} remote folder(s) "
            f"before parallel uploads"
        )

        # Sort folders by depth to create parents before children
        sorted_folders = sorted(folders_to_create, key=lambda p: p.count("/"))

        for folder_path in sorted_folders:
            # Get the folder name (last component)
            folder_name = folder_path.split("/")[-1]
            # Get parent path
            parent_path = "/".join(folder_path.split("/")[:-1])

            try:
                # Find parent folder ID if parent exists
                parent_id = None
                if parent_path:
                    manager = FileEntriesManager(self.client, pair.workspace_id)
                    parent_entry = manager.find_folder_by_name(
                        parent_path.lstrip("/").split("/")[-1]
                    )
                    if parent_entry:
                        parent_id = parent_entry.id

                # Create the folder
                result = self.client.create_folder(
                    name=folder_name, parent_id=parent_id
                )
                if result.get("status") == "success":
                    logger.debug(f"Created folder: {folder_path}")
                else:
                    logger.debug(
                        f"Folder creation returned: {result.get('status')} "
                        f"for {folder_path}"
                    )
            except Exception as e:
                # Folder might already exist or other error
                # Continue anyway - upload will handle any issues
                logger.debug(f"Could not pre-create folder {folder_path}: {e}")

    def _execute_decisions_parallel(
        self,
        decisions: list[SyncDecision],
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int,
        start_delay: float = 0.0,
    ) -> dict:
        """Execute sync decisions in parallel using ThreadPoolExecutor.

        Uses semaphore-based concurrency control following filen-sync's pattern:
        - Transfers (uploads/downloads) use a separate semaphore (default: 10)
        - Normal operations use a separate semaphore (default: 20)

        This method handles Ctrl+C (KeyboardInterrupt) properly on Windows by using
        timeouts on future.result() calls to periodically check for interrupts.

        Args:
            decisions: List of sync decisions to execute
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            progress_callback: Optional progress callback
            max_workers: Number of parallel workers
            start_delay: Delay in seconds between starting each parallel operation

        Returns:
            Dictionary with stats of successful operations
        """
        logger.debug(f"Executing {len(decisions)} actions with {max_workers} workers")
        if start_delay > 0:
            logger.debug(
                f"Using staggered start delay of {start_delay}s between workers"
            )

        # Track stats for successful operations
        stats = {
            "uploads": 0,
            "downloads": 0,
            "deletes_local": 0,
            "deletes_remote": 0,
            "renames_local": 0,
            "renames_remote": 0,
        }

        def execute_with_semaphore(
            decision: SyncDecision,
            worker_delay: float,
        ) -> tuple[str, float, bool, SyncAction]:
            """Execute a single decision with semaphore control."""
            # Check for cancellation before starting
            if self.pause_controller.removed:
                return decision.relative_path, 0.0, False, decision.action

            # Apply staggered start delay
            if worker_delay > 0:
                time.sleep(worker_delay)

            # Determine if this is a transfer operation
            is_transfer = decision.action in [SyncAction.UPLOAD, SyncAction.DOWNLOAD]
            semaphore = self.concurrency_limits.get_semaphore_for_operation(is_transfer)

            # Acquire semaphore before execution
            with semaphore:
                start = time.time()
                success = True
                try:
                    self._execute_single_decision(
                        decision,
                        pair,
                        chunk_size,
                        multipart_threshold,
                        progress_callback,
                    )
                except Exception as e:
                    if not self.output.quiet:
                        self.output.error(
                            f"Error syncing {decision.relative_path}: {e}"
                        )
                    success = False
                elapsed = time.time() - start
                return decision.relative_path, elapsed, success, decision.action

        # Execute in parallel with staggered start delays and semaphore control
        executor = ThreadPoolExecutor(max_workers=max_workers)
        try:
            futures: dict[Future, SyncDecision] = {
                executor.submit(
                    execute_with_semaphore, decision, i * start_delay
                ): decision
                for i, decision in enumerate(decisions)
            }

            # Process completed futures with timeout to allow Ctrl+C on Windows
            pending = set(futures.keys())
            while pending:
                # Check for cancellation
                if self.pause_controller.removed:
                    logger.debug("Sync cancelled, stopping parallel execution")
                    break

                # Wait for any future with timeout to allow interrupt handling
                done, pending = self._wait_for_futures_with_timeout(
                    pending, timeout=_FUTURE_RESULT_TIMEOUT
                )

                for future in done:
                    try:
                        path, elapsed, success, action = future.result(timeout=0)
                        if success:
                            # Update stats for successful operations
                            if action == SyncAction.UPLOAD:
                                stats["uploads"] += 1
                            elif action == SyncAction.DOWNLOAD:
                                stats["downloads"] += 1
                            elif action == SyncAction.DELETE_LOCAL:
                                stats["deletes_local"] += 1
                            elif action == SyncAction.DELETE_REMOTE:
                                stats["deletes_remote"] += 1
                            elif action == SyncAction.RENAME_LOCAL:
                                stats["renames_local"] += 1
                            elif action == SyncAction.RENAME_REMOTE:
                                stats["renames_remote"] += 1

                            logger.debug(f"Completed {path} in {elapsed:.2f}s")
                        else:
                            logger.debug(f"Failed {path} in {elapsed:.2f}s")
                    except Exception as e:
                        if not self.output.quiet:
                            self.output.error(
                                f"Unexpected error in parallel execution: {e}"
                            )

        except KeyboardInterrupt:
            logger.debug("KeyboardInterrupt received, cancelling parallel execution")
            self.pause_controller.cancel()
            raise
        finally:
            # Shutdown executor and cancel pending futures
            executor.shutdown(wait=False, cancel_futures=True)

        return stats

    def _wait_for_futures_with_timeout(
        self,
        futures: set[Future],
        timeout: float,
    ) -> tuple[set[Future], set[Future]]:
        """Wait for futures with a timeout to allow interrupt handling.

        This is a workaround for Python's ThreadPoolExecutor not handling
        KeyboardInterrupt properly on Windows. By using a timeout, we can
        periodically check for interrupts and handle them gracefully.

        Args:
            futures: Set of futures to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            Tuple of (done futures, pending futures)
        """
        import concurrent.futures

        done, pending = concurrent.futures.wait(
            futures,
            timeout=timeout,
            return_when=concurrent.futures.FIRST_COMPLETED,
        )
        return done, pending

    def _execute_upload_decisions(
        self,
        decisions: list[SyncDecision],
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
        max_workers: int = 1,
        start_delay: float = 0.0,
    ) -> dict:
        """Execute upload decisions with parallel or sequential processing.

        Args:
            decisions: List of upload decisions to execute
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            progress_callback: Optional progress callback
            max_workers: Number of parallel workers (1 = sequential)
            start_delay: Delay between starting parallel workers

        Returns:
            Dictionary with 'uploads' and 'errors' counts
        """
        stats = {"uploads": 0, "errors": 0}

        if max_workers > 1 and len(decisions) > 1:
            # Pre-create remote folders before parallel uploads to avoid race conditions
            self._ensure_remote_folders_exist(decisions, pair)

            # Parallel execution
            upload_stats = self._execute_decisions_parallel(
                decisions,
                pair,
                chunk_size=chunk_size,
                multipart_threshold=multipart_threshold,
                progress_callback=progress_callback,
                max_workers=max_workers,
                start_delay=start_delay,
            )
            stats["uploads"] = upload_stats["uploads"]
            stats["errors"] = len(decisions) - upload_stats["uploads"]
        elif not self.output.quiet:
            # Sequential with progress bar
            with Progress() as progress:
                task = progress.add_task("Uploading files...", total=len(decisions))
                for decision in decisions:
                    try:
                        self._execute_single_decision(
                            decision,
                            pair,
                            chunk_size,
                            multipart_threshold,
                            progress_callback,
                        )
                        stats["uploads"] += 1
                    except Exception as e:
                        stats["errors"] += 1
                        self.output.error(
                            f"Failed to upload {decision.relative_path}: {e}"
                        )
                    progress.update(task, advance=1)
        else:
            # Sequential without progress bar (quiet mode)
            for decision in decisions:
                try:
                    self._execute_single_decision(
                        decision,
                        pair,
                        chunk_size,
                        multipart_threshold,
                        progress_callback,
                    )
                    stats["uploads"] += 1
                except Exception:
                    stats["errors"] += 1

        return stats

    def _execute_download_decisions(
        self,
        decisions: list[SyncDecision],
        pair: SyncPair,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        max_workers: int = 1,
    ) -> dict:
        """Execute download decisions with parallel or sequential processing.

        Args:
            decisions: List of download decisions to execute
            pair: Sync pair configuration
            progress_callback: Optional progress callback
            max_workers: Number of parallel workers (1 = sequential)

        Returns:
            Dictionary with 'downloads' and 'errors' counts
        """
        stats = {"downloads": 0, "errors": 0}
        # Use default constants for chunk sizes
        chunk_size = DEFAULT_CHUNK_SIZE
        multipart_threshold = DEFAULT_MULTIPART_THRESHOLD

        if max_workers > 1 and len(decisions) > 1:
            # Parallel execution
            download_stats = self._execute_decisions_parallel(
                decisions,
                pair,
                chunk_size=chunk_size,
                multipart_threshold=multipart_threshold,
                progress_callback=progress_callback,
                max_workers=max_workers,
                start_delay=0.0,
            )
            stats["downloads"] = download_stats["downloads"]
            stats["errors"] = len(decisions) - download_stats["downloads"]
        elif not self.output.quiet:
            # Sequential with progress bar
            with Progress() as progress:
                task = progress.add_task("Downloading files...", total=len(decisions))
                for decision in decisions:
                    try:
                        self._execute_single_decision(
                            decision,
                            pair,
                            chunk_size,
                            multipart_threshold,
                            progress_callback,
                        )
                        stats["downloads"] += 1
                    except Exception as e:
                        stats["errors"] += 1
                        self.output.error(
                            f"Failed to download {decision.relative_path}: {e}"
                        )
                    progress.update(task, advance=1)
        else:
            # Sequential without progress bar (quiet mode)
            for decision in decisions:
                try:
                    self._execute_single_decision(
                        decision,
                        pair,
                        chunk_size,
                        multipart_threshold,
                        progress_callback,
                    )
                    stats["downloads"] += 1
                except Exception:
                    stats["errors"] += 1

        return stats

    def _display_summary(self, stats: dict, dry_run: bool) -> None:
        """Display sync summary.

        Args:
            stats: Statistics dictionary
            dry_run: Whether this was a dry run
        """
        self.output.print("")
        if dry_run:
            self.output.success("Dry run complete!")
        else:
            self.output.success("Sync complete!")

        # Show statistics
        total_actions = (
            stats["uploads"]
            + stats["downloads"]
            + stats["deletes_local"]
            + stats["deletes_remote"]
            + stats.get("renames_local", 0)
            + stats.get("renames_remote", 0)
        )
        errors = stats.get("errors", 0)

        if total_actions > 0 or errors > 0:
            self.output.info(f"Total actions: {total_actions}")
            if stats["uploads"] > 0:
                self.output.info(f"  Uploaded: {stats['uploads']}")
            if stats["downloads"] > 0:
                self.output.info(f"  Downloaded: {stats['downloads']}")
            if stats.get("renames_local", 0) > 0:
                self.output.info(f"  Renamed locally: {stats['renames_local']}")
            if stats.get("renames_remote", 0) > 0:
                self.output.info(f"  Renamed remotely: {stats['renames_remote']}")
            if stats["deletes_local"] > 0:
                self.output.info(f"  Deleted locally: {stats['deletes_local']}")
            if stats["deletes_remote"] > 0:
                self.output.info(f"  Deleted remotely: {stats['deletes_remote']}")
            if stats.get("skips", 0) > 0:
                self.output.info(f"  Already synced: {stats['skips']}")
            if errors > 0:
                self.output.error(f"  Failed: {errors}")
        else:
            skips = stats.get("skips", 0)
            if skips > 0:
                self.output.info(
                    f"No changes needed - {skips} file(s) already in sync!"
                )
            else:
                self.output.info("No changes needed - everything is in sync!")

    def download_folder(
        self,
        remote_entry: FileEntry,
        local_path: Path,
        workspace_id: int = 0,
        overwrite: bool = True,
        max_workers: int = 1,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> dict:
        """Download a folder and its contents using sync infrastructure.

        This method provides a sync-based alternative to recursive folder downloads,
        reusing the tested SyncEngine infrastructure for better reliability.

        Args:
            remote_entry: The remote folder entry to download
            local_path: Local directory path to download into
            workspace_id: Workspace ID (0 for personal workspace)
            overwrite: If True, overwrite existing local files (CLOUD_BACKUP mode).
                      If False, delete local files not in cloud (CLOUD_TO_LOCAL mode).
            max_workers: Number of parallel download workers
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with download statistics:
            - downloads: Number of files downloaded
            - skips: Number of files skipped
            - errors: Number of failed downloads

        Examples:
            >>> engine = SyncEngine(client)
            >>> entry = client.get_folder_entry(folder_id)
            >>> stats = engine.download_folder(
            ...     entry,
            ...     Path("/local/dest"),
            ...     overwrite=True
            ... )
            >>> print(f"Downloaded {stats['downloads']} files")
        """
        # Validate local path
        if local_path.exists() and local_path.is_file():
            raise ValueError(
                f"Cannot download to {local_path}: a file with this name exists"
            )

        # Create local directory if it doesn't exist
        local_path.mkdir(parents=True, exist_ok=True)

        # Choose sync mode based on overwrite flag
        # CLOUD_BACKUP: Download only, never delete local files
        # CLOUD_TO_LOCAL: Mirror cloud to local (deletes local files not in cloud)
        sync_mode = SyncMode.CLOUD_BACKUP if overwrite else SyncMode.CLOUD_TO_LOCAL

        if not self.output.quiet:
            mode_desc = "overwrite" if overwrite else "mirror (will delete local-only)"
            self.output.info(f"Downloading folder: {remote_entry.name}")
            self.output.info(f"Mode: {mode_desc}")
            self.output.info(f"Destination: {local_path}")
            self.output.print("")

        # Get all remote files recursively from the folder
        manager = FileEntriesManager(self.client, workspace_id)
        scanner = DirectoryScanner()

        # Scan remote folder
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Scanning remote folder...", total=None)

            # Get all files in the remote folder recursively
            entries_with_paths = manager.get_all_recursive(
                folder_id=remote_entry.id,
                path_prefix="",
            )

            progress.update(
                task, description=f"Found {len(entries_with_paths)} remote file(s)"
            )

        # Convert to RemoteFile objects (filters out folders)
        remote_files = scanner.scan_remote(entries_with_paths)

        if not remote_files:
            if not self.output.quiet:
                self.output.info("No files found in remote folder")
            return {"downloads": 0, "skips": 0, "errors": 0}

        # Build remote file map
        remote_file_map = {f.relative_path: f for f in remote_files}

        # Scan local files if needed for comparison
        local_file_map: dict[str, LocalFile] = {}
        if sync_mode.requires_local_scan:
            local_files = scanner.scan_local(local_path)
            local_file_map = {f.relative_path: f for f in local_files}

        # Compare and create decisions
        comparator = FileComparator(sync_mode)
        decisions = comparator.compare_files(local_file_map, remote_file_map)

        # Filter to only download actions
        download_decisions = [d for d in decisions if d.action == SyncAction.DOWNLOAD]
        skip_decisions = [d for d in decisions if d.action == SyncAction.SKIP]

        if not self.output.quiet:
            self.output.info(f"Files to download: {len(download_decisions)}")
            if skip_decisions:
                self.output.info(
                    f"Files to skip (already exist): {len(skip_decisions)}"
                )
            self.output.print("")

        # Execute downloads
        stats = {"downloads": 0, "skips": len(skip_decisions), "errors": 0}

        if not download_decisions:
            if not self.output.quiet:
                self.output.info("All files already exist locally")
            return stats

        # Create a temporary pair for execution
        temp_pair = SyncPair(
            local=local_path,
            remote="",  # Not used for download-only
            sync_mode=sync_mode,
            workspace_id=workspace_id,
        )

        # Execute downloads using the same pattern as upload_folder
        download_stats = self._execute_download_decisions(
            download_decisions,
            temp_pair,
            progress_callback=progress_callback,
            max_workers=max_workers,
        )
        stats["downloads"] = download_stats["downloads"]
        stats["errors"] = download_stats["errors"]

        # Display summary
        if not self.output.quiet:
            self.output.print("")
            self.output.success("Download complete!")
            self.output.info(f"  Downloaded: {stats['downloads']} file(s)")
            if stats["skips"] > 0:
                self.output.info(f"  Skipped: {stats['skips']} file(s)")
            if stats["errors"] > 0:
                self.output.warning(f"  Failed: {stats['errors']} file(s)")

        return stats

    def upload_folder(
        self,
        local_path: Path,
        remote_path: str,
        workspace_id: int = 0,
        parent_id: Optional[int] = None,
        max_workers: int = 1,
        start_delay: float = 0.0,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD,
        files_to_skip: Optional[set[str]] = None,
        file_renames: Optional[dict[str, str]] = None,
    ) -> dict:
        """Upload a local folder using sync infrastructure.

        This method provides a sync-based approach to folder uploads,
        reusing the tested SyncEngine infrastructure for better reliability.

        Args:
            local_path: Local directory path to upload
            remote_path: Remote path prefix for uploads (e.g., "folder/subfolder")
            workspace_id: Workspace ID (0 for personal workspace)
            parent_id: Optional parent folder ID in remote storage
            max_workers: Number of parallel upload workers
            start_delay: Delay in seconds between starting each parallel upload
            progress_callback: Optional callback for progress updates
            chunk_size: Chunk size for multipart uploads (bytes)
            multipart_threshold: Threshold for using multipart upload (bytes)
            files_to_skip: Set of relative paths to skip (for duplicate handling)
            file_renames: Dict mapping original paths to renamed paths

        Returns:
            Dictionary with upload statistics:
            - uploads: Number of files uploaded
            - skips: Number of files skipped
            - errors: Number of failed uploads

        Examples:
            >>> engine = SyncEngine(client)
            >>> stats = engine.upload_folder(
            ...     Path("/local/folder"),
            ...     "remote_folder",
            ...     max_workers=4
            ... )
            >>> print(f"Uploaded {stats['uploads']} files")
        """
        # Validate local path
        if not local_path.exists():
            raise ValueError(f"Local path does not exist: {local_path}")
        if not local_path.is_dir():
            raise ValueError(f"Local path is not a directory: {local_path}")

        files_to_skip = files_to_skip or set()
        file_renames = file_renames or {}

        if not self.output.quiet:
            self.output.info(f"Uploading folder: {local_path}")
            self.output.info(f"Remote path: {remote_path}")
            if max_workers > 1:
                self.output.info(f"Parallel workers: {max_workers}")
            self.output.print("")

        # Scan local files
        scanner = DirectoryScanner()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Scanning local directory...", total=None)
            local_files = scanner.scan_local(local_path)
            progress.update(task, description=f"Found {len(local_files)} local file(s)")

        if not local_files:
            if not self.output.quiet:
                self.output.info("No files found in local folder")
            return {"uploads": 0, "skips": 0, "errors": 0}

        # Filter skipped files and apply renames
        upload_decisions: list[SyncDecision] = []
        skipped_count = 0

        for local_file in local_files:
            rel_path = local_file.relative_path

            # Check if file should be skipped
            if rel_path in files_to_skip:
                skipped_count += 1
                if not self.output.quiet:
                    self.output.info(f"Skipping: {rel_path}")
                continue

            # Apply rename if specified
            upload_path = file_renames.get(rel_path, rel_path)

            # Create upload decision
            decision = SyncDecision(
                action=SyncAction.UPLOAD,
                reason="New local file",
                local_file=local_file,
                remote_file=None,
                relative_path=upload_path,
            )
            upload_decisions.append(decision)

        if not self.output.quiet:
            self.output.info(f"Files to upload: {len(upload_decisions)}")
            if skipped_count > 0:
                self.output.info(f"Files to skip: {skipped_count}")
            self.output.print("")

        # Track statistics
        stats = {"uploads": 0, "skips": skipped_count, "errors": 0}

        if not upload_decisions:
            if not self.output.quiet:
                self.output.info("No files to upload")
            return stats

        # Create a temporary pair for execution
        temp_pair = SyncPair(
            local=local_path,
            remote=remote_path,
            sync_mode=SyncMode.LOCAL_BACKUP,
            workspace_id=workspace_id,
        )

        # Execute uploads
        upload_stats = self._execute_upload_decisions(
            upload_decisions,
            temp_pair,
            chunk_size=chunk_size,
            multipart_threshold=multipart_threshold,
            progress_callback=progress_callback,
            max_workers=max_workers,
            start_delay=start_delay,
        )
        stats["uploads"] = upload_stats["uploads"]
        stats["errors"] = upload_stats["errors"]

        # Display summary
        if not self.output.quiet:
            self.output.print("")
            self.output.success("Upload complete!")
            self.output.info(f"  Uploaded: {stats['uploads']} file(s)")
            if stats["skips"] > 0:
                self.output.info(f"  Skipped: {stats['skips']} file(s)")
            if stats["errors"] > 0:
                self.output.warning(f"  Failed: {stats['errors']} file(s)")

        return stats
