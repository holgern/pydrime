"""Core sync engine for executing sync operations."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
)
from .comparator import FileComparator, SyncAction, SyncDecision
from .modes import SyncMode
from .operations import SyncOperations
from .pair import SyncPair
from .scanner import DirectoryScanner, LocalFile, RemoteFile
from .state import SyncStateManager

logger = logging.getLogger(__name__)


class SyncEngine:
    """Core sync engine that orchestrates file synchronization."""

    def __init__(
        self,
        client: DrimeClient,
        output: Optional[OutputFormatter] = None,
        state_manager: Optional[SyncStateManager] = None,
    ):
        """Initialize sync engine.

        Args:
            client: Drime API client
            output: Output formatter for displaying progress/status
            state_manager: Optional state manager for tracking sync history.
                          If None, a default one will be created for TWO_WAY mode.
        """
        self.client = client
        self.output = output or OutputFormatter()
        self.operations = SyncOperations(client)
        self.state_manager = state_manager or SyncStateManager()

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
    ) -> dict:
        """Sync a single sync pair.

        Args:
            pair: Sync pair to synchronize
            dry_run: If True, only show what would be done without actually syncing
            chunk_size: Chunk size for multipart uploads (bytes)
            multipart_threshold: Threshold for using multipart upload (bytes)
            progress_callback: Optional callback for progress updates
            batch_size: Number of files to process per batch (for streaming mode)
            use_streaming: If True, use streaming mode to process files in batches
                          If False, scan all files upfront (original behavior)
            max_workers: Number of parallel workers for uploads/downloads (default: 1)
            start_delay: Delay in seconds between starting each parallel operation
                        (default: 0.0, useful for preventing server overload)

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
        # Always use traditional (scan-all) mode for dry-run to show complete plan
        if use_streaming and not dry_run and pair.sync_mode.requires_remote_scan:
            # Use streaming mode for better performance
            return self._sync_pair_streaming(
                pair,
                chunk_size,
                multipart_threshold,
                progress_callback,
                batch_size,
                max_workers,
                start_delay,
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

        Returns:
            Dictionary with sync statistics
        """
        # Load previous sync state for TWO_WAY mode (for deletion detection)
        previous_synced_files: set[str] = set()
        if pair.sync_mode == SyncMode.TWO_WAY:
            state = self.state_manager.load_state(pair.local, pair.remote)
            if state:
                previous_synced_files = state.synced_files
                logger.debug(
                    f"Loaded previous sync state with "
                    f"{len(previous_synced_files)} files"
                )

        # Step 1: Scan files
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

        # Step 2: Compare files and determine actions
        comparator = FileComparator(pair.sync_mode, previous_synced_files)
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

                self.state_manager.save_state(
                    pair.local, pair.remote, current_synced_files
                )
                logger.debug(f"Saved sync state with {len(current_synced_files)} files")

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

        Returns:
            Dictionary with sync statistics
        """
        start_time = time.time()
        logger.debug(f"Starting streaming sync at {start_time:.2f}")

        # Load previous sync state for TWO_WAY mode (for deletion detection)
        previous_synced_files: set[str] = set()
        if pair.sync_mode == SyncMode.TWO_WAY:
            state = self.state_manager.load_state(pair.local, pair.remote)
            if state:
                previous_synced_files = state.synced_files
                logger.debug(
                    f"Loaded previous sync state with "
                    f"{len(previous_synced_files)} files"
                )

        # Step 1: Scan local files if needed
        local_files = self._scan_local_files_streaming(pair)

        # Build dictionary for comparison
        local_file_map = {f.relative_path: f for f in local_files}

        # Track statistics
        stats = self._create_empty_stats()

        # Track which remote files we've seen (for delete detection)
        seen_remote_paths: set[str] = set()

        # Track successfully synced files for state saving
        synced_files: set[str] = set()

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
            )

        # Step 4: Save sync state after successful sync (for TWO_WAY mode)
        if pair.sync_mode == SyncMode.TWO_WAY:
            self.state_manager.save_state(pair.local, pair.remote, synced_files)
            logger.debug(f"Saved sync state with {len(synced_files)} files")

        # Step 5: Display summary
        if not self.output.quiet:
            self._display_summary(stats, dry_run=False)

        return stats

    def _scan_local_files_streaming(self, pair: SyncPair) -> list:
        """Scan local files for streaming sync mode.

        Args:
            pair: Sync pair configuration

        Returns:
            List of local files
        """
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            if pair.sync_mode.requires_local_scan:
                scan_start = time.time()
                task = progress.add_task("Scanning local directory...", total=None)
                scanner = DirectoryScanner(
                    ignore_patterns=pair.ignore,
                    exclude_dot_files=pair.exclude_dot_files,
                )
                local_files = scanner.scan_local(pair.local)
                scan_elapsed = time.time() - scan_start
                progress.update(
                    task, description=f"Found {len(local_files)} local file(s)"
                )
                logger.debug(
                    f"Local scan took {scan_elapsed:.2f}s for {len(local_files)} files"
                )
                return local_files
            return []

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
            "skips": 0,
            "conflicts": 0,
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
                folder_name = pair.remote.lstrip("/")
                logger.debug("Resolving folder: %s", folder_name)

                # Search in root folder (parent_id=0 for root)
                folder_entry = manager.find_folder_by_name(folder_name, parent_id=0)
                if folder_entry:
                    remote_folder_id = folder_entry.id
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
                batch_stats, batch_synced = self._process_single_remote_batch(
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
    ) -> tuple[dict, set[str]]:
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
            Tuple of (dictionary with batch statistics, set of synced file paths)
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

        # Track successfully synced files
        synced_in_batch: set[str] = set()
        for decision in batch_decisions:
            if decision.action == SyncAction.SKIP:
                # Files that were already in sync
                synced_in_batch.add(decision.relative_path)
            elif decision.action == SyncAction.UPLOAD:
                # Files uploaded to remote (now exist in both)
                synced_in_batch.add(decision.relative_path)
            elif decision.action == SyncAction.DOWNLOAD:
                # Files downloaded to local (now exist in both)
                synced_in_batch.add(decision.relative_path)
            # DELETE_LOCAL and DELETE_REMOTE are NOT added (they no longer exist)

        # Debug: print when batch is complete
        logger.debug("Batch %d complete, waiting for next batch...", batch_num)

        return batch_stats, synced_in_batch

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
        """Execute sync decisions.

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

        if self.output.quiet:
            # Execute without progress display
            if max_workers > 1 and len(actionable) > 1:
                self._execute_decisions_parallel(
                    actionable,
                    pair,
                    chunk_size,
                    multipart_threshold,
                    progress_callback,
                    max_workers,
                    start_delay,
                )
            else:
                for decision in actionable:
                    try:
                        self._execute_single_decision(
                            decision, pair, chunk_size, multipart_threshold, None
                        )
                    except Exception:
                        # Log error but continue with remaining files
                        pass  # Already logged in _execute_single_decision
        else:
            # Execute with progress display
            if max_workers > 1 and len(actionable) > 1:
                with Progress() as progress:
                    task = progress.add_task("Syncing files...", total=len(actionable))
                    # For parallel execution, we update progress as futures complete
                    stats = self._execute_decisions_parallel(
                        actionable,
                        pair,
                        chunk_size,
                        multipart_threshold,
                        progress_callback,
                        max_workers,
                        start_delay,
                    )
                    # Update progress to completion
                    completed = (
                        stats["uploads"]
                        + stats["downloads"]
                        + stats["deletes_local"]
                        + stats["deletes_remote"]
                    )
                    progress.update(task, completed=completed)
            else:
                with Progress() as progress:
                    task = progress.add_task("Syncing files...", total=len(actionable))

                    for decision in actionable:
                        try:
                            self._execute_single_decision(
                                decision,
                                pair,
                                chunk_size,
                                multipart_threshold,
                                progress_callback,
                            )
                        except Exception:
                            # Log error but continue with remaining files
                            pass  # Already logged in _execute_single_decision
                        progress.update(task, advance=1)

    def _execute_single_decision(
        self,
        decision: SyncDecision,
        pair: SyncPair,
        chunk_size: int,
        multipart_threshold: int,
        progress_callback: Optional[Callable[[int, int], None]],
    ) -> None:
        """Execute a single sync decision.

        Args:
            decision: Sync decision to execute
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            progress_callback: Optional progress callback
        """
        try:
            action_start = time.time()

            if decision.action == SyncAction.UPLOAD:
                local_file = decision.local_file
                if local_file:
                    logger.debug(f"Uploading {decision.relative_path}...")

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
                    logger.debug(
                        f"Upload of {decision.relative_path} took {action_elapsed:.2f}s"
                    )

            elif decision.action == SyncAction.DOWNLOAD:
                remote_file = decision.remote_file
                if remote_file:
                    logger.debug(f"Downloading {decision.relative_path}...")
                    local_path = pair.local / decision.relative_path

                    # Retry download for transient errors
                    # This handles cases where recently uploaded files aren't
                    # immediately available for download, or server-side issues
                    max_retries = DEFAULT_MAX_RETRIES
                    retry_delay = DEFAULT_RETRY_DELAY

                    for attempt in range(max_retries):
                        try:
                            self.operations.download_file(
                                remote_file=remote_file,
                                local_path=local_path,
                                progress_callback=progress_callback,
                            )
                            break  # Success, exit retry loop
                        except Exception as e:
                            error_str = str(e)
                            # Retry on 429 (rate limit), 500/502/503/504 (server errors)
                            # Note: 403 usually means permission denied, not transient
                            is_retryable = any(
                                code in error_str
                                for code in ["429", "500", "502", "503", "504"]
                            )
                            if is_retryable and attempt < max_retries - 1:
                                # Transient error, wait and retry
                                msg = (
                                    f"Download failed "
                                    f"(attempt {attempt + 1}/{max_retries}), "
                                    f"retrying in {retry_delay:.1f}s: {e}"
                                )
                                logger.debug(msg)
                                if not self.output.quiet:
                                    self.output.warning(
                                        f"Retrying {decision.relative_path} "
                                        f"({attempt + 1}/{max_retries})..."
                                    )
                                time.sleep(retry_delay)
                                retry_delay *= 2.0  # Exponential backoff
                            else:
                                # Not retryable or final attempt, re-raise
                                raise

                    action_elapsed = time.time() - action_start
                    logger.debug(
                        "Download of %s took %.2fs",
                        decision.relative_path,
                        action_elapsed,
                    )

            elif decision.action == SyncAction.DELETE_LOCAL:
                local_file = decision.local_file
                if local_file:
                    self.operations.delete_local(
                        local_file=local_file,
                        use_trash=pair.use_local_trash,
                    )

            elif decision.action == SyncAction.DELETE_REMOTE:
                remote_file = decision.remote_file
                if remote_file:
                    self.operations.delete_remote(
                        remote_file=remote_file,
                        permanent=False,
                    )

        except Exception as e:
            if not self.output.quiet:
                self.output.error(f"Error syncing {decision.relative_path}: {e}")
            raise  # Re-raise for parallel executor to track failure

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
        }

        def execute_with_timing(
            decision: SyncDecision,
            worker_delay: float,
        ) -> tuple[str, float, bool, SyncAction]:
            """Execute a single decision and return timing info."""
            # Apply staggered start delay
            if worker_delay > 0:
                time.sleep(worker_delay)

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
                    self.output.error(f"Error syncing {decision.relative_path}: {e}")
                success = False
            elapsed = time.time() - start
            return decision.relative_path, elapsed, success, decision.action

        # Execute in parallel with staggered start delays
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    execute_with_timing, decision, i * start_delay
                ): decision
                for i, decision in enumerate(decisions)
            }

            for future in as_completed(futures):
                try:
                    path, elapsed, success, action = future.result()
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

                        logger.debug(f"Completed {path} in {elapsed:.2f}s")
                    else:
                        logger.debug(f"Failed {path} in {elapsed:.2f}s")
                except Exception as e:
                    if not self.output.quiet:
                        self.output.error(
                            f"Unexpected error in parallel execution: {e}"
                        )

        return stats

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
        )

        if total_actions > 0:
            self.output.info(f"Total actions: {total_actions}")
            if stats["uploads"] > 0:
                self.output.info(f"  Uploaded: {stats['uploads']}")
            if stats["downloads"] > 0:
                self.output.info(f"  Downloaded: {stats['downloads']}")
            if stats["deletes_local"] > 0:
                self.output.info(f"  Deleted locally: {stats['deletes_local']}")
            if stats["deletes_remote"] > 0:
                self.output.info(f"  Deleted remotely: {stats['deletes_remote']}")
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
