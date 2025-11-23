"""Core sync engine for executing sync operations."""

from typing import Callable, Optional

from rich.progress import Progress, SpinnerColumn, TextColumn

from ..api import DrimeClient
from ..file_entries_manager import FileEntriesManager
from ..output import OutputFormatter
from .comparator import FileComparator, SyncAction, SyncDecision
from .operations import SyncOperations
from .pair import SyncPair
from .scanner import DirectoryScanner, RemoteFile


class SyncEngine:
    """Core sync engine that orchestrates file synchronization."""

    def __init__(
        self,
        client: DrimeClient,
        output: Optional[OutputFormatter] = None,
    ):
        """Initialize sync engine.

        Args:
            client: Drime API client
            output: Output formatter for displaying progress/status
        """
        self.client = client
        self.output = output or OutputFormatter()
        self.operations = SyncOperations(client)

    def sync_pair(
        self,
        pair: SyncPair,
        dry_run: bool = False,
        chunk_size: int = 25 * 1024 * 1024,
        multipart_threshold: int = 30 * 1024 * 1024,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> dict:
        """Sync a single sync pair.

        Args:
            pair: Sync pair to synchronize
            dry_run: If True, only show what would be done without actually syncing
            chunk_size: Chunk size for multipart uploads (bytes)
            multipart_threshold: Threshold for using multipart upload (bytes)
            progress_callback: Optional callback for progress updates

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
            self.output.print("")

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
        comparator = FileComparator(pair.sync_mode)
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
            )

        # Step 6: Display summary
        if not self.output.quiet:
            self._display_summary(stats, dry_run)

        return stats

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
                # Try to find the folder by name
                folder_entry = manager.find_folder_by_name(pair.remote)
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
    ) -> None:
        """Execute sync decisions.

        Args:
            decisions: List of sync decisions
            pair: Sync pair configuration
            chunk_size: Chunk size for multipart uploads
            multipart_threshold: Threshold for multipart uploads
            progress_callback: Optional progress callback
        """
        if self.output.quiet:
            # Execute without progress
            for decision in decisions:
                self._execute_single_decision(
                    decision, pair, chunk_size, multipart_threshold, None
                )
        else:
            # Execute with progress display
            with Progress() as progress:
                task = progress.add_task(
                    "Syncing files...",
                    total=len(
                        [
                            d
                            for d in decisions
                            if d.action not in [SyncAction.SKIP, SyncAction.CONFLICT]
                        ]
                    ),
                )

                for decision in decisions:
                    if decision.action not in [SyncAction.SKIP, SyncAction.CONFLICT]:
                        self._execute_single_decision(
                            decision,
                            pair,
                            chunk_size,
                            multipart_threshold,
                            progress_callback,
                        )
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
            if decision.action == SyncAction.UPLOAD:
                local_file = decision.local_file
                if local_file:
                    self.operations.upload_file(
                        local_file=local_file,
                        remote_path=decision.relative_path,
                        workspace_id=pair.workspace_id,
                        chunk_size=chunk_size,
                        multipart_threshold=multipart_threshold,
                        progress_callback=progress_callback,
                    )

            elif decision.action == SyncAction.DOWNLOAD:
                remote_file = decision.remote_file
                if remote_file:
                    local_path = pair.local / decision.relative_path
                    self.operations.download_file(
                        remote_file=remote_file,
                        local_path=local_path,
                        progress_callback=progress_callback,
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
