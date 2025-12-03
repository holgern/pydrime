"""Concurrency control utilities for sync operations.

This module provides semaphore-based concurrency control inspired by filen-sync,
allowing fine-grained control over parallel transfers and operations.
"""

import asyncio
import logging
import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Optional

logger = logging.getLogger(__name__)


class Semaphore:
    """Thread-safe semaphore for controlling concurrent operations.

    This is a synchronous semaphore implementation that can be used with
    ThreadPoolExecutor to limit concurrent operations.

    Attributes:
        max_permits: Maximum number of concurrent operations allowed
        available: Current number of available permits

    Examples:
        >>> semaphore = Semaphore(10)  # Allow 10 concurrent transfers
        >>> with semaphore:
        ...     # Only 10 operations can run concurrently
        ...     upload_file(file)
    """

    def __init__(self, max_permits: int = 10):
        """Initialize semaphore with maximum permits.

        Args:
            max_permits: Maximum number of concurrent operations (default: 10)
        """
        if max_permits < 1:
            raise ValueError("max_permits must be at least 1")

        self._max_permits = max_permits
        self._semaphore = threading.Semaphore(max_permits)
        self._lock = threading.Lock()
        self._available = max_permits

    @property
    def max_permits(self) -> int:
        """Get the maximum number of permits."""
        return self._max_permits

    @property
    def available(self) -> int:
        """Get the current number of available permits."""
        with self._lock:
            return self._available

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire a permit from the semaphore.

        Args:
            blocking: If True, block until a permit is available.
                     If False, return immediately.
            timeout: Maximum time to wait for a permit (seconds).
                    Only used if blocking is True.

        Returns:
            True if a permit was acquired, False otherwise.
        """
        result = self._semaphore.acquire(blocking=blocking, timeout=timeout)
        if result:
            with self._lock:
                self._available -= 1
        return result

    def release(self) -> None:
        """Release a permit back to the semaphore."""
        self._semaphore.release()
        with self._lock:
            self._available += 1

    @contextmanager
    def __call__(self) -> Generator[None, None, None]:
        """Context manager for acquiring/releasing permits.

        Yields:
            None

        Examples:
            >>> semaphore = Semaphore(5)
            >>> with semaphore():
            ...     do_work()
        """
        self.acquire()
        try:
            yield
        finally:
            self.release()

    def __enter__(self) -> "Semaphore":
        """Enter context manager."""
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Exit context manager."""
        self.release()


class AsyncSemaphore:
    """Async semaphore for controlling concurrent async operations.

    This wraps asyncio.Semaphore with additional tracking and a
    similar interface to the sync Semaphore class.

    Examples:
        >>> semaphore = AsyncSemaphore(10)
        >>> async with semaphore:
        ...     await upload_file(file)
    """

    def __init__(self, max_permits: int = 10):
        """Initialize async semaphore with maximum permits.

        Args:
            max_permits: Maximum number of concurrent operations (default: 10)
        """
        if max_permits < 1:
            raise ValueError("max_permits must be at least 1")

        self._max_permits = max_permits
        self._semaphore = asyncio.Semaphore(max_permits)
        self._lock = asyncio.Lock()
        self._available = max_permits

    @property
    def max_permits(self) -> int:
        """Get the maximum number of permits."""
        return self._max_permits

    @property
    def available(self) -> int:
        """Get the current number of available permits (approximate)."""
        return self._available

    async def acquire(self) -> bool:
        """Acquire a permit from the semaphore.

        Returns:
            True when a permit is acquired.
        """
        await self._semaphore.acquire()
        async with self._lock:
            self._available -= 1
        return True

    def release(self) -> None:
        """Release a permit back to the semaphore."""
        self._semaphore.release()
        # Note: We can't use async lock here since release is sync
        self._available += 1

    async def __aenter__(self) -> "AsyncSemaphore":
        """Enter async context manager."""
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Exit async context manager."""
        self.release()


class ConcurrencyLimits:
    """Configuration for concurrency limits in sync operations.

    Following filen-sync's pattern, we use different limits for:
    - Transfers (uploads/downloads): More resource-intensive, limited to 10
    - Normal operations (deletes, renames): Less intensive, limited to 20

    Attributes:
        transfers_limit: Maximum concurrent transfers (uploads/downloads)
        operations_limit: Maximum concurrent normal operations
    """

    # Default limits matching filen-sync
    DEFAULT_TRANSFERS_LIMIT = 10
    DEFAULT_OPERATIONS_LIMIT = 20

    def __init__(
        self,
        transfers_limit: int = DEFAULT_TRANSFERS_LIMIT,
        operations_limit: int = DEFAULT_OPERATIONS_LIMIT,
    ):
        """Initialize concurrency limits.

        Args:
            transfers_limit: Maximum concurrent transfers (default: 10)
            operations_limit: Maximum concurrent normal operations (default: 20)
        """
        self.transfers_limit = transfers_limit
        self.operations_limit = operations_limit

        # Create semaphores
        self._transfers_semaphore = Semaphore(transfers_limit)
        self._operations_semaphore = Semaphore(operations_limit)

    @property
    def transfers_semaphore(self) -> Semaphore:
        """Get the semaphore for transfer operations."""
        return self._transfers_semaphore

    @property
    def operations_semaphore(self) -> Semaphore:
        """Get the semaphore for normal operations."""
        return self._operations_semaphore

    def get_semaphore_for_operation(self, is_transfer: bool) -> Semaphore:
        """Get the appropriate semaphore for an operation type.

        Args:
            is_transfer: True for uploads/downloads, False for other operations

        Returns:
            The appropriate semaphore for the operation type
        """
        if is_transfer:
            return self._transfers_semaphore
        return self._operations_semaphore


class SyncPauseController:
    """Controller for pause/resume functionality in sync operations.

    This allows pausing sync operations gracefully, similar to filen-sync's
    waitForPause() pattern. When paused, workers will wait at checkpoints
    until resumed or cancelled.

    Attributes:
        paused: Whether sync is currently paused
        removed: Whether sync has been cancelled/removed

    Examples:
        >>> controller = SyncPauseController()
        >>> controller.pause()  # Pause sync
        >>> # ... later ...
        >>> controller.resume()  # Resume sync
    """

    def __init__(self) -> None:
        """Initialize pause controller."""
        self._paused = False
        self._removed = False
        self._pause_event = threading.Event()
        self._pause_event.set()  # Initially not paused
        self._lock = threading.Lock()

    @property
    def paused(self) -> bool:
        """Check if sync is paused."""
        with self._lock:
            return self._paused

    @property
    def removed(self) -> bool:
        """Check if sync has been cancelled."""
        with self._lock:
            return self._removed

    def pause(self) -> None:
        """Pause sync operations.

        Workers will wait at the next checkpoint until resumed.
        """
        with self._lock:
            if not self._paused:
                self._paused = True
                self._pause_event.clear()
                logger.debug("Sync paused")

    def resume(self) -> None:
        """Resume sync operations.

        Workers waiting at checkpoints will continue.
        """
        with self._lock:
            if self._paused:
                self._paused = False
                self._pause_event.set()
                logger.debug("Sync resumed")

    def cancel(self) -> None:
        """Cancel sync operations.

        This sets removed=True and unpauses to allow workers to exit.
        """
        with self._lock:
            self._removed = True
            self._paused = False
            self._pause_event.set()
            logger.debug("Sync cancelled")

    def reset(self) -> None:
        """Reset the controller for a new sync operation."""
        with self._lock:
            self._paused = False
            self._removed = False
            self._pause_event.set()

    def wait_if_paused(self, timeout: Optional[float] = None) -> bool:
        """Wait at a checkpoint if sync is paused.

        This should be called at appropriate points in sync operations
        to allow graceful pausing.

        Args:
            timeout: Maximum time to wait (None = wait indefinitely)

        Returns:
            True if sync should continue, False if cancelled

        Examples:
            >>> if not controller.wait_if_paused():
            ...     return  # Sync was cancelled
            >>> # Continue with operation
        """
        if self._removed:
            return False

        if self._paused:
            logger.debug("Waiting at pause checkpoint...")
            self._pause_event.wait(timeout)

        return not self._removed


class AsyncSyncPauseController:
    """Async version of SyncPauseController for async sync operations."""

    def __init__(self) -> None:
        """Initialize async pause controller."""
        self._paused = False
        self._removed = False
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Initially not paused
        self._lock = asyncio.Lock()

    @property
    def paused(self) -> bool:
        """Check if sync is paused."""
        return self._paused

    @property
    def removed(self) -> bool:
        """Check if sync has been cancelled."""
        return self._removed

    async def pause(self) -> None:
        """Pause sync operations."""
        async with self._lock:
            if not self._paused:
                self._paused = True
                self._pause_event.clear()
                logger.debug("Sync paused")

    async def resume(self) -> None:
        """Resume sync operations."""
        async with self._lock:
            if self._paused:
                self._paused = False
                self._pause_event.set()
                logger.debug("Sync resumed")

    async def cancel(self) -> None:
        """Cancel sync operations."""
        async with self._lock:
            self._removed = True
            self._paused = False
            self._pause_event.set()
            logger.debug("Sync cancelled")

    async def reset(self) -> None:
        """Reset the controller for a new sync operation."""
        async with self._lock:
            self._paused = False
            self._removed = False
            self._pause_event.set()

    async def wait_if_paused(self, timeout: Optional[float] = None) -> bool:
        """Wait at a checkpoint if sync is paused.

        Args:
            timeout: Maximum time to wait (None = wait indefinitely)

        Returns:
            True if sync should continue, False if cancelled
        """
        if self._removed:
            return False

        if self._paused:
            logger.debug("Waiting at pause checkpoint...")
            try:
                await asyncio.wait_for(self._pause_event.wait(), timeout)
            except asyncio.TimeoutError:
                pass

        return not self._removed
