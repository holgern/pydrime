"""Unit tests for sync concurrency control utilities."""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from pydrime.sync.concurrency import (
    AsyncSemaphore,
    AsyncSyncPauseController,
    ConcurrencyLimits,
    Semaphore,
    SyncPauseController,
)


class TestSemaphore:
    """Tests for synchronous Semaphore class."""

    def test_basic_initialization(self):
        """Test basic semaphore initialization."""
        sem = Semaphore(10)
        assert sem.max_permits == 10
        assert sem.available == 10

    def test_invalid_max_permits_raises_error(self):
        """Test that max_permits < 1 raises ValueError."""
        with pytest.raises(ValueError, match="max_permits must be at least 1"):
            Semaphore(0)

        with pytest.raises(ValueError, match="max_permits must be at least 1"):
            Semaphore(-1)

    def test_acquire_and_release(self):
        """Test basic acquire and release."""
        sem = Semaphore(3)
        assert sem.available == 3

        result = sem.acquire()
        assert result is True
        assert sem.available == 2

        sem.release()
        assert sem.available == 3

    def test_acquire_non_blocking(self):
        """Test non-blocking acquire."""
        sem = Semaphore(1)

        # First acquire should succeed
        result1 = sem.acquire(blocking=False)
        assert result1 is True
        assert sem.available == 0

        # Second non-blocking acquire should fail
        result2 = sem.acquire(blocking=False)
        assert result2 is False
        assert sem.available == 0

        sem.release()
        assert sem.available == 1

    def test_acquire_with_timeout(self):
        """Test acquire with timeout."""
        sem = Semaphore(1)

        # Acquire the only permit
        sem.acquire()
        assert sem.available == 0

        # Try to acquire with short timeout - should fail
        start = time.time()
        result = sem.acquire(blocking=True, timeout=0.1)
        elapsed = time.time() - start

        assert result is False
        assert elapsed >= 0.1
        assert elapsed < 0.5  # Should not take too long

        sem.release()

    def test_context_manager_call(self):
        """Test using semaphore as context manager via __call__."""
        sem = Semaphore(2)
        assert sem.available == 2

        with sem():
            assert sem.available == 1

        assert sem.available == 2

    def test_context_manager_enter_exit(self):
        """Test using semaphore as context manager via __enter__/__exit__."""
        sem = Semaphore(2)
        assert sem.available == 2

        with sem:
            assert sem.available == 1

        assert sem.available == 2

    def test_concurrent_access(self):
        """Test concurrent access with multiple threads."""
        sem = Semaphore(3)
        active_count = [0]
        max_concurrent = [0]
        lock = threading.Lock()

        def worker():
            with sem:
                with lock:
                    active_count[0] += 1
                    max_concurrent[0] = max(max_concurrent[0], active_count[0])
                time.sleep(0.05)
                with lock:
                    active_count[0] -= 1

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(worker) for _ in range(10)]
            for f in futures:
                f.result()

        assert max_concurrent[0] <= 3
        assert sem.available == 3


class TestAsyncSemaphore:
    """Tests for AsyncSemaphore class."""

    def test_basic_initialization(self):
        """Test basic async semaphore initialization."""
        sem = AsyncSemaphore(5)
        assert sem.max_permits == 5
        assert sem.available == 5

    def test_invalid_max_permits_raises_error(self):
        """Test that max_permits < 1 raises ValueError."""
        with pytest.raises(ValueError, match="max_permits must be at least 1"):
            AsyncSemaphore(0)

    def test_acquire_and_release(self):
        """Test basic acquire and release."""

        async def run_test():
            sem = AsyncSemaphore(3)
            assert sem.available == 3

            result = await sem.acquire()
            assert result is True
            assert sem.available == 2

            sem.release()
            assert sem.available == 3

        asyncio.run(run_test())

    def test_async_context_manager(self):
        """Test async context manager."""

        async def run_test():
            sem = AsyncSemaphore(2)
            assert sem.available == 2

            async with sem:
                assert sem.available == 1

            assert sem.available == 2

        asyncio.run(run_test())

    def test_concurrent_async_access(self):
        """Test concurrent async access."""

        async def run_test():
            sem = AsyncSemaphore(3)
            active_count = 0
            max_concurrent = 0

            async def worker():
                nonlocal active_count, max_concurrent
                async with sem:
                    active_count += 1
                    max_concurrent = max(max_concurrent, active_count)
                    await asyncio.sleep(0.01)
                    active_count -= 1

            await asyncio.gather(*[worker() for _ in range(10)])

            assert max_concurrent <= 3
            assert sem.available == 3

        asyncio.run(run_test())


class TestConcurrencyLimits:
    """Tests for ConcurrencyLimits class."""

    def test_default_limits(self):
        """Test default concurrency limits."""
        limits = ConcurrencyLimits()
        assert limits.transfers_limit == 10
        assert limits.operations_limit == 20

    def test_custom_limits(self):
        """Test custom concurrency limits."""
        limits = ConcurrencyLimits(transfers_limit=5, operations_limit=10)
        assert limits.transfers_limit == 5
        assert limits.operations_limit == 10

    def test_transfers_semaphore(self):
        """Test transfers semaphore access."""
        limits = ConcurrencyLimits(transfers_limit=5)
        sem = limits.transfers_semaphore
        assert sem.max_permits == 5
        assert sem.available == 5

    def test_operations_semaphore(self):
        """Test operations semaphore access."""
        limits = ConcurrencyLimits(operations_limit=15)
        sem = limits.operations_semaphore
        assert sem.max_permits == 15
        assert sem.available == 15

    def test_get_semaphore_for_transfer(self):
        """Test getting semaphore for transfer operations."""
        limits = ConcurrencyLimits(transfers_limit=5, operations_limit=20)
        sem = limits.get_semaphore_for_operation(is_transfer=True)
        assert sem.max_permits == 5

    def test_get_semaphore_for_non_transfer(self):
        """Test getting semaphore for non-transfer operations."""
        limits = ConcurrencyLimits(transfers_limit=5, operations_limit=20)
        sem = limits.get_semaphore_for_operation(is_transfer=False)
        assert sem.max_permits == 20


class TestSyncPauseController:
    """Tests for SyncPauseController class."""

    def test_initial_state(self):
        """Test initial state is not paused and not removed."""
        controller = SyncPauseController()
        assert controller.paused is False
        assert controller.removed is False

    def test_pause(self):
        """Test pausing sync."""
        controller = SyncPauseController()
        controller.pause()
        assert controller.paused is True
        assert controller.removed is False

    def test_pause_idempotent(self):
        """Test that multiple pause calls are idempotent."""
        controller = SyncPauseController()
        controller.pause()
        controller.pause()  # Should not error
        assert controller.paused is True

    def test_resume(self):
        """Test resuming sync."""
        controller = SyncPauseController()
        controller.pause()
        assert controller.paused is True

        controller.resume()
        assert controller.paused is False
        assert controller.removed is False

    def test_resume_idempotent(self):
        """Test that multiple resume calls are idempotent."""
        controller = SyncPauseController()
        controller.pause()
        controller.resume()
        controller.resume()  # Should not error
        assert controller.paused is False

    def test_cancel(self):
        """Test canceling sync."""
        controller = SyncPauseController()
        controller.cancel()
        assert controller.removed is True
        assert controller.paused is False

    def test_cancel_unpauses(self):
        """Test that cancel unpauses when paused."""
        controller = SyncPauseController()
        controller.pause()
        assert controller.paused is True

        controller.cancel()
        assert controller.removed is True
        assert controller.paused is False

    def test_reset(self):
        """Test resetting controller."""
        controller = SyncPauseController()
        controller.pause()
        controller.cancel()

        controller.reset()
        assert controller.paused is False
        assert controller.removed is False

    def test_wait_if_paused_not_paused(self):
        """Test wait_if_paused when not paused."""
        controller = SyncPauseController()
        result = controller.wait_if_paused()
        assert result is True

    def test_wait_if_paused_cancelled(self):
        """Test wait_if_paused when cancelled."""
        controller = SyncPauseController()
        controller.cancel()
        result = controller.wait_if_paused()
        assert result is False

    def test_wait_if_paused_with_timeout(self):
        """Test wait_if_paused with timeout when paused."""
        controller = SyncPauseController()
        controller.pause()

        # Start a thread to resume after short delay
        def resume_later():
            time.sleep(0.1)
            controller.resume()

        thread = threading.Thread(target=resume_later)
        thread.start()

        start = time.time()
        result = controller.wait_if_paused(timeout=1.0)
        elapsed = time.time() - start

        assert result is True
        assert elapsed >= 0.09  # Allow small timing variance
        assert elapsed < 0.5

        thread.join()

    def test_wait_if_paused_timeout_expires(self):
        """Test wait_if_paused when timeout expires while paused."""
        controller = SyncPauseController()
        controller.pause()

        start = time.time()
        result = controller.wait_if_paused(timeout=0.1)
        elapsed = time.time() - start

        # Result is True because removed is still False
        assert result is True
        assert elapsed >= 0.09  # Allow small timing variance
        assert elapsed < 0.5


class TestAsyncSyncPauseController:
    """Tests for AsyncSyncPauseController class."""

    def test_initial_state(self):
        """Test initial state is not paused and not removed."""

        async def run_test():
            controller = AsyncSyncPauseController()
            assert controller.paused is False
            assert controller.removed is False

        asyncio.run(run_test())

    def test_pause(self):
        """Test pausing sync."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.pause()
            assert controller.paused is True
            assert controller.removed is False

        asyncio.run(run_test())

    def test_pause_idempotent(self):
        """Test that multiple pause calls are idempotent."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.pause()
            await controller.pause()  # Should not error
            assert controller.paused is True

        asyncio.run(run_test())

    def test_resume(self):
        """Test resuming sync."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.pause()
            assert controller.paused is True

            await controller.resume()
            assert controller.paused is False
            assert controller.removed is False

        asyncio.run(run_test())

    def test_resume_idempotent(self):
        """Test that multiple resume calls are idempotent."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.pause()
            await controller.resume()
            await controller.resume()  # Should not error
            assert controller.paused is False

        asyncio.run(run_test())

    def test_cancel(self):
        """Test canceling sync."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.cancel()
            assert controller.removed is True
            assert controller.paused is False

        asyncio.run(run_test())

    def test_cancel_unpauses(self):
        """Test that cancel unpauses when paused."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.pause()
            assert controller.paused is True

            await controller.cancel()
            assert controller.removed is True
            assert controller.paused is False

        asyncio.run(run_test())

    def test_reset(self):
        """Test resetting controller."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.pause()
            await controller.cancel()

            await controller.reset()
            assert controller.paused is False
            assert controller.removed is False

        asyncio.run(run_test())

    def test_wait_if_paused_not_paused(self):
        """Test wait_if_paused when not paused."""

        async def run_test():
            controller = AsyncSyncPauseController()
            result = await controller.wait_if_paused()
            assert result is True

        asyncio.run(run_test())

    def test_wait_if_paused_cancelled(self):
        """Test wait_if_paused when cancelled."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.cancel()
            result = await controller.wait_if_paused()
            assert result is False

        asyncio.run(run_test())

    def test_wait_if_paused_with_timeout(self):
        """Test wait_if_paused with timeout when paused."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.pause()

            # Start a task to resume after short delay
            async def resume_later():
                await asyncio.sleep(0.1)
                await controller.resume()

            task = asyncio.create_task(resume_later())

            start = time.time()
            result = await controller.wait_if_paused(timeout=1.0)
            elapsed = time.time() - start

            assert result is True
            assert elapsed >= 0.09  # Allow small timing variance
            assert elapsed < 0.5

            await task

        asyncio.run(run_test())

    def test_wait_if_paused_timeout_expires(self):
        """Test wait_if_paused when timeout expires while paused."""

        async def run_test():
            controller = AsyncSyncPauseController()
            await controller.pause()

            start = time.time()
            result = await controller.wait_if_paused(timeout=0.1)
            elapsed = time.time() - start

            # Result is True because removed is still False
            assert result is True
            assert elapsed >= 0.1
            assert elapsed < 0.5

        asyncio.run(run_test())
