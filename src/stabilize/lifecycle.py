"""
Lifecycle management for graceful shutdown.

This module provides the LifecycleManager class for:
- Signal handling (SIGTERM, SIGINT)
- atexit cleanup registration
- Graceful shutdown of QueueProcessor and BulkheadManager
- Waiting for active tasks with configurable timeout

Usage:
    from stabilize.lifecycle import LifecycleManager, get_lifecycle_manager

    # Get the global lifecycle manager
    manager = get_lifecycle_manager()

    # Register components for shutdown
    manager.register_processor(processor)
    manager.register_bulkhead_manager(bulkhead_manager)

    # Install signal and atexit handlers
    manager.install_handlers()

    # Optionally add custom shutdown callbacks
    @manager.on_shutdown
    def cleanup():
        print("Cleaning up...")
"""

from __future__ import annotations

import atexit
import logging
import signal
import threading
import time
from collections.abc import Callable
from types import FrameType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.resilience.bulkheads import TaskBulkheadManager

# Type alias for signal handlers
SignalHandler = Callable[[int, FrameType | None], Any] | int | None

logger = logging.getLogger(__name__)


class LifecycleManager:
    """
    Manages application lifecycle for graceful shutdown.

    Handles signal registration, atexit hooks, and coordinated shutdown
    of processing components.

    Attributes:
        shutdown_timeout: Maximum time to wait for active tasks (seconds)
        graceful_shutdown_delay: Time to wait between stop request and force (seconds)
    """

    def __init__(
        self,
        shutdown_timeout: float = 30.0,
        graceful_shutdown_delay: float = 5.0,
    ) -> None:
        """
        Initialize the lifecycle manager.

        Args:
            shutdown_timeout: Max seconds to wait for active tasks during shutdown
            graceful_shutdown_delay: Seconds to wait between stop request and force kill
        """
        self.shutdown_timeout = shutdown_timeout
        self.graceful_shutdown_delay = graceful_shutdown_delay

        self._processors: list[QueueProcessor] = []
        self._bulkhead_managers: list[TaskBulkheadManager] = []
        self._shutdown_callbacks: list[Callable[[], None]] = []

        self._shutdown_requested = threading.Event()
        self._shutdown_in_progress = threading.Event()
        self._handlers_installed = False
        self._lock = threading.Lock()

        # Original signal handlers to restore
        self._original_sigterm: SignalHandler = signal.SIG_DFL
        self._original_sigint: SignalHandler = signal.SIG_DFL

    def register_processor(self, processor: QueueProcessor) -> None:
        """
        Register a QueueProcessor for graceful shutdown.

        Args:
            processor: The queue processor to manage
        """
        with self._lock:
            if processor not in self._processors:
                self._processors.append(processor)
                logger.debug("Registered QueueProcessor for lifecycle management")

    def unregister_processor(self, processor: QueueProcessor) -> None:
        """
        Unregister a QueueProcessor.

        Args:
            processor: The queue processor to unregister
        """
        with self._lock:
            if processor in self._processors:
                self._processors.remove(processor)

    def register_bulkhead_manager(self, manager: TaskBulkheadManager) -> None:
        """
        Register a TaskBulkheadManager for graceful shutdown.

        Args:
            manager: The bulkhead manager to manage
        """
        with self._lock:
            if manager not in self._bulkhead_managers:
                self._bulkhead_managers.append(manager)
                logger.debug("Registered TaskBulkheadManager for lifecycle management")

    def unregister_bulkhead_manager(self, manager: TaskBulkheadManager) -> None:
        """
        Unregister a TaskBulkheadManager.

        Args:
            manager: The bulkhead manager to unregister
        """
        with self._lock:
            if manager in self._bulkhead_managers:
                self._bulkhead_managers.remove(manager)

    def on_shutdown(self, callback: Callable[[], None]) -> Callable[[], None]:
        """
        Decorator to register a shutdown callback.

        Callbacks are executed in registration order during shutdown.

        Args:
            callback: Function to call on shutdown

        Returns:
            The original callback (for use as decorator)

        Example:
            @lifecycle.on_shutdown
            def cleanup():
                print("Cleaning up resources...")
        """
        with self._lock:
            self._shutdown_callbacks.append(callback)
        return callback

    def add_shutdown_callback(self, callback: Callable[[], None]) -> None:
        """
        Add a shutdown callback.

        Args:
            callback: Function to call on shutdown
        """
        with self._lock:
            self._shutdown_callbacks.append(callback)

    def install_handlers(self) -> None:
        """
        Install signal and atexit handlers.

        Registers handlers for SIGTERM and SIGINT for graceful shutdown.
        Also registers an atexit handler as a fallback.

        Safe to call multiple times - will only install once.
        """
        if self._handlers_installed:
            return

        with self._lock:
            if self._handlers_installed:
                return

            # Store original handlers
            self._original_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
            self._original_sigint = signal.signal(signal.SIGINT, self._handle_signal)

            # Register atexit handler
            atexit.register(self._shutdown)

            self._handlers_installed = True
            logger.info("Lifecycle handlers installed (SIGTERM, SIGINT, atexit)")

    def uninstall_handlers(self) -> None:
        """
        Uninstall signal and atexit handlers.

        Restores original signal handlers and unregisters atexit callback.
        """
        if not self._handlers_installed:
            return

        with self._lock:
            if not self._handlers_installed:
                return

            # Restore original handlers
            signal.signal(signal.SIGTERM, self._original_sigterm)
            signal.signal(signal.SIGINT, self._original_sigint)

            # Unregister atexit (best effort, may not work if already called)
            try:
                atexit.unregister(self._shutdown)
            except Exception:
                pass

            self._handlers_installed = False
            logger.debug("Lifecycle handlers uninstalled")

    def request_shutdown(self) -> None:
        """
        Request graceful shutdown.

        This sets the shutdown flag and begins the shutdown sequence.
        Can be called from any thread.
        """
        self._shutdown_requested.set()
        logger.info("Shutdown requested")

    @property
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_requested.is_set()

    @property
    def is_shutdown_in_progress(self) -> bool:
        """Check if shutdown is currently in progress."""
        return self._shutdown_in_progress.is_set()

    def wait_for_shutdown(self, timeout: float | None = None) -> bool:
        """
        Wait for shutdown to complete.

        Args:
            timeout: Maximum seconds to wait (None = wait forever)

        Returns:
            True if shutdown completed, False if timeout
        """
        return self._shutdown_in_progress.wait(timeout=timeout)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Handle SIGTERM or SIGINT signal."""
        signal_name = signal.Signals(signum).name
        logger.info("Received %s, initiating graceful shutdown", signal_name)

        # Only initiate shutdown once
        if not self._shutdown_requested.is_set():
            self._shutdown_requested.set()
            # Run shutdown in separate thread to avoid blocking signal handler
            threading.Thread(target=self._shutdown, daemon=True).start()

    def _shutdown(self) -> None:
        """
        Execute graceful shutdown sequence.

        1. Stop accepting new work (request_stop on processors)
        2. Wait for active tasks to complete
        3. Shutdown bulkhead managers
        4. Run custom shutdown callbacks
        """
        # Prevent concurrent shutdown
        if self._shutdown_in_progress.is_set():
            return

        self._shutdown_in_progress.set()
        logger.info("Starting graceful shutdown (timeout=%ss)", self.shutdown_timeout)

        # Use monotonic time for elapsed time calculations to avoid
        # issues with clock drift, NTP adjustments, or leap seconds
        start_time = time.monotonic()
        remaining = self.shutdown_timeout

        # Step 1: Stop accepting new work
        with self._lock:
            processors = list(self._processors)
            bulkheads = list(self._bulkhead_managers)
            callbacks = list(self._shutdown_callbacks)

        for processor in processors:
            try:
                # Use request_stop if available, otherwise stop directly
                if hasattr(processor, "request_stop"):
                    processor.request_stop()
                else:
                    processor.stop(wait=False)
                logger.debug("Requested stop on QueueProcessor")
            except Exception as e:
                logger.warning("Error requesting processor stop: %s", e)

        # Step 2: Wait for active tasks to complete
        logger.info("Waiting for active tasks to complete...")
        for processor in processors:
            try:
                elapsed = time.monotonic() - start_time
                remaining = max(0, self.shutdown_timeout - elapsed)

                if remaining > 0:
                    self._wait_for_processor(processor, timeout=remaining)
            except Exception as e:
                logger.warning("Error waiting for processor: %s", e)

        # Step 3: Force stop processors if still running
        for processor in processors:
            try:
                processor.stop(wait=True)
                logger.debug("Stopped QueueProcessor")
            except Exception as e:
                logger.warning("Error stopping processor: %s", e)

        # Step 4: Shutdown bulkhead managers
        for manager in bulkheads:
            try:
                if hasattr(manager, "shutdown"):
                    manager.shutdown(wait=True)
                    logger.debug("Shutdown TaskBulkheadManager")
            except Exception as e:
                logger.warning("Error shutting down bulkhead manager: %s", e)

        # Step 5: Execute pending finalizers
        try:
            from stabilize.finalizers import get_finalizer_registry

            registry = get_finalizer_registry()
            pending_count = registry.pending_count()
            if pending_count > 0:
                logger.info("Executing %d pending finalizers...", pending_count)
                results = registry.execute_all_pending(timeout=30.0)
                failed_count = sum(
                    1 for stage_results in results.values() for r in stage_results if not r.success
                )
                if failed_count > 0:
                    logger.warning(
                        "Shutdown: %d/%d finalizers failed",
                        failed_count,
                        pending_count,
                    )
        except Exception as e:
            logger.warning("Error executing finalizers during shutdown: %s", e)

        # Step 6: Run custom callbacks
        for callback in callbacks:
            try:
                callback()
            except Exception as e:
                logger.warning("Error in shutdown callback: %s", e)

        elapsed = time.monotonic() - start_time
        logger.info("Graceful shutdown completed in %.2fs", elapsed)

    def _wait_for_processor(self, processor: QueueProcessor, timeout: float) -> None:
        """Wait for a processor's active tasks to complete."""
        # Use monotonic time for elapsed time calculations
        deadline = time.monotonic() + timeout
        poll_interval = 0.1

        while time.monotonic() < deadline:
            active = getattr(processor, "active_count", 0)
            if active == 0:
                return
            time.sleep(poll_interval)

        # Timeout reached
        active = getattr(processor, "active_count", 0)
        if active > 0:
            logger.warning(
                "Timeout waiting for processor - %d tasks still active",
                active,
            )


# Global lifecycle manager instance
_lifecycle_manager: LifecycleManager | None = None


def get_lifecycle_manager() -> LifecycleManager:
    """
    Get the global lifecycle manager.

    Creates a new manager if one doesn't exist.

    Returns:
        The global LifecycleManager instance
    """
    global _lifecycle_manager
    if _lifecycle_manager is None:
        _lifecycle_manager = LifecycleManager()
    return _lifecycle_manager


def set_lifecycle_manager(manager: LifecycleManager) -> None:
    """
    Set the global lifecycle manager.

    Args:
        manager: The manager to use globally
    """
    global _lifecycle_manager
    _lifecycle_manager = manager


def install_shutdown_handlers(
    shutdown_timeout: float = 30.0,
    graceful_shutdown_delay: float = 5.0,
) -> LifecycleManager:
    """
    Convenience function to install shutdown handlers.

    Creates and installs a lifecycle manager with the given configuration.

    Args:
        shutdown_timeout: Max seconds to wait for active tasks
        graceful_shutdown_delay: Seconds between stop and force kill

    Returns:
        The configured LifecycleManager

    Example:
        from stabilize.lifecycle import install_shutdown_handlers

        manager = install_shutdown_handlers(shutdown_timeout=60)
        manager.register_processor(my_processor)
    """
    manager = LifecycleManager(
        shutdown_timeout=shutdown_timeout,
        graceful_shutdown_delay=graceful_shutdown_delay,
    )
    manager.install_handlers()
    set_lifecycle_manager(manager)
    return manager
