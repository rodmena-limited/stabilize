"""
Finalizer registry for cleanup callbacks.

Provides a centralized registry for tracking and executing cleanup
callbacks when stages enter terminal states. This ensures resources
are properly released even on failures.

Usage:
    from stabilize.finalizers import get_finalizer_registry, FinalizerResult

    registry = get_finalizer_registry()

    # Register a cleanup callback
    registry.register(
        stage_id="01HXYZ123",
        finalizer_name="cleanup_temp_files",
        callback=lambda: shutil.rmtree("/tmp/stage-01HXYZ123"),
    )

    # Execute finalizers when stage completes
    results = registry.execute(stage_id="01HXYZ123")
    for result in results:
        if not result.success:
            logger.warning("Finalizer %s failed: %s", result.name, result.error)
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class FinalizerResult:
    """Result of executing a single finalizer.

    Attributes:
        name: Name of the finalizer that was executed
        success: True if finalizer completed without error
        error: Error message if finalizer failed
        duration_ms: Execution time in milliseconds
    """

    name: str
    success: bool
    error: str | None = None
    duration_ms: float = 0


@dataclass
class RegisteredFinalizer:
    """A registered finalizer callback.

    Attributes:
        name: Human-readable name for logging/debugging
        callback: The cleanup function to execute
        registered_at: Timestamp when registered
    """

    name: str
    callback: Callable[[], Any]
    registered_at: float = field(default_factory=time.time)


class FinalizerRegistry:
    """Track and execute cleanup callbacks for stages.

    The registry maintains a thread-safe collection of finalizers
    organized by stage ID. When a stage enters a terminal state,
    all registered finalizers for that stage are executed.

    Finalizers are executed with a per-finalizer timeout to prevent
    hanging on cleanup operations. Results are collected for logging
    and monitoring.

    Thread Safety:
        All methods are thread-safe. Finalizers can be registered
        and executed concurrently from different threads.

    Example:
        registry = FinalizerRegistry()

        # In task execution
        registry.register(
            stage.id,
            "docker_container",
            lambda: docker_client.stop(container_id),
        )

        # In error handler
        results = registry.execute(stage.id, timeout=30.0)
    """

    def __init__(self) -> None:
        """Initialize the finalizer registry."""
        self._finalizers: dict[str, list[RegisteredFinalizer]] = {}
        self._lock = threading.Lock()

    def register(
        self,
        stage_id: str,
        finalizer_name: str,
        callback: Callable[[], Any],
    ) -> None:
        """Register a cleanup callback for a stage.

        The callback will be executed when the stage enters a terminal
        state (TERMINAL, CANCELED, STOPPED) or during shutdown.

        Args:
            stage_id: ID of the stage this finalizer belongs to
            finalizer_name: Human-readable name for logging
            callback: The cleanup function to execute (no arguments)

        Example:
            registry.register(
                stage_id=stage.id,
                finalizer_name="cleanup_docker",
                callback=lambda: client.stop(container),
            )
        """
        finalizer = RegisteredFinalizer(name=finalizer_name, callback=callback)

        with self._lock:
            if stage_id not in self._finalizers:
                self._finalizers[stage_id] = []
            self._finalizers[stage_id].append(finalizer)

        logger.debug(
            "Registered finalizer '%s' for stage %s",
            finalizer_name,
            stage_id,
        )

    def execute(
        self,
        stage_id: str,
        timeout: float = 30.0,
    ) -> list[FinalizerResult]:
        """Run all registered finalizers for a stage.

        Executes each finalizer with an individual timeout. If a finalizer
        exceeds its timeout, it's marked as failed but doesn't block other
        finalizers.

        After execution, all finalizers for the stage are removed from
        the registry (whether they succeeded or failed).

        Args:
            stage_id: ID of the stage to run finalizers for
            timeout: Maximum seconds per finalizer (default: 30s)

        Returns:
            List of FinalizerResult for each executed finalizer.
            Returns empty list if no finalizers registered.

        Example:
            results = registry.execute(stage.id)
            failed = [r for r in results if not r.success]
            if failed:
                logger.warning("Failed finalizers: %s", failed)
        """
        # Get and remove finalizers atomically
        with self._lock:
            finalizers = self._finalizers.pop(stage_id, [])

        if not finalizers:
            return []

        results: list[FinalizerResult] = []

        for finalizer in finalizers:
            result = self._execute_one(finalizer, timeout)
            results.append(result)

        return results

    def _execute_one(
        self,
        finalizer: RegisteredFinalizer,
        timeout: float,
    ) -> FinalizerResult:
        """Execute a single finalizer with timeout."""
        start_time = time.monotonic()

        try:
            # Use a thread pool to enforce timeout
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(finalizer.callback)
                try:
                    future.result(timeout=timeout)
                    duration_ms = (time.monotonic() - start_time) * 1000

                    logger.debug(
                        "Finalizer '%s' completed in %.1fms",
                        finalizer.name,
                        duration_ms,
                    )

                    return FinalizerResult(
                        name=finalizer.name,
                        success=True,
                        duration_ms=duration_ms,
                    )

                except FuturesTimeoutError:
                    duration_ms = (time.monotonic() - start_time) * 1000
                    error = f"Timeout after {timeout}s"

                    logger.warning(
                        "Finalizer '%s' timed out after %.1fms",
                        finalizer.name,
                        duration_ms,
                    )

                    return FinalizerResult(
                        name=finalizer.name,
                        success=False,
                        error=error,
                        duration_ms=duration_ms,
                    )

        except Exception as e:
            duration_ms = (time.monotonic() - start_time) * 1000
            error = str(e)

            logger.warning(
                "Finalizer '%s' failed after %.1fms: %s",
                finalizer.name,
                duration_ms,
                error,
            )

            return FinalizerResult(
                name=finalizer.name,
                success=False,
                error=error,
                duration_ms=duration_ms,
            )

    def pending(self) -> list[str]:
        """Return stage IDs with unexecuted finalizers.

        Useful for shutdown to ensure all finalizers are run before
        process exit.

        Returns:
            List of stage IDs that have pending finalizers.
        """
        with self._lock:
            return list(self._finalizers.keys())

    def pending_count(self) -> int:
        """Return total number of pending finalizers across all stages."""
        with self._lock:
            return sum(len(f) for f in self._finalizers.values())

    def clear(self) -> None:
        """Remove all registered finalizers without executing them.

        Use with caution - this can leak resources.
        """
        with self._lock:
            self._finalizers.clear()

    def execute_all_pending(self, timeout: float = 30.0) -> dict[str, list[FinalizerResult]]:
        """Execute all pending finalizers for all stages.

        Used during shutdown to ensure all cleanup is performed.

        Args:
            timeout: Maximum seconds per finalizer

        Returns:
            Dict mapping stage_id to list of FinalizerResults.
        """
        # Get all stage IDs with pending finalizers
        stage_ids = self.pending()

        results: dict[str, list[FinalizerResult]] = {}
        for stage_id in stage_ids:
            stage_results = self.execute(stage_id, timeout=timeout)
            if stage_results:
                results[stage_id] = stage_results

        return results


# Global finalizer registry instance
_finalizer_registry: FinalizerRegistry | None = None


def get_finalizer_registry() -> FinalizerRegistry:
    """Get the global finalizer registry.

    Creates a new registry if one doesn't exist.

    Returns:
        The global FinalizerRegistry instance.
    """
    global _finalizer_registry
    if _finalizer_registry is None:
        _finalizer_registry = FinalizerRegistry()
    return _finalizer_registry


def set_finalizer_registry(registry: FinalizerRegistry) -> None:
    """Set the global finalizer registry.

    Args:
        registry: The registry to use globally.
    """
    global _finalizer_registry
    _finalizer_registry = registry


def reset_finalizer_registry() -> None:
    """Reset the global finalizer registry.

    Useful for testing.
    """
    global _finalizer_registry
    _finalizer_registry = None
