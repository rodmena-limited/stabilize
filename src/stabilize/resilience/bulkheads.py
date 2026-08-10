"""
Bulkhead management for Stabilize.

Provides per-task-type bulkheads using BulkheadThreading from bulkman.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any, TypeVar

from bulkman.config import BulkheadConfig as BulkmanConfig
from bulkman.config import ExecutionResult
from bulkman.threading import BulkheadThreading

from stabilize.resilience.config import ResilienceConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TaskBulkheadManager:
    """
    Manages per-task-type bulkheads.

    Each task type (shell, python, http, docker, ssh) gets its own
    BulkheadThreading instance with independent thread pool and
    capacity limits.

    This provides isolation between task types - slow shell tasks
    can't starve HTTP tasks, for example.

    Example:
        config = ResilienceConfig.from_env()
        manager = TaskBulkheadManager(config)

        # Execute a shell task with timeout
        result = manager.execute_with_timeout(
            task_type="shell",
            func=task.execute,
            stage,
            timeout=60.0
        )
    """

    def __init__(self, config: ResilienceConfig) -> None:
        """
        Initialize bulkhead manager with per-task-type bulkheads.

        Args:
            config: Resilience configuration with bulkhead settings
        """
        self._bulkheads: dict[str, BulkheadThreading] = {}
        self._config = config
        # bulkman >=2.0.0 makes shutdown terminal: a later execute raises rather
        # than returning an ExecutionResult. Track the transition here so callers
        # can refuse dispatch *before* entering a circuit breaker — bulkman 2.0.1's
        # BulkheadShutdownError classifies the race correctly, but an exception
        # raised from inside a protected call is still recorded as a failure
        # against that circuit, which a shutdown must not do.
        self._shutdown = threading.Event()

        # Create a bulkhead for each configured task type
        for task_type, bulkhead_config in config.bulkheads:
            self._bulkheads[task_type] = BulkheadThreading(
                BulkmanConfig(
                    name=f"stabilize_{task_type}",
                    max_concurrent_calls=bulkhead_config.max_concurrent,
                    max_queue_size=bulkhead_config.max_queue_size,
                    timeout_seconds=bulkhead_config.timeout_seconds,
                    # Disable bulkman's built-in circuit breaker
                    # We use WorkflowCircuitFactory for per-workflow circuits
                    circuit_breaker_enabled=False,
                )
            )
            logger.debug(
                f"Created bulkhead for task type '{task_type}' with max_concurrent={bulkhead_config.max_concurrent}"
            )

        # Create a default bulkhead for unknown task types
        self._default_bulkhead = BulkheadThreading(
            BulkmanConfig(
                name="stabilize_default",
                max_concurrent_calls=5,
                max_queue_size=20,
                timeout_seconds=14400.0,  # 4 hours for long-running workflows
                circuit_breaker_enabled=False,
            )
        )

    def get(self, task_type: str) -> BulkheadThreading:
        """
        Get the bulkhead for a task type.

        Args:
            task_type: The task type (shell, python, http, docker, ssh)

        Returns:
            The BulkheadThreading instance for this task type,
            or the default bulkhead if type is unknown
        """
        return self._bulkheads.get(task_type, self._default_bulkhead)

    @property
    def is_shutdown(self) -> bool:
        """
        Whether shutdown() has been entered on this manager.

        Once true, every underlying bulkhead is terminal: an execute attempt
        raises RuntimeError instead of returning an ExecutionResult. Callers
        use this to classify that RuntimeError as a shutdown race (retryable)
        rather than a task failure (permanent).
        """
        return self._shutdown.is_set()

    def execute_with_timeout(
        self,
        task_type: str,
        func: Callable[..., T],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> ExecutionResult:
        """
        Execute a function through the appropriate bulkhead with timeout.

        Args:
            task_type: The task type to select the bulkhead
            func: The function to execute
            *args: Positional arguments for the function
            timeout: Timeout in seconds (overrides bulkhead default)
            **kwargs: Keyword arguments for the function

        Returns:
            ExecutionResult with success/failure status and result/error
        """
        bulkhead = self.get(task_type)
        return bulkhead.execute_with_timeout(func, *args, timeout=timeout, **kwargs)

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """
        Get statistics for all bulkheads.

        Returns:
            Dict mapping task type to bulkhead statistics
        """
        stats = {}
        for name, bulkhead in self._bulkheads.items():
            stats[name] = bulkhead.get_stats()
        stats["default"] = self._default_bulkhead.get_stats()
        return stats

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        """
        Shutdown all bulkheads.

        Distributes timeout budget across bulkheads. If a bulkhead finishes
        quickly, remaining time is redistributed to subsequent bulkheads.
        Under bulkman >=2.0.0 the per-bulkhead timeout is actually honored, so
        this budget bounds total shutdown time rather than merely describing it.

        Idempotent: calling this more than once is safe.

        Args:
            wait: Whether to wait for pending tasks to complete
            timeout: Maximum time to wait for shutdown (total across all bulkheads)
        """
        # Mark terminal before touching the bulkheads, so a concurrent
        # execute_with_timeout that loses the race can still be classified.
        self._shutdown.set()

        start = time.monotonic()
        remaining = timeout

        for name, bulkhead in self._bulkheads.items():
            logger.debug("Shutting down bulkhead '%s'", name)
            if remaining is not None and remaining <= 0:
                # No time left - force immediate shutdown
                bulkhead.shutdown(wait=False)
            else:
                bulkhead.shutdown(wait=wait, timeout=remaining)

            if timeout is not None:
                elapsed = time.monotonic() - start
                remaining = timeout - elapsed

        # Shutdown default bulkhead with remaining time
        if remaining is not None and remaining <= 0:
            self._default_bulkhead.shutdown(wait=False)
        else:
            self._default_bulkhead.shutdown(wait=wait, timeout=remaining)

        logger.info("All bulkheads shut down")
