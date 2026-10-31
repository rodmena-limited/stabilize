import threading
import time
from datetime import timedelta
from fractions import Fraction

import pytest
from bulkman.exceptions import BulkheadError, BulkheadShutdownError
from resilient_circuit import CircuitProtectorPolicy
from resilient_circuit.storage import InMemoryStorage

from stabilize.errors import TransientError
from stabilize.resilience.bulkheads import TaskBulkheadManager
from stabilize.resilience.config import ResilienceConfig
from stabilize.resilience.executor import execute_with_resilience

# =============================================================================
# Test bulkman 2.0.0 adoption (issuedb #10)
# =============================================================================


class TestBulkheadShutdownSemantics:
    """Shutdown behavior that changed when bulkman became >=2.0.0.

    Under bulkman 1.x the shutdown timeout was ignored and shutdown blocked
    until every task returned; 2.0.0 honors it and makes shutdown terminal.
    """

    def test_shutdown_honors_total_timeout_budget(self) -> None:
        """shutdown(timeout=...) returns within budget even with a stuck task."""
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        released = threading.Event()

        def stuck_task() -> str:
            # Far longer than the shutdown budget; released in teardown so the
            # worker thread does not outlive the test.
            released.wait(timeout=30.0)
            return "done"

        worker = threading.Thread(
            target=lambda: manager.execute_with_timeout("shell", stuck_task, timeout=60.0),
            daemon=True,
        )
        worker.start()
        try:
            # Let the task actually occupy a bulkhead slot before shutting down,
            # otherwise the timeout is never exercised and the test is vacuous.
            time.sleep(0.3)

            start = time.monotonic()
            manager.shutdown(wait=True, timeout=1.0)
            elapsed = time.monotonic() - start
        finally:
            released.set()

        # Budget is 1.0s total across all bulkheads. Allow scheduling slack, but
        # this must be nowhere near the task's own 30s.
        assert elapsed < 10.0, f"shutdown ignored its timeout budget ({elapsed:.2f}s)"

    def test_is_shutdown_reflects_state(self) -> None:
        """is_shutdown is False before shutdown and True after."""
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        assert manager.is_shutdown is False
        manager.shutdown(wait=False)
        assert manager.is_shutdown is True

    def test_shutdown_is_idempotent(self) -> None:
        """Calling shutdown twice does not raise."""
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        manager.shutdown(wait=False)
        manager.shutdown(wait=False)  # must not raise

    def test_underlying_bulkhead_is_terminal_after_shutdown(self) -> None:
        """The bulkman contract this repo now depends on: execute after
        shutdown raises BulkheadShutdownError rather than silently succeeding
        or leaking an untyped error.

        bulkman <=2.0.0 raised a bare RuntimeError here, which callers could not
        distinguish from a RuntimeError raised by the task itself. 2.0.1 made it
        a typed BulkheadError subclass; executor.py classifies on that type.
        """
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)
        bulkhead = manager.get("shell")

        manager.shutdown(wait=False)

        with pytest.raises(BulkheadShutdownError):
            bulkhead.execute_with_timeout(lambda: 1, timeout=1.0)

    def test_shutdown_error_is_typed_not_a_bare_runtime_error(self) -> None:
        """Pin the property the classification relies on.

        BulkheadShutdownError must be a BulkheadError (so it is part of bulkman's
        typed hierarchy) and must NOT be a RuntimeError — if it ever became one,
        a task's own RuntimeError and a shutdown race would be indistinguishable
        again and the retryable/permanent split would silently break.
        """
        assert issubclass(BulkheadShutdownError, BulkheadError)
        assert not issubclass(BulkheadShutdownError, RuntimeError)

    def test_task_failure_is_not_a_shutdown_error(self) -> None:
        """Known-positive control for the classification.

        A task that fails on a LIVE bulkhead comes back as BulkheadError inside
        an ExecutionResult. BulkheadError is BulkheadShutdownError's parent, not
        its subclass, so it can never be caught by the shutdown clause. Without
        this check, the shutdown tests above prove only that the happy path is
        typed — not that the distinction actually holds.
        """
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        def boom() -> None:
            raise RuntimeError("task blew up")

        result = manager.execute_with_timeout("shell", boom, timeout=5.0)

        assert result.success is False
        assert result.error is not None
        assert isinstance(result.error, BulkheadError)
        assert not isinstance(result.error, BulkheadShutdownError)
        manager.shutdown(wait=False)


class TestExecuteWithResilienceDuringShutdown:
    """A shutdown race must be retryable, not a permanent task failure."""

    @staticmethod
    def _circuit(key: str) -> CircuitProtectorPolicy:
        return CircuitProtectorPolicy(
            resource_key=key,
            storage=InMemoryStorage(),
            failure_limit=Fraction(5, 10),
            cooldown=timedelta(seconds=30),
        )

    def test_shutdown_does_not_trip_the_workflow_circuit(self) -> None:
        """The reason the pre-dispatch guard survives bulkman 2.0.1.

        BulkheadShutdownError classifies the race correctly, but an exception
        raised from INSIDE the circuit-protected call is still recorded by the
        circuit as a failure. Repeated dispatches during a rolling restart would
        therefore trip circuits for workflows that never actually failed, and
        leave them open through the cooldown after the new process is healthy.
        Refusing before entering the circuit is what prevents that.
        """
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)
        manager.shutdown(wait=False)

        storage = InMemoryStorage()
        circuit = CircuitProtectorPolicy(
            resource_key="shutdown_must_not_trip",
            storage=storage,
            failure_limit=Fraction(1, 2),  # trips fast, so a leak would show
            cooldown=timedelta(seconds=60),
        )

        # Far more dispatches than the circuit tolerates from real failures.
        for _ in range(6):
            with pytest.raises(TransientError):
                execute_with_resilience(
                    bulkhead_manager=manager,
                    circuit=circuit,
                    task_type="shell",
                    func=lambda: "never runs",
                    func_args=(),
                    timeout=5.0,
                )

        # The circuit must still be usable: a live call through it succeeds.
        # If shutdowns had been recorded as failures this raises ProtectedCallError.
        @circuit
        def healthy_call() -> str:
            return "ok"

        assert healthy_call() == "ok", "shutdown refusals were recorded as circuit failures"

    def test_dispatch_after_shutdown_raises_transient_error(self) -> None:
        """Work submitted to a shut-down manager is TransientError (retry),
        not a permanent failure that would burn the task's attempts."""
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)
        manager.shutdown(wait=False)

        with pytest.raises(TransientError) as excinfo:
            execute_with_resilience(
                bulkhead_manager=manager,
                circuit=self._circuit("shutdown_dispatch"),
                task_type="shell",
                func=lambda: "never runs",
                func_args=(),
                timeout=5.0,
            )

        assert "shutting down" in str(excinfo.value).lower()

    def test_task_runtime_error_is_not_reclassified(self) -> None:
        """A RuntimeError raised by the task itself, on a live manager, stays a
        permanent failure — the shutdown mapping must not swallow it.

        bulkman wraps a task's exception in BulkheadError rather than letting it
        propagate, so this also pins the boundary the shutdown mapping relies on:
        the only bare RuntimeError reaching that except clause is bulkman's own
        post-shutdown "cannot schedule new futures" error.
        """
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        def boom() -> None:
            raise RuntimeError("task blew up")

        with pytest.raises(Exception) as excinfo:
            execute_with_resilience(
                bulkhead_manager=manager,
                circuit=self._circuit("live_runtime_error"),
                task_type="shell",
                func=boom,
                func_args=(),
                timeout=5.0,
            )

        assert not isinstance(excinfo.value, TransientError), (
            "a live task failure was misclassified as a retryable shutdown race"
        )
        assert "task blew up" in str(excinfo.value)
        manager.shutdown(wait=False)


class TestLifecycleBulkheadShutdownBudget:
    """LifecycleManager must hand its remaining budget to the bulkhead manager."""

    def test_lifecycle_passes_bounded_timeout(self) -> None:
        """The registered manager receives a finite timeout, not None."""
        from stabilize.lifecycle import LifecycleManager

        recorded: dict[str, object] = {}

        class RecordingManager:
            is_shutdown = False

            def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
                recorded["wait"] = wait
                recorded["timeout"] = timeout

        lifecycle = LifecycleManager(shutdown_timeout=7.0)
        lifecycle._bulkhead_managers.append(RecordingManager())  # type: ignore[arg-type]

        lifecycle._shutdown()

        assert recorded, "bulkhead manager was never shut down"
        timeout = recorded["timeout"]
        assert timeout is not None, "lifecycle passed no shutdown budget"
        assert isinstance(timeout, float)
        assert 0.0 < timeout <= 7.0, f"budget out of range: {timeout}"

    def test_exhausted_budget_does_not_wait(self) -> None:
        """When the budget is already spent, the manager is shut down with
        wait=False rather than being allowed to block."""
        from stabilize.lifecycle import LifecycleManager

        recorded: dict[str, object] = {}

        class RecordingManager:
            is_shutdown = False

            def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
                recorded["wait"] = wait
                recorded["timeout"] = timeout

        # A zero budget means every bulkhead is already out of time.
        lifecycle = LifecycleManager(shutdown_timeout=0.0)
        lifecycle._bulkhead_managers.append(RecordingManager())  # type: ignore[arg-type]

        lifecycle._shutdown()

        assert recorded, "bulkhead manager was never shut down"
        assert recorded["wait"] is False, "shutdown waited despite an exhausted budget"
        assert recorded["timeout"] == 0.0
