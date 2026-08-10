"""Tests for resilience module (bulkhead and circuit breaker integration)."""

import os
import threading
import time
from datetime import timedelta
from fractions import Fraction
from unittest.mock import patch

import pytest
from bulkman.exceptions import BulkheadError, BulkheadShutdownError
from resilient_circuit import CircuitProtectorPolicy
from resilient_circuit.exceptions import ProtectedCallError
from resilient_circuit.storage import InMemoryStorage

from stabilize.errors import TransientError
from stabilize.resilience.bulkheads import TaskBulkheadManager
from stabilize.resilience.circuits import WorkflowCircuitFactory
from stabilize.resilience.config import BulkheadConfig, ResilienceConfig
from stabilize.resilience.executor import execute_with_resilience

# =============================================================================
# Test Circuit Breaker State Transitions
# =============================================================================


class TestCircuitBreaker:
    """Test circuit breaker behavior."""

    def test_circuit_starts_closed(self) -> None:
        """Circuit breaker starts in CLOSED state."""
        storage = InMemoryStorage()
        circuit = CircuitProtectorPolicy(
            resource_key="test",
            storage=storage,
            failure_limit=Fraction(2, 4),  # 50% failure
            cooldown=timedelta(seconds=30),
        )

        # Circuit should start closed (allow calls)
        @circuit
        def success_call() -> str:
            return "ok"

        result = success_call()
        assert result == "ok"

    def test_circuit_opens_after_failures(self) -> None:
        """Circuit opens after failure_limit exceeded."""
        storage = InMemoryStorage()
        circuit = CircuitProtectorPolicy(
            resource_key="test_open",
            storage=storage,
            failure_limit=Fraction(2, 4),  # Opens at 50% failure rate
            cooldown=timedelta(seconds=30),
        )

        @circuit
        def failing_call() -> None:
            raise RuntimeError("Simulated failure")

        # Cause enough failures to trip the circuit
        for _ in range(5):
            try:
                failing_call()
            except (RuntimeError, ProtectedCallError):
                pass

        # Now circuit should be open
        with pytest.raises(ProtectedCallError):
            failing_call()

    def test_circuit_blocks_calls_when_open(self) -> None:
        """Open circuit raises ProtectedCallError."""
        storage = InMemoryStorage()
        circuit = CircuitProtectorPolicy(
            resource_key="test_block",
            storage=storage,
            failure_limit=Fraction(1, 2),  # Trip quickly
            cooldown=timedelta(seconds=60),
        )

        @circuit
        def failing_call() -> None:
            raise RuntimeError("Fail")

        # Trip the circuit
        for _ in range(5):
            try:
                failing_call()
            except (RuntimeError, ProtectedCallError):
                pass

        # Verify it blocks new calls
        @circuit
        def new_call() -> str:
            return "should not run"

        with pytest.raises(ProtectedCallError):
            new_call()

    def test_circuit_allows_probe_after_cooldown(self) -> None:
        """Circuit allows a probe call after cooldown period."""
        storage = InMemoryStorage()
        # Very short cooldown for testing
        circuit = CircuitProtectorPolicy(
            resource_key="test_cooldown",
            storage=storage,
            failure_limit=Fraction(1, 2),
            cooldown=timedelta(milliseconds=100),
        )

        @circuit
        def failing_call() -> None:
            raise RuntimeError("Fail")

        # Trip the circuit
        for _ in range(5):
            try:
                failing_call()
            except (RuntimeError, ProtectedCallError):
                pass

        # Wait for cooldown
        time.sleep(0.15)

        # Circuit should allow probe (half-open)
        @circuit
        def success_call() -> str:
            return "success"

        # This should either succeed or raise original error (not ProtectedCallError)
        result = success_call()
        assert result == "success"


# =============================================================================
# Test Bulkhead Behavior
# =============================================================================


class TestBulkhead:
    """Test bulkhead manager behavior."""

    def test_bulkhead_per_task_type_isolation(self) -> None:
        """Each task type has independent bulkhead."""
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        # Each task type should have its own bulkhead
        shell_bulkhead = manager.get("shell")
        http_bulkhead = manager.get("http")
        python_bulkhead = manager.get("python")

        # They should be different instances
        assert shell_bulkhead is not http_bulkhead
        assert http_bulkhead is not python_bulkhead

    def test_default_bulkhead_for_unknown_type(self) -> None:
        """Unknown task types use default bulkhead."""
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        # Unknown type should get default bulkhead
        unknown_bulkhead = manager.get("unknown_task_type")
        default_bulkhead = manager._default_bulkhead

        assert unknown_bulkhead is default_bulkhead

    def test_bulkhead_executes_function(self) -> None:
        """Bulkhead successfully executes functions."""
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        def simple_func(x: int) -> int:
            return x * 2

        result = manager.execute_with_timeout(
            task_type="shell",
            func=simple_func,
            x=5,
            timeout=10.0,
        )

        assert result.success
        assert result.result == 10

    def test_bulkhead_get_all_stats(self) -> None:
        """Can get stats for all bulkheads."""
        config = ResilienceConfig()
        manager = TaskBulkheadManager(config)

        stats = manager.get_all_stats()

        # Should have stats for each configured task type + default
        assert "shell" in stats
        assert "python" in stats
        assert "http" in stats
        assert "docker" in stats
        assert "ssh" in stats
        assert "default" in stats


# =============================================================================
# Test Resilience Configuration
# =============================================================================


class TestResilienceConfig:
    """Test resilience configuration."""

    def test_default_bulkhead_config(self) -> None:
        """Verify default per-task-type settings."""
        config = ResilienceConfig()

        assert config.get_bulkhead("shell").max_concurrent == 5
        assert config.get_bulkhead("python").max_concurrent == 3
        assert config.get_bulkhead("http").max_concurrent == 10
        assert config.get_bulkhead("docker").max_concurrent == 3
        assert config.get_bulkhead("ssh").max_concurrent == 5

    def test_default_circuit_config(self) -> None:
        """Verify default failure threshold and cooldown."""
        config = ResilienceConfig()

        assert config.circuit_failure_threshold == Fraction(5, 10)
        assert config.circuit_cooldown_seconds == 30.0

    def test_bulkhead_config_defaults(self) -> None:
        """Verify BulkheadConfig defaults."""
        config = BulkheadConfig()

        assert config.max_concurrent == 5
        assert config.max_queue_size == 20
        assert config.timeout_seconds == 14400.0  # 4 hours for long-running workflows

    def test_config_from_env(self) -> None:
        """Config loads from environment variables."""
        with patch.dict(
            os.environ,
            {
                "MG_DATABASE_URL": "postgresql://user:pass@localhost/db",
                "STABILIZE_BULKHEAD_SHELL_MAX_CONCURRENT": "10",
                "STABILIZE_CIRCUIT_FAILURE_THRESHOLD": "0.3",
                "STABILIZE_CIRCUIT_COOLDOWN_SECONDS": "60",
            },
        ):
            config = ResilienceConfig.from_env()

            assert config.database_url == "postgresql://user:pass@localhost/db"
            assert config.get_bulkhead("shell").max_concurrent == 10
            assert config.circuit_failure_threshold == Fraction(3, 10)
            assert config.circuit_cooldown_seconds == 60.0


# =============================================================================
# Test Workflow Circuit Factory
# =============================================================================


class TestWorkflowCircuitFactory:
    """Test workflow circuit factory behavior."""

    def test_factory_creates_circuits_per_workflow(self) -> None:
        """Each workflow gets isolated circuits."""
        config = ResilienceConfig()
        factory = WorkflowCircuitFactory(config)

        circuit1 = factory.get_circuit("workflow_1", "http")
        circuit2 = factory.get_circuit("workflow_2", "http")

        # Different workflows get different circuits
        assert circuit1 is not circuit2

    def test_factory_caches_circuits(self) -> None:
        """Same workflow+task_type returns same circuit."""
        config = ResilienceConfig()
        factory = WorkflowCircuitFactory(config)

        circuit1 = factory.get_circuit("workflow_1", "http")
        circuit2 = factory.get_circuit("workflow_1", "http")

        # Same key returns same circuit
        assert circuit1 is circuit2

    def test_factory_different_task_types(self) -> None:
        """Different task types in same workflow get different circuits."""
        config = ResilienceConfig()
        factory = WorkflowCircuitFactory(config)

        http_circuit = factory.get_circuit("workflow_1", "http")
        shell_circuit = factory.get_circuit("workflow_1", "shell")

        assert http_circuit is not shell_circuit

    def test_factory_clear_workflow_circuits(self) -> None:
        """Can clear circuits for a completed workflow."""
        config = ResilienceConfig()
        factory = WorkflowCircuitFactory(config)

        # Create some circuits
        factory.get_circuit("workflow_1", "http")
        factory.get_circuit("workflow_1", "shell")
        factory.get_circuit("workflow_2", "http")

        # Clear workflow_1 circuits
        factory.clear_workflow_circuits("workflow_1")

        # workflow_1 circuits should be gone, workflow_2 should remain
        assert ("workflow_1", "http") not in factory._circuits
        assert ("workflow_1", "shell") not in factory._circuits
        assert ("workflow_2", "http") in factory._circuits

    def test_factory_uses_inmemory_for_sqlite(self) -> None:
        """Factory uses InMemoryStorage when no PostgreSQL URL."""
        config = ResilienceConfig(database_url=None)
        factory = WorkflowCircuitFactory(config)

        assert isinstance(factory._storage, InMemoryStorage)

    def test_factory_uses_inmemory_for_sqlite_url(self) -> None:
        """Factory uses InMemoryStorage for SQLite URLs."""
        config = ResilienceConfig(database_url="sqlite:///test.db")
        factory = WorkflowCircuitFactory(config)

        assert isinstance(factory._storage, InMemoryStorage)

    def test_postgres_url_passes_tls_params_through(self) -> None:
        """TLS query params on a postgres URL survive _create_storage.

        Regression for the resilient-circuit pin widen: hand-parsing the URL
        into libpq `host=... dbname=...` used to drop sslmode/sslcert/sslkey,
        silently forcing the breaker onto in-memory storage on TLS databases.
        """
        tls_url = (
            "postgresql://user:pass@db.example:5432/stabilize"
            "?sslmode=verify-full&sslcert=/etc/ssl/client.crt&sslkey=/etc/ssl/client.key"
        )
        config = ResilienceConfig(database_url=tls_url)

        with patch("resilient_circuit.storage.PostgresStorage") as mock_storage:
            factory = WorkflowCircuitFactory(config)
            factory.get_circuit("wf1", "http")

        # PostgresStorage must receive the URL verbatim (only the +psycopg
        # driver suffix stripped) so psycopg sees the TLS query params.
        assert mock_storage.called
        conn_arg = mock_storage.call_args.kwargs.get("connection_string")
        if conn_arg is None:
            conn_arg = mock_storage.call_args[0][0]
        assert conn_arg == tls_url
        assert "sslmode=verify-full" in conn_arg
        assert "sslcert=" in conn_arg and "sslkey=" in conn_arg

    def test_postgres_plus_psycopg_scheme_stripped(self) -> None:
        """postgresql+psycopg:// scheme is normalized to postgresql://."""
        url = "postgresql+psycopg://user:pass@db.example:5432/stabilize?sslmode=require"
        config = ResilienceConfig(database_url=url)

        with patch("resilient_circuit.storage.PostgresStorage") as mock_storage:
            factory = WorkflowCircuitFactory(config)
            factory.get_circuit("wf1", "http")

        assert mock_storage.called
        conn_arg = mock_storage.call_args.kwargs.get("connection_string")
        if conn_arg is None:
            conn_arg = mock_storage.call_args[0][0]
        assert conn_arg == "postgresql://user:pass@db.example:5432/stabilize?sslmode=require"


# =============================================================================
# Test Execute With Resilience
# =============================================================================


class TestExecuteWithResilience:
    """Test the unified execute_with_resilience function."""

    def test_successful_execution(self) -> None:
        """Successful function execution returns result."""
        config = ResilienceConfig()
        bulkhead_manager = TaskBulkheadManager(config)
        storage = InMemoryStorage()
        circuit = CircuitProtectorPolicy(
            resource_key="test",
            storage=storage,
            failure_limit=Fraction(5, 10),
            cooldown=timedelta(seconds=30),
        )

        def success_func(x: int) -> int:
            return x * 2

        result = execute_with_resilience(
            bulkhead_manager=bulkhead_manager,
            circuit=circuit,
            task_type="shell",
            func=success_func,
            func_args=(5,),
            timeout=10.0,
        )

        assert result == 10

    def test_circuit_open_raises_transient_error(self) -> None:
        """Circuit open condition raises TransientError when circuit is pre-opened."""
        config = ResilienceConfig()
        bulkhead_manager = TaskBulkheadManager(config)
        storage = InMemoryStorage()
        circuit = CircuitProtectorPolicy(
            resource_key="test_transient",
            storage=storage,
            failure_limit=Fraction(1, 2),  # Trip quickly
            cooldown=timedelta(seconds=60),
        )

        # Pre-trip the circuit by calling it directly (without bulkhead)
        @circuit
        def direct_fail() -> None:
            raise RuntimeError("Direct fail")

        for _ in range(5):
            try:
                direct_fail()
            except (RuntimeError, ProtectedCallError):
                pass

        # Now circuit should be open, causing TransientError when we try execute_with_resilience
        def any_func() -> str:
            return "should not run"

        with pytest.raises(TransientError) as exc_info:
            execute_with_resilience(
                bulkhead_manager=bulkhead_manager,
                circuit=circuit,
                task_type="shell",
                func=any_func,
                func_args=(),
                timeout=10.0,
            )

        assert "Circuit breaker open" in str(exc_info.value)

    def test_function_with_kwargs(self) -> None:
        """Function can be called with kwargs."""
        config = ResilienceConfig()
        bulkhead_manager = TaskBulkheadManager(config)
        storage = InMemoryStorage()
        circuit = CircuitProtectorPolicy(
            resource_key="test_kwargs",
            storage=storage,
            failure_limit=Fraction(5, 10),
            cooldown=timedelta(seconds=30),
        )

        def func_with_kwargs(a: int, b: int, multiplier: int = 1) -> int:
            return (a + b) * multiplier

        result = execute_with_resilience(
            bulkhead_manager=bulkhead_manager,
            circuit=circuit,
            task_type="python",
            func=func_with_kwargs,
            func_args=(2, 3),
            func_kwargs={"multiplier": 10},
            timeout=10.0,
        )

        assert result == 50

    def test_exception_propagates(self) -> None:
        """Non-resilience exceptions propagate through (wrapped by bulkhead)."""
        from bulkman.exceptions import BulkheadError

        config = ResilienceConfig()
        bulkhead_manager = TaskBulkheadManager(config)
        storage = InMemoryStorage()
        circuit = CircuitProtectorPolicy(
            resource_key="test_propagate",
            storage=storage,
            failure_limit=Fraction(5, 10),
            cooldown=timedelta(seconds=30),
        )

        def raising_func() -> None:
            raise ValueError("Custom error")

        # Bulkhead wraps exceptions in BulkheadError
        with pytest.raises(BulkheadError) as exc_info:
            execute_with_resilience(
                bulkhead_manager=bulkhead_manager,
                circuit=circuit,
                task_type="shell",
                func=raising_func,
                func_args=(),
                timeout=10.0,
            )

        # The original error message should be in the BulkheadError
        assert "Custom error" in str(exc_info.value)


# =============================================================================
# Test Integration with Handler (Unit Level)
# =============================================================================


class TestResilienceIntegration:
    """Test resilience integration patterns."""

    def test_manager_and_factory_work_together(self) -> None:
        """BulkheadManager and CircuitFactory work together."""
        config = ResilienceConfig()
        bulkhead_manager = TaskBulkheadManager(config)
        circuit_factory = WorkflowCircuitFactory(config)

        # Simulate workflow execution
        workflow_id = "test_workflow_123"
        task_type = "http"

        circuit = circuit_factory.get_circuit(workflow_id, task_type)

        def task_execute() -> dict:
            return {"status": "success"}

        result = execute_with_resilience(
            bulkhead_manager=bulkhead_manager,
            circuit=circuit,
            task_type=task_type,
            func=task_execute,
            func_args=(),
            timeout=60.0,
        )

        assert result == {"status": "success"}

        # Cleanup
        circuit_factory.clear_workflow_circuits(workflow_id)
        bulkhead_manager.shutdown(wait=True, timeout=5.0)

    def test_multiple_workflows_isolated(self) -> None:
        """Multiple workflows have isolated circuit breakers."""
        config = ResilienceConfig()
        circuit_factory = WorkflowCircuitFactory(config)

        # Create circuits for different workflows
        workflow1_circuit = circuit_factory.get_circuit("wf1", "http")
        workflow2_circuit = circuit_factory.get_circuit("wf2", "http")

        # They should be independent
        assert workflow1_circuit is not workflow2_circuit

        # Tripping one shouldn't affect the other
        # (Internal state is per-circuit via namespace)

    def test_shutdown_cleanup(self) -> None:
        """Resources are properly cleaned up on shutdown."""
        config = ResilienceConfig()
        bulkhead_manager = TaskBulkheadManager(config)

        # Execute some work
        def simple_task() -> str:
            return "done"

        bulkhead_manager.execute_with_timeout("shell", simple_task, timeout=5.0)

        # Shutdown should not raise
        bulkhead_manager.shutdown(wait=True, timeout=5.0)


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
            target=lambda: manager.execute_with_timeout(
                "shell", stuck_task, timeout=60.0
            ),
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

        assert healthy_call() == "ok", (
            "shutdown refusals were recorded as circuit failures"
        )

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
