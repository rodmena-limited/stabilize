"""Tests for resilience module (bulkhead and circuit breaker integration)."""

import os
import time
from datetime import timedelta
from fractions import Fraction
from unittest.mock import patch

import pytest
from resilient_circuit import CircuitProtectorPolicy
from resilient_circuit.exceptions import ProtectedCallError
from resilient_circuit.storage import InMemoryStorage

from stabilize.errors import TransientError
from stabilize.resilience.bulkheads import TaskBulkheadManager
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
        """A task's own exception propagates as itself, not as bulkman's BulkheadError wrapper."""
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

        with pytest.raises(ValueError) as exc_info:
            execute_with_resilience(
                bulkhead_manager=bulkhead_manager,
                circuit=circuit,
                task_type="shell",
                func=raising_func,
                func_args=(),
                timeout=10.0,
            )

        assert str(exc_info.value) == "Custom error"
