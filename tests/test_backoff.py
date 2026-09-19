"""Tests for exponential backoff with jitter, and transient-error classification.

The backoff tests drive the engine's own calculator and ``get_backoff_period``
rather than re-deriving the formula, so they fail if the engine's behaviour
changes.
"""

import tempfile
from datetime import timedelta

from resilient_circuit import ExponentialDelay

from stabilize import RunTaskHandler, SqliteQueue, SqliteWorkflowStore
from stabilize.errors import PermanentError, TransientError, is_transient
from stabilize.handlers.run_task.result import get_backoff_period
from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.queue.messages import RunTask
from stabilize.resilience.config import HandlerConfig
from stabilize.tasks.interface import RetryableTask
from stabilize.tasks.registry import TaskRegistry
from stabilize.tasks.result import TaskResult


def _live_run_task_handler() -> RunTaskHandler:
    """A RunTaskHandler wired to throwaway infrastructure."""
    tmp = tempfile.mkdtemp()
    url = f"sqlite:///{tmp}/backoff.db"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url, table_name="queue_messages")
    queue._create_table()
    return RunTaskHandler(queue, store, TaskRegistry())


def _retry_fixture(
    implementing_class: str = "noop",
) -> tuple[StageExecution, TaskExecution, RunTask]:
    """A stage/task/message triple shaped like a task about to be retried."""
    task_model = TaskExecution.create(
        name="t",
        implementing_class=implementing_class,
        stage_start=True,
        stage_end=True,
    )
    task_model.start_time = 0
    stage = StageExecution(ref_id="s", type="test", name="S", tasks=[task_model])
    message = RunTask(
        execution_type="PIPELINE",
        execution_id="e",
        stage_id=stage.id,
        task_id=task_model.id,
        task_type=implementing_class,
    )
    return stage, task_model, message


class TestExponentialBackoff:
    """Backoff as the engine actually computes it.

    These exercise ``RunTaskHandler``'s own delay calculator and
    ``get_backoff_period``. An earlier version of this class re-implemented the
    formula inline and asserted Python arithmetic against itself, so it passed
    whatever the engine did.
    """

    @staticmethod
    def _engine_delay(jitter: float | None = None) -> ExponentialDelay:
        """The delay calculator a live RunTaskHandler holds.

        Taken off a real handler rather than rebuilt, so a change to how the
        handler configures its backoff fails these tests.
        """
        handler = _live_run_task_handler()
        delay = handler._task_backoff
        if jitter is None:
            return delay
        return ExponentialDelay(
            min_delay=delay.min_delay,
            max_delay=delay.max_delay,
            factor=delay.factor,
            jitter=jitter,
        )

    def test_growth_and_cap_come_from_configured_delay(self) -> None:
        delay = self._engine_delay(jitter=0.0)
        observed = [delay.for_attempt(n) for n in range(1, 9)]
        assert observed == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]

    def test_jitter_stays_within_the_configured_band(self) -> None:
        config = HandlerConfig()
        delay = self._engine_delay()
        base = self._engine_delay(jitter=0.0).for_attempt(3)
        samples = [delay.for_attempt(3) for _ in range(200)]

        assert min(samples) >= base * (1 - config.concurrency_jitter)
        assert max(samples) <= base * (1 + config.concurrency_jitter)
        assert len(set(samples)) > 1

    def test_get_backoff_period_uses_the_configured_delay(self) -> None:
        stage, task_model, message = _retry_fixture()
        period = get_backoff_period(
            stage,
            task_model,
            message,
            attempt=3,
            task_registry=TaskRegistry(),
            task_backoff=self._engine_delay(jitter=0.0),
            current_time_fn=lambda: 0,
        )
        assert period == timedelta(seconds=4.0)

    def test_get_backoff_period_escalates_with_attempt(self) -> None:
        stage, task_model, message = _retry_fixture()
        registry = TaskRegistry()
        backoff = self._engine_delay(jitter=0.0)
        periods = [
            get_backoff_period(
                stage, task_model, message, n, registry, backoff, lambda: 0
            )
            for n in range(1, 6)
        ]
        assert periods == sorted(periods)
        assert periods[0] < periods[-1]

    def test_retryable_task_backoff_overrides_the_configured_delay(self) -> None:
        class SlowRetryable(RetryableTask):
            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.success()

            def get_timeout(self) -> timedelta:
                return timedelta(seconds=60)

            def get_backoff_period(
                self, stage: StageExecution, duration: timedelta
            ) -> timedelta:
                return timedelta(seconds=42)

        registry = TaskRegistry()
        registry.register("slow", SlowRetryable)
        stage, task_model, message = _retry_fixture(implementing_class="slow")

        period = get_backoff_period(
            stage,
            task_model,
            message,
            attempt=1,
            task_registry=registry,
            task_backoff=self._engine_delay(jitter=0.0),
            current_time_fn=lambda: 0,
        )
        assert period == timedelta(seconds=42)


class TestIsTransient:
    """Tests for the is_transient error classification."""

    def test_transient_error_is_transient(self) -> None:
        """TransientError should be classified as transient."""
        error = TransientError("Connection timeout")
        assert is_transient(error)

    def test_permanent_error_is_not_transient(self) -> None:
        """PermanentError should not be classified as transient."""
        error = PermanentError("Invalid input")
        assert not is_transient(error)

    def test_timeout_errors_are_transient(self) -> None:
        """Errors with 'timeout' in name should be transient."""

        class SomeTimeoutError(Exception):
            pass

        error = SomeTimeoutError("Request timed out")
        assert is_transient(error)

    def test_connection_errors_are_transient(self) -> None:
        """Errors with 'connection' in name should be transient."""

        class ConnectionRefusedError(Exception):
            pass

        error = ConnectionRefusedError("Connection refused")
        assert is_transient(error)

    def test_temporary_errors_are_transient(self) -> None:
        """Errors with 'temporary' in name should be transient."""

        class TemporaryFailureError(Exception):  # noqa: N818 - intentional name for testing
            pass

        error = TemporaryFailureError("Service temporarily unavailable")
        assert is_transient(error)

    def test_validation_errors_are_not_transient(self) -> None:
        """Validation errors should not be classified as transient."""
        from stabilize.errors import is_permanent

        class ValidationError(Exception):
            pass

        error = ValidationError("Invalid input")
        assert is_permanent(error)
        assert not is_transient(error)

    def test_standard_exceptions_not_transient_by_default(self) -> None:
        """Standard exceptions should not be transient by default."""
        error = ValueError("Bad value")
        assert not is_transient(error)

        error = TypeError("Wrong type")
        assert not is_transient(error)

    def test_rate_limit_errors_are_transient(self) -> None:
        """Rate limit errors should be transient."""

        class RateLimitExceededError(Exception):
            pass

        error = RateLimitExceededError("Too many requests")
        assert is_transient(error)

    def test_throttling_errors_are_transient(self) -> None:
        """Throttling errors should be transient."""

        class ThrottlingError(Exception):
            pass

        error = ThrottlingError("Request throttled")
        assert is_transient(error)
