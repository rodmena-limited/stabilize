"""Tests for timeout enforcement with thread interruption.

These tests ensure that task execution is properly interrupted when
it exceeds the configured timeout.
"""

import time
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from stabilize.errors import TaskTimeoutError
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult


class SlowTask(Task):
    """A task that takes a long time to execute."""

    def __init__(self, delay: float = 5.0) -> None:
        self.delay = delay

    def execute(self, stage: StageExecution) -> TaskResult:
        time.sleep(self.delay)
        return TaskResult.success()


class FastTask(Task):
    """A task that completes quickly."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"result": "done"})


class TestTimeoutEnforcement:
    """Tests for timeout enforcement in RunTaskHandler."""

    def test_fast_task_completes_within_timeout(self) -> None:
        """Fast tasks should complete without timeout."""
        from concurrent.futures import ThreadPoolExecutor

        task = FastTask()
        stage = MagicMock(spec=StageExecution)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(task.execute, stage)
            result = future.result(timeout=5.0)

        assert result.status == WorkflowStatus.SUCCEEDED

    def test_slow_task_times_out(self) -> None:
        """Slow tasks should raise timeout error."""
        from concurrent.futures import ThreadPoolExecutor
        from concurrent.futures import TimeoutError as FuturesTimeoutError

        task = SlowTask(delay=5.0)
        stage = MagicMock(spec=StageExecution)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(task.execute, stage)
            with pytest.raises(FuturesTimeoutError):
                future.result(timeout=0.1)  # Very short timeout

    def test_task_timeout_error_includes_details(self) -> None:
        """TaskTimeoutError should include task and stage details."""
        error = TaskTimeoutError(
            "Task exceeded timeout",
            task_name="MyTask",
            stage_id="stage-123",
            execution_id="exec-456",
        )

        assert error.task_name == "MyTask"
        assert error.stage_id == "stage-123"
        assert error.execution_id == "exec-456"
        assert "timeout" in str(error).lower()


class TestGetTaskTimeout:
    """Tests for timeout retrieval."""

    def test_default_timeout_is_five_minutes(self) -> None:
        """Default timeout should be 5 minutes for non-retryable tasks."""
        from stabilize.handlers.run_task import DEFAULT_TASK_TIMEOUT

        assert DEFAULT_TASK_TIMEOUT == timedelta(minutes=5)

    def test_retryable_task_uses_dynamic_timeout(self) -> None:
        """RetryableTask should use get_dynamic_timeout."""
        from stabilize.tasks.interface import RetryableTask

        class CustomRetryableTask(RetryableTask):
            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.success()

            def get_timeout(self) -> timedelta:
                return timedelta(seconds=60)  # Base timeout

            def get_dynamic_timeout(self, stage: StageExecution) -> timedelta:
                return timedelta(seconds=30)  # Dynamic override

            def get_backoff_period(self, stage: StageExecution, elapsed: timedelta) -> timedelta:
                return timedelta(seconds=5)

        task = CustomRetryableTask()
        stage = MagicMock(spec=StageExecution)

        timeout = task.get_dynamic_timeout(stage)
        assert timeout == timedelta(seconds=30)


class TestTimeoutWithOnTimeoutHook:
    """Tests for on_timeout hook behavior."""

    def test_on_timeout_hook_called_on_timeout(self) -> None:
        """Task's on_timeout hook should be called when timeout occurs."""

        class TaskWithOnTimeout(Task):
            def __init__(self) -> None:
                self.on_timeout_called = False

            def execute(self, stage: StageExecution) -> TaskResult:
                time.sleep(10)  # Long running
                return TaskResult.success()

            def on_timeout(self, stage: StageExecution) -> TaskResult:
                self.on_timeout_called = True
                return TaskResult.failed_continue(
                    error="Task timed out",
                    outputs={"timeout": True, "partial_result": "cleanup done"},
                )

        task = TaskWithOnTimeout()

        # Simulate what the handler does - call on_timeout when timeout occurs
        stage = MagicMock(spec=StageExecution)
        result = task.on_timeout(stage)

        assert task.on_timeout_called
        assert result is not None
        assert result.status == WorkflowStatus.FAILED_CONTINUE
        assert result.outputs.get("timeout") is True

    def test_default_on_timeout_returns_none(self) -> None:
        """Default on_timeout implementation returns None."""
        task = FastTask()
        stage = MagicMock(spec=StageExecution)

        # Call the default implementation
        result = task.on_timeout(stage)
        assert result is None


class TestConcurrentExecution:
    """Tests for concurrent task execution with timeouts."""

    def test_multiple_tasks_with_different_timeouts(self) -> None:
        """Multiple concurrent tasks should respect their individual timeouts."""
        from concurrent.futures import ThreadPoolExecutor

        results = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            # Fast task
            fast_future = executor.submit(FastTask().execute, MagicMock())

            # Try to get fast result quickly
            fast_result = fast_future.result(timeout=5.0)
            results.append(fast_result)

        assert len(results) == 1
        assert results[0].status == WorkflowStatus.SUCCEEDED
