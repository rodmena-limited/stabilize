"""
Task interface definitions.

This module defines the Task interface and its variants (RetryableTask,
SkippableTask) that all task implementations must follow.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


class Task(ABC):
    """
    Base interface for all tasks.

    Tasks are the atomic units of work in a pipeline. Each task:
    - Receives the current stage context
    - Performs some work
    - Returns a TaskResult indicating status and any outputs

    Example:
        class DeployTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                # Get inputs from context
                cluster = stage.context.get("cluster")
                image = stage.context.get("image")

                # Do the work
                deployment_id = deploy(cluster, image)

                # Return result with outputs
                return TaskResult.success(
                    outputs={"deploymentId": deployment_id}
                )
    """

    reset_context_keys: tuple[str, ...] = ()

    @abstractmethod
    def execute(self, stage: StageExecution) -> TaskResult:
        """
        Execute the task.

        Args:
            stage: The stage execution context

        Returns:
            TaskResult indicating status and any outputs

        Raises:
            Exception: Any exception will be caught and handled by the runner
        """
        pass

    def on_timeout(self, stage: StageExecution) -> TaskResult | None:
        """
        Called when the task times out.

        Override to provide custom timeout handling. If None is returned,
        the default timeout behavior applies.

        Args:
            stage: The stage execution context

        Returns:
            Optional TaskResult to use instead of default timeout
        """
        return None

    def on_cancel(self, stage: StageExecution) -> TaskResult | None:
        """
        Called when the execution is canceled.

        Override to provide cleanup logic when execution is canceled.

        Args:
            stage: The stage execution context

        Returns:
            Optional TaskResult with cleanup results
        """
        return None

    def on_cleanup(self, stage: StageExecution) -> None:
        """
        Called when stage enters terminal state.

        Override to clean up resources when the stage completes, fails,
        or is canceled. This is called after the stage status is set
        but before the CompleteStage message is pushed.

        Unlike on_cancel (which is called only on cancellation), on_cleanup
        is called for all terminal states: TERMINAL, CANCELED, STOPPED,
        SUCCEEDED, FAILED_CONTINUE, and SKIPPED.

        Use this for:
        - Releasing external resources (containers, connections)
        - Cleaning up temporary files
        - Sending notifications
        - Recording metrics

        Args:
            stage: The stage execution context

        Example:
            def on_cleanup(self, stage: StageExecution) -> None:
                container_id = stage.context.get("container_id")
                if container_id:
                    docker_client.remove(container_id, force=True)
        """
        pass

    @property
    def aliases(self) -> list[str]:
        """
        Alternative names for this task type.

        Used for backward compatibility when task types are renamed.

        Returns:
            List of alternative type names
        """
        return []


class RetryableTask(Task):
    """
    A task that can be retried with timeout and backoff.

    Retryable tasks return RUNNING status while waiting for some condition.
    They are re-executed after a backoff period until they succeed, fail,
    or timeout.

    Example:
        class WaitForDeployTask(RetryableTask):
            def get_timeout(self) -> timedelta:
                return timedelta(minutes=30)

            def get_backoff_period(self, stage, duration) -> timedelta:
                return timedelta(seconds=10)

            def execute(self, stage: StageExecution) -> TaskResult:
                deployment_id = stage.context.get("deploymentId")
                status = check_deployment_status(deployment_id)

                if status == "complete":
                    return TaskResult.success()
                elif status == "failed":
                    return TaskResult.terminal("Deployment failed")
                else:
                    return TaskResult.running()
    """

    @abstractmethod
    def get_timeout(self) -> timedelta:
        """
        Get the total time this task may spend across all its executions.

        Measured from the task's first start. When it is exceeded the engine
        calls on_timeout(stage); a returned TaskResult is processed as the
        task's result, and None completes the task as TERMINAL.

        Returns:
            Total lifecycle limit
        """
        pass

    def get_execution_timeout(self, stage: StageExecution) -> timedelta:
        """
        Get the limit for a single execute() call.

        Defaults to get_dynamic_timeout(stage), the lifecycle limit. Override
        to bound each call separately, e.g. a 30 s poll inside a 2 h wait.

        Args:
            stage: The stage execution context

        Returns:
            Per-call limit
        """
        return self.get_dynamic_timeout(stage)

    def get_backoff_period(
        self,
        stage: StageExecution,
        duration: timedelta,
    ) -> timedelta:
        """
        Get the backoff period before retrying.

        Override to implement dynamic backoff based on how long
        the task has been running.

        Args:
            stage: The stage execution context
            duration: How long the task has been running

        Returns:
            Time to wait before retrying
        """
        return timedelta(seconds=1)

    def get_dynamic_timeout(self, stage: StageExecution) -> timedelta:
        """
        Get dynamic timeout based on stage context.

        Override to implement context-based timeouts.

        Args:
            stage: The stage execution context

        Returns:
            Timeout duration
        """
        return self.get_timeout()

    def get_dynamic_backoff_period(
        self,
        stage: StageExecution,
        duration: timedelta,
    ) -> timedelta:
        """
        Get dynamic backoff based on stage context.

        Args:
            stage: The stage execution context
            duration: How long the task has been running

        Returns:
            Time to wait before retrying
        """
        return self.get_backoff_period(stage, duration)


class OverridableTimeoutRetryableTask(RetryableTask):
    """
    A retryable task whose timeout can be overridden by the stage.

    The stage can set a 'stageTimeoutMs' context value to override
    the default timeout.
    """

    def get_dynamic_timeout(self, stage: StageExecution) -> timedelta:
        """Get timeout, potentially overridden by stage context."""
        if "stageTimeoutMs" in stage.context:
            return timedelta(milliseconds=stage.context["stageTimeoutMs"])
        return self.get_timeout()


class SkippableTask(Task):
    """
    A task that can be conditionally skipped.

    Override is_enabled() to control when the task should be skipped.
    """

    def is_enabled(self, stage: StageExecution) -> bool:
        """
        Check if this task is enabled.

        Override to implement skip logic.

        Args:
            stage: The stage execution context

        Returns:
            True if task should execute, False to skip
        """
        return True

    def execute(self, stage: StageExecution) -> TaskResult:
        """Run do_execute.

        The engine evaluates is_enabled once, before the task first runs
        (StartTask), and skips the task when it returns False; execute does not
        evaluate it again.
        """
        return self.do_execute(stage)

    @abstractmethod
    def do_execute(self, stage: StageExecution) -> TaskResult:
        """
        Perform the actual task execution.

        Args:
            stage: The stage execution context

        Returns:
            TaskResult indicating status
        """
        pass


class CallableTask(Task):
    """
    A task that wraps a callable function.

    Allows using simple functions as tasks without creating a class.

    Example:
        def my_task(stage: StageExecution) -> TaskResult:
            return TaskResult.success(outputs={"result": "done"})

        task = CallableTask(my_task)
    """

    def __init__(
        self,
        func: Callable[[StageExecution], TaskResult],
        name: str | None = None,
    ) -> None:
        """
        Initialize with a callable.

        Args:
            func: The function to call
            name: Optional name for the task
        """
        self._func = func
        self._name = name or func.__name__

    def execute(self, stage: StageExecution) -> TaskResult:
        """Execute the wrapped function."""
        return self._func(stage)

    @property
    def name(self) -> str:
        """Get the task name."""
        return self._name


class NoOpTask(Task):
    """
    A task that does nothing.

    Useful for testing or placeholder stages.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        """Return success immediately."""
        return TaskResult.success()


TIMER_ARMED_AT = "_timer_armed_at"
TIMER_DUE_AT = "_timer_due_at"


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


def parse_until(value: object) -> int:
    """The epoch millisecond an ``until`` value names: ISO 8601 with an offset, or epoch seconds."""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"until must be an ISO 8601 time with an offset or epoch seconds, not {value!r}")
    if isinstance(value, (int, float)):
        return int(float(value) * 1000)
    if isinstance(value, str):
        text = value.strip()
        try:
            return int(float(text) * 1000)
        except ValueError:
            pass
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(f"until {value!r} is not an ISO 8601 time: {e}") from e
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"until {value!r} carries no UTC offset")
        return int(parsed.timestamp() * 1000)
    raise ValueError(f"until must be an ISO 8601 time with an offset or epoch seconds, not {value!r}")


class TimerTask(RetryableTask):
    """
    A durable timer: fires at an absolute time and re-arms on an early delivery.

    Reads exactly one of ``until`` (ISO 8601 with an offset, or epoch seconds) and
    ``duration_seconds`` (counted from the first execution) from the stage context.
    The first execution writes ``_timer_armed_at`` and ``_timer_due_at`` (epoch ms) and
    asks to run again at the due time; every later execution before the due time asks
    again; the one at or after it succeeds.
    """

    reset_context_keys: tuple[str, ...] = (TIMER_ARMED_AT, TIMER_DUE_AT)
    grace = timedelta(minutes=5)
    unarmed_timeout = timedelta(hours=1)

    def get_timeout(self) -> timedelta:
        return self.unarmed_timeout

    def get_dynamic_timeout(self, stage: StageExecution) -> timedelta:
        due = stage.context.get(TIMER_DUE_AT)
        armed = stage.context.get(TIMER_ARMED_AT)
        if isinstance(due, int) and isinstance(armed, int):
            return timedelta(milliseconds=max(0, due - armed)) + self.grace
        return self.get_timeout()

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        due = stage.context.get(TIMER_DUE_AT)
        if isinstance(due, int):
            return timedelta(milliseconds=max(0, due - _now_ms()))
        return timedelta(seconds=1)

    def due_at(self, stage: StageExecution, now_ms: int) -> int:
        """The epoch millisecond this timer fires at; raises ValueError when the context is not a timer."""
        until = stage.context.get("until")
        duration = stage.context.get("duration_seconds")
        if (until is None) == (duration is None):
            raise ValueError("TimerTask needs exactly one of 'until' and 'duration_seconds' in the stage context")
        if until is not None:
            return parse_until(until)
        try:
            seconds = float(duration)  # type: ignore[arg-type]
        except (TypeError, ValueError) as e:
            raise ValueError(f"duration_seconds {duration!r} is not a number") from e
        if not seconds > 0:
            raise ValueError(f"duration_seconds must be greater than zero, not {duration!r}")
        return now_ms + int(seconds * 1000)

    def armed_context(self, stage: StageExecution, now_ms: int, due_ms: int) -> dict[str, Any]:
        return {}

    def fired(self, stage: StageExecution, now_ms: int, due_ms: int) -> TaskResult:
        return TaskResult.success()

    def execute(self, stage: StageExecution) -> TaskResult:
        now = _now_ms()
        due = stage.context.get(TIMER_DUE_AT)
        if not isinstance(due, int):
            try:
                due = self.due_at(stage, now)
            except ValueError as e:
                return TaskResult.terminal(str(e), context={"exception": {"details": {"error": str(e)}}})
            context = {TIMER_ARMED_AT: now, TIMER_DUE_AT: due}
            context.update(self.armed_context(stage, now, due))
            return TaskResult.running(context=context, scheduled_time=due)
        if now >= due:
            return self.fired(stage, now, due)
        return TaskResult.running(scheduled_time=due)


class WaitTask(TimerTask):
    """
    Waits ``waitTime`` seconds from its first execution.

    Keeps ``waitStartTime`` (epoch seconds) in the stage context and reports
    ``waitedSeconds`` in the outputs, as it always has; the due time is
    ``waitStartTime + waitTime`` and the task runs twice, to arm and to fire.
    """

    reset_context_keys: tuple[str, ...] = (TIMER_ARMED_AT, TIMER_DUE_AT, "waitStartTime")

    def _start_seconds(self, stage: StageExecution, now_ms: int) -> int:
        start = stage.context.get("waitStartTime")
        if isinstance(start, (int, float)) and not isinstance(start, bool):
            return int(start)
        return now_ms // 1000

    def due_at(self, stage: StageExecution, now_ms: int) -> int:
        wait_time = stage.context.get("waitTime", 0)
        try:
            seconds = float(wait_time)
        except (TypeError, ValueError) as e:
            raise ValueError(f"waitTime {wait_time!r} is not a number") from e
        if seconds < 0:
            raise ValueError(f"waitTime must not be negative, not {wait_time!r}")
        return self._start_seconds(stage, now_ms) * 1000 + int(seconds * 1000)

    def armed_context(self, stage: StageExecution, now_ms: int, due_ms: int) -> dict[str, Any]:
        return {"waitStartTime": self._start_seconds(stage, now_ms)}

    def fired(self, stage: StageExecution, now_ms: int, due_ms: int) -> TaskResult:
        return TaskResult.success(outputs={"waitedSeconds": now_ms // 1000 - self._start_seconds(stage, now_ms)})
