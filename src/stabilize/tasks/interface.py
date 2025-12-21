"""
Task interface definitions.

This module defines the Task interface and its variants (RetryableTask,
SkippableTask) that all task implementations must follow.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING

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
        Get the maximum time this task can run before timing out.

        Returns:
            Maximum execution time
        """
        pass

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
        """Execute the task if enabled."""
        if not self.is_enabled(stage):
            return TaskResult.skipped()
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


class WaitTask(RetryableTask):
    """
    A task that waits for a specified duration.

    Reads 'waitTime' from stage context (in seconds).
    """

    def get_timeout(self) -> timedelta:
        """Wait tasks have a long timeout."""
        return timedelta(hours=24)

    def get_backoff_period(
        self,
        stage: StageExecution,
        duration: timedelta,
    ) -> timedelta:
        """Check every second."""
        return timedelta(seconds=1)

    def execute(self, stage: StageExecution) -> TaskResult:
        """Wait for the specified time."""
        import time

        wait_time = stage.context.get("waitTime", 0)
        start_time = stage.context.get("waitStartTime")
        current_time = int(time.time())

        if start_time is None:
            # First execution - record start time
            return TaskResult.running(context={"waitStartTime": current_time})

        elapsed = current_time - start_time
        if elapsed >= wait_time:
            return TaskResult.success(outputs={"waitedSeconds": elapsed})

        return TaskResult.running()
