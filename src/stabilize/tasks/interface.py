from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING
from stabilize.tasks.result import TaskResult

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
