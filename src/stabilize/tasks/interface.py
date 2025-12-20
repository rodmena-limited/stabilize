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
