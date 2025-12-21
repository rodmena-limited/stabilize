from __future__ import annotations
import logging
from collections.abc import Callable
from stabilize.tasks.interface import CallableTask, Task
from stabilize.tasks.result import TaskResult
logger = logging.getLogger(__name__)
TaskCallable = Callable[["StageExecution"], TaskResult]
TaskImplementation = type[Task] | Task | TaskCallable
_default_registry: TaskRegistry | None = None

class TaskNotFoundError(Exception):
    """Raised when a task type cannot be resolved."""
    def __init__(self, task_type: str):
        self.task_type = task_type
        super().__init__(f"No task found for type: {task_type}")

class TaskRegistry:
    """
    Registry for task implementations.

    Allows registering tasks by name and resolving them at runtime.
    Supports:
    - Task classes (instantiated on resolve)
    - Task instances (used directly)
    - Callable functions (wrapped in CallableTask)

    Example:
        registry = TaskRegistry()

        # Register a task class
        registry.register("deploy", DeployTask)

        # Register a task instance
        registry.register("notify", NotifyTask(slack_client))

        # Register a function
        @registry.task("validate")
        def validate_inputs(stage):
            return TaskResult.success()

        # Resolve and use
        task = registry.get("deploy")
        result = task.execute(stage)
    """
    def __init__(self) -> None:
        self._tasks: dict[str, TaskImplementation] = {}
        self._aliases: dict[str, str] = {}
