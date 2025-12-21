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

    def register(
        self,
        name: str,
        task: TaskImplementation,
        aliases: list[str] | None = None,
    ) -> None:
        """
        Register a task implementation.

        Args:
            name: The task type name
            task: Task class, instance, or callable
            aliases: Optional alternative names

        Raises:
            ValueError: If name is already registered
        """
        if name in self._tasks:
            logger.warning(f"Overwriting existing task registration: {name}")

        self._tasks[name] = task

        # Register aliases
        if aliases:
            for alias in aliases:
                self._aliases[alias] = name

        # Check for aliases on the task itself
        if isinstance(task, type) and issubclass(task, Task):
            instance = task()
            for alias in instance.aliases:
                self._aliases[alias] = name
        elif isinstance(task, Task):
            for alias in task.aliases:
                self._aliases[alias] = name

        logger.debug(f"Registered task: {name}")

    def register_class(
        self,
        task_class: type[Task],
        name: str | None = None,
    ) -> None:
        """
        Register a task class using its class name.

        Args:
            task_class: The task class to register
            name: Optional name override
        """
        task_name = name or task_class.__name__
        self.register(task_name, task_class)
