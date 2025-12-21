"""
Task registry for resolving task implementations.

This module provides the TaskRegistry class for registering and resolving
task implementations by name or type.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from stabilize.tasks.interface import CallableTask, Task
from stabilize.tasks.result import TaskResult

if False:  # TYPE_CHECKING
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)

# Type for task callable
TaskCallable = Callable[["StageExecution"], TaskResult]

# Type for task implementation - can be a Task class or callable
TaskImplementation = type[Task] | Task | TaskCallable


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

    def task(
        self,
        name: str,
        aliases: list[str] | None = None,
    ) -> Callable[[TaskCallable], TaskCallable]:
        """
        Decorator to register a function as a task.

        Args:
            name: The task type name
            aliases: Optional alternative names

        Returns:
            Decorator function

        Example:
            @registry.task("validate")
            def validate_inputs(stage):
                return TaskResult.success()
        """

        def decorator(func: TaskCallable) -> TaskCallable:
            self.register(name, func, aliases)
            return func

        return decorator

    def get(self, name: str) -> Task:
        """
        Get a task implementation by name.

        Args:
            name: The task type name

        Returns:
            A Task instance

        Raises:
            TaskNotFoundError: If task not found
        """
        # Check aliases first
        resolved_name = self._aliases.get(name, name)

        if resolved_name not in self._tasks:
            raise TaskNotFoundError(resolved_name)

        impl = self._tasks[resolved_name]

        # Handle different registration types
        if isinstance(impl, Task):
            return impl
        elif isinstance(impl, type) and issubclass(impl, Task):
            return impl()
        elif callable(impl):
            # Cast to the proper type for CallableTask
            from typing import cast

            func = cast(TaskCallable, impl)
            return CallableTask(func, name=resolved_name)
        else:
            raise TaskNotFoundError(resolved_name)

    def get_by_class(self, class_name: str) -> Task:
        """
        Get a task by its implementing class name.

        Args:
            class_name: Fully qualified or simple class name

        Returns:
            A Task instance

        Raises:
            TaskNotFoundError: If task not found
        """
        # Try exact match first
        if class_name in self._tasks:
            return self.get(class_name)

        # Try simple class name
        simple_name = class_name.split(".")[-1]
        if simple_name in self._tasks:
            return self.get(simple_name)

        # Try lowercase
        if simple_name.lower() in self._tasks:
            return self.get(simple_name.lower())

        raise TaskNotFoundError(class_name)

    def has(self, name: str) -> bool:
        """Check if a task is registered."""
        resolved_name = self._aliases.get(name, name)
        return resolved_name in self._tasks

    def list_tasks(self) -> list[str]:
        """Get all registered task names."""
        return list(self._tasks.keys())

    def clear(self) -> None:
        """Clear all registrations."""
        self._tasks.clear()
        self._aliases.clear()


# Global registry instance
_default_registry: TaskRegistry | None = None


def get_default_registry() -> TaskRegistry:
    """Get the default global task registry."""
    global _default_registry
    if _default_registry is None:
        _default_registry = TaskRegistry()
    return _default_registry


def register_task(
    name: str,
    task: TaskImplementation,
    aliases: list[str] | None = None,
) -> None:
    """Register a task in the default registry."""
    get_default_registry().register(name, task, aliases)


def get_task(name: str) -> Task:
    """Get a task from the default registry."""
    return get_default_registry().get(name)


def task(name: str, aliases: list[str] | None = None) -> Callable[[TaskCallable], TaskCallable]:
    """Decorator to register a task in the default registry."""
    return get_default_registry().task(name, aliases)
