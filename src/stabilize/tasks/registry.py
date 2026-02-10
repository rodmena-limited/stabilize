"""
Task registry for resolving task implementations.

This module provides the TaskRegistry class for registering and resolving
task implementations by name or type.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from stabilize.tasks.interface import CallableTask, Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.verification import Verifier, VerifyResult

logger = logging.getLogger(__name__)

# Type for task callable
TaskCallable = Callable[["StageExecution"], TaskResult]

# Type for verifier callable
VerifierCallable = Callable[["StageExecution"], "VerifyResult"]

# Type for verifier implementation - can be a Verifier instance or callable
VerifierImplementation = "Verifier | VerifierCallable"

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
        # Stores either Verifier instances or callable functions
        self._verifiers: dict[str, Verifier | VerifierCallable] = {}

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
            logger.warning("Overwriting existing task registration: %s", name)

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

        logger.debug("Registered task: %s", name)

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
        self._verifiers.clear()

    # ========== Verifier Registry Methods ==========

    def register_verifier(
        self,
        name: str,
        verifier: Verifier | VerifierCallable,
    ) -> None:
        """
        Register a verifier function or Verifier instance.

        Verifiers validate stage outputs after task completion.
        They can be referenced in stage context verification config.

        When registering a Verifier instance, its own max_retries and
        retry_delay_seconds properties will be used instead of config defaults.

        Args:
            name: The verifier name
            verifier: Verifier instance or callable that takes StageExecution
                     and returns VerifyResult

        Example:
            # Register a simple callable
            def check_url(stage):
                url = stage.outputs.get("url")
                if not url:
                    return VerifyResult.failed("No URL")
                return VerifyResult.ok()

            registry.register_verifier("check_url", check_url)

            # Register a Verifier instance with custom retry settings
            class URLVerifier(Verifier):
                @property
                def max_retries(self) -> int:
                    return 5  # Custom retry count

                def verify(self, stage):
                    ...

            registry.register_verifier("url_verifier", URLVerifier())
        """
        if name in self._verifiers:
            logger.warning("Overwriting existing verifier registration: %s", name)

        self._verifiers[name] = verifier
        logger.debug("Registered verifier: %s", name)

    def verifier(
        self,
        name: str,
    ) -> Callable[[VerifierCallable], VerifierCallable]:
        """
        Decorator to register a function as a verifier.

        Args:
            name: The verifier name

        Returns:
            Decorator function

        Example:
            @registry.verifier("check_url")
            def check_url(stage):
                url = stage.outputs.get("url")
                return VerifyResult.ok() if url else VerifyResult.failed("No URL")
        """

        def decorator(func: VerifierCallable) -> VerifierCallable:
            self.register_verifier(name, func)
            return func

        return decorator

    def get_verifier(self, name: str) -> Verifier | VerifierCallable:
        """
        Get a verifier by name.

        Args:
            name: The verifier name

        Returns:
            The verifier (either a Verifier instance or a callable)

        Raises:
            TaskNotFoundError: If verifier not found

        Note:
            If a Verifier instance is returned, its max_retries and
            retry_delay_seconds properties should be used for retry config.
            If a callable is returned, it should be wrapped in CallableVerifier
            with config defaults.
        """
        if name not in self._verifiers:
            raise TaskNotFoundError(f"verifier:{name}")

        return self._verifiers[name]

    def has_verifier(self, name: str) -> bool:
        """Check if a verifier is registered."""
        return name in self._verifiers

    def list_verifiers(self) -> list[str]:
        """Get all registered verifier names."""
        return list(self._verifiers.keys())


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


def register_verifier(name: str, verifier: Verifier | VerifierCallable) -> None:
    """Register a verifier in the default registry."""
    get_default_registry().register_verifier(name, verifier)


def get_verifier(name: str) -> Verifier | VerifierCallable:
    """Get a verifier from the default registry."""
    return get_default_registry().get_verifier(name)


def verifier(name: str) -> Callable[[VerifierCallable], VerifierCallable]:
    """Decorator to register a verifier in the default registry."""
    return get_default_registry().verifier(name)
