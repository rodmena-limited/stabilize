from __future__ import annotations
import logging
from collections.abc import Callable
from stabilize.tasks.interface import CallableTask, Task
from stabilize.tasks.result import TaskResult
logger = logging.getLogger(__name__)
TaskCallable = Callable[["StageExecution"], TaskResult]
TaskImplementation = type[Task] | Task | TaskCallable
_default_registry: TaskRegistry | None = None
