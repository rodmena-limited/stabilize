"""
Stabilize - Highway Workflow Engine execution layer.

This package provides a message-driven DAG execution engine for running
workflows with full support for:
- Parallel and sequential stage execution
- Synthetic stages (before/after/onFailure)
- PostgreSQL and SQLite persistence
- Pluggable task system
"""

__version__ = "0.9.0"

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.tasks.interface import RetryableTask, Task
from stabilize.tasks.result import TaskResult

__all__ = [
    "WorkflowStatus",
    "Workflow",
    "StageExecution",
    "TaskExecution",
    "TaskResult",
    "Task",
    "RetryableTask",
]
