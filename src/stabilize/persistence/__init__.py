"""Persistence layer for pipeline execution."""

from stabilize.persistence.factory import (
    create_queue,
    create_repository,
    detect_backend,
)
from stabilize.persistence.memory import InMemoryWorkflowStore
from stabilize.persistence.postgres import PostgresWorkflowStore
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.persistence.store import (
    WorkflowNotFoundError,
    WorkflowStore,
)

__all__ = [
    # Abstract interface
    "WorkflowStore",
    "WorkflowNotFoundError",
    # Implementations
    "PostgresWorkflowStore",
    "SqliteWorkflowStore",
    "InMemoryWorkflowStore",
    # Factory functions
    "create_repository",
    "create_queue",
    "detect_backend",
]
