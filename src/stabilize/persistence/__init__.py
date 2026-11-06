"""Persistence layer for pipeline execution."""

from __future__ import annotations

from stabilize.persistence.factory import (
    create_queue,
    create_repository,
    detect_backend,
)
from stabilize.persistence.postgres import PostgresWorkflowStore
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.persistence.store import (
    WorkflowNotFoundError,
    WorkflowStore,
)

__all__ = [
    # Implementations
    "PostgresWorkflowStore",
    "SqliteWorkflowStore",
    "WorkflowNotFoundError",
    # Abstract interface
    "WorkflowStore",
    "create_queue",
    # Factory functions
    "create_repository",
    "detect_backend",
]
