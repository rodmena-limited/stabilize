import os
import subprocess
from collections.abc import Generator
from typing import Any
import pytest
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    Task,
    TaskRegistry,
    TaskResult,
)
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.postgres import PostgresWorkflowStore
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import PostgresQueue, Queue

def reset_connection_manager() -> Generator[None, None, None]:
    """Reset singleton ConnectionManager between tests for isolation."""
    yield
    # Reset the singleton after each test
    SingletonMeta.reset(ConnectionManager)

def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start PostgreSQL container once per test session."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres

class SuccessTask(Task):
    """A task that always succeeds."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"success": True})

class FailTask(Task):
    """A task that always fails."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("Task failed intentionally")

class CounterTask(Task):
    """A task that increments a counter."""
    counter: int = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        CounterTask.counter += 1
        return TaskResult.success(outputs={"count": CounterTask.counter})
