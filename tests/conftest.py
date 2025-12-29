"""Shared pytest fixtures for parameterized backend testing."""

import os
import subprocess
from collections.abc import Generator
from typing import Any

import pytest
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from stabilize.models.stage import StageExecution
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.postgres import PostgresWorkflowStore
from stabilize.persistence.store import WorkflowStore
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.queue import PostgresQueue, Queue
from stabilize.queue.sqlite_queue import SqliteQueue
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult


@pytest.fixture(autouse=True)
def reset_connection_manager() -> Generator[None, None, None]:
    """Reset singleton ConnectionManager between tests for isolation."""
    yield
    # Reset the singleton after each test
    SingletonMeta.reset(ConnectionManager)


# =============================================================================
# Shared Test Task Implementations
# =============================================================================


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


# =============================================================================
# PostgreSQL Container (Session-Scoped)
# =============================================================================


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start PostgreSQL container once per test session."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def postgres_url(postgres_container: PostgresContainer) -> str:
    """Get PostgreSQL connection URL and run migrations."""
    # testcontainers returns psycopg2 style URL, convert to psycopg3
    url = postgres_container.get_connection_url()
    # Replace 'postgresql+psycopg2://' with 'postgresql://' for psycopg3
    if "+psycopg2" in url:
        url = url.replace("+psycopg2", "")

    # Run migrations using mg command
    # mg needs to run from project root where mg.yaml and migrations/ exist
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = os.environ.copy()
    env["MG_DATABASE_URL"] = url
    result = subprocess.run(
        ["mg", "apply"],
        env=env,
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Migration failed: {result.stderr}")

    return str(url)


# =============================================================================
# Parameterized Backend Fixtures
# =============================================================================


@pytest.fixture(params=["sqlite", "postgres"])
def backend(request: pytest.FixtureRequest) -> str:
    """Parameterized backend - runs tests on both SQLite and PostgreSQL."""
    return str(request.param)


@pytest.fixture
def repository(
    backend: str,
    postgres_url: str,
) -> Generator[WorkflowStore, None, None]:
    """Create repository for current backend."""
    repo: WorkflowStore
    if backend == "sqlite":
        repo = SqliteWorkflowStore(
            connection_string="sqlite:///:memory:",
            create_tables=True,
        )
        yield repo
    else:
        # PostgreSQL: migrations already created tables via mg apply
        repo = PostgresWorkflowStore(
            connection_string=postgres_url,
            create_tables=False,
        )
        yield repo
        repo.close()


@pytest.fixture
def queue(
    backend: str,
    repository: WorkflowStore,
    postgres_url: str,
) -> Generator[Queue, None, None]:
    """Create queue for current backend."""
    q: Queue
    if backend == "sqlite":
        # Use same connection string as repository - singleton ConnectionManager
        # ensures they share the same thread-local connection for in-memory SQLite
        sqlite_q = SqliteQueue(
            connection_string="sqlite:///:memory:",
            table_name="queue_messages",
        )
        sqlite_q._create_table()
        q = sqlite_q
        yield q
        q.clear()
    else:
        # PostgreSQL: migrations already created tables via mg apply
        postgres_q = PostgresQueue(
            connection_string=postgres_url,
            table_name="queue_messages",
        )
        postgres_q.clear()
        q = postgres_q
        yield q
        q.clear()
        postgres_q.close()


# =============================================================================
# Pipeline Runner Setup Helper
# =============================================================================


def setup_stabilize(
    repository: WorkflowStore,
    queue: Queue,
) -> tuple[Any, Any]:
    """Set up a complete pipeline runner with all handlers."""
    from stabilize.handlers.complete_workflow import CompleteWorkflowHandler
    from stabilize.handlers.complete_stage import CompleteStageHandler
    from stabilize.handlers.complete_task import CompleteTaskHandler
    from stabilize.handlers.run_task import RunTaskHandler
    from stabilize.handlers.start_workflow import StartWorkflowHandler
    from stabilize.handlers.start_stage import StartStageHandler
    from stabilize.handlers.start_task import StartTaskHandler
    from stabilize.queue.processor import QueueProcessor
    from stabilize.orchestrator import Orchestrator
    from stabilize.tasks.registry import TaskRegistry

    task_registry = TaskRegistry()
    task_registry.register("success", SuccessTask)
    task_registry.register("fail", FailTask)
    task_registry.register("counter", CounterTask)

    processor = QueueProcessor(queue)

    handlers: list[Any] = [
        StartWorkflowHandler(queue, repository),
        StartStageHandler(queue, repository),
        StartTaskHandler(queue, repository),
        RunTaskHandler(queue, repository, task_registry),
        CompleteTaskHandler(queue, repository),
        CompleteStageHandler(queue, repository),
        CompleteWorkflowHandler(queue, repository),
    ]

    for handler in handlers:
        processor.register_handler(handler)

    runner = Orchestrator(queue)
    return processor, runner
