"""Shared pytest fixtures for parameterized backend testing."""

import os
import subprocess
from collections.abc import Generator
from typing import Any

import pytest

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
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

# Optional PostgreSQL dependencies - only required for postgres tests
try:
    from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False
    PostgresContainer = None  # type: ignore[misc, assignment]

try:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.queue import PostgresQueue

    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    PostgresWorkflowStore = None  # type: ignore[misc, assignment]
    PostgresQueue = None  # type: ignore[misc, assignment]


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
def postgres_container() -> Generator[Any, None, None]:
    """Start PostgreSQL container once per test session."""
    if not HAS_TESTCONTAINERS:
        pytest.skip("testcontainers not installed")
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def postgres_url(postgres_container: Any) -> str:
    """Get PostgreSQL connection URL and run migrations."""
    if not HAS_TESTCONTAINERS:
        pytest.skip("testcontainers not installed")

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


def _get_available_backends() -> list[str]:
    """Return list of available backends based on installed dependencies."""
    backends = ["sqlite"]
    if HAS_TESTCONTAINERS and HAS_POSTGRES:
        backends.append("postgres")
    return backends


@pytest.fixture(params=_get_available_backends())
def backend(request: pytest.FixtureRequest) -> str:
    """Parameterized backend - runs tests on available backends."""
    return str(request.param)


@pytest.fixture
def repository(
    backend: str,
    request: pytest.FixtureRequest,
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
        if not HAS_POSTGRES:
            pytest.skip("psycopg not installed")
        postgres_url = request.getfixturevalue("postgres_url")
        repo = PostgresWorkflowStore(connection_string=postgres_url)
        yield repo
        repo.close()


@pytest.fixture
def queue(
    backend: str,
    repository: WorkflowStore,
    request: pytest.FixtureRequest,
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
        if not HAS_POSTGRES:
            pytest.skip("psycopg not installed")
        postgres_url = request.getfixturevalue("postgres_url")
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
