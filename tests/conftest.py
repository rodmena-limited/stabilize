"""Shared pytest fixtures for parameterized backend testing."""

import os
import subprocess
import threading
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from stabilize import (
    CancelStageHandler,
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    ContinueParentStageHandler,
    JumpToStageHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    SkipStageHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWaitingWorkflowsHandler,
    StartWorkflowHandler,
    Task,
    TaskRegistry,
    TaskResult,
)
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue

# Optional PostgreSQL dependencies - only required for postgres tests
try:
    from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False
    PostgresContainer = None  # type: ignore[misc, assignment]

try:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue import PostgresQueue

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
    # Use the venv's mg, not the system mg (which might be a text editor)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    venv_mg = os.path.join(project_root, ".venv", "bin", "mg")
    env = os.environ.copy()
    env["MG_DATABASE_URL"] = url
    result = subprocess.run(
        [venv_mg, "apply", "--yes"],
        env=env,
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Migration failed (exit {result.returncode}): stdout={result.stdout}, stderr={result.stderr}"
        )

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
        # Clear all test data to ensure isolation between tests
        # Use the internal pool to run cleanup queries
        pool = repo._pool  # Access internal pool for cleanup
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Clear in correct order due to foreign keys (children first)
                cur.execute("DELETE FROM task_executions")
                cur.execute("DELETE FROM stage_executions")
                cur.execute("DELETE FROM pipeline_executions")
                cur.execute("DELETE FROM processed_messages")
            conn.commit()
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
        try:
            q.clear()
        except Exception:
            pass  # Table may not exist if test created its own DB
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
    extra_tasks: dict[str, type] | None = None,
) -> tuple[Any, Any, TaskRegistry]:
    """Set up a complete pipeline runner with all handlers.

    Args:
        repository: The workflow store
        queue: The message queue
        extra_tasks: Optional dict of task_name -> task_class to register

    Returns:
        Tuple of (processor, orchestrator, task_registry)
    """
    task_registry = TaskRegistry()
    task_registry.register("success", SuccessTask)
    task_registry.register("fail", FailTask)
    task_registry.register("counter", CounterTask)

    # Register any extra tasks
    if extra_tasks:
        for name, task_class in extra_tasks.items():
            task_registry.register(name, task_class)

    processor = QueueProcessor(queue)

    handlers: list[Any] = [
        StartWorkflowHandler(queue, repository),
        StartWaitingWorkflowsHandler(queue, repository),
        StartStageHandler(queue, repository),
        SkipStageHandler(queue, repository),
        CancelStageHandler(queue, repository),
        ContinueParentStageHandler(queue, repository),
        JumpToStageHandler(queue, repository),
        StartTaskHandler(queue, repository, task_registry),
        RunTaskHandler(queue, repository, task_registry),
        CompleteTaskHandler(queue, repository),
        CompleteStageHandler(queue, repository),
        CompleteWorkflowHandler(queue, repository),
    ]

    for handler in handlers:
        processor.register_handler(handler)

    runner = Orchestrator(queue)
    return processor, runner, task_registry


# =============================================================================
# Stress Test Fixtures
# =============================================================================


@pytest.fixture
def file_db_path(tmp_path: Path) -> Path:
    """Provide a file-based SQLite database path for cross-thread tests."""
    return tmp_path / "test.db"


@pytest.fixture
def file_repository(
    file_db_path: Path,
    backend: str,
    request: pytest.FixtureRequest,
) -> Generator[WorkflowStore, None, None]:
    """
    Create repository using file-based SQLite for thread safety tests.

    For PostgreSQL backend, returns the same repository as the regular fixture.
    For SQLite backend, uses file-based storage instead of :memory:.
    """
    repo: WorkflowStore
    if backend == "sqlite":
        connection_string = f"sqlite:///{file_db_path}"
        repo = SqliteWorkflowStore(
            connection_string=connection_string,
            create_tables=True,
        )
        yield repo
        repo.close()
    else:
        # PostgreSQL: reuse the regular repository
        if not HAS_POSTGRES:
            pytest.skip("psycopg not installed")
        postgres_url = request.getfixturevalue("postgres_url")
        repo = PostgresWorkflowStore(connection_string=postgres_url)
        yield repo
        repo.close()


@pytest.fixture
def file_queue(
    file_db_path: Path,
    backend: str,
    request: pytest.FixtureRequest,
) -> Generator[Queue, None, None]:
    """
    Create queue using file-based SQLite for thread safety tests.

    For PostgreSQL backend, returns the same queue as the regular fixture.
    For SQLite backend, uses file-based storage instead of :memory:.
    """
    q: Queue
    if backend == "sqlite":
        connection_string = f"sqlite:///{file_db_path}"
        sqlite_q = SqliteQueue(
            connection_string=connection_string,
            table_name="queue_messages",
        )
        sqlite_q._create_table()
        q = sqlite_q
        yield q
        try:
            q.clear()
        except Exception:
            pass  # Table may not exist if test created its own DB
        q.close()
    else:
        # PostgreSQL: reuse the regular queue
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


@pytest.fixture
def thread_pool() -> Generator[ThreadPoolExecutor, None, None]:
    """Provide a thread pool executor for concurrent tests."""
    executor = ThreadPoolExecutor(max_workers=20)
    yield executor
    executor.shutdown(wait=True)


@pytest.fixture
def timing_barrier() -> threading.Barrier:
    """Provide a threading barrier for coordinated race condition tests."""
    return threading.Barrier(2)


@pytest.fixture
def stress_test_config() -> dict[str, Any]:
    """Configuration for stress tests."""
    return {
        "num_workflows": 100,
        "num_threads": 10,
        "timeout_seconds": 30,
    }
