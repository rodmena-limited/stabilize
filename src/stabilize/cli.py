"""Stabilize CLI for database migrations and developer tools."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


# =============================================================================
# RAG PROMPT - Comprehensive documentation for AI coding agents
# =============================================================================

PROMPT_TEXT = r'''
===============================================================================
STABILIZE WORKFLOW ENGINE - COMPLETE REFERENCE FOR CODE GENERATION
===============================================================================

Stabilize is a Python DAG-based workflow orchestration engine. Workflows consist
of Stages (nodes in the DAG) containing Tasks (atomic work units). Stages can
run sequentially or in parallel based on their dependencies.

===============================================================================
1. COMPLETE WORKING EXAMPLE - COPY THIS AS YOUR STARTING TEMPLATE
===============================================================================

#!/usr/bin/env python3
"""Minimal working Stabilize workflow example."""

from stabilize import Workflow, StageExecution, TaskExecution, WorkflowStatus
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.sqlite_queue import SqliteQueue
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.queue import Queue
from stabilize.persistence.store import WorkflowStore
from stabilize.orchestrator import Orchestrator
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from stabilize.tasks.registry import TaskRegistry
from stabilize.handlers.complete_workflow import CompleteWorkflowHandler
from stabilize.handlers.complete_stage import CompleteStageHandler
from stabilize.handlers.complete_task import CompleteTaskHandler
from stabilize.handlers.run_task import RunTaskHandler
from stabilize.handlers.start_workflow import StartWorkflowHandler
from stabilize.handlers.start_stage import StartStageHandler
from stabilize.handlers.start_task import StartTaskHandler


# Step 1: Define your custom Task
class MyTask(Task):
    """Custom task implementation."""

    def execute(self, stage: StageExecution) -> TaskResult:
        # Read inputs from stage.context
        input_value = stage.context.get("my_input", "default")

        # Do your work here
        result = f"Processed: {input_value}"

        # Return success with outputs for downstream stages
        return TaskResult.success(outputs={"result": result})


# Step 2: Setup infrastructure
def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with task registered."""
    task_registry = TaskRegistry()
    task_registry.register("my_task", MyTask)  # Register task with name

    processor = QueueProcessor(queue)

    # Register all handlers in order
    handlers = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, task_registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]
    for handler in handlers:
        processor.register_handler(handler)

    orchestrator = Orchestrator(queue)
    return processor, orchestrator


# Step 3: Create and run workflow
def main():
    # Initialize storage (in-memory SQLite for development)
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Create workflow with stages
    workflow = Workflow.create(
        application="my-app",
        name="My Pipeline",
        stages=[
            StageExecution(
                ref_id="1",
                type="my_task",
                name="First Stage",
                context={"my_input": "hello world"},
                tasks=[
                    TaskExecution.create(
                        name="Run MyTask",
                        implementing_class="my_task",  # Must match registered name
                        stage_start=True,              # REQUIRED for first task
                        stage_end=True,                # REQUIRED for last task
                    ),
                ],
            ),
        ],
    )

    # Execute workflow
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Check result
    result = store.retrieve(workflow.id)
    print(f"Status: {result.status}")
    print(f"Output: {result.stages[0].outputs}")


if __name__ == "__main__":
    main()

===============================================================================
2. CORE CLASSES API
===============================================================================

2.1 Workflow
-------------
Factory: Workflow.create(application, name, stages, trigger=None, pipeline_config_id=None)

Fields:
  id: str                    - Unique ULID identifier (auto-generated)
  status: WorkflowStatus     - Current execution status
  stages: list[StageExecution] - All stages in the workflow
  application: str           - Application name
  name: str                  - Pipeline name

Methods:
  stage_by_id(stage_id) -> StageExecution    - Get stage by internal ID
  stage_by_ref_id(ref_id) -> StageExecution  - Get stage by reference ID
  get_context() -> dict                      - Get merged outputs from all stages


2.2 StageExecution
-------------------
Constructor: StageExecution(ref_id, type, name, context, tasks, requisite_stage_ref_ids=set())

Fields:
  ref_id: str                         - UNIQUE reference ID for DAG (e.g., "1", "deploy", "build")
  type: str                           - Stage type (usually matches task name)
  name: str                           - Human-readable name
  context: dict[str, Any]             - INPUT parameters for this stage
  outputs: dict[str, Any]             - OUTPUT values for downstream stages (populated by tasks)
  tasks: list[TaskExecution]          - Tasks to execute (sequentially)
  requisite_stage_ref_ids: set[str]   - Dependencies (ref_ids of upstream stages)
  status: WorkflowStatus              - Current status

DAG Dependencies:
  - Empty set: Stage runs immediately (initial stage)
  - {"A"}: Stage runs after stage with ref_id="A" completes
  - {"A", "B"}: Stage waits for BOTH A and B to complete (join point)


2.3 TaskExecution
------------------
Factory: TaskExecution.create(name, implementing_class, stage_start=False, stage_end=False)

Fields:
  name: str                  - Human-readable task name
  implementing_class: str    - MUST match the name used in TaskRegistry.register()
  stage_start: bool          - MUST be True for first task in stage
  stage_end: bool            - MUST be True for last task in stage
  status: WorkflowStatus     - Current status

CRITICAL: If a stage has only one task, set BOTH stage_start=True AND stage_end=True


2.4 WorkflowStatus
-------------------
All status values:
  NOT_STARTED     - Not yet started
  RUNNING         - Currently executing
  PAUSED          - Paused, can be resumed
  SUSPENDED       - Waiting for external trigger
  SUCCEEDED       - Completed successfully
  FAILED_CONTINUE - Failed but pipeline continues
  TERMINAL        - Failed, pipeline halts
  CANCELED        - Execution was canceled
  STOPPED         - Execution was stopped
  SKIPPED         - Stage/task was skipped
  REDIRECT        - Decision branch redirect
  BUFFERED        - Buffered, waiting

Properties:
  .is_complete: bool    - Has finished executing
  .is_halt: bool        - Blocks downstream stages
  .is_successful: bool  - SUCCEEDED, STOPPED, or SKIPPED
  .is_failure: bool     - TERMINAL, STOPPED, or FAILED_CONTINUE

===============================================================================
3. TASK IMPLEMENTATION
===============================================================================

3.1 Task Interface (Abstract Base Class)
-----------------------------------------
from stabilize.tasks.interface import Task

class MyTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        # Read from stage.context (includes upstream outputs)
        value = stage.context.get("key")

        # Return TaskResult
        return TaskResult.success(outputs={"output_key": "value"})

    # Optional: Handle timeout (for RetryableTask)
    def on_timeout(self, stage: StageExecution) -> TaskResult | None:
        return TaskResult.terminal(error="Task timed out")

    # Optional: Handle cancellation
    def on_cancel(self, stage: StageExecution) -> TaskResult | None:
        return TaskResult.canceled()


3.2 TaskResult Factory Methods - CRITICAL REFERENCE
----------------------------------------------------
from stabilize.tasks.result import TaskResult

SUCCESS - Task completed successfully, pipeline continues:
    TaskResult.success(outputs=None, context=None)
    Parameters:
      outputs: dict  - Values available to downstream stages
      context: dict  - Values stored in stage.context (stage-scoped)

RUNNING - Task needs to poll again (for RetryableTask):
    TaskResult.running(context=None)
    Parameters:
      context: dict  - Updated state for next poll iteration

TERMINAL - Task failed, pipeline HALTS:
    TaskResult.terminal(error, context=None)
    Parameters:
      error: str     - Error message (REQUIRED)
      context: dict  - Additional context data
    WARNING: Does NOT accept 'outputs' parameter!

FAILED_CONTINUE - Task failed but pipeline continues:
    TaskResult.failed_continue(error, outputs=None, context=None)
    Parameters:
      error: str     - Error message (REQUIRED)
      outputs: dict  - Values still available downstream
      context: dict  - Additional context data

SKIPPED - Task was skipped:
    TaskResult.skipped()

CANCELED - Task was canceled:
    TaskResult.canceled(outputs=None)

STOPPED - Task was stopped:
    TaskResult.stopped(outputs=None)


3.3 RetryableTask - For Polling Operations
-------------------------------------------
from datetime import timedelta
from stabilize.tasks.interface import RetryableTask

class PollTask(RetryableTask):
    def get_timeout(self) -> timedelta:
        """Maximum time before task times out."""
        return timedelta(minutes=30)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        """Time to wait between poll attempts."""
        return timedelta(seconds=10)

    def execute(self, stage: StageExecution) -> TaskResult:
        status = check_external_system()

        if status == "complete":
            return TaskResult.success(outputs={"status": "done"})
        elif status == "failed":
            return TaskResult.terminal(error="External system failed")
        else:
            # Keep polling - will be called again after backoff
            return TaskResult.running(context={"last_check": time.time()})


3.4 SkippableTask - Conditional Execution
------------------------------------------
from stabilize.tasks.interface import SkippableTask

class ConditionalTask(SkippableTask):
    def is_enabled(self, stage: StageExecution) -> bool:
        """Return False to skip this task."""
        return stage.context.get("should_run", True)

    def do_execute(self, stage: StageExecution) -> TaskResult:
        """Actual task logic (only called if is_enabled returns True)."""
        return TaskResult.success()

===============================================================================
4. TASK REGISTRY
===============================================================================

from stabilize.tasks.registry import TaskRegistry

registry = TaskRegistry()

# Register a task class
registry.register("my_task", MyTask)

# Register with aliases
registry.register("http", HTTPTask, aliases=["http_request", "web_request"])

# The implementing_class in TaskExecution MUST match the registered name:
TaskExecution.create(
    name="Do something",
    implementing_class="my_task",  # Must match registry.register() name
    stage_start=True,
    stage_end=True,
)

===============================================================================
5. DAG PATTERNS
===============================================================================

5.1 Sequential Stages (A -> B -> C)
------------------------------------
stages=[
    StageExecution(ref_id="A", ..., requisite_stage_ref_ids=set()),      # Initial
    StageExecution(ref_id="B", ..., requisite_stage_ref_ids={"A"}),      # After A
    StageExecution(ref_id="C", ..., requisite_stage_ref_ids={"B"}),      # After B
]


5.2 Parallel Stages
--------------------
       A
      / \
     B   C    <- B and C run in parallel after A
      \ /
       D

stages=[
    StageExecution(ref_id="A", ..., requisite_stage_ref_ids=set()),
    StageExecution(ref_id="B", ..., requisite_stage_ref_ids={"A"}),    # Parallel
    StageExecution(ref_id="C", ..., requisite_stage_ref_ids={"A"}),    # Parallel
    StageExecution(ref_id="D", ..., requisite_stage_ref_ids={"B", "C"}), # Join
]


5.3 Complex DAG
----------------
     A
    /|\
   B C D     <- All parallel after A
   |/ \|
   E   F     <- E waits for B,C; F waits for C,D
    \ /
     G       <- G waits for E and F

stages=[
    StageExecution(ref_id="A", ..., requisite_stage_ref_ids=set()),
    StageExecution(ref_id="B", ..., requisite_stage_ref_ids={"A"}),
    StageExecution(ref_id="C", ..., requisite_stage_ref_ids={"A"}),
    StageExecution(ref_id="D", ..., requisite_stage_ref_ids={"A"}),
    StageExecution(ref_id="E", ..., requisite_stage_ref_ids={"B", "C"}),
    StageExecution(ref_id="F", ..., requisite_stage_ref_ids={"C", "D"}),
    StageExecution(ref_id="G", ..., requisite_stage_ref_ids={"E", "F"}),
]

===============================================================================
6. CONTEXT AND OUTPUTS DATA FLOW
===============================================================================

stage.context  - INPUT: Parameters passed when creating the stage
                 Also includes outputs from upstream stages (automatic lookup)

stage.outputs  - OUTPUT: Values produced by tasks for downstream stages
                 Set via TaskResult.success(outputs={...})

Example flow:
  Stage A context: {"input": "hello"}
  Stage A task returns: TaskResult.success(outputs={"result": "processed"})
  Stage B context: {"input": "hello", "result": "processed"}  <- Includes A's output

Accessing in tasks:
  def execute(self, stage):
      # Read from context (includes upstream outputs)
      upstream_result = stage.context.get("result")  # From upstream stage

      # Write to outputs (available downstream)
      return TaskResult.success(outputs={"my_output": "value"})

===============================================================================
7. COMMON MISTAKES AND HOW TO FIX THEM
===============================================================================

MISTAKE 1: Using 'outputs' parameter with TaskResult.terminal()
---------------------------------------------------------------
WRONG:
    return TaskResult.terminal(error="Failed", outputs={"data": value})

RIGHT:
    return TaskResult.terminal(error="Failed", context={"data": value})

terminal() only accepts: error (required), context (optional)


MISTAKE 2: Forgetting stage_start and stage_end on tasks
---------------------------------------------------------
WRONG:
    TaskExecution.create(name="X", implementing_class="y")

RIGHT:
    TaskExecution.create(name="X", implementing_class="y", stage_start=True, stage_end=True)


MISTAKE 3: implementing_class doesn't match registered name
------------------------------------------------------------
WRONG:
    registry.register("http_task", HTTPTask)
    TaskExecution.create(..., implementing_class="HTTPTask")  # Class name, not registered name

RIGHT:
    registry.register("http_task", HTTPTask)
    TaskExecution.create(..., implementing_class="http_task")  # Matches registered name


MISTAKE 4: Duplicate ref_id values
-----------------------------------
WRONG:
    StageExecution(ref_id="1", name="Stage A", ...)
    StageExecution(ref_id="1", name="Stage B", ...)  # Same ref_id!

RIGHT:
    StageExecution(ref_id="1", name="Stage A", ...)
    StageExecution(ref_id="2", name="Stage B", ...)  # Unique ref_ids


MISTAKE 5: Missing handlers
----------------------------
All 7 handlers are REQUIRED for the engine to work:
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler, CompleteWorkflowHandler

===============================================================================
8. COMPLETE EXAMPLE: SEQUENTIAL PIPELINE WITH ERROR HANDLING
===============================================================================

#!/usr/bin/env python3
from stabilize import Workflow, StageExecution, TaskExecution, WorkflowStatus
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.sqlite_queue import SqliteQueue
from stabilize.queue.processor import QueueProcessor
from stabilize.orchestrator import Orchestrator
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from stabilize.tasks.registry import TaskRegistry
from stabilize.handlers.complete_workflow import CompleteWorkflowHandler
from stabilize.handlers.complete_stage import CompleteStageHandler
from stabilize.handlers.complete_task import CompleteTaskHandler
from stabilize.handlers.run_task import RunTaskHandler
from stabilize.handlers.start_workflow import StartWorkflowHandler
from stabilize.handlers.start_stage import StartStageHandler
from stabilize.handlers.start_task import StartTaskHandler


class ValidateTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        data = stage.context.get("data")
        if not data:
            return TaskResult.terminal(error="No data provided")
        return TaskResult.success(outputs={"validated": True, "data": data})


class ProcessTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        data = stage.context.get("data")
        validated = stage.context.get("validated")
        if not validated:
            return TaskResult.terminal(error="Data not validated")
        result = data.upper()
        return TaskResult.success(outputs={"processed_data": result})


class NotifyTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        processed = stage.context.get("processed_data")
        # Even if notification fails, we don't want to fail the pipeline
        try:
            send_notification(processed)
            return TaskResult.success(outputs={"notified": True})
        except Exception as e:
            # Use failed_continue to not halt the pipeline
            return TaskResult.failed_continue(
                error=f"Notification failed: {e}",
                outputs={"notified": False}
            )


def setup_pipeline_runner(store, queue):
    registry = TaskRegistry()
    registry.register("validate", ValidateTask)
    registry.register("process", ProcessTask)
    registry.register("notify", NotifyTask)

    processor = QueueProcessor(queue)
    handlers = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]
    for h in handlers:
        processor.register_handler(h)

    return processor, Orchestrator(queue)


def main():
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="data-pipeline",
        name="Process Data",
        stages=[
            StageExecution(
                ref_id="validate",
                type="validate",
                name="Validate Input",
                context={"data": "hello world"},
                tasks=[TaskExecution.create("Validate", "validate", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="process",
                type="process",
                name="Process Data",
                requisite_stage_ref_ids={"validate"},
                context={},  # Will receive 'data' from upstream
                tasks=[TaskExecution.create("Process", "process", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="notify",
                type="notify",
                name="Send Notification",
                requisite_stage_ref_ids={"process"},
                context={},
                tasks=[TaskExecution.create("Notify", "notify", stage_start=True, stage_end=True)],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"Final status: {result.status}")
    for stage in result.stages:
        print(f"  {stage.name}: {stage.status} - {stage.outputs}")


if __name__ == "__main__":
    main()

===============================================================================
9. COMPLETE EXAMPLE: PARALLEL STAGES WITH JOIN
===============================================================================

#!/usr/bin/env python3
from stabilize import Workflow, StageExecution, TaskExecution
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.sqlite_queue import SqliteQueue
from stabilize.queue.processor import QueueProcessor
from stabilize.orchestrator import Orchestrator
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from stabilize.tasks.registry import TaskRegistry
from stabilize.handlers.complete_workflow import CompleteWorkflowHandler
from stabilize.handlers.complete_stage import CompleteStageHandler
from stabilize.handlers.complete_task import CompleteTaskHandler
from stabilize.handlers.run_task import RunTaskHandler
from stabilize.handlers.start_workflow import StartWorkflowHandler
from stabilize.handlers.start_stage import StartStageHandler
from stabilize.handlers.start_task import StartTaskHandler


class FetchDataTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        source = stage.context.get("source")
        # Simulate fetching data from different sources
        data = f"data_from_{source}"
        return TaskResult.success(outputs={f"{source}_data": data})


class AggregateTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        # Collect data from all upstream parallel stages
        api_data = stage.context.get("api_data")
        db_data = stage.context.get("db_data")
        cache_data = stage.context.get("cache_data")
        combined = f"{api_data} + {db_data} + {cache_data}"
        return TaskResult.success(outputs={"combined_data": combined})


def setup_pipeline_runner(store, queue):
    registry = TaskRegistry()
    registry.register("fetch", FetchDataTask)
    registry.register("aggregate", AggregateTask)

    processor = QueueProcessor(queue)
    for h in [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]:
        processor.register_handler(h)

    return processor, Orchestrator(queue)


def main():
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    #        Start
    #       /  |  \
    #     API  DB  Cache    <- Run in parallel
    #       \  |  /
    #      Aggregate        <- Join point

    workflow = Workflow.create(
        application="parallel-fetch",
        name="Parallel Data Fetch",
        stages=[
            StageExecution(
                ref_id="api",
                type="fetch",
                name="Fetch from API",
                context={"source": "api"},
                tasks=[TaskExecution.create("Fetch API", "fetch", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="db",
                type="fetch",
                name="Fetch from Database",
                context={"source": "db"},
                tasks=[TaskExecution.create("Fetch DB", "fetch", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="cache",
                type="fetch",
                name="Fetch from Cache",
                context={"source": "cache"},
                tasks=[TaskExecution.create("Fetch Cache", "fetch", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="aggregate",
                type="aggregate",
                name="Aggregate Results",
                requisite_stage_ref_ids={"api", "db", "cache"},  # Wait for ALL three
                context={},
                tasks=[TaskExecution.create("Aggregate", "aggregate", stage_start=True, stage_end=True)],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"Final status: {result.status}")
    print(f"Combined data: {result.stages[-1].outputs.get('combined_data')}")


if __name__ == "__main__":
    main()

===============================================================================
10. COMPLETE IMPORTS REFERENCE
===============================================================================

# Core models
from stabilize import Workflow, StageExecution, TaskExecution, WorkflowStatus

# Persistence
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.persistence.store import WorkflowStore

# Queue
from stabilize.queue.sqlite_queue import SqliteQueue
from stabilize.queue.queue import Queue
from stabilize.queue.processor import QueueProcessor

# Orchestration
from stabilize.orchestrator import Orchestrator

# Tasks
from stabilize.tasks.interface import Task, RetryableTask, SkippableTask
from stabilize.tasks.result import TaskResult
from stabilize.tasks.registry import TaskRegistry

# Handlers (all 7 required)
from stabilize.handlers.start_workflow import StartWorkflowHandler
from stabilize.handlers.start_stage import StartStageHandler
from stabilize.handlers.start_task import StartTaskHandler
from stabilize.handlers.run_task import RunTaskHandler
from stabilize.handlers.complete_task import CompleteTaskHandler
from stabilize.handlers.complete_stage import CompleteStageHandler
from stabilize.handlers.complete_workflow import CompleteWorkflowHandler

===============================================================================
END OF REFERENCE
===============================================================================
'''

# Migration tracking table
MIGRATION_TABLE = "stabilize_migrations"


def load_config() -> dict[str, Any]:
    """Load database config from mg.yaml or environment."""
    db_url = os.environ.get("MG_DATABASE_URL")
    if db_url:
        return parse_db_url(db_url)

    # Try to load mg.yaml
    mg_yaml = Path("mg.yaml")
    if mg_yaml.exists():
        try:
            import yaml  # type: ignore[import-untyped]

            with open(mg_yaml) as f:
                config = yaml.safe_load(f)
                db_config: dict[str, Any] = config.get("database", {}) if config else {}
                return db_config
        except ImportError:
            print("Warning: PyYAML not installed, cannot read mg.yaml")
            print("Set MG_DATABASE_URL environment variable instead")
            sys.exit(1)

    print("Error: No database configuration found")
    print("Either create mg.yaml or set MG_DATABASE_URL environment variable")
    sys.exit(1)


def parse_db_url(url: str) -> dict[str, Any]:
    """Parse a database URL into connection parameters."""
    # postgres://user:pass@host:port/dbname
    pattern = r"postgres(?:ql)?://(?:(?P<user>[^:]+)(?::(?P<password>[^@]+))?@)?(?P<host>[^:/]+)(?::(?P<port>\d+))?/(?P<dbname>[^?]+)"
    match = re.match(pattern, url)
    if not match:
        print(f"Error: Invalid database URL: {url}")
        sys.exit(1)

    return {
        "host": match.group("host"),
        "port": int(match.group("port") or 5432),
        "user": match.group("user") or "postgres",
        "password": match.group("password") or "",
        "dbname": match.group("dbname"),
    }


def get_migrations() -> list[tuple[str, str]]:
    """Get all migration files from the package."""
    migrations_pkg = files("stabilize.migrations")
    migrations = []

    for item in migrations_pkg.iterdir():
        if item.name.endswith(".sql"):
            content = item.read_text()
            migrations.append((item.name, content))

    # Sort by filename (ULID prefix ensures chronological order)
    migrations.sort(key=lambda x: x[0])
    return migrations


def extract_up_migration(content: str) -> str:
    """Extract the UP migration from SQL content."""
    # Find content between "-- migrate: up" and "-- migrate: down"
    up_match = re.search(
        r"--\s*migrate:\s*up\s*\n(.*?)(?:--\s*migrate:\s*down|$)",
        content,
        re.DOTALL | re.IGNORECASE,
    )
    if up_match:
        return up_match.group(1).strip()
    return content


def compute_checksum(content: str) -> str:
    """Compute MD5 checksum of migration content."""
    return hashlib.md5(content.encode()).hexdigest()


def mg_up(db_url: str | None = None) -> None:
    """Apply pending migrations to PostgreSQL database."""
    try:
        import psycopg
    except ImportError:
        print("Error: psycopg not installed")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)

    # Load config
    if db_url:
        config = parse_db_url(db_url)
    else:
        config = load_config()

    # Connect to database
    conninfo = f"host={config['host']} port={config.get('port', 5432)} user={config.get('user', 'postgres')} password={config.get('password', '')} dbname={config['dbname']}"

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Ensure migration tracking table exists
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL UNIQUE,
                        checksum VARCHAR(32) NOT NULL,
                        applied_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                conn.commit()

                # Get applied migrations
                cur.execute(f"SELECT name, checksum FROM {MIGRATION_TABLE}")
                applied = {row[0]: row[1] for row in cur.fetchall()}

                # Get available migrations
                migrations = get_migrations()

                if not migrations:
                    print("No migrations found in package")
                    return

                # Apply pending migrations
                pending = 0
                for name, content in migrations:
                    if name in applied:
                        # Verify checksum
                        expected = compute_checksum(content)
                        if applied[name] != expected:
                            print(f"Warning: Checksum mismatch for {name}")
                        continue

                    pending += 1
                    print(f"Applying: {name}")

                    up_sql = extract_up_migration(content)
                    cur.execute(up_sql)

                    checksum = compute_checksum(content)
                    cur.execute(
                        f"INSERT INTO {MIGRATION_TABLE} (name, checksum) VALUES (%s, %s)",
                        (name, checksum),
                    )
                    conn.commit()

                if pending == 0:
                    print("All migrations already applied")
                else:
                    print(f"Applied {pending} migration(s)")

    except psycopg.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


def prompt() -> None:
    """Output comprehensive documentation for RAG systems and coding agents."""
    print(PROMPT_TEXT)


def rag_init(
    db_url: str | None = None,
    force: bool = False,
    additional_context: list[str] | None = None,
) -> None:
    """Initialize RAG embeddings from examples, documentation, and additional context."""
    try:
        from stabilize.rag import StabilizeRAG, get_cache
    except ImportError:
        print("Error: RAG support requires: pip install stabilize[rag]")
        sys.exit(1)

    cache = get_cache(db_url)
    rag = StabilizeRAG(cache)

    print("Initializing embeddings...")
    count = rag.init(force=force, additional_paths=additional_context)
    if count > 0:
        print(f"Cached {count} embeddings")
    else:
        print("Embeddings already initialized (use --force to regenerate)")


def rag_clear(db_url: str | None = None) -> None:
    """Clear all cached embeddings."""
    try:
        from stabilize.rag import get_cache
    except ImportError:
        print("Error: RAG support requires: pip install stabilize[rag]")
        sys.exit(1)

    cache = get_cache(db_url)
    cache.clear()
    print("Embedding cache cleared")


def rag_generate(
    prompt_text: str,
    db_url: str | None = None,
    execute: bool = False,
    top_k: int = 5,
    temperature: float = 0.3,
    llm_model: str | None = None,
) -> None:
    """Generate pipeline code from natural language prompt."""
    try:
        from stabilize.rag import StabilizeRAG, get_cache
    except ImportError:
        print("Error: RAG support requires: pip install stabilize[rag]")
        sys.exit(1)

    cache = get_cache(db_url)
    rag = StabilizeRAG(cache, llm_model=llm_model)

    try:
        code = rag.generate(prompt_text, top_k=top_k, temperature=temperature)
    except RuntimeError as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(code)

    if execute:
        print("\n--- Executing generated code ---\n")
        try:
            exec(code, {"__name__": "__main__"})
        except ImportError as e:
            print("\n--- Execution failed: Import error ---")
            print(f"Error: {e}")
            print("\nThe generated code has incorrect imports.")
            print("Review the imports above and compare with examples/shell-example.py")
            sys.exit(1)
        except Exception as e:
            print("\n--- Execution failed ---")
            print(f"Error: {type(e).__name__}: {e}")
            print("\nThe generated code may need manual adjustments.")
            sys.exit(1)


def mg_status(db_url: str | None = None) -> None:
    """Show migration status."""
    try:
        import psycopg
    except ImportError:
        print("Error: psycopg not installed")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)

    # Load config
    if db_url:
        config = parse_db_url(db_url)
    else:
        config = load_config()

    conninfo = f"host={config['host']} port={config.get('port', 5432)} user={config.get('user', 'postgres')} password={config.get('password', '')} dbname={config['dbname']}"

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Check if tracking table exists
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """,
                    (MIGRATION_TABLE,),
                )
                row = cur.fetchone()
                table_exists = row[0] if row else False

                applied = {}
                if table_exists:
                    cur.execute(f"SELECT name, checksum, applied_at FROM {MIGRATION_TABLE} ORDER BY applied_at")
                    applied = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

                migrations = get_migrations()

                print(f"{'Status':<10} {'Migration':<50} {'Applied At'}")
                print("-" * 80)

                for name, content in migrations:
                    if name in applied:
                        checksum, applied_at = applied[name]
                        expected = compute_checksum(content)
                        status = "applied" if checksum == expected else "MISMATCH"
                        print(f"{status:<10} {name:<50} {applied_at}")
                    else:
                        print(f"{'pending':<10} {name:<50} -")

    except psycopg.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="stabilize",
        description="Stabilize - Workflow Engine CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # mg-up command
    up_parser = subparsers.add_parser("mg-up", help="Apply pending PostgreSQL migrations")
    up_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://user:pass@host:port/dbname)",
    )

    # mg-status command
    status_parser = subparsers.add_parser("mg-status", help="Show migration status")
    status_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://user:pass@host:port/dbname)",
    )

    # prompt command
    subparsers.add_parser(
        "prompt",
        help="Output comprehensive RAG context for pipeline code generation",
    )

    # rag command (with subcommands)
    rag_parser = subparsers.add_parser(
        "rag",
        help="RAG-powered pipeline generation",
    )
    rag_subparsers = rag_parser.add_subparsers(dest="rag_command")

    # rag init
    init_parser = rag_subparsers.add_parser(
        "init",
        help="Initialize embeddings cache from examples and documentation",
    )
    init_parser.add_argument(
        "--db-url",
        help="Database URL for caching (postgres://... or sqlite path)",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration even if cache exists",
    )
    init_parser.add_argument(
        "--additional-context",
        action="append",
        metavar="PATH",
        help="Additional file or directory to include in training context (can be specified multiple times)",
    )

    # rag generate
    gen_parser = rag_subparsers.add_parser(
        "generate",
        help="Generate pipeline code from natural language prompt",
    )
    gen_parser.add_argument(
        "prompt",
        help="Natural language description of the desired pipeline",
    )
    gen_parser.add_argument(
        "--db-url",
        help="Database URL for caching",
    )
    gen_parser.add_argument(
        "-x",
        "--execute",
        action="store_true",
        help="Execute the generated code after displaying it",
    )
    gen_parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of context chunks to retrieve (default: 10)",
    )
    gen_parser.add_argument(
        "--temperature",
        type=float,
        default=0.3,
        help="LLM temperature for generation (default: 0.3)",
    )
    gen_parser.add_argument(
        "--llm-model",
        default=None,
        help="LLM model for generation (default: qwen3-vl:235b)",
    )

    # rag clear
    clear_parser = rag_subparsers.add_parser(
        "clear",
        help="Clear all cached embeddings",
    )
    clear_parser.add_argument(
        "--db-url",
        help="Database URL for caching",
    )

    args = parser.parse_args()

    if args.command == "mg-up":
        mg_up(args.db_url)
    elif args.command == "mg-status":
        mg_status(args.db_url)
    elif args.command == "prompt":
        prompt()
    elif args.command == "rag":
        if args.rag_command == "init":
            rag_init(args.db_url, args.force, args.additional_context)
        elif args.rag_command == "generate":
            rag_generate(
                args.prompt,
                args.db_url,
                args.execute,
                args.top_k,
                args.temperature,
                args.llm_model,
            )
        elif args.rag_command == "clear":
            rag_clear(args.db_url)
        else:
            rag_parser.print_help()
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
