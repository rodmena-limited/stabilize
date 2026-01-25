# Stabilize

A lightweight full featured Python workflow execution engine with DAG-based stage orchestration.

## Requirements

- Python 3.11+
- SQLite (included) or PostgreSQL 12+

## Installation

```bash
pip install stabilize            # SQLite support only
pip install stabilize[postgres]  # PostgreSQL support
pip install stabilize[all]       # All features
```

## Features

- Message-driven DAG execution engine
- Parallel and sequential stage execution
- Synthetic stages (before/after/onFailure)
- PostgreSQL and SQLite persistence
- Pluggable task system
- Retry and timeout support

## Quick Start

```python
from stabilize import (
    Workflow, StageExecution, TaskExecution,
    SqliteWorkflowStore, SqliteQueue, QueueProcessor, Orchestrator,
    Task, TaskResult, TaskRegistry,
    # All 12 handlers are required
    StartWorkflowHandler, StartWaitingWorkflowsHandler, StartStageHandler,
    SkipStageHandler, CancelStageHandler, ContinueParentStageHandler,
    JumpToStageHandler, StartTaskHandler, RunTaskHandler, CompleteTaskHandler,
    CompleteStageHandler, CompleteWorkflowHandler,
)

# Define a custom task
class HelloTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        name = stage.context.get("name", "World")
        return TaskResult.success(outputs={"greeting": f"Hello, {name}!"})

# Create a workflow
workflow = Workflow.create(
    application="my-app",
    name="Hello Workflow",
    stages=[
        StageExecution(
            ref_id="1",
            type="hello",
            name="Say Hello",
            tasks=[
                TaskExecution.create(
                    name="Hello Task",
                    implementing_class="hello",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
            context={"name": "Stabilize"},
        ),
    ],
)

# Setup persistence and queue
store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
queue = SqliteQueue("sqlite:///:memory:")
queue._create_table()

# Register tasks
registry = TaskRegistry()
registry.register("hello", HelloTask)

# Create processor and register handlers
processor = QueueProcessor(queue)
for handler in [
    StartWorkflowHandler(queue, store),
    StartWaitingWorkflowsHandler(queue, store),
    StartStageHandler(queue, store),
    SkipStageHandler(queue, store),
    CancelStageHandler(queue, store),
    ContinueParentStageHandler(queue, store),
    JumpToStageHandler(queue, store),
    StartTaskHandler(queue, store, registry),
    RunTaskHandler(queue, store, registry),
    CompleteTaskHandler(queue, store),
    CompleteStageHandler(queue, store),
    CompleteWorkflowHandler(queue, store),
]:
    processor.register_handler(handler)

orchestrator = Orchestrator(queue)

# Run workflow
store.store(workflow)
orchestrator.start(workflow)
processor.process_all(timeout=10.0)

# Check result
result = store.retrieve(workflow.id)
print(f"Status: {result.status}")  # WorkflowStatus.SUCCEEDED
print(f"Output: {result.stages[0].outputs}")  # {'greeting': 'Hello, Stabilize!'}
```

## Built-in Tasks

Stabilize includes ready-to-use tasks for common operations:

### ShellTask - Execute Shell Commands

```python
from stabilize import ShellTask

registry.register("shell", ShellTask)

# Use in stage context
context = {
    "command": "npm install && npm test",
    "cwd": "/app",
    "timeout": 300,
    "env": {"NODE_ENV": "test"},
}
```

### HTTPTask - HTTP/API Requests

```python
from stabilize import HTTPTask

registry.register("http", HTTPTask)

# GET with JSON parsing
context = {"url": "https://api.example.com/data", "parse_json": True}

# POST with JSON body
context = {"url": "https://api.example.com/users", "method": "POST", "json": {"name": "John"}}

# With authentication
context = {"url": "https://api.example.com/private", "bearer_token": "token"}

# File upload
context = {"url": "https://api.example.com/upload", "method": "POST", "upload_file": "/path/to/file.pdf"}
```

See `examples/` directory for complete examples.

## Parallel Stages

Stages with shared dependencies run in parallel:

```python
#     Setup
#    /     \
#  Test   Lint
#    \     /
#    Deploy

workflow = Workflow.create(
    application="my-app",
    name="CI/CD Pipeline",
    stages=[
        StageExecution(ref_id="setup", type="setup", name="Setup", ...),
        StageExecution(ref_id="test", type="test", name="Test",
                      requisite_stage_ref_ids={"setup"}, ...),
        StageExecution(ref_id="lint", type="lint", name="Lint",
                      requisite_stage_ref_ids={"setup"}, ...),
        StageExecution(ref_id="deploy", type="deploy", name="Deploy",
                      requisite_stage_ref_ids={"test", "lint"}, ...),
    ],
)
```

## Dynamic Routing

Stabilize supports dynamic flow control with `TaskResult.jump_to()` for conditional branching and retry loops:

```python
from stabilize import Task, TaskResult, TransientError

class RouterTask(Task):
    """Route to different stages based on conditions."""
    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.context.get("tests_passed"):
            return TaskResult.success()
        else:
            # Jump to another stage with context
            return TaskResult.jump_to(
                "retry_stage",
                context={"retry_reason": "tests failed"}
            )
```

### Stateful Retries

Preserve progress across transient error retries with `context_update`:

```python
class ProgressTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        processed = stage.context.get("processed_items", 0)
        try:
            # Process next batch
            new_processed = process_batch(processed)
            return TaskResult.success(outputs={"total": new_processed})
        except RateLimitError:
            # Preserve progress for next retry
            raise TransientError(
                "Rate limited",
                retry_after=30,
                context_update={"processed_items": processed + 10}
            )
```

The `context_update` is merged into the stage context before the retry, allowing tasks to resume from where they left off.

## Database Setup

### SQLite

No setup required. Schema is created automatically.

### PostgreSQL

Apply migrations using the CLI:

```bash
# Using mg.yaml in current directory
stabilize mg-up

# Using database URL
stabilize mg-up --db-url postgres://user:pass@host:5432/dbname

# Using environment variable
MG_DATABASE_URL=postgres://user:pass@host:5432/dbname stabilize mg-up

# Check migration status
stabilize mg-status
```

Example mg.yaml:

```yaml
database:
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  dbname: stabilize
```

## CLI Reference

```
stabilize mg-up [--db-url URL]      Apply pending PostgreSQL migrations
stabilize mg-status [--db-url URL]  Show migration status
stabilize monitor [--db-url URL]    Real-time workflow monitoring dashboard
stabilize prompt                    Output documentation for pipeline code generation
```

## Running Tests

```bash
# All tests (requires Docker for PostgreSQL)
pytest tests/ -v

# SQLite tests only (no Docker)
pytest tests/ -v -k sqlite
```

## License

Apache 2.0
