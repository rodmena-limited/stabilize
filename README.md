# Stabilize

Highway Workflow Engine - Stabilize execution layer.

A lightweight Python workflow execution engine with DAG-based stage orchestration.

## Requirements

- Python 3.11+
- SQLite (included) or PostgreSQL 12+

## Installation

```bash
pip install stabilize            # SQLite support only
pip install stabilize[postgres]  # PostgreSQL support
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
from stabilize import Workflow, StageExecution, TaskExecution, WorkflowStatus
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.sqlite_queue import SqliteQueue
from stabilize.queue.processor import QueueProcessor
from stabilize.orchestrator import Orchestrator
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from stabilize.tasks.registry import TaskRegistry

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
store = SqliteWorkflowStore(":memory:")
queue = SqliteQueue(":memory:")

# Register tasks
registry = TaskRegistry()
registry.register("hello", HelloTask)

# Create processor and orchestrator
processor = QueueProcessor.create(queue, store, registry)
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
```

## Naming Alignment with highway_dsl

| highway_dsl | stabilize |
|-------------|-----------|
| Workflow | Workflow |
| TaskOperator | Task interface |
| RetryPolicy | RetryableTask |
| TimeoutPolicy | OverridableTimeoutRetryableTask |

## Running Tests

```bash
# All tests (requires Docker for PostgreSQL)
pytest tests/ -v

# SQLite tests only (no Docker)
pytest tests/ -v -k sqlite
```

## License

Apache 2.0
