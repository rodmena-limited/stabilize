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
pip install stabilize[rag]       # RAG-powered pipeline generation
pip install stabilize[all]       # All features
```

## Features

- Message-driven DAG execution engine
- Parallel and sequential stage execution
- Synthetic stages (before/after/onFailure)
- PostgreSQL and SQLite persistence
- Pluggable task system
- Retry and timeout support
- RAG-powered pipeline generation from natural language

## Quick Start

```python
from stabilize import (
    Workflow, StageExecution, TaskExecution,
    SqliteWorkflowStore, SqliteQueue, QueueProcessor, Orchestrator,
    Task, TaskResult, TaskRegistry,
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler, CompleteWorkflowHandler,
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
    StartStageHandler(queue, store),
    StartTaskHandler(queue, store),
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

## RAG-Powered Pipeline Generation

Stabilize includes an AI-powered assistant that generates pipeline code from natural language descriptions using RAG (Retrieval-Augmented Generation).

### Requirements

```bash
pip install stabilize[rag]  # Installs ragit dependency
```

You also need:
- **Local Ollama** (required for embeddings - ollama.com doesn't support embeddings API):
  ```bash
  # Install from https://ollama.com/download, then:
  ollama serve                    # Start the server
  ollama pull nomic-embed-text    # Download embedding model
  ```
- **Ollama API key** for LLM generation (uses ollama.com cloud)

### Setup

Create a `.env` file with your API key:

```bash
OLLAMA_API_KEY=your_api_key_here
```

Initialize the embedding cache:

```bash
# Initialize with default context (Stabilize docs + examples)
stabilize rag init

# Include your own code as additional training context
stabilize rag init --additional-context /path/to/your/code/

# Force regenerate embeddings
stabilize rag init --force
```

### Generate Pipelines

```bash
# Generate a pipeline from natural language
stabilize rag generate "create a pipeline that processes CSV files in parallel"

# Save to file
stabilize rag generate "build a CI/CD pipeline with test and deploy stages" > my_pipeline.py
```

### Clear Cache

```bash
stabilize rag clear
```

### Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_API_KEY` | (required) | API key for ollama.com |
| `OLLAMA_BASE_URL` | `https://ollama.com` | LLM endpoint URL |
| `OLLAMA_EMBEDDING_URL` | `http://localhost:11434` | Local Ollama for embeddings |

### Example

```bash
$ stabilize rag generate "create a hello world pipeline"

from stabilize import Workflow, StageExecution, TaskExecution, Task, TaskResult
...
```

## CLI Reference

```
stabilize mg-up [--db-url URL]      Apply pending PostgreSQL migrations
stabilize mg-status [--db-url URL]  Show migration status
stabilize rag init [--force] [--additional-context PATH]  Initialize RAG embeddings
stabilize rag generate "prompt"     Generate pipeline from natural language
stabilize rag clear                 Clear embedding cache
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
