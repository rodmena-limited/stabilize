# Stabilize - Gemini Context

## Project Overview

**Stabilize** is a lightweight Python workflow execution engine with DAG-based stage orchestration. It serves as the execution layer for the "Highway" workflow engine.

**Key Features:**
- **DAG Execution:** Parallel and sequential stage orchestration.
- **Persistence:** Supports both SQLite (built-in, zero-conf) and PostgreSQL.
- **Resilience:** Built-in support for retries, timeouts, and circuit breakers.
- **RAG Integration:** "Retrieval-Augmented Generation" capabilities to generate pipelines from natural language prompts.
- **Pluggable Tasks:** extensible task system (Shell, HTTP, Docker, Python).

## Tech Stack

- **Language:** Python 3.11+
- **Build System:** Hatchling
- **Core Dependencies:** `pydantic`, `bulkman`, `resilient-circuit`, `python-ulid`
- **Optional Dependencies:** `psycopg[pool]` (Postgres), `ragit` (RAG), `structlog`, `opentelemetry`
- **Testing:** `pytest`
- **Linting/Formatting:** `ruff`
- **Type Checking:** `mypy`

## Development Workflow

### 1. Setup

Install the project in editable mode with development dependencies:

```bash
make install-dev
# OR
pip install -e ".[dev]"
```

### 2. Testing

The project distinguishes between standard unit tests and "Golden Standard" integration tests. It also supports running tests against different database backends.

**Standard Unit Tests:**
```bash
make test                  # Run all unit tests (SQLite & Postgres)
make test-sqlite           # Run unit tests on SQLite only
make test-postgres         # Run unit tests on PostgreSQL only (requires Docker)
```

**Golden Standard Tests (Integration):**
```bash
make golden-tests          # Run all golden tests
make golden-tests-sqlite   # Run golden tests on SQLite
```

**All Tests:**
```bash
make test-all              # Run unit + golden tests
```

### 3. Code Quality

Ensure code quality before committing:

```bash
make check                 # Runs lint + type-check + tests
```

*   **Linting:** `make lint` (uses `ruff`)
*   **Auto-fix:** `make lint-fix`
*   **Type Checking:** `make type-check` (uses `mypy`)

## Directory Structure

- **`src/stabilize/`**: Core source code.
    - **`orchestrator.py`**: Main engine logic.
    - **`persistence/`**: Database adapters (SQLite/Postgres).
    - **`tasks/`**: Built-in task implementations (HTTP, Shell, etc.).
    - **`rag/`**: AI pipeline generation logic.
- **`tests/`**: Unit tests.
- **`golden_standard_tests/`**: Integration tests comparing execution output against expected "golden" results.
- **`migrations/`**: SQL migration files (managed by `migretti`).
- **`examples/`**: Practical usage examples.

## CLI Usage

The `stabilize` CLI is the entry point for administrative tasks.

**Database Migrations (PostgreSQL):**
```bash
stabilize mg-up                     # Apply migrations
stabilize mg-status                 # Check status
```

**RAG Pipeline Generation:**
```bash
stabilize rag init                  # Initialize embeddings
stabilize rag generate "prompt"     # Generate code from natural language
```
