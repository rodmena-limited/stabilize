from __future__ import annotations

import pytest

from stabilize import (
    DockerTask,
    HTTPTask,
    Orchestrator,
    PythonTask,
    QueueProcessor,
    ShellTask,
    SqliteQueue,
    SqliteWorkflowStore,
    TaskRegistry,
    Workflow,
)


@pytest.fixture
def workflow_engine():
    """Create a complete workflow engine with all handlers."""
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()

    registry = TaskRegistry()
    registry.register("shell", ShellTask)
    registry.register("python", PythonTask)
    registry.register("docker", DockerTask)
    registry.register("http", HTTPTask)

    processor = QueueProcessor(queue, store=store, task_registry=registry)

    orchestrator = Orchestrator(queue)

    return {
        "store": store,
        "queue": queue,
        "processor": processor,
        "orchestrator": orchestrator,
        "registry": registry,
    }


def run_workflow(engine: dict, workflow: Workflow, timeout: float = 30.0) -> Workflow:
    """Execute a workflow and return the result."""
    engine["store"].store(workflow)
    engine["orchestrator"].start(workflow)
    engine["processor"].process_all(timeout=timeout)
    return engine["store"].retrieve(workflow.id)
