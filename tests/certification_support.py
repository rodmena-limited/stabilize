from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from stabilize import (
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)
from stabilize.events import reset_event_bus, reset_event_recorder
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.tasks.http import HTTPTask
from stabilize.tasks.shell import ShellTask

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def _make_infra():
    """Create fresh isolated in-memory infrastructure for a single test.

    Returns (store, queue, processor, orchestrator, task_registry).
    """
    # Reset singletons for isolation
    SingletonMeta.reset(ConnectionManager)
    RunTaskHandler._executing_tasks.clear()
    reset_event_bus()
    reset_event_recorder()

    store = SqliteWorkflowStore(
        connection_string="sqlite:///:memory:",
        create_tables=True,
    )
    queue = SqliteQueue(
        connection_string="sqlite:///:memory:",
        table_name="queue_messages",
    )
    queue._create_table()

    task_registry = TaskRegistry()
    task_registry.register("shell", ShellTask)
    task_registry.register("http", HTTPTask)

    processor = QueueProcessor(queue, store=store, task_registry=task_registry)
    orchestrator = Orchestrator(queue)

    return store, queue, processor, orchestrator, task_registry


def _shell_stage(
    ref_id: str,
    name: str,
    command: str,
    deps: set[str] | None = None,
    timeout: int = 60,
    continue_on_failure: bool = False,
    restart_on_failure: bool = False,
    extra_context: dict | None = None,
) -> StageExecution:
    """Build a StageExecution wired to ShellTask."""
    context: dict = {"command": command, "timeout": timeout}
    if continue_on_failure:
        context["continue_on_failure"] = True
        context["continuePipelineOnFailure"] = True
    if restart_on_failure:
        context["restart_on_failure"] = True
    if extra_context:
        context.update(extra_context)
    return StageExecution(
        ref_id=ref_id,
        type="shell",
        name=name,
        context=context,
        requisite_stage_ref_ids=deps or set(),
        tasks=[
            TaskExecution.create(
                name=f"{name} task",
                implementing_class="shell",
                stage_start=True,
                stage_end=True,
            ),
        ],
    )


def _http_stage(
    ref_id: str,
    name: str,
    url: str,
    method: str = "GET",
    deps: set[str] | None = None,
    timeout: int = 30,
    continue_on_failure: bool = False,
    extra_context: dict | None = None,
) -> StageExecution:
    """Build a StageExecution wired to HTTPTask."""
    context: dict = {"url": url, "method": method, "timeout": timeout}
    if continue_on_failure:
        context["continue_on_failure"] = True
        context["continuePipelineOnFailure"] = True
    if extra_context:
        context.update(extra_context)
    return StageExecution(
        ref_id=ref_id,
        type="http",
        name=name,
        context=context,
        requisite_stage_ref_ids=deps or set(),
        tasks=[
            TaskExecution.create(
                name=f"{name} task",
                implementing_class="http",
                stage_start=True,
                stage_end=True,
            ),
        ],
    )


def _run(store, queue, processor, orchestrator, workflow, timeout=30.0):
    """Store, start, process, and retrieve a workflow result."""
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=timeout)
    return store.retrieve(workflow.id)


def _get_stage(result: Workflow, ref_id: str) -> StageExecution | None:
    """Find a stage by ref_id in the result."""
    for s in result.stages:
        if s.ref_id == ref_id:
            return s
    return None


@contextmanager
def local_json_server() -> Iterator[str]:
    """An HTTP server on the loopback interface answering GET with JSON: a real counterparty for tests."""
    import json
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            body = json.dumps({"url": self.path, "ok": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
