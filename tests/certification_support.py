from __future__ import annotations

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
