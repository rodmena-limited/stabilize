"""
SubWorkflowTask - executes a child workflow and waits for completion.

Implements WCP-22: Recursion.

A task that starts a child workflow and polls until it completes.
Supports recursion depth tracking to prevent infinite recursion.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import timedelta
from typing import Any

from stabilize.models.stage.definition import stage_from_definition
from stabilize.models.status import WorkflowStatus
from stabilize.tasks.handles import EngineHandles, current_engine
from stabilize.tasks.interface import RetryableTask
from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)

DEFAULT_MAX_RECURSION_DEPTH = 10
TIMEOUT_KEY = "sub_workflow_timeout_seconds"
CHILD_ID_SCHEME = "stabilize-child/1"


def child_workflow_id(tenant: str, execution_id: str, task_id: str, generation: int, task_start_ms: int) -> str:
    """The child's id: the parent task's start time and a hash of what it is a child of, so a retry reuses it."""
    from ulid import ULID

    digest = hashlib.sha256(f"{CHILD_ID_SCHEME}|{tenant}|{execution_id}|{task_id}|{generation}".encode()).digest()
    return str(ULID.from_bytes(int(task_start_ms).to_bytes(6, "big") + digest[:10]))


class SubWorkflowTask(RetryableTask):
    """Task that starts and monitors a child workflow execution.

    The child workflow is started on the first execution, in one transaction with
    its StartWorkflow message, on the store and queue of the processor running the
    task. Later executions poll the child's status until it completes.

    Context keys:
        _sub_workflow_config: dict with workflow configuration
        _sub_workflow_id: set after child is started (for polling)
        _recursion_depth: current recursion depth (incremented per level)
        _max_recursion_depth: maximum allowed depth (default: 10)
        sub_workflow_timeout_seconds: optional total time the child may take

    Outputs:
        sub_workflow_id: ID of the child workflow
        sub_workflow_status: Final status of the child workflow
        sub_workflow_outputs: Merged outputs from the child workflow
    """

    reset_context_keys: tuple[str, ...] = ("_sub_workflow_id",)

    @property
    def aliases(self) -> list[str]:
        return ["subWorkflow", "sub_workflow", "childWorkflow"]

    def get_timeout(self) -> timedelta:
        return timedelta(days=36500)

    def get_dynamic_timeout(self, stage: Any) -> timedelta:
        seconds = stage.context.get(TIMEOUT_KEY)
        if isinstance(seconds, (int, float)) and not isinstance(seconds, bool) and seconds > 0:
            return timedelta(seconds=float(seconds))
        return self.get_timeout()

    def get_backoff_period(self, stage: Any = None, duration: timedelta | None = None) -> timedelta:
        elapsed = duration.total_seconds() if duration is not None else 0.0
        return timedelta(seconds=min(30.0, max(1.0, elapsed / 10.0)))

    def execute(self, stage: Any) -> TaskResult:
        from stabilize.resilience import process_executor

        if process_executor.IN_WORKER_PROCESS:
            return TaskResult.terminal(
                "SubWorkflowTask cannot run under process isolation; it needs the processor's store and queue"
            )
        engine = self._engine()
        if engine is None:
            return TaskResult.terminal("SubWorkflowTask has no store and queue; run it under a QueueProcessor")
        sub_workflow_id = stage.context.get("_sub_workflow_id")
        if sub_workflow_id is None:
            return self._start_child(stage, engine)
        return self._poll_child(stage, sub_workflow_id, engine)

    def on_cancel(self, stage: Any) -> TaskResult | None:
        self._cancel_child(stage, "parent task cancelled")
        return None

    def on_timeout(self, stage: Any) -> TaskResult | None:
        self._cancel_child(stage, "parent task timed out")
        return None

    def _engine(self) -> EngineHandles | None:
        engine = current_engine()
        if engine is not None:
            return engine
        from stabilize.orchestrator import Orchestrator

        orchestrator = Orchestrator.get_instance()
        if orchestrator is None or orchestrator.store is None:
            return None
        return EngineHandles(orchestrator.store, orchestrator.queue)

    def _running_task(self, stage: Any) -> Any:
        for task in stage.tasks:
            if task.status == WorkflowStatus.RUNNING:
                return task
        return stage.tasks[0] if stage.tasks else None

    def _start_child(self, stage: Any, engine: EngineHandles) -> TaskResult:
        context = stage.context
        depth = context.get("_recursion_depth", 0)
        max_depth = context.get("_max_recursion_depth", DEFAULT_MAX_RECURSION_DEPTH)
        if depth >= max_depth:
            return TaskResult.terminal(
                f"Maximum recursion depth ({max_depth}) exceeded",
                context={"_recursion_depth": depth},
            )
        config = context.get("_sub_workflow_config", {})
        if not config:
            return TaskResult.terminal("No _sub_workflow_config in context")

        from stabilize.models.workflow import Workflow
        from stabilize.queue.messages import StartWorkflow
        from stabilize.queue.ownership import queue_table_name

        parent_id = stage.execution.id if hasattr(stage, "execution") and stage.has_execution() else ""
        child_context = dict(config.get("context", {}))
        child_context["_recursion_depth"] = depth + 1
        child_context["_max_recursion_depth"] = max_depth
        child_context["_parent_workflow_id"] = parent_id
        task = self._running_task(stage)
        task_id = task.id if task is not None else stage.id
        generation = int(getattr(task, "generation", 0) or 0)
        start_ms = int(getattr(task, "start_time", None) or stage.start_time or 0)
        tenant = str(getattr(engine.store, "tenant", "") or "")
        child_id = child_workflow_id(tenant, parent_id or stage.id, task_id, generation, start_ms)
        try:
            child = Workflow.create(
                application=config.get("application", "stabilize"),
                name=config.get("name", f"sub-workflow-depth-{depth + 1}"),
                stages=[stage_from_definition(s) for s in config.get("stages", [])],
                context=child_context,
            )
        except (TypeError, ValueError, KeyError) as e:
            return TaskResult.terminal(f"Invalid _sub_workflow_config: {e}")
        child.id = child_id
        child.queue_table = queue_table_name(engine.queue)
        started_here = False
        with engine.store.transaction(engine.queue) as txn:
            txn.lock_admission(f"subworkflow\x1f{tenant}\x1f{task_id}\x1f{generation}")
            if not engine.store.exists(child_id):
                txn.store_workflow(child)
                txn.push_message(StartWorkflow(execution_type=child.type.value, execution_id=child_id))
                started_here = True
        logger.info(
            "%s child workflow %s at recursion depth %d",
            "Started" if started_here else "Found already started",
            child_id,
            depth + 1,
        )
        return TaskResult.running(
            context={"_sub_workflow_id": child_id, "_recursion_depth": depth + 1},
        )

    def _poll_child(self, stage: Any, sub_workflow_id: str, engine: EngineHandles) -> TaskResult:
        from stabilize.persistence.store import WorkflowNotFoundError

        try:
            child = engine.store.retrieve(sub_workflow_id)
        except WorkflowNotFoundError:
            return TaskResult.terminal(f"Child workflow {sub_workflow_id} not found")
        if child.status == WorkflowStatus.SUCCEEDED:
            logger.info("Child workflow %s completed successfully", sub_workflow_id)
            return TaskResult.success(
                outputs={
                    "sub_workflow_id": sub_workflow_id,
                    "sub_workflow_status": "SUCCEEDED",
                    "sub_workflow_outputs": child.context.get("outputs", {}),
                },
            )
        if child.status.is_halt:
            logger.warning("Child workflow %s halted with status %s", sub_workflow_id, child.status)
            return TaskResult.terminal(
                f"Child workflow failed with status {child.status.name}",
                context={"sub_workflow_id": sub_workflow_id, "sub_workflow_status": child.status.name},
            )
        if child.status.is_complete:
            return TaskResult.success(
                outputs={"sub_workflow_id": sub_workflow_id, "sub_workflow_status": child.status.name},
            )
        return TaskResult.running()

    def _cancel_child(self, stage: Any, reason: str) -> None:
        child_id = stage.context.get("_sub_workflow_id")
        engine = self._engine()
        if not child_id or engine is None:
            return
        from stabilize.queue.messages import CancelWorkflow

        try:
            with engine.store.transaction(engine.queue) as txn:
                txn.push_message(
                    CancelWorkflow(execution_type="PIPELINE", execution_id=child_id, user="system", reason=reason)
                )
        except Exception as e:
            logger.warning("Could not cancel child workflow %s: %s", child_id, e)
