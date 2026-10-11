"""Admission of workflows whose pipeline config limits concurrent executions.

StartWorkflow and StartWaitingWorkflows both decide inside one store
transaction that first takes the config's admission lock
(StoreTransaction.lock_admission), so the RUNNING count they read cannot change
before their status write commits.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import StartStage, StartWorkflow
from stabilize.queue.ownership import queue_table_name

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import StoreTransaction


def limits_concurrency(execution: Workflow) -> bool:
    return bool(
        execution.is_limit_concurrent and execution.pipeline_config_id and execution.max_concurrent_executions > 0
    )


def start_running(handler: Any, txn: StoreTransaction, execution: Workflow, initial_stages: list[StageExecution]) -> None:
    handler.set_workflow_status(execution, WorkflowStatus.RUNNING)
    execution.start_time = handler.current_time_millis()
    execution.queue_table = queue_table_name(handler.queue)
    txn.update_workflow_status(execution)
    for stage in initial_stages:
        txn.push_message(
            StartStage(
                execution_type=execution.type.value,
                execution_id=execution.id,
                stage_id=stage.id,
            )
        )


def promote_waiting(
    handler: Any, txn: StoreTransaction, config_id: str, purge: bool
) -> tuple[list[tuple[Workflow, list[StageExecution]]], list[Workflow]]:
    """Start the oldest BUFFERED workflows up to the free slots; cancel the rest when purging."""
    started: list[tuple[Workflow, list[StageExecution]]] = []
    canceled: list[Workflow] = []
    running = txn.count_running(config_id)
    for workflow_id in txn.buffered_ids(config_id):
        execution = handler.repository.retrieve(workflow_id)
        if execution.status != WorkflowStatus.BUFFERED:
            continue
        limit = execution.max_concurrent_executions
        free = limit <= 0 or running < limit
        if execution.is_canceled or (not free and purge):
            handler.set_workflow_status(execution, WorkflowStatus.CANCELED)
            execution.end_time = handler.current_time_millis()
            txn.update_workflow_status(execution)
            canceled.append(execution)
            continue
        if not free:
            continue
        initial = execution.initial_stages()
        if not initial:
            handler.set_workflow_status(execution, WorkflowStatus.NOT_STARTED)
            txn.update_workflow_status(execution)
            txn.push_message(StartWorkflow(execution_type=execution.type.value, execution_id=execution.id))
            continue
        start_running(handler, txn, execution, initial)
        running += 1
        started.append((execution, initial))
    return started, canceled
