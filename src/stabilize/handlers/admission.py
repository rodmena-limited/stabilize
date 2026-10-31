"""Admission of workflows whose pipeline config limits concurrent executions.

StartWorkflow and StartWaitingWorkflows both decide inside one store
transaction that first takes the config's admission lock
(StoreTransaction.lock_admission), so the RUNNING count they read cannot change
before their status write commits.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from stabilize import metrics
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowCriteria
from stabilize.queue.messages import StartStage
from stabilize.queue.ownership import queue_table_name

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import StoreTransaction

logger = logging.getLogger(__name__)


_UNSUPPORTED_WARNED: set[str] = set()


def try_lock_admission(txn: StoreTransaction, key: str) -> bool:
    """Take the admission lock; False when the store's transactions do not support admission control."""
    try:
        txn.lock_admission(key)
    except NotImplementedError:
        name = type(txn).__name__
        if name not in _UNSUPPORTED_WARNED:
            _UNSUPPORTED_WARNED.add(name)
            logger.warning("%s has no admission lock: limitConcurrent is not enforced on this store", name)
        return False
    return True


def limits_concurrency(execution: Workflow) -> bool:
    return bool(
        execution.is_limit_concurrent and execution.pipeline_config_id and execution.max_concurrent_executions > 0
    )


def start_running(
    handler: Any, txn: StoreTransaction, execution: Workflow, initial_stages: list[StageExecution]
) -> None:
    metrics.increment("workflow_count", application=execution.application)
    handler.set_workflow_status(execution, WorkflowStatus.RUNNING)
    execution.start_time = handler.current_time_millis()
    execution.queue_table = execution.queue_table or queue_table_name(handler.queue)
    txn.update_workflow_status(execution)
    for stage in initial_stages:
        txn.push_message(
            StartStage(
                execution_type=execution.type.value,
                execution_id=execution.id,
                stage_id=stage.id,
            )
        )


def promote_waiting(handler: Any, config_id: str, purge: bool, message: Any) -> tuple[list[Any], list[Any]]:
    """Start the oldest BUFFERED workflows of config_id up to the free slots; cancel the rest when purging.

    The candidates are listed under the admission lock, loaded outside it (no
    second connection is taken while the lock is held), and decided under the
    lock again, each one re-checked for still being BUFFERED.
    """
    repository = handler.repository
    with repository.transaction(handler.queue) as txn:
        if try_lock_admission(txn, config_id):
            ids = txn.buffered_ids(config_id, queue_table=queue_table_name(handler.queue))
        else:
            ids = sorted(w.id for w in _by_status(repository, config_id, WorkflowStatus.BUFFERED))
    candidates = [repository.retrieve(i) for i in ids]
    started: list[tuple[Workflow, list[StageExecution]]] = []
    canceled: list[Workflow] = []
    with repository.transaction(handler.queue) as txn:
        locked = try_lock_admission(txn, config_id)
        if locked:
            running = txn.count_running(config_id)
        else:
            running = sum(1 for _ in _by_status(repository, config_id, WorkflowStatus.RUNNING))
        now = handler.current_time_millis()
        for execution in candidates:
            if locked:
                status, is_canceled = txn.workflow_status(execution.id)
                if status != WorkflowStatus.BUFFERED.name:
                    continue
                execution.is_canceled = execution.is_canceled or is_canceled
            limit = execution.max_concurrent_executions
            free = limit <= 0 or running < limit
            expired = execution.start_time_expiry is not None and now > execution.start_time_expiry
            if execution.is_canceled or expired or (not free and purge):
                handler.set_workflow_status(execution, WorkflowStatus.CANCELED)
                execution.end_time = now
                txn.update_workflow_status(execution)
                canceled.append(execution)
                continue
            if not free:
                continue
            initial = execution.initial_stages()
            if not initial:
                handler.set_workflow_status(execution, WorkflowStatus.RUNNING)
                handler.set_workflow_status(execution, WorkflowStatus.TERMINAL)
                execution.end_time = now
                txn.update_workflow_status(execution)
                continue
            start_running(handler, txn, execution, initial)
            running += 1
            started.append((execution, initial))
        if message.message_id:
            txn.mark_message_processed(
                message_id=message.message_id,
                handler_type="StartWaitingWorkflows",
                execution_id=None,
            )
    return started, canceled


def _by_status(repository: Any, config_id: str, status: WorkflowStatus) -> Any:
    return repository.retrieve_by_pipeline_config_id(config_id, WorkflowCriteria(statuses={status}, page_size=None))
