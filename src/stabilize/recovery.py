"""Startup recovery for Stabilize workflows.

This module provides automatic recovery of in-progress workflows after
a crash or restart. It follows the DBOS pattern of:

1. On startup, find all workflows in RUNNING/PENDING status
2. Re-queue their current stage for continuation
3. Log recovery actions for audit

Recovery is idempotent - running recovery multiple times has no effect
on already-recovered workflows.

Usage:
    from stabilize.recovery import WorkflowRecovery

    # At application startup
    recovery = WorkflowRecovery(store, queue)
    recovered = recovery.recover_pending_workflows()
    print(f"Recovered {len(recovered)} workflows")

Configuration:
    recovery = WorkflowRecovery(
        store,
        queue,
        max_recovery_age_hours=24,  # Only recover recent workflows
    )
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from stabilize.errors import RecoveryError
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.committed_marks import delivery_exempt
from stabilize.queue.ownership import DEFAULT_TABLE, queue_table_name, workflow_queue_table
from stabilize.recovery_plan import plan_running_workflow

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


@dataclass
class RecoveryResult:
    """Result of workflow recovery operation."""

    workflow_id: str
    status: str  # "recovered" | "skipped" | "failed" | "partial"
    message: str
    stages_requeued: int = 0
    failed_pushes: int = 0


_COUNTED_OUTCOMES = frozenset(
    {"recovered", "partial", "skipped", "stale", "failed", "locked", "dead_lettered", "dead_letter_stale"}
)


@dataclass
class RecoverySummary:
    """What the last recovery sweep did, by outcome."""

    results: list[RecoveryResult] = field(default_factory=list)
    recovered: int = 0
    partial: int = 0
    skipped: int = 0
    stale: int = 0
    failed: int = 0
    locked: int = 0
    dead_lettered: int = 0
    dead_letter_stale: int = 0
    error: str | None = None

    @classmethod
    def of(cls, results: list[RecoveryResult]) -> RecoverySummary:
        summary = cls(results=list(results))
        for result in results:
            if result.status in _COUNTED_OUTCOMES:
                setattr(summary, result.status, getattr(summary, result.status) + 1)
        return summary


class WorkflowRecovery:
    """Recovers in-progress workflows after crash or restart.

    On startup, this class finds workflows that were interrupted mid-execution
    and re-queues them for continuation. This ensures no work is lost due to
    crashes.

    The recovery process:
    1. Query store for workflows in RUNNING/PENDING status
    2. For each workflow, find stages that need continuation
    3. Re-queue StartStage messages for incomplete stages
    4. Log all recovery actions

    Recovery is safe to run multiple times - it only affects workflows
    that actually need recovery.
    """

    def __init__(
        self,
        store: WorkflowStore,
        queue: Queue,
        max_recovery_age_hours: float = 24.0,
        batch_size: int = 100,
    ) -> None:
        """Initialize the recovery handler.

        Args:
            store: Workflow store for querying state
            queue: Queue for re-queuing messages
            max_recovery_age_hours: Only recover workflows started within
                this time window (default 24 hours)
            batch_size: Accepted for compatibility; recovery examines every
                pending workflow in the window regardless of this value.
        """
        self.store = store
        self.queue = queue
        self.max_recovery_age_hours = max_recovery_age_hours
        self.batch_size = batch_size
        self.page_size = 500
        self._cutoff_ms = 0

    def recover_pending_workflows(
        self,
        application: str | None = None,
    ) -> list[RecoveryResult]:
        """Recover all pending workflows.

        Finds workflows in RUNNING or PENDING status and re-queues
        their incomplete stages for continuation.

        Args:
            application: Optional filter by application name

        Returns:
            List of recovery results

        Raises:
            RecoveryError: If recovery fails critically
        """
        results: list[RecoveryResult] = []
        current_time_ms = int(time.time() * 1000)
        max_age_ms = int(self.max_recovery_age_hours * 3600 * 1000)
        cutoff_time = current_time_ms - max_age_ms
        self._cutoff_ms = cutoff_time

        logger.info(
            "Starting workflow recovery (max_age=%s hours, application=%s)",
            self.max_recovery_age_hours,
            application or "all",
        )

        try:
            # Get workflows needing recovery
            workflows = self._get_workflows_for_recovery(application, cutoff_time)
            own_table = queue_table_name(self.queue)
            applications: set[str] = set()
            waiting_configs: set[str] = set()
            waiting_limits: dict[str, int] = {}

            unowned = 0
            for workflow in workflows:
                if getattr(workflow, "queue_table", None) is None and own_table != DEFAULT_TABLE:
                    unowned += 1
                if workflow_queue_table(workflow) != own_table:
                    continue
                applications.add(workflow.application)
                if workflow.status == WorkflowStatus.BUFFERED:
                    if workflow.pipeline_config_id:
                        waiting_configs.add(workflow.pipeline_config_id)
                        waiting_limits[workflow.pipeline_config_id] = max(
                            waiting_limits.get(workflow.pipeline_config_id, 0), workflow.max_concurrent_executions
                        )
                    continue
                try:
                    result = self._recover_workflow(workflow)
                    results.append(result)

                    if result.status == "recovered":
                        logger.info(
                            "Recovered workflow %s (%d stages requeued)",
                            workflow.id,
                            result.stages_requeued,
                        )
                except Exception as e:
                    logger.error(
                        "Failed to recover workflow %s: %s",
                        workflow.id,
                        e,
                        exc_info=True,
                    )
                    results.append(
                        RecoveryResult(
                            workflow_id=workflow.id,
                            status="failed",
                            message=str(e),
                        )
                    )

            if unowned:
                logger.warning(
                    "Recovery on queue table %s skipped %d workflow(s) with no recorded queue table "
                    "(written before 0.33.0, read as queue_messages)",
                    own_table,
                    unowned,
                )
            for config_id in sorted(waiting_configs):
                results.append(self._wake_waiting(config_id, waiting_limits.get(config_id, 0)))
            if application is None and len(applications) > 1:
                logger.warning(
                    "Unfiltered recovery on queue table %s examined %d applications (%s); "
                    "set recovery_application when processors with different task registries share a store",
                    own_table,
                    len(applications),
                    ", ".join(sorted(applications)),
                )

        except Exception as e:
            raise RecoveryError(
                f"Workflow recovery failed: {e}",
                cause=e,
            ) from e

        # Summary
        recovered = sum(1 for r in results if r.status == "recovered")
        partial = sum(1 for r in results if r.status == "partial")
        skipped = sum(1 for r in results if r.status == "skipped")
        stale = sum(1 for r in results if r.status == "stale")
        locked = sum(1 for r in results if r.status == "locked")
        dead = sum(1 for r in results if r.status in ("dead_lettered", "dead_letter_stale"))
        failed = sum(1 for r in results if r.status == "failed")

        logger.info(
            "Recovery complete: %d recovered, %d partial, %d skipped, %d stale, %d locked, %d dead-lettered, %d failed",
            recovered,
            partial,
            skipped,
            stale,
            locked,
            dead,
            failed,
        )

        return results

    def _get_workflows_for_recovery(
        self,
        application: str | None,
        cutoff_time: int,
    ) -> Iterator[Workflow]:
        """Yield every RUNNING or NOT_STARTED workflow started after cutoff_time (or not yet started).

        Args:
            application: Optional application filter
            cutoff_time: Only consider workflows started after this time

        Returns:
            Iterator of Workflow objects to check
        """
        from stabilize.persistence.store import WorkflowCriteria

        candidates = getattr(self.store, "recovery_candidate_ids", None)
        if callable(candidates):
            return self._paged_candidates(candidates, application)
        logger.warning("Store has no recovery_candidate_ids; recovery scans every pending workflow")
        criteria = WorkflowCriteria(
            statuses={WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED, WorkflowStatus.BUFFERED},
            page_size=None,
        )

        if application:
            return self.store.retrieve_by_application(application, criteria)
        if hasattr(self.store, "get_all_pending_workflows"):
            return iter(getattr(self.store, "get_all_pending_workflows")(criteria))
        logger.warning(
            "Store doesn't support get_all_pending_workflows, recovery may be incomplete without application filter"
        )
        return iter(())

    def _paged_candidates(self, candidates: Any, application: str | None) -> Iterator[Workflow]:
        own_table = queue_table_name(self.queue)
        after_id: str | None = None
        while True:
            ids = candidates(own_table, after_id, self.page_size)
            if not ids:
                return
            for workflow_id in ids:
                try:
                    workflow = self.store.retrieve(workflow_id)
                except Exception as e:
                    logger.warning("Recovery could not load workflow %s: %s", workflow_id, e)
                    continue
                if application is not None and workflow.application != application:
                    continue
                yield workflow
            after_id = ids[-1]

    def _stale(self, workflow: Workflow) -> RecoveryResult | None:
        from stabilize.recovery_age import workflow_activity_ms

        activity = workflow_activity_ms(workflow, self.store)
        if activity is not None and activity < self._cutoff_ms:
            age_hours = (int(time.time() * 1000) - activity) / 3_600_000
            return RecoveryResult(
                workflow_id=workflow.id,
                status="stale",
                message=f"No activity for {age_hours:.1f} hours (window {self.max_recovery_age_hours} hours)",
            )
        return None

    def _wake_waiting(self, pipeline_config_id: str, limit: int = 0) -> RecoveryResult:
        from stabilize.queue.messages import StartWaitingWorkflows

        if self.queue.has_pending_message_for_config(pipeline_config_id):
            return RecoveryResult(
                workflow_id=pipeline_config_id, status="skipped", message="A wake-up is already queued"
            )
        try:
            with self.store.transaction(self.queue) as txn:
                if not txn.try_lock_recovery(f"config\x1f{pipeline_config_id}"):
                    return RecoveryResult(
                        workflow_id=pipeline_config_id, status="locked", message="Another sweep holds this config"
                    )
                running = txn.count_running(pipeline_config_id) if limit > 0 else 0
                if limit > 0 and running >= limit:
                    return RecoveryResult(
                        workflow_id=pipeline_config_id,
                        status="skipped",
                        message=f"No free slot ({running} running of {limit})",
                    )
                if self.queue.has_pending_message_for_config(pipeline_config_id):
                    return RecoveryResult(
                        workflow_id=pipeline_config_id, status="skipped", message="A wake-up is already queued"
                    )
                txn.push_message(StartWaitingWorkflows(pipeline_config_id=pipeline_config_id, purge_queue=False))
        except NotImplementedError:
            with delivery_exempt():
                self.queue.push(StartWaitingWorkflows(pipeline_config_id=pipeline_config_id, purge_queue=False))
        except Exception as e:
            logger.warning("Failed to re-queue StartWaitingWorkflows for %s: %s", pipeline_config_id, e)
            return RecoveryResult(workflow_id=pipeline_config_id, status="failed", message=str(e), failed_pushes=1)
        return RecoveryResult(
            workflow_id=pipeline_config_id, status="recovered", message="Re-queued StartWaitingWorkflows"
        )

    def _recover_not_started(self, workflow: Workflow) -> RecoveryResult:
        """Re-queue StartWorkflow, so its handler's status and concurrency checks run."""
        from stabilize.queue.messages import StartWorkflow

        if self.queue.has_pending_message_for_execution(workflow.id):
            return RecoveryResult(workflow_id=workflow.id, status="skipped", message="A message is already queued")
        try:
            with delivery_exempt():
                self.queue.push(StartWorkflow(execution_type=workflow.type.value, execution_id=workflow.id))
        except Exception as e:
            logger.warning("Failed to re-queue StartWorkflow for %s: %s", workflow.id, e)
            return RecoveryResult(
                workflow_id=workflow.id,
                status="failed",
                message=f"Failed to re-queue workflow start: {e}",
                failed_pushes=1,
            )
        return RecoveryResult(workflow_id=workflow.id, status="recovered", message="Re-queued workflow start")

    def _recover_workflow(self, workflow: Workflow) -> RecoveryResult:
        """Recover a single workflow.

        Args:
            workflow: The workflow to recover

        Returns:
            RecoveryResult describing what happened
        """
        # Check if workflow is actually in a state needing recovery
        if workflow.status.is_complete:
            return RecoveryResult(
                workflow_id=workflow.id,
                status="skipped",
                message=f"Workflow already complete ({workflow.status.name})",
            )

        full_workflow = self.store.retrieve(workflow.id)
        stale = self._stale(full_workflow)
        if stale is not None:
            return stale
        if full_workflow.status == WorkflowStatus.NOT_STARTED:
            return self._recover_not_started(full_workflow)

        from stabilize.recovery_age import workflow_activity_ms

        activity = workflow_activity_ms(full_workflow, self.store)
        withheld = 0
        stale_letters = 0
        try:
            with self.store.transaction(self.queue) as txn:
                if not txn.try_lock_recovery(workflow.id):
                    return RecoveryResult(
                        workflow_id=workflow.id, status="locked", message="Another sweep holds this workflow"
                    )
                recovery_messages = plan_running_workflow(full_workflow, self.queue, self.store)
                to_push = []
                for msg in recovery_messages:
                    moved_at = self._dead_letter_moved_at(msg, workflow.id)
                    if moved_at is not None and (activity is None or moved_at >= activity):
                        withheld += 1
                        continue
                    if moved_at is not None:
                        stale_letters += 1
                    to_push.append(msg)
                if not to_push:
                    if withheld:
                        return RecoveryResult(
                            workflow_id=workflow.id,
                            status="dead_lettered",
                            message=f"{withheld} message(s) sit in the dead-letter queue since the last activity",
                        )
                    return RecoveryResult(
                        workflow_id=workflow.id, status="skipped", message="No recovery messages to push"
                    )
                for msg in to_push:
                    txn.push_message(msg)
        except Exception as e:
            logger.warning("Failed to push recovery messages atomically for workflow %s: %s", workflow.id, e)
            return RecoveryResult(
                workflow_id=workflow.id,
                status="partial",
                message=f"Recovery transaction failed: {e}",
                failed_pushes=1,
            )
        stages_requeued = len({getattr(m, "stage_id", None) for m in to_push})
        status = "dead_letter_stale" if stale_letters else "recovered"
        return RecoveryResult(
            workflow_id=workflow.id,
            status=status,
            message=f"Re-queued {stages_requeued} stages ({len(to_push)} messages, {withheld} withheld)",
            stages_requeued=stages_requeued,
            failed_pushes=0,
        )

    def _dead_letter_moved_at(self, message: Any, execution_id: str) -> int | None:
        lookup = getattr(self.queue, "dead_letter_moved_at_ms", None)
        if not callable(lookup):
            return None
        from stabilize.queue.messages import get_message_type_name

        target = getattr(message, "task_id", None) or getattr(message, "stage_id", None)
        try:
            moved = lookup(get_message_type_name(message), execution_id, target)
            return int(moved) if moved is not None else None
        except Exception as e:
            logger.warning("Dead-letter lookup failed for workflow %s: %s", execution_id, e)
            return None


def recover_on_startup(
    store: WorkflowStore,
    queue: Queue,
    application: str | None = None,
    max_age_hours: float = 24.0,
) -> list[RecoveryResult]:
    """Convenience function to run recovery at application startup.

    Args:
        store: Workflow store
        queue: Message queue
        application: Optional application filter
        max_age_hours: Maximum age of workflows to recover

    Returns:
        List of recovery results

    Example:
        from stabilize.recovery import recover_on_startup

        # In your app startup
        results = recover_on_startup(store, queue, application="my-app")
        for r in results:
            if r.status == "failed":
                logger.error("Failed to recover %s: %s", r.workflow_id, r.message)
    """
    recovery = WorkflowRecovery(
        store=store,
        queue=queue,
        max_recovery_age_hours=max_age_hours,
    )
    return recovery.recover_pending_workflows(application=application)
