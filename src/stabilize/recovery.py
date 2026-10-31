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
from dataclasses import dataclass
from typing import TYPE_CHECKING

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
                results.append(self._wake_waiting(config_id))
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
        failed = sum(1 for r in results if r.status == "failed")

        logger.info(
            "Recovery complete: %d recovered, %d partial, %d skipped, %d failed",
            recovered,
            partial,
            skipped,
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

        criteria = WorkflowCriteria(
            statuses={WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED, WorkflowStatus.BUFFERED},
            page_size=None,
            start_time_after=cutoff_time,
        )

        if application:
            return self.store.retrieve_by_application(application, criteria)
        if hasattr(self.store, "get_all_pending_workflows"):
            return iter(getattr(self.store, "get_all_pending_workflows")(criteria))
        logger.warning(
            "Store doesn't support get_all_pending_workflows, recovery may be incomplete without application filter"
        )
        return iter(())

    def _wake_waiting(self, pipeline_config_id: str) -> RecoveryResult:
        from stabilize.queue.messages import StartWaitingWorkflows

        try:
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
        if full_workflow.status == WorkflowStatus.NOT_STARTED:
            return self._recover_not_started(full_workflow)

        recovery_messages = plan_running_workflow(full_workflow, self.queue)
        messages_queued = 0
        failed_pushes = 0
        if not recovery_messages:
            return RecoveryResult(
                workflow_id=workflow.id,
                status="skipped",
                message="No recovery messages to push",
            )
        stages_requeued = len({getattr(m, "stage_id", None) for m in recovery_messages})

        # Push all messages atomically in a single transaction
        try:
            with self.store.transaction(self.queue) as txn:
                for msg in recovery_messages:
                    txn.push_message(msg)
            messages_queued = len(recovery_messages)
        except Exception as e:
            logger.warning(
                "Failed to push recovery messages atomically for workflow %s: %s",
                workflow.id,
                e,
            )
            failed_pushes = len(recovery_messages)

        return RecoveryResult(
            workflow_id=workflow.id,
            status="recovered" if failed_pushes == 0 else "partial",
            message=f"Re-queued {stages_requeued} stages ({messages_queued} messages, {failed_pushes} failed)",
            stages_requeued=stages_requeued,
            failed_pushes=failed_pushes,
        )


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
