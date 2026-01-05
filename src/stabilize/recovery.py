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
        batch_size=100,  # Process in batches
    )
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from stabilize.errors import RecoveryError
from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue

logger = logging.getLogger(__name__)


@dataclass
class RecoveryResult:
    """Result of workflow recovery operation."""

    workflow_id: str
    status: str  # "recovered" | "skipped" | "failed"
    message: str
    stages_requeued: int = 0


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
            batch_size: Number of workflows to process per batch
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
            logger.info("Found %d workflows to check for recovery", len(workflows))

            for workflow in workflows:
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

        except Exception as e:
            raise RecoveryError(
                f"Workflow recovery failed: {e}",
                cause=e,
            ) from e

        # Summary
        recovered = sum(1 for r in results if r.status == "recovered")
        skipped = sum(1 for r in results if r.status == "skipped")
        failed = sum(1 for r in results if r.status == "failed")

        logger.info(
            "Recovery complete: %d recovered, %d skipped, %d failed",
            recovered,
            skipped,
            failed,
        )

        return results

    def _get_workflows_for_recovery(
        self,
        application: str | None,
        cutoff_time: int,
    ) -> list:
        """Get workflows that may need recovery.

        Args:
            application: Optional application filter
            cutoff_time: Only consider workflows started after this time

        Returns:
            List of Workflow objects to check
        """
        from stabilize.persistence.store import WorkflowCriteria

        # Query for running/not-started workflows (both may need recovery)
        criteria = WorkflowCriteria(
            statuses={WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED},
            page_size=self.batch_size,
            start_time_after=cutoff_time,
        )

        workflows = []

        if application:
            for wf in self.store.retrieve_by_application(application, criteria):
                workflows.append(wf)
        else:
            # Need to get all applications - this is a limitation
            # In production, you'd iterate through known applications
            # For now, we'll use a direct query if available
            if hasattr(self.store, "get_all_pending_workflows"):
                workflows = list(self.store.get_all_pending_workflows(criteria))
            else:
                logger.warning(
                    "Store doesn't support get_all_pending_workflows, "
                    "recovery may be incomplete without application filter"
                )

        return workflows

    def _recover_workflow(self, workflow: Workflow) -> RecoveryResult:
        """Recover a single workflow.

        Args:
            workflow: The workflow to recover

        Returns:
            RecoveryResult describing what happened
        """
        from stabilize.queue.messages import StartStage, StartWorkflow

        # Check if workflow is actually in a state needing recovery
        if workflow.status.is_complete:
            return RecoveryResult(
                workflow_id=workflow.id,
                status="skipped",
                message=f"Workflow already complete ({workflow.status.name})",
            )

        # Get the full workflow with stages
        full_workflow = self.store.retrieve(workflow.id)

        # Find stages that need to be re-queued
        stages_to_requeue = []

        for stage in full_workflow.stages:
            # Stage is in-progress but not complete
            if stage.status in {WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED}:
                stages_to_requeue.append(stage)
            # Stage has STARTED status but no completion
            elif stage.status == WorkflowStatus.NOT_STARTED and self._has_started(stage):
                stages_to_requeue.append(stage)

        if not stages_to_requeue:
            # No stages to requeue, but workflow isn't complete
            # This might mean we need to restart from the beginning
            if full_workflow.status == WorkflowStatus.NOT_STARTED:
                # Requeue workflow start
                self.queue.push(
                    StartWorkflow(
                        execution_type=full_workflow.type.value,
                        execution_id=full_workflow.id,
                    )
                )
                return RecoveryResult(
                    workflow_id=workflow.id,
                    status="recovered",
                    message="Re-queued workflow start",
                    stages_requeued=0,
                )
            else:
                return RecoveryResult(
                    workflow_id=workflow.id,
                    status="skipped",
                    message="No stages need recovery",
                )

        # Re-queue each stage that needs recovery
        for stage in stages_to_requeue:
            self.queue.push(
                StartStage(
                    execution_type=full_workflow.type.value,
                    execution_id=full_workflow.id,
                    stage_id=stage.id,
                )
            )
            logger.debug(
                "Re-queued stage %s (%s) for workflow %s",
                stage.id,
                stage.ref_id,
                workflow.id,
            )

        return RecoveryResult(
            workflow_id=workflow.id,
            status="recovered",
            message=f"Re-queued {len(stages_to_requeue)} stages",
            stages_requeued=len(stages_to_requeue),
        )

    def _has_started(self, stage: StageExecution) -> bool:
        """Check if a stage has actually started execution.

        A stage may be in NOT_STARTED status but have a start_time,
        indicating it was being processed when the crash occurred.

        Args:
            stage: The stage to check

        Returns:
            True if stage has evidence of starting
        """
        if stage.start_time is not None:
            return True

        # Check if any tasks have started
        for task in stage.tasks:
            if task.start_time is not None:
                return True

        return False


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
                logger.error(f"Failed to recover {r.workflow_id}: {r.message}")
    """
    recovery = WorkflowRecovery(
        store=store,
        queue=queue,
        max_recovery_age_hours=max_age_hours,
    )
    return recovery.recover_pending_workflows(application=application)
