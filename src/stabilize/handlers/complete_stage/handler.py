"""
CompleteStageHandler - handles stage completion.

This is a critical handler that:
1. Determines stage status from tasks and synthetic stages
2. Plans and starts after stages
3. Plans and starts on-failure stages
4. Triggers downstream stages via startNext()
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.errors import is_store_unavailable, is_transient
from stabilize.errors.delivery import DuplicateDeliveryError
from stabilize.handlers.complete_stage.deferred import CompleteStageDeferredMixin
from stabilize.handlers.complete_stage.fan_out import CompleteStageFanOutMixin
from stabilize.handlers.complete_stage.support import CompleteStageSupport
from stabilize.handlers.deferred_choice import settles_on_complete
from stabilize.models.stage.enums import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.tenancy import TenancyModeChanged, TenantContextError, TenantMismatchError
from stabilize.queue.messages import (
    CancelStage,
    CompleteStage,
    CompleteWorkflow,
    ContinueParentStage,
    StartStage,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.tasks.registry import TaskRegistry

logger = logging.getLogger(__name__)


class CompleteStageHandler(CompleteStageDeferredMixin, CompleteStageFanOutMixin, CompleteStageSupport):
    """
    Handler for CompleteStage messages.

    Execution flow:
    1. Check if stage already complete
    2. Determine status from synthetic stages and tasks
    3. If success: Plan and start after stages
    4. If failure: Plan and start on-failure stages
    5. Update stage status and end time
    6. If status allows continuation: startNext()
    7. Otherwise: CancelStage + CompleteWorkflow
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
        task_registry: TaskRegistry | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder)
        self.task_registry = task_registry

    @property
    def message_type(self) -> type[CompleteStage]:
        return CompleteStage

    def handle(self, message: CompleteStage) -> None:
        """Handle the CompleteStage message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        settled: dict[str, StageExecution] = {}
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message, settled),
            f"completing stage {message.stage_id}",
        )
        stage = settled.get("stage")
        if stage is not None:
            self._invoke_task_cleanup(stage)
            self.run_stage_finalizers(stage)

    def _handle_with_retry(self, message: CompleteStage, settled: dict[str, StageExecution]) -> None:
        """Inner handle logic to be retried."""
        settled.clear()

        def on_stage(stage: StageExecution) -> None:
            if stage.status != WorkflowStatus.RUNNING:
                self._handle_not_running(stage, message)
                return

            try:
                # Determine status from tasks and synthetic stages
                status = stage.determine_status()

                # Handle after stages
                # Note: status may be RUNNING if after-stages exist but are NOT_STARTED
                # In that case, we still need to start the after-stages if core work is done
                should_handle_after_stages = status.is_complete and not status.is_halt
                if status == WorkflowStatus.RUNNING:
                    # Check if core work (tasks + before-stages) is actually done
                    # and status is RUNNING only because after-stages are NOT_STARTED
                    after_stages_check = stage.first_after_stages()
                    if after_stages_check:
                        after_statuses = {s.status for s in after_stages_check}
                        if after_statuses == {WorkflowStatus.NOT_STARTED}:
                            # All after-stages are NOT_STARTED, check if core work is done
                            task_statuses = [t.status for t in stage.tasks]
                            before_statuses = [s.status for s in stage.before_stages()]
                            all_core = before_statuses + task_statuses
                            if all_core and all(
                                s
                                in {
                                    WorkflowStatus.SUCCEEDED,
                                    WorkflowStatus.SKIPPED,
                                    WorkflowStatus.FAILED_CONTINUE,
                                }
                                for s in all_core
                            ):
                                should_handle_after_stages = True

                planned: list[StageExecution] = []
                if should_handle_after_stages:
                    after_stages = stage.first_after_stages()
                    if not after_stages:
                        planned = self._plan_after_stages(stage)
                        after_stages = stage.first_after_stages()

                    not_started = [s for s in after_stages if s.status == WorkflowStatus.NOT_STARTED]
                    if not_started:
                        # Atomic: store stage + mark source message processed +
                        # push all after stage messages together. The mark must
                        # join this transaction: a redelivery after commit but
                        # before ack would otherwise re-enter while the
                        # after-stages run and cancel a healthy workflow.
                        with self.repository.transaction(self.queue) as txn:
                            txn.store_stage(stage)
                            for s in planned:
                                txn.store_stage(s)
                            if message.message_id:
                                txn.mark_message_processed(
                                    message_id=message.message_id,
                                    handler_type="CompleteStage",
                                    execution_id=message.execution_id,
                                )
                            for s in not_started:
                                txn.push_message(
                                    StartStage(
                                        execution_type=message.execution_type,
                                        execution_id=message.execution_id,
                                        stage_id=s.id,
                                    )
                                )
                        return

                    # If status is NOT_STARTED with no after stages, it's weird
                    if status == WorkflowStatus.NOT_STARTED:
                        logger.warning("Stage %s had no tasks or synthetic stages", stage.name)
                        status = WorkflowStatus.SKIPPED

                # Handle failure - plan on-failure stages
                elif status.is_failure:
                    # Synthetic children (on-failure or after stages) still in
                    # flight drive the parent through their own completion
                    # messages; a CompleteStage arriving meanwhile is stale or
                    # redelivered and must not re-plan or finalize the stage.
                    in_flight_children = [s for s in stage.first_after_stages() if not s.status.is_complete]
                    if in_flight_children:
                        if message.message_id:
                            with self.repository.transaction(self.queue) as txn:
                                txn.mark_message_processed(
                                    message_id=message.message_id,
                                    handler_type="CompleteStage",
                                    execution_id=message.execution_id,
                                )
                        return

                    # Plan on-failure stages exactly once per failure: replay
                    # after they completed must fall through to final failure
                    # handling, not spawn duplicate on-failure stages.
                    already_planned = bool(stage.context.get("_on_failure_planned", False))
                    planned = [] if already_planned else self._plan_on_failure_stages(stage)
                    if planned:
                        stage.context["_on_failure_planned"] = True
                        after_stages = stage.first_after_stages()
                        # Only push StartStage for on-failure stages that are NOT_STARTED
                        not_started_on_failure = [s for s in after_stages if s.status == WorkflowStatus.NOT_STARTED]
                        if not_started_on_failure:
                            # Atomic: store stage (persists the planned flag) +
                            # mark source message processed + push all
                            # on-failure stage messages together
                            with self.repository.transaction(self.queue) as txn:
                                txn.store_stage(stage)
                                for s in planned:
                                    txn.store_stage(s)
                                if message.message_id:
                                    txn.mark_message_processed(
                                        message_id=message.message_id,
                                        handler_type="CompleteStage",
                                        execution_id=message.execution_id,
                                    )
                                for s in not_started_on_failure:
                                    txn.push_message(
                                        StartStage(
                                            execution_type=message.execution_type,
                                            execution_id=message.execution_id,
                                            stage_id=s.id,
                                        )
                                    )
                            return

                # A RUNNING result here means core tasks or synthetic stages
                # are still in flight and nothing needed starting above: this
                # message is stale or a redelivery. Progress is driven by the
                # in-flight work's own completion messages — falling through
                # would misclassify the stage as failed and cancel a healthy
                # workflow.
                if status == WorkflowStatus.RUNNING:
                    if message.message_id:
                        with self.repository.transaction(self.queue) as txn:
                            txn.mark_message_processed(
                                message_id=message.message_id,
                                handler_type="CompleteStage",
                                execution_id=message.execution_id,
                            )
                    return

                if status == WorkflowStatus.FAILED_CONTINUE and stage.context.get("_blocking_failure", False):
                    logger.warning(
                        "Stage %s has _blocking_failure=True, converting FAILED_CONTINUE to TERMINAL",
                        stage.name,
                    )
                    status = WorkflowStatus.TERMINAL
                if settles_on_complete(stage):
                    self._complete_deferred_member(stage, status, message, settled)
                    return
                self.set_stage_status(stage, status)
                stage.end_time = self.current_time_millis()

                settled["stage"] = stage

                logger.info("Stage %s completed with status %s", stage.name, status)

                # Handle FAILED_CONTINUE propagation to parent
                if (
                    status == WorkflowStatus.FAILED_CONTINUE
                    and stage.synthetic_stage_owner is not None
                    and not stage.allow_sibling_stages_to_continue_on_failure
                    and stage.parent_stage_id is not None
                ):
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
                        self._record_completion_event(stage, status)
                        self.mark_in(txn, message, "CompleteStage")
                        if stage.synthetic_stage_owner == SyntheticStageOwner.STAGE_BEFORE:
                            txn.push_message(
                                ContinueParentStage(
                                    execution_type=message.execution_type,
                                    execution_id=message.execution_id,
                                    stage_id=stage.parent_stage_id,
                                    phase=SyntheticStageOwner.STAGE_BEFORE,
                                )
                            )
                        else:
                            txn.push_message(
                                CompleteStage(
                                    execution_type=message.execution_type,
                                    execution_id=message.execution_id,
                                    stage_id=stage.parent_stage_id,
                                )
                            )
                elif status in {
                    WorkflowStatus.SUCCEEDED,
                    WorkflowStatus.FAILED_CONTINUE,
                    WorkflowStatus.SKIPPED,
                }:
                    self._complete_and_fan_out(stage, status, message)
                else:
                    with self.repository.transaction(self.queue) as txn:
                        self._halt_in(txn, stage, status, message)

            except (TenancyModeChanged, TenantMismatchError, TenantContextError, DuplicateDeliveryError):
                raise
            except Exception as e:
                if is_transient(e) or is_store_unavailable(e):
                    raise

                logger.error(
                    "Error completing stage %s: %s",
                    stage.name,
                    e,
                    exc_info=True,
                )
                stage.context["exception"] = {
                    "details": {"error": str(e)},
                }
                stage.status = WorkflowStatus.TERMINAL
                stage.end_time = self.current_time_millis()
                settled["stage"] = stage

                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    self.mark_in(txn, message, "CompleteStage")
                    txn.push_message(
                        CancelStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                        )
                    )
                    txn.push_message(
                        CompleteWorkflow(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                        )
                    )

        self.with_stage(message, on_stage)
