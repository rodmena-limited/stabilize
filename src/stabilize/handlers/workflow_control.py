"""
Workflow-level control handlers: cancel, restart, resume, pause.

These handle the control messages pushed by the Orchestrator facade
(`cancel()`, `restart()`, `unpause()`) and by the engine itself
(start-time expiry cancellation, RunTask parking on a paused workflow).
Before these existed the messages were consumed with no registered
handler and silently dropped.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.jump_to_stage.reset import reset_stage_for_retry
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CancelWorkflow,
    CompleteWorkflow,
    PauseTask,
    RestartStage,
    ResumeStage,
    RunTask,
    StartStage,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)


class _ControlHandler:
    """Shared helper mixin for the control handlers."""

    def _mark_processed_only(self, message: Message, handler_type: str) -> None:
        """Consume a message idempotently without any state change."""
        message_id = getattr(message, "message_id", None)
        if not message_id:
            return
        with self.repository.transaction(self.queue) as txn:  # type: ignore[attr-defined]
            txn.mark_message_processed(
                message_id=message_id,
                handler_type=handler_type,
                execution_id=getattr(message, "execution_id", None),
            )


class CancelWorkflowHandler(StabilizeHandler[CancelWorkflow], _ControlHandler):
    """
    Handler for CancelWorkflow messages.

    Execution flow:
    1. If the execution is already complete, consume idempotently.
    2. Persist the cancellation flag (is_canceled, canceled_by, reason).
    3. Push CancelStage for every incomplete top-level stage and a
       CompleteWorkflow to drive final status determination, atomically
       with message deduplication.
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)

    @property
    def message_type(self) -> type[CancelWorkflow]:
        return CancelWorkflow

    def handle(self, message: CancelWorkflow) -> None:
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"canceling workflow {message.execution_id}",
        )

    def _handle_with_retry(self, message: CancelWorkflow) -> None:
        def on_execution(execution: Workflow) -> None:
            if execution.status.is_complete:
                logger.debug(
                    "Ignoring CancelWorkflow for %s - already %s",
                    execution.id,
                    execution.status,
                )
                self._mark_processed_only(message, "CancelWorkflow")
                return

            user = message.user or "unknown"
            reason = message.reason or ""

            # Idempotent flag write; safe to repeat on message redelivery.
            self.repository.cancel(execution.id, user, reason)
            execution.cancel(user, reason)

            to_cancel = [s for s in execution.top_level_stages() if not s.status.is_complete]

            with self.repository.transaction(self.queue) as txn:
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="CancelWorkflow",
                        execution_id=message.execution_id,
                    )
                for stage in to_cancel:
                    txn.push_message(
                        CancelStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.id,
                        )
                    )
                txn.push_message(
                    CompleteWorkflow(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                    )
                )

            logger.info(
                "Canceling execution %s (%d stage(s)) by %s: %s",
                execution.id,
                len(to_cancel),
                user,
                reason,
            )

        self.with_execution(message, on_execution)


class RestartStageHandler(StabilizeHandler[RestartStage], _ControlHandler):
    """
    Handler for RestartStage messages.

    Resets a terminal stage (and its tasks) to NOT_STARTED, brings a
    completed execution back to RUNNING, and pushes StartStage.
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)

    @property
    def message_type(self) -> type[RestartStage]:
        return RestartStage

    def handle(self, message: RestartStage) -> None:
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"restarting stage {message.stage_id}",
        )

    def _handle_with_retry(self, message: RestartStage) -> None:
        def on_stage(stage: StageExecution) -> None:
            execution = stage.execution
            if execution is None:
                execution = self.repository.retrieve(message.execution_id)

            if execution.is_canceled:
                # Restarting inside a canceled execution would race the
                # RunTask-level is_canceled guard; refuse loudly instead
                # of half-running a stage that can never complete.
                logger.warning(
                    "Refusing RestartStage for %s: execution %s is canceled",
                    stage.id,
                    execution.id,
                )
                self._mark_processed_only(message, "RestartStage")
                return

            if not stage.status.is_complete:
                logger.warning(
                    "Ignoring RestartStage for %s (%s) - not in a terminal state",
                    stage.name,
                    stage.status,
                )
                self._mark_processed_only(message, "RestartStage")
                return

            reset_stage_for_retry(stage)

            execution_changed = False
            if execution.status.is_complete:
                execution.update_status(WorkflowStatus.RUNNING)
                execution.end_time = None
                execution_changed = True

            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if execution_changed:
                    txn.update_workflow_status(execution)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="RestartStage",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=stage.id,
                    )
                )

            logger.info("Restarted stage %s (%s)", stage.name, stage.id)

        self.with_stage(message, on_stage)


class ResumeStageHandler(StabilizeHandler[ResumeStage], _ControlHandler):
    """
    Handler for ResumeStage messages.

    Returns a PAUSED stage to RUNNING, re-arms its paused tasks as
    NOT_STARTED, lifts a workflow-level PAUSED status, and pushes
    StartTask for the first re-armed task.
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)

    @property
    def message_type(self) -> type[ResumeStage]:
        return ResumeStage

    def handle(self, message: ResumeStage) -> None:
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"resuming stage {message.stage_id}",
        )

    def _handle_with_retry(self, message: ResumeStage) -> None:
        def on_stage(stage: StageExecution) -> None:
            if stage.status != WorkflowStatus.PAUSED:
                logger.debug(
                    "Ignoring ResumeStage for %s (%s) - not paused",
                    stage.name,
                    stage.status,
                )
                self._mark_processed_only(message, "ResumeStage")
                return

            execution = stage.execution
            if execution is None:
                execution = self.repository.retrieve(message.execution_id)

            self.set_stage_status(stage, WorkflowStatus.RUNNING)
            paused_tasks = [t for t in stage.tasks if t.status == WorkflowStatus.PAUSED]
            if len(paused_tasks) > 1:
                logger.warning(
                    "Stage %s has %d paused tasks; expected at most one (sequential tasks)",
                    stage.id,
                    len(paused_tasks),
                )
            # PAUSED -> RUNNING is the legal transition; RunTask executes a
            # task already in RUNNING (StartTask would demand NOT_STARTED).
            for task in paused_tasks[:1]:
                self.set_task_status(task, WorkflowStatus.RUNNING)

            execution_changed = False
            if execution.status == WorkflowStatus.PAUSED:
                execution.resume()
                execution_changed = True

            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if execution_changed:
                    txn.update_workflow_status(execution)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="ResumeStage",
                        execution_id=message.execution_id,
                    )
                if paused_tasks:
                    # Tasks are sequential within a stage: re-dispatch the
                    # first; CompleteTask drives the rest as usual.
                    txn.push_message(
                        RunTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.id,
                            task_id=paused_tasks[0].id,
                        )
                    )

            logger.info(
                "Resumed stage %s (%s), re-armed %d task(s)",
                stage.name,
                stage.id,
                len(paused_tasks),
            )

        self.with_stage(message, on_stage)


class PauseTaskHandler(StabilizeHandler[PauseTask], _ControlHandler):
    """
    Handler for PauseTask messages.

    Pushed by RunTaskHandler when it finds the workflow PAUSED: parks the
    task and its stage as PAUSED so ResumeStage can later re-arm and
    re-dispatch them. Before this existed the in-flight task was lost.
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)

    @property
    def message_type(self) -> type[PauseTask]:
        return PauseTask

    def handle(self, message: PauseTask) -> None:
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"pausing task {message.task_id}",
        )

    def _handle_with_retry(self, message: PauseTask) -> None:
        def on_task(stage: StageExecution, task: TaskExecution) -> None:
            if task.status.is_complete:
                logger.debug(
                    "Ignoring PauseTask for %s (%s) - already complete",
                    task.id,
                    task.status,
                )
                self._mark_processed_only(message, "PauseTask")
                return

            self.set_task_status(task, WorkflowStatus.PAUSED)
            self.set_stage_status(stage, WorkflowStatus.PAUSED)

            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="PauseTask",
                        execution_id=message.execution_id,
                    )

            logger.info("Paused task %s in stage %s (%s)", task.id, stage.name, stage.id)

        self.with_task(message, on_task)
