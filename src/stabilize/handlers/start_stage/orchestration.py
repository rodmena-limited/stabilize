"""
Orchestration methods for StartStageHandler.

These methods collect start messages and cancel deferred-choice siblings.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteStage,
    StartStage,
    StartTask,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)


class StartStageOrchestrationMixin:
    """Mixin providing orchestration methods used by StartStageHandler."""

    repository: WorkflowStore
    queue: Queue

    def _collect_start_messages(
        self,
        stage: StageExecution,
        message: StartStage,
    ) -> list[Message]:
        """Collect messages needed to start the stage.

        This method queries the repository but doesn't push any messages.
        The caller is responsible for pushing the returned messages atomically.
        """
        messages: list[Message] = []

        # Fetch synthetic stages (returns empty list if none)
        synthetic_stages = self.repository.get_synthetic_stages(stage.execution.id, stage.id)
        if synthetic_stages is None:
            synthetic_stages = []

        # Check for before stages
        before_stages = [
            s
            for s in synthetic_stages
            if s is not None
            and s.synthetic_stage_owner == SyntheticStageOwner.STAGE_BEFORE
            and s.is_initial()
        ]

        if before_stages:
            for before in before_stages:
                messages.append(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=before.id,
                    )
                )
            return messages

        # No before stages - start first task
        first_task = stage.first_task()
        if first_task:
            messages.append(
                StartTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=first_task.id,
                )
            )
            return messages

        # No tasks - check for after stages
        after_stages = [
            s
            for s in synthetic_stages
            if s is not None
            and s.synthetic_stage_owner == SyntheticStageOwner.STAGE_AFTER
            and s.is_initial()
        ]

        if after_stages:
            for after in after_stages:
                messages.append(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=after.id,
                    )
                )
            return messages

        # No tasks or synthetic stages - complete immediately
        messages.append(
            CompleteStage(
                execution_type=message.execution_type,
                execution_id=message.execution_id,
                stage_id=message.stage_id,
            )
        )
        return messages

    def _cancel_deferred_choice_siblings(
        self,
        stage: StageExecution,
        message: StartStage,
    ) -> None:
        """Cancel sibling stages in the same deferred choice group (WCP-16).

        When one branch of a deferred choice is claimed (set to RUNNING),
        all other branches in the same group are cancelled.
        """
        all_stages = self.repository.retrieve(stage.execution.id).stages
        for s in all_stages:
            if s.id == stage.id:
                continue
            if (
                s.deferred_choice_group == stage.deferred_choice_group
                and s.status == WorkflowStatus.NOT_STARTED
            ):
                logger.info(
                    "Deferred choice: cancelling sibling %s (group=%s) because %s was claimed",
                    s.name,
                    stage.deferred_choice_group,
                    stage.name,
                )
                self.queue.push(
                    CancelStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=s.id,
                    )
                )
