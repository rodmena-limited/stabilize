"""
AddMultiInstanceHandler - adds a new instance to a running MI activity.

Implements WCP-15: Multiple Instances without a priori Run-Time Knowledge.
Dynamically creates new parallel instances during execution.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.queue.messages import AddMultiInstance, StartStage
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class AddMultiInstanceHandler(StabilizeHandler[AddMultiInstance]):
    """
    Handler for AddMultiInstance messages.

    Execution flow:
    1. Load the parent MI stage
    2. Verify it allows dynamic instances (mi_config.allow_dynamic)
    3. Create a new child stage instance
    4. Update the MI join threshold if needed
    5. Push StartStage for the new instance
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config)

    @property
    def message_type(self) -> type[AddMultiInstance]:
        return AddMultiInstance

    def handle(self, message: AddMultiInstance) -> None:
        """Handle the AddMultiInstance message."""
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"adding multi-instance to stage {message.stage_id}",
        )

    def _handle_with_retry(self, message: AddMultiInstance) -> None:
        """Inner handle logic to be retried."""

        def on_stage(stage: StageExecution) -> None:
            from stabilize.models.stage import StageExecution as SE

            if stage.mi_config is None:
                logger.warning(
                    "AddMultiInstance for stage %s which has no MI config, ignoring",
                    stage.name,
                )
                return

            if not stage.mi_config.allow_dynamic:
                logger.warning(
                    "AddMultiInstance for stage %s which does not allow dynamic instances, ignoring",
                    stage.name,
                )
                return

            if stage.status.is_complete:
                logger.warning(
                    "AddMultiInstance for completed stage %s, ignoring",
                    stage.name,
                )
                return

            # Track instance count
            instance_count = stage.context.get("_mi_instance_count", 0)
            instance_count += 1
            stage.context["_mi_instance_count"] = instance_count

            # Create new child instance stage
            instance_ref_id = f"{stage.ref_id}_instance_{instance_count}"
            instance_context = dict(message.instance_context)
            instance_context["_mi_parent_ref_id"] = stage.ref_id
            instance_context["_mi_instance_index"] = instance_count

            new_instance = SE.create(
                type=stage.type,
                name=f"{stage.name} [instance {instance_count}]",
                ref_id=instance_ref_id,
                context=instance_context,
                requisite_stage_ref_ids={stage.ref_id},
            )

            # Add to execution
            execution = stage.execution
            new_instance.execution = execution
            execution.stages.append(new_instance)

            # Persist the new instance and update parent
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="AddMultiInstance",
                        execution_id=message.execution_id,
                    )

            # Add stage to repository (outside transaction since add_stage is separate)
            self.repository.add_stage(new_instance)

            # Push start message for the new instance
            self.queue.push(
                StartStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=new_instance.id,
                )
            )

            logger.info(
                "Added MI instance %s to stage %s (total: %d)",
                instance_ref_id,
                stage.name,
                instance_count,
            )

        self.with_stage(message, on_stage)
