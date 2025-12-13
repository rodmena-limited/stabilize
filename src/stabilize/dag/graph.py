from __future__ import annotations
from collections.abc import Callable
from typing import TYPE_CHECKING

class StageGraphBuilder:
    """
    Builder for constructing graphs of synthetic stages.

    Used by StageDefinitionBuilder to add before/after stages to a parent stage.
    Manages dependencies between synthetic stages and connects them properly.

    Example:
        # In a StageDefinitionBuilder.before_stages():
        def before_stages(self, stage, graph):
            # Add setup stage
            setup = StageExecution.create_synthetic(
                type="setup",
                name="Setup",
                parent=stage,
                owner=SyntheticStageOwner.STAGE_BEFORE,
            )
            graph.add(setup)

            # Add validation stage after setup
            validation = StageExecution.create_synthetic(
                type="validate",
                name="Validate",
                parent=stage,
                owner=SyntheticStageOwner.STAGE_BEFORE,
            )
            graph.add(validation)
            graph.connect(setup, validation)
    """
    def __init__(
        self,
        parent: StageExecution,
        owner: SyntheticStageOwner,
        requisite_stage_ref_ids: set[str] | None = None,
    ):
        """
        Initialize a StageGraphBuilder.

        Args:
            parent: The parent stage these synthetic stages belong to
            owner: STAGE_BEFORE or STAGE_AFTER
            requisite_stage_ref_ids: Initial requisites for first stage
        """

        self.parent = parent
        self.owner = owner
        self.requisite_stage_ref_ids = requisite_stage_ref_ids or set()
        self._stages: list[StageExecution] = []
        self._last_stage: StageExecution | None = None
