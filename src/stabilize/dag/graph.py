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

    def before_stages(cls, parent: StageExecution) -> StageGraphBuilder:
        """
        Create a builder for before stages.

        Before stages run before the parent's tasks.
        """
        from stabilize.models.stage import SyntheticStageOwner

        return cls(parent, SyntheticStageOwner.STAGE_BEFORE)

    def after_stages(
        cls,
        parent: StageExecution,
        requisite_stage_ref_ids: set[str] | None = None,
    ) -> StageGraphBuilder:
        """
        Create a builder for after stages.

        After stages run after the parent completes.
        """
        from stabilize.models.stage import SyntheticStageOwner

        return cls(
            parent,
            SyntheticStageOwner.STAGE_AFTER,
            requisite_stage_ref_ids or set(),
        )

    def add(self, stage: StageExecution) -> StageGraphBuilder:
        """
        Add a stage to the graph.

        The stage will be configured as a synthetic stage of the parent.
        If this is the first stage and requisite_stage_ref_ids is set,
        those requisites will be applied.

        Args:
            stage: The stage to add

        Returns:
            self for method chaining
        """
        # Configure as synthetic stage
        stage.parent_stage_id = self.parent.id
        stage.synthetic_stage_owner = self.owner

        # Set execution reference if parent has one
        if self.parent.has_execution():
            stage._execution = self.parent._execution

        # Apply initial requisites to first stage
        if not self._stages and self.requisite_stage_ref_ids:
            stage.requisite_stage_ref_ids = stage.requisite_stage_ref_ids.union(self.requisite_stage_ref_ids)

        self._stages.append(stage)
        self._last_stage = stage

        return self

    def append(self, stage: StageExecution) -> StageGraphBuilder:
        """
        Append a stage after the last added stage.

        Creates a sequential dependency from the last stage to this one.

        Args:
            stage: The stage to append

        Returns:
            self for method chaining
        """
        if self._last_stage:
            self.connect(self._last_stage, stage)
        return self.add(stage)

    def connect(self, previous: StageExecution, next_stage: StageExecution) -> None:
        """
        Connect two stages with a dependency.

        The next stage will depend on the previous stage.

        Args:
            previous: The stage that must complete first
            next_stage: The stage that depends on previous
        """
        requisites = set(next_stage.requisite_stage_ref_ids)
        requisites.add(previous.ref_id)
        next_stage.requisite_stage_ref_ids = requisites

    def build(self) -> list[StageExecution]:
        """
        Build and return the list of stages.

        Returns:
            List of configured synthetic stages
        """
        return list(self._stages)
