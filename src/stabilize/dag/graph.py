"""
Stage graph builder for constructing synthetic stages.

This module provides the StageGraphBuilder class for building graphs of
synthetic stages (before/after stages) that are dynamically injected
by StageDefinitionBuilders.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution, SyntheticStageOwner


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

    @classmethod
    def before_stages(cls, parent: StageExecution) -> StageGraphBuilder:
        """
        Create a builder for before stages.

        Before stages run before the parent's tasks.
        """
        from stabilize.models.stage import SyntheticStageOwner

        return cls(parent, SyntheticStageOwner.STAGE_BEFORE)

    @classmethod
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

    @property
    def stages(self) -> list[StageExecution]:
        """Get the current list of stages."""
        return list(self._stages)

    @property
    def last_stage(self) -> StageExecution | None:
        """Get the last added stage."""
        return self._last_stage

    def is_empty(self) -> bool:
        """Check if no stages have been added."""
        return len(self._stages) == 0


def connect_stages_linearly(stages: list[StageExecution]) -> None:
    """
    Connect a list of stages in linear sequence.

    Each stage will depend on the previous one.

    Args:
        stages: List of stages to connect
    """
    for i in range(1, len(stages)):
        previous = stages[i - 1]
        current = stages[i]
        requisites = set(current.requisite_stage_ref_ids)
        requisites.add(previous.ref_id)
        current.requisite_stage_ref_ids = requisites


def add_stages_to_execution(
    stages: list[StageExecution],
    add_stage: Callable[[StageExecution], None],
) -> None:
    """
    Add multiple stages to an execution.

    Calls the provided add_stage function for each stage.

    Args:
        stages: List of stages to add
        add_stage: Function to call to add each stage
    """
    for stage in stages:
        add_stage(stage)
