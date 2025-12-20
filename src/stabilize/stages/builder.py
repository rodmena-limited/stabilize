from __future__ import annotations
from abc import ABC
from typing import TYPE_CHECKING
from stabilize.dag.graph import StageGraphBuilder
from stabilize.models.task import TaskExecution
_default_factory: StageDefinitionBuilderFactory | None = None

class StageDefinitionBuilder(ABC):
    """
    Abstract base class for stage definition builders.

    Each stage type has a corresponding builder that defines:
    - What tasks the stage should execute
    - What synthetic stages run before/after
    - What happens on failure

    Example:
        class DeployStageBuilder(StageDefinitionBuilder):
            @property
            def type(self) -> str:
                return "deploy"

            def build_tasks(self, stage: StageExecution) -> List[TaskExecution]:
                return [
                    TaskExecution.create(
                        name="Determine Target Server Group",
                        implementing_class="DetermineTargetServerGroupTask",
                        stage_start=True,
                    ),
                    TaskExecution.create(
                        name="Deploy Server Group",
                        implementing_class="DeployServerGroupTask",
                        stage_end=True,
                    ),
                ]

            def before_stages(
                self,
                stage: StageExecution,
                graph: StageGraphBuilder,
            ) -> None:
                # Add validation stage before deploy
                validation = StageExecution.create_synthetic(
                    type="validation",
                    name="Validate Deploy Configuration",
                    parent=stage,
                    owner=SyntheticStageOwner.STAGE_BEFORE,
                )
                graph.add(validation)
    """

    def type(self) -> str:
        """
        Get the stage type this builder handles.

        Defaults to lowercase class name without "StageBuilder" suffix.
        """
        name = self.__class__.__name__
        if name.endswith("StageBuilder"):
            name = name[:-12]
        return name.lower()

    def aliases(self) -> list[str]:
        """Get alternative names for this stage type."""
        return []

    def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
        """
        Build the tasks for this stage.

        Override to define what tasks the stage should execute.

        Args:
            stage: The stage being built

        Returns:
            List of tasks to execute
        """
        return []

    def before_stages(
        self,
        stage: StageExecution,
        graph: StageGraphBuilder,
    ) -> None:
        """
        Build synthetic stages that run before this stage's tasks.

        Override to add setup, validation, or other pre-requisite stages.

        Args:
            stage: The parent stage
            graph: Builder for adding synthetic stages
        """
        pass

    def after_stages(
        self,
        stage: StageExecution,
        graph: StageGraphBuilder,
    ) -> None:
        """
        Build synthetic stages that run after this stage completes.

        Override to add cleanup, notification, or other post-processing stages.

        Args:
            stage: The parent stage
            graph: Builder for adding synthetic stages
        """
        pass

    def on_failure_stages(
        self,
        stage: StageExecution,
        graph: StageGraphBuilder,
    ) -> None:
        """
        Build synthetic stages that run when this stage fails.

        Override to add rollback, alerting, or other failure-handling stages.

        Args:
            stage: The failed stage
            graph: Builder for adding synthetic stages
        """
        pass

    def add_context_flags(self, stage: StageExecution) -> None:
        """
        Add any required context flags to the stage.

        Called before task execution to set up stage context.

        Args:
            stage: The stage to modify
        """
        pass
