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
