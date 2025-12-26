"""
Stage definition builders.

Stage definition builders are responsible for:
1. Building tasks for a stage
2. Building before stages (setup, validation)
3. Building after stages (cleanup, notification)
4. Building on-failure stages (rollback, alerts)
"""

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING

from stabilize.dag.graph import StageGraphBuilder
from stabilize.models.task import TaskExecution

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


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

    @property
    def type(self) -> str:
        """
        Get the stage type this builder handles.

        Defaults to lowercase class name without "StageBuilder" suffix.
        """
        name = self.__class__.__name__
        if name.endswith("StageBuilder"):
            name = name[:-12]
        return name.lower()

    @property
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


class NoOpStageBuilder(StageDefinitionBuilder):
    """A stage builder that does nothing."""

    @property
    def type(self) -> str:
        return "noop"


class WaitStageBuilder(StageDefinitionBuilder):
    """Builder for wait stages."""

    @property
    def type(self) -> str:
        return "wait"

    def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
        return [
            TaskExecution.create(
                name="Wait",
                implementing_class="WaitTask",
                stage_start=True,
                stage_end=True,
            ),
        ]


class StageDefinitionBuilderFactory:
    """Factory for resolving stage definition builders."""

    def __init__(self) -> None:
        self._builders: dict[str, StageDefinitionBuilder] = {}
        self._default_builder = NoOpStageBuilder()

        # Register built-in builders
        self.register(NoOpStageBuilder())
        self.register(WaitStageBuilder())

    def register(self, builder: StageDefinitionBuilder) -> None:
        """
        Register a stage definition builder.

        Args:
            builder: The builder to register
        """
        self._builders[builder.type] = builder
        for alias in builder.aliases:
            self._builders[alias] = builder

    def register_class(
        self,
        builder_class: type[StageDefinitionBuilder],
    ) -> None:
        """Register a builder by class."""
        self.register(builder_class())

    def get(self, stage_type: str) -> StageDefinitionBuilder:
        """
        Get the builder for a stage type.

        Args:
            stage_type: The stage type

        Returns:
            The builder (or default if not found)
        """
        return self._builders.get(stage_type, self._default_builder)

    def has(self, stage_type: str) -> bool:
        """Check if a builder is registered for a stage type."""
        return stage_type in self._builders

    def list_types(self) -> list[str]:
        """Get all registered stage types."""
        return list(self._builders.keys())


# Global factory instance
_default_factory: StageDefinitionBuilderFactory | None = None


def get_default_factory() -> StageDefinitionBuilderFactory:
    """Get the default global stage definition builder factory."""
    global _default_factory
    if _default_factory is None:
        _default_factory = StageDefinitionBuilderFactory()
    return _default_factory


def register_builder(builder: StageDefinitionBuilder) -> None:
    """Register a builder in the default factory."""
    get_default_factory().register(builder)


def get_builder(stage_type: str) -> StageDefinitionBuilder:
    """Get a builder from the default factory."""
    return get_default_factory().get(stage_type)
