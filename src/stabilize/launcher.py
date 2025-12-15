from __future__ import annotations
import logging
from typing import TYPE_CHECKING, Any
from stabilize.models.stage import StageExecution
from stabilize.models.workflow import (
    Trigger,
    Workflow,
)
from stabilize.stages.builder import StageDefinitionBuilderFactory
logger = logging.getLogger(__name__)

class WorkflowLauncher:
    """
    Launcher for pipeline executions.

    Creates executions from configuration and starts them via the runner.
    """
    def __init__(
        self,
        repository: WorkflowStore,
        runner: Orchestrator,
        stage_builder_factory: StageDefinitionBuilderFactory | None = None,
        task_registry: TaskRegistry | None = None,
    ) -> None:
        """
        Initialize the launcher.

        Args:
            repository: The execution repository
            runner: The execution runner
            stage_builder_factory: Factory for stage builders
            task_registry: Registry for task implementations
        """
        self.repository = repository
        self.runner = runner
        self.stage_builder_factory = stage_builder_factory or StageDefinitionBuilderFactory()
        self.task_registry = task_registry

    def start(
        self,
        pipeline_config: dict[str, Any],
        trigger: dict[str, Any] | None = None,
    ) -> Workflow:
        """
        Start a pipeline execution from configuration.

        Args:
            pipeline_config: Pipeline configuration dictionary
            trigger: Optional trigger information

        Returns:
            The created execution
        """
        # Parse the execution
        execution = self.parse_execution(pipeline_config, trigger)

        # Store it
        self.repository.store(execution)

        # Start it
        self.runner.start(execution)

        logger.info(f"Launched execution {execution.id} for pipeline {execution.name}")

        return execution
