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

    def parse_execution(
        self,
        config: dict[str, Any],
        trigger_config: dict[str, Any] | None = None,
    ) -> Workflow:
        """
        Parse a pipeline configuration into an execution.

        Args:
            config: Pipeline configuration
            trigger_config: Optional trigger configuration

        Returns:
            A Workflow ready to run
        """
        # Parse trigger
        trigger = Trigger()
        if trigger_config:
            trigger = Trigger(
                type=trigger_config.get("type", "manual"),
                user=trigger_config.get("user", "anonymous"),
                parameters=trigger_config.get("parameters", {}),
                artifacts=trigger_config.get("artifacts", []),
                payload=trigger_config.get("payload", {}),
            )

        # Parse stages
        stages = self._parse_stages(config.get("stages", []))

        # Create execution
        execution = Workflow.create(
            application=config.get("application", "unknown"),
            name=config.get("name", "Unnamed Pipeline"),
            stages=stages,
            trigger=trigger,
            pipeline_config_id=config.get("id"),
        )

        # Set additional properties
        execution.is_limit_concurrent = config.get("limitConcurrent", False)
        execution.max_concurrent_executions = config.get("maxConcurrentExecutions", 0)
        execution.keep_waiting_pipelines = config.get("keepWaitingPipelines", False)

        return execution

    def _parse_stages(
        self,
        stage_configs: list[dict[str, Any]],
    ) -> list[StageExecution]:
        """
        Parse stage configurations into StageExecution objects.

        Args:
            stage_configs: List of stage configuration dictionaries

        Returns:
            List of StageExecution objects
        """
        stages = []

        for config in stage_configs:
            stage = self._parse_stage(config)
            stages.append(stage)

        return stages
