"""
WorkflowLauncher - creates and starts pipeline executions.

This module provides the high-level interface for launching pipelines.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from stabilize.models.stage import StageExecution
from stabilize.models.workflow import (
    Trigger,
    Workflow,
)
from stabilize.stages.builder import StageDefinitionBuilderFactory

if TYPE_CHECKING:
    from stabilize.orchestrator import Orchestrator
    from stabilize.persistence.store import WorkflowStore
    from stabilize.tasks.registry import TaskRegistry

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

    def _parse_stage(self, config: dict[str, Any]) -> StageExecution:
        """
        Parse a single stage configuration.

        Args:
            config: Stage configuration dictionary

        Returns:
            A StageExecution object
        """
        # Get requisite stages
        requisite_ids: set[str] = set()
        if "requisiteStageRefIds" in config:
            requisite_ids = set(config["requisiteStageRefIds"])

        # Create context from config (excluding metadata fields)
        context = {
            k: v
            for k, v in config.items()
            if k
            not in {
                "id",
                "refId",
                "type",
                "name",
                "requisiteStageRefIds",
                "parentStageId",
                "syntheticStageOwner",
            }
        }

        stage = StageExecution.create(
            type=config.get("type", "unknown"),
            name=config.get("name", config.get("type", "Unknown")),
            ref_id=config.get("refId", config.get("id", "")),
            context=context,
            requisite_stage_ref_ids=requisite_ids,
        )

        # Build tasks using stage definition builder
        builder = self.stage_builder_factory.get(stage.type)
        stage.tasks = builder.build_tasks(stage)

        # Mark first/last tasks
        if stage.tasks:
            stage.tasks[0].stage_start = True
            stage.tasks[-1].stage_end = True

        return stage

    def create_orchestration(
        self,
        application: str,
        name: str,
        stages: list[dict[str, Any]],
    ) -> Workflow:
        """
        Create an ad-hoc orchestration (single execution not from a pipeline).

        Args:
            application: Application name
            name: Orchestration name
            stages: Stage configurations

        Returns:
            The created execution
        """
        parsed_stages = self._parse_stages(stages)

        execution = Workflow.create_orchestration(
            application=application,
            name=name,
            stages=parsed_stages,
        )

        self.repository.store(execution)
        self.runner.start(execution)

        return execution


def create_simple_pipeline(
    name: str,
    application: str,
    stages: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Create a simple pipeline configuration.

    Helper function for building pipeline configs programmatically.

    Args:
        name: Pipeline name
        application: Application name
        stages: List of stage configurations

    Returns:
        A pipeline configuration dictionary

    Example:
        config = create_simple_pipeline(
            name="Deploy to Prod",
            application="myapp",
            stages=[
                {"refId": "1", "type": "wait", "name": "Wait", "waitTime": 30},
                {"refId": "2", "type": "deploy", "name": "Deploy",
                 "requisiteStageRefIds": ["1"]},
            ],
        )
    """
    return {
        "name": name,
        "application": application,
        "stages": stages,
    }


def create_stage_config(
    ref_id: str,
    stage_type: str,
    name: str,
    requisites: list[str] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Create a stage configuration.

    Helper function for building stage configs.

    Args:
        ref_id: Stage reference ID
        stage_type: Stage type
        name: Stage name
        requisites: Prerequisite stage ref IDs
        **kwargs: Additional stage context

    Returns:
        A stage configuration dictionary

    Example:
        stage = create_stage_config(
            ref_id="1",
            stage_type="wait",
            name="Wait for approval",
            waitTime=3600,
        )
    """
    config: dict[str, Any] = {
        "refId": ref_id,
        "type": stage_type,
        "name": name,
    }

    if requisites:
        config["requisiteStageRefIds"] = requisites

    config.update(kwargs)

    return config
