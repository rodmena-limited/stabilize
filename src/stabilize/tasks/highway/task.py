from __future__ import annotations
import json
import logging
import urllib.error
import urllib.request
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from stabilize.tasks.highway.config import HighwayConfig
from stabilize.tasks.interface import RetryableTask
from stabilize.tasks.result import TaskResult
logger = logging.getLogger(__name__)

class HighwayTask(RetryableTask):
    """Execute workflows on Highway Workflow Engine.

    This task implements Black Box execution with Glass Box observability:

    - Execution (Black Box): Stabilize sends `start`, waits for `completed`.
      Highway handles retries, transactions, loops internally.

    - State (Black Box): Stabilize stores only `run_id`. Never Highway's
      internal variables.

    - Observability (Glass Box): Stabilize proxies logs/current_step for UI.
      Never for control decisions.

    Stage Context Inputs:
        highway_workflow_definition: dict - Highway JSON workflow (required)
        highway_inputs: dict - Workflow inputs (optional)
        highway_api_endpoint: str - Override API endpoint (optional)
        highway_api_key: str - Override API key (optional)

    Stage Context Outputs (during execution):
        highway_run_id: str - The workflow run ID
        highway_current_step: str - Current step name (for UI)
        highway_progress: dict - Progress info (for UI)

    Task Result Outputs (on completion):
        highway_run_id: str - The workflow run ID
        highway_status: str - Final status (completed/failed/cancelled)
        highway_result: Any - Workflow result from Highway

    Example:
        context = {
            "highway_workflow_definition": {
                "name": "my_workflow",
                "version": "1.0.0",
                "start_task": "step1",
                "tasks": {...}
            },
            "highway_inputs": {"param": "value"},
        }
    """
    TERMINAL_STATES = frozenset({'completed', 'failed', 'cancelled'})

    def aliases(self) -> list[str]:
        """Alternative names for this task."""
        return ["highway_workflow", "highway-workflow"]

    def get_timeout(self) -> timedelta:
        """Default timeout for Highway workflow execution."""
        return timedelta(minutes=30)

    def get_backoff_period(
        self,
        stage: StageExecution,
        duration: timedelta,
    ) -> timedelta:
        """Get poll interval from config."""
        config = HighwayConfig.from_stage_context(stage.context)
        return config.poll_interval

    def get_dynamic_timeout(self, stage: StageExecution) -> timedelta:
        """Get timeout from config or stage context."""
        config = HighwayConfig.from_stage_context(stage.context)
        return config.timeout

    def execute(self, stage: StageExecution) -> TaskResult:
        """Execute Highway workflow.

        Two-phase execution:
        1. Submit: If no run_id, submit workflow to Highway
        2. Poll: Check status until terminal state

        Args:
            stage: The stage execution context

        Returns:
            TaskResult indicating status
        """
        config = HighwayConfig.from_stage_context(stage.context)

        # Validate configuration
        errors = config.validate()
        if errors:
            return TaskResult.terminal(error="; ".join(errors))

        # Log config for debugging (helps diagnose endpoint/key issues)
        logger.info(
            "Highway config: endpoint=%s, api_key=%s...",
            config.api_endpoint,
            config.api_key[:15] if len(config.api_key) > 15 else "(short key)",
        )

        # Phase 1: Submit (if not already submitted)
        run_id = stage.context.get("highway_run_id")
        if not run_id:
            return self._submit_workflow(stage, config)

        # Phase 2: Poll status (Black Box - just check if done)
        return self._poll_workflow(run_id, stage, config)
