"""Highway Task implementation for Stabilize.

This module provides HighwayTask, a RetryableTask that executes
workflows on Highway Workflow Engine with Black Box execution
and Glass Box observability.

The Golden Rule:
    Stabilize knows THAT Highway is running.
    Highway knows HOW it is running.
"""

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

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

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

    TERMINAL_STATES = frozenset({"completed", "failed", "cancelled"})

    @property
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

    def _submit_workflow(
        self,
        stage: StageExecution,
        config: HighwayConfig,
    ) -> TaskResult:
        """Submit workflow to Highway API.

        Uses deterministic idempotency key for crash recovery.
        If Stabilize crashes after sending but before saving state,
        the retry sends the SAME key and Highway returns the EXISTING run_id.

        Args:
            stage: The stage execution context
            config: Highway configuration

        Returns:
            TaskResult with run_id in context (running) or error (terminal)
        """
        workflow_def = stage.context.get("highway_workflow_definition")
        if not workflow_def:
            return TaskResult.terminal(error="highway_workflow_definition not provided in stage context")

        # CRITICAL: Deterministic Idempotency Key
        # Format: stabilize-{execution_id}-{stage_id}
        exec_id = stage.execution.id if hasattr(stage, "execution") else "unknown"
        stage_id = stage.id if hasattr(stage, "id") else "unknown"
        idempotency_key = f"stabilize-{exec_id}-{stage_id}"

        inputs = dict(stage.context.get("highway_inputs", {}))

        # Support dynamic input mappings from context paths
        # e.g., {"_artifact_id": "body_json.artifact_id"} maps context body_json.artifact_id to input _artifact_id
        # Input mappings can reference both stage context AND ancestor outputs
        input_mappings = stage.context.get("highway_input_mappings", {})
        if input_mappings:
            # Build merged context including ancestor outputs
            merged_context = self._get_merged_context(stage)
            for input_key, context_path in input_mappings.items():
                value = self._resolve_context_path(merged_context, context_path)
                if value is not None:
                    inputs[input_key] = value
                    logger.debug("Mapped context path %s to input %s = %s", context_path, input_key, value)

        payload = {
            "workflow_definition": workflow_def,
            "inputs": inputs,
        }

        url = f"{config.api_endpoint.rstrip('/')}/api/v1/workflows"

        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {config.api_key}",
                    "Content-Type": "application/json",
                    "X-Idempotency-Key": idempotency_key,
                },
                method="POST",
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = json.loads(response.read().decode("utf-8"))

            # Extract run_id from response
            # Highway API returns: {"data": {"workflow_run_id": "..."}}
            run_id = None
            if isinstance(response_data, dict):
                data = response_data.get("data", response_data)
                run_id = data.get("workflow_run_id") or data.get("run_id")

            if not run_id:
                logger.error(
                    "Highway response missing run_id: %s",
                    response_data,
                )
                return TaskResult.terminal(error="Highway response missing workflow_run_id")

            logger.info(
                "Highway workflow submitted: run_id=%s, idempotency_key=%s",
                run_id,
                idempotency_key,
            )

            # Store run_id immediately for crash recovery
            return TaskResult.running(
                context={
                    "highway_run_id": run_id,
                    "highway_idempotency_key": idempotency_key,
                }
            )

        except urllib.error.HTTPError as e:
            error_body = ""
            try:
                error_body = e.read().decode("utf-8")
            except Exception:
                pass

            # 401/403: Auth error - terminal
            if e.code in (401, 403):
                logger.error(
                    "Highway auth error %s: %s",
                    e.code,
                    error_body,
                )
                return TaskResult.terminal(error=f"Highway authentication failed: {error_body}")

            # 404: Endpoint not found - terminal
            if e.code == 404:
                logger.error("Highway endpoint not found: %s", url)
                return TaskResult.terminal(error=f"Highway endpoint not found: {url}")

            # 409: Conflict (duplicate idempotency key with different payload)
            if e.code == 409:
                logger.warning(
                    "Highway idempotency conflict for key=%s: %s",
                    idempotency_key,
                    error_body,
                )
                # Try to extract existing run_id from error response
                try:
                    error_data = json.loads(error_body)
                    existing_run_id = error_data.get("existing_run_id")
                    if existing_run_id:
                        return TaskResult.running(context={"highway_run_id": existing_run_id})
                except Exception:
                    pass
                return TaskResult.terminal(error=f"Highway idempotency conflict: {error_body}")

            # 5xx: Server error - retry (return running)
            if e.code >= 500:
                logger.warning(
                    "Highway server error %s, will retry: %s",
                    e.code,
                    error_body,
                )
                return TaskResult.running()

            # Other errors - terminal
            logger.error(
                "Highway HTTP error %s: %s",
                e.code,
                error_body,
            )
            return TaskResult.terminal(error=f"Highway error {e.code}: {error_body}")

        except urllib.error.URLError as e:
            # Network error - retry
            logger.warning(
                "Highway network error, will retry: %s",
                e.reason,
            )
            return TaskResult.running()

        except Exception as e:
            logger.exception("Unexpected error submitting to Highway")
            return TaskResult.terminal(error=f"Unexpected error: {e}")

    def _poll_workflow(
        self,
        run_id: str,
        stage: StageExecution,
        config: HighwayConfig,
    ) -> TaskResult:
        """Poll Highway for workflow status.

        Black Box: Only check if done, trust Highway for everything else.
        Glass Box: Expose current_step for UI observability.

        Args:
            run_id: The Highway workflow run ID
            stage: The stage execution context
            config: Highway configuration

        Returns:
            TaskResult based on Highway status
        """
        url = f"{config.api_endpoint.rstrip('/')}/api/v1/workflows/{run_id}"

        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Authorization": f"Bearer {config.api_key}",
                },
                method="GET",
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = json.loads(response.read().decode("utf-8"))

            # Extract status from response
            # Highway API returns: {"data": {"state": "...", "result": ...}}
            data = response_data.get("data", response_data)
            state = data.get("state", data.get("status", "unknown"))
            result = data.get("result")
            current_step = data.get("current_step", data.get("current_task"))
            progress = data.get("progress", {})
            error_message = data.get("error") or data.get("failure_reason")

            logger.debug(
                "Highway workflow %s status: %s, current_step: %s",
                run_id,
                state,
                current_step,
            )

            # Terminal states
            if state == "completed":
                return TaskResult.success(
                    outputs={
                        "highway_run_id": run_id,
                        "highway_status": state,
                        "highway_result": result,
                    }
                )

            if state in ("failed", "cancelled"):
                return TaskResult.terminal(
                    error=f"Highway workflow {state}: {error_message or 'No error message'}",
                    context={
                        "highway_run_id": run_id,
                        "highway_status": state,
                        "highway_result": result,
                    },
                )

            # Still running - Glass Box: expose current_step for UI
            context_updates: dict[str, Any] = {
                "highway_run_id": run_id,
            }
            if current_step:
                context_updates["highway_current_step"] = current_step
            if progress:
                context_updates["highway_progress"] = progress

            return TaskResult.running(context=context_updates)

        except urllib.error.HTTPError as e:
            error_body = ""
            try:
                error_body = e.read().decode("utf-8")
            except Exception:
                pass

            # 401/403: Auth error - terminal
            if e.code in (401, 403):
                return TaskResult.terminal(error=f"Highway authentication failed: {error_body}")

            # 404: Run not found - terminal
            if e.code == 404:
                return TaskResult.terminal(error=f"Highway workflow run not found: {run_id}")

            # 5xx: Server error - retry
            if e.code >= 500:
                logger.warning(
                    "Highway server error %s during poll, will retry: %s",
                    e.code,
                    error_body,
                )
                return TaskResult.running(context={"highway_run_id": run_id})

            # Other errors - terminal
            return TaskResult.terminal(error=f"Highway poll error {e.code}: {error_body}")

        except urllib.error.URLError as e:
            # Network error - retry
            logger.warning(
                "Highway network error during poll, will retry: %s",
                e.reason,
            )
            return TaskResult.running(context={"highway_run_id": run_id})

        except Exception as e:
            logger.exception("Unexpected error polling Highway")
            return TaskResult.terminal(error=f"Unexpected poll error: {e}")

    def _get_merged_context(self, stage: StageExecution) -> dict[str, Any]:
        """Get stage context merged with ancestor outputs.

        This allows input mappings to reference values from upstream stages.

        Args:
            stage: The stage execution

        Returns:
            Merged context dictionary
        """
        merged = {}

        # Collect ancestor outputs (closest ancestor overwrites earlier)
        try:
            for ancestor in reversed(stage.ancestors()):
                merged.update(ancestor.outputs or {})
        except (AttributeError, ValueError):
            # Stage might not be attached to an execution yet
            pass

        # Own context takes precedence
        merged.update(stage.context or {})

        return merged

    def _resolve_context_path(
        self,
        context: dict[str, Any],
        path: str,
    ) -> Any:
        """Resolve a dotted path in context.

        Examples:
            _resolve_context_path({"a": {"b": 1}}, "a.b") -> 1
            _resolve_context_path({"body_json": {"artifact_id": "x"}}, "body_json.artifact_id") -> "x"

        Args:
            context: The context dictionary
            path: Dotted path like "body_json.artifact_id"

        Returns:
            The resolved value, or None if not found
        """
        parts = path.split(".")
        value = context

        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None

        return value
