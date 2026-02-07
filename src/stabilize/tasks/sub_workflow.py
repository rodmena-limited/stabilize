"""
SubWorkflowTask - executes a child workflow and waits for completion.

Implements WCP-22: Recursion.
A task that starts a child workflow and polls until it completes.
Supports recursion depth tracking to prevent infinite recursion.
"""

from __future__ import annotations

import logging
from typing import Any

from stabilize.models.status import WorkflowStatus
from stabilize.tasks.interface import RetryableTask
from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)

# Default maximum recursion depth
DEFAULT_MAX_RECURSION_DEPTH = 10


class SubWorkflowTask(RetryableTask):
    """Task that starts and monitors a child workflow execution.

    The child workflow is started on the first execution. Subsequent
    executions poll the child's status until it completes.

    Context keys:
        _sub_workflow_config: dict with workflow configuration
        _sub_workflow_id: set after child is started (for polling)
        _recursion_depth: current recursion depth (incremented per level)
        _max_recursion_depth: maximum allowed depth (default: 10)

    Outputs:
        sub_workflow_id: ID of the child workflow
        sub_workflow_status: Final status of the child workflow
        sub_workflow_outputs: Merged outputs from the child workflow
    """

    @property
    def aliases(self) -> list[str]:
        return ["subWorkflow", "sub_workflow", "childWorkflow"]

    def get_timeout(self) -> int:
        """Default timeout: 30 minutes."""
        return 30 * 60 * 1000

    def get_backoff_period(self) -> int:
        """Poll every 5 seconds."""
        return 5000

    def execute(self, stage: Any) -> TaskResult:
        """Execute or poll the sub-workflow."""

        context = stage.context
        sub_workflow_id = context.get("_sub_workflow_id")

        if sub_workflow_id is None:
            # First execution - start the child workflow
            return self._start_child(stage, context)

        # Subsequent executions - poll the child
        return self._poll_child(stage, sub_workflow_id)

    def _start_child(self, stage: Any, context: dict[str, Any]) -> TaskResult:
        """Start a new child workflow."""
        # Check recursion depth
        depth = context.get("_recursion_depth", 0)
        max_depth = context.get("_max_recursion_depth", DEFAULT_MAX_RECURSION_DEPTH)

        if depth >= max_depth:
            return TaskResult.terminal(
                f"Maximum recursion depth ({max_depth}) exceeded",
                context={"_recursion_depth": depth},
            )

        config = context.get("_sub_workflow_config", {})
        if not config:
            return TaskResult.terminal("No _sub_workflow_config in context")

        # Build child workflow context with incremented recursion depth
        child_context = dict(config.get("context", {}))
        child_context["_recursion_depth"] = depth + 1
        child_context["_max_recursion_depth"] = max_depth
        child_context["_parent_workflow_id"] = (
            stage.execution.id if hasattr(stage, "execution") and stage.has_execution() else ""
        )

        try:
            from stabilize.orchestrator import Orchestrator

            orchestrator = Orchestrator.get_instance()
            if orchestrator is None:
                return TaskResult.terminal("No Orchestrator instance available for sub-workflow")

            child_id = orchestrator.start(
                name=config.get("name", f"sub-workflow-depth-{depth + 1}"),
                application=config.get("application", "stabilize"),
                stages=config.get("stages", []),
                context=child_context,
            )

            logger.info(
                "Started child workflow %s at recursion depth %d",
                child_id,
                depth + 1,
            )

            return TaskResult.running(
                context={
                    "_sub_workflow_id": child_id,
                    "_recursion_depth": depth + 1,
                },
            )

        except Exception as e:
            logger.error("Failed to start child workflow: %s", e)
            return TaskResult.terminal(f"Failed to start child workflow: {e}")

    def _poll_child(self, stage: Any, sub_workflow_id: str) -> TaskResult:
        """Poll a running child workflow."""
        try:
            from stabilize.orchestrator import Orchestrator

            orchestrator = Orchestrator.get_instance()
            if orchestrator is None:
                return TaskResult.terminal("No Orchestrator instance available for polling")

            child = orchestrator.get_execution(sub_workflow_id)
            if child is None:
                return TaskResult.terminal(f"Child workflow {sub_workflow_id} not found")

            if child.status == WorkflowStatus.SUCCEEDED:
                logger.info("Child workflow %s completed successfully", sub_workflow_id)
                return TaskResult.success(
                    outputs={
                        "sub_workflow_id": sub_workflow_id,
                        "sub_workflow_status": "SUCCEEDED",
                        "sub_workflow_outputs": child.context.get("outputs", {}),
                    },
                )

            if child.status.is_halt:
                logger.warning(
                    "Child workflow %s halted with status %s",
                    sub_workflow_id,
                    child.status,
                )
                return TaskResult.terminal(
                    f"Child workflow failed with status {child.status.name}",
                    context={
                        "sub_workflow_id": sub_workflow_id,
                        "sub_workflow_status": child.status.name,
                    },
                )

            if child.status.is_complete:
                return TaskResult.success(
                    outputs={
                        "sub_workflow_id": sub_workflow_id,
                        "sub_workflow_status": child.status.name,
                    },
                )

            # Still running
            return TaskResult.running()

        except Exception as e:
            logger.error("Failed to poll child workflow %s: %s", sub_workflow_id, e)
            return TaskResult.running()  # Retry on transient errors
