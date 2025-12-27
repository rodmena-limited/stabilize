"""Highway integration for Stabilize pipelines.

This module provides the HighwayTask for triggering and monitoring
Highway Workflow Engine executions from Stabilize pipelines.

Architecture:
    Stabilize (Control Plane) -> Highway (Execution Plane)

    - Execution: Black Box - Stabilize sends `start`, waits for `completed`
    - State: Black Box - Stabilize stores only `run_id`
    - Observability: Glass Box - Stabilize proxies logs/current_step for UI

Example:
    from stabilize.tasks.highway import HighwayTask, HighwayConfig

    # Register the task
    registry.register("highway", HighwayTask)

    # Use in stage context:
    context = {
        "highway_workflow_definition": {...},  # Highway JSON DSL
        "highway_inputs": {"key": "value"},
    }
"""

from stabilize.tasks.highway.config import HighwayConfig
from stabilize.tasks.highway.task import HighwayTask

__all__ = [
    "HighwayConfig",
    "HighwayTask",
]
