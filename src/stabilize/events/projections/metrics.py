"""
Stage metrics projection.

Aggregates metrics across stage executions for analytics.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any

from stabilize.events.base import EntityType, Event, EventType
from stabilize.events.projections.base import Projection


@dataclass
class StageMetrics:
    """Aggregated metrics for a stage type."""

    stage_type: str
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    skip_count: int = 0
    durations: list[int] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        completed = self.success_count + self.failure_count
        if completed == 0:
            return 0.0
        return (self.success_count / completed) * 100

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate as a percentage."""
        completed = self.success_count + self.failure_count
        if completed == 0:
            return 0.0
        return (self.failure_count / completed) * 100

    @property
    def avg_duration_ms(self) -> float:
        """Calculate average duration in milliseconds."""
        if not self.durations:
            return 0.0
        return statistics.mean(self.durations)

    @property
    def median_duration_ms(self) -> float:
        """Calculate median duration in milliseconds."""
        if not self.durations:
            return 0.0
        return statistics.median(self.durations)

    @property
    def p95_duration_ms(self) -> float:
        """Calculate 95th percentile duration in milliseconds."""
        if not self.durations:
            return 0.0
        if len(self.durations) < 20:
            # Not enough data for meaningful percentile
            return max(self.durations)
        sorted_durations = sorted(self.durations)
        index = int(len(sorted_durations) * 0.95)
        return sorted_durations[min(index, len(sorted_durations) - 1)]

    @property
    def min_duration_ms(self) -> int:
        """Get minimum duration in milliseconds."""
        if not self.durations:
            return 0
        return min(self.durations)

    @property
    def max_duration_ms(self) -> int:
        """Get maximum duration in milliseconds."""
        if not self.durations:
            return 0
        return max(self.durations)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "stage_type": self.stage_type,
            "execution_count": self.execution_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "skip_count": self.skip_count,
            "success_rate": round(self.success_rate, 2),
            "failure_rate": round(self.failure_rate, 2),
            "avg_duration_ms": round(self.avg_duration_ms, 2),
            "median_duration_ms": round(self.median_duration_ms, 2),
            "p95_duration_ms": round(self.p95_duration_ms, 2),
            "min_duration_ms": self.min_duration_ms,
            "max_duration_ms": self.max_duration_ms,
        }


@dataclass
class TaskMetrics:
    """Aggregated metrics for a task type."""

    task_name: str
    implementing_class: str | None = None
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    retry_count: int = 0
    durations: list[int] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        completed = self.success_count + self.failure_count
        if completed == 0:
            return 0.0
        return (self.success_count / completed) * 100

    @property
    def avg_duration_ms(self) -> float:
        """Calculate average duration in milliseconds."""
        if not self.durations:
            return 0.0
        return statistics.mean(self.durations)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "task_name": self.task_name,
            "implementing_class": self.implementing_class,
            "execution_count": self.execution_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "retry_count": self.retry_count,
            "success_rate": round(self.success_rate, 2),
            "avg_duration_ms": round(self.avg_duration_ms, 2),
        }


class StageMetricsProjection(Projection):
    """
    Aggregates stage execution metrics for analytics.

    Tracks:
    - Execution counts per stage type
    - Success/failure rates
    - Duration statistics (avg, median, p95)
    """

    def __init__(self) -> None:
        """Initialize the metrics projection."""
        self._stage_metrics: dict[str, StageMetrics] = {}
        self._task_metrics: dict[str, TaskMetrics] = {}
        self._processed_sequences: set[int] = set()

    @property
    def name(self) -> str:
        return "stage-metrics"

    def apply(self, event: Event) -> None:
        if event.sequence and event.sequence in self._processed_sequences:
            return
        if event.sequence:
            self._processed_sequences.add(event.sequence)

        if event.entity_type == EntityType.STAGE:
            self._apply_stage_event(event)
        elif event.entity_type == EntityType.TASK:
            self._apply_task_event(event)

    def _apply_stage_event(self, event: Event) -> None:
        """Apply a stage event."""
        stage_type = event.data.get("type", "unknown")

        # Ensure metrics entry exists
        if stage_type not in self._stage_metrics:
            self._stage_metrics[stage_type] = StageMetrics(stage_type=stage_type)

        metrics = self._stage_metrics[stage_type]

        if event.event_type == EventType.STAGE_STARTED:
            metrics.execution_count += 1

        elif event.event_type == EventType.STAGE_COMPLETED:
            metrics.success_count += 1
            duration = event.data.get("duration_ms")
            if duration is not None:
                metrics.durations.append(int(duration))

        elif event.event_type == EventType.STAGE_FAILED:
            metrics.failure_count += 1
            duration = event.data.get("duration_ms")
            if duration is not None:
                metrics.durations.append(int(duration))

        elif event.event_type == EventType.STAGE_SKIPPED:
            metrics.skip_count += 1

    def _apply_task_event(self, event: Event) -> None:
        """Apply a task event."""
        task_name = event.data.get("name", "unknown")

        # Ensure metrics entry exists
        if task_name not in self._task_metrics:
            self._task_metrics[task_name] = TaskMetrics(
                task_name=task_name,
                implementing_class=event.data.get("implementing_class"),
            )

        metrics = self._task_metrics[task_name]

        if event.event_type == EventType.TASK_STARTED:
            metrics.execution_count += 1

        elif event.event_type == EventType.TASK_COMPLETED:
            metrics.success_count += 1
            duration = event.data.get("duration_ms")
            if duration is not None:
                metrics.durations.append(int(duration))

        elif event.event_type == EventType.TASK_FAILED:
            metrics.failure_count += 1
            duration = event.data.get("duration_ms")
            if duration is not None:
                metrics.durations.append(int(duration))

        elif event.event_type == EventType.TASK_RETRIED:
            metrics.retry_count += 1

    def get_state(self) -> dict[str, StageMetrics]:
        """Get all stage metrics."""
        return self._stage_metrics.copy()

    def get_task_state(self) -> dict[str, TaskMetrics]:
        """Get all task metrics."""
        return self._task_metrics.copy()

    def get_metrics_for_type(self, stage_type: str) -> StageMetrics | None:
        """Get metrics for a specific stage type."""
        return self._stage_metrics.get(stage_type)

    def get_task_metrics_for_name(self, task_name: str) -> TaskMetrics | None:
        """Get metrics for a specific task name."""
        return self._task_metrics.get(task_name)

    def reset(self) -> None:
        """Reset all metrics."""
        self._stage_metrics.clear()
        self._task_metrics.clear()
        self._processed_sequences.clear()

    def get_top_slowest_stages(self, n: int = 10) -> list[tuple[str, float]]:
        """Get the n slowest stage types by average duration."""
        stages_with_avg = [
            (stage_type, metrics.avg_duration_ms)
            for stage_type, metrics in self._stage_metrics.items()
            if metrics.durations
        ]
        return sorted(stages_with_avg, key=lambda x: x[1], reverse=True)[:n]

    def get_top_failing_stages(self, n: int = 10) -> list[tuple[str, float]]:
        """Get the n stage types with highest failure rates."""
        stages_with_rate = [
            (stage_type, metrics.failure_rate)
            for stage_type, metrics in self._stage_metrics.items()
            if metrics.execution_count > 0
        ]
        return sorted(stages_with_rate, key=lambda x: x[1], reverse=True)[:n]

    def to_dict(self) -> dict[str, Any]:
        """Convert all metrics to dictionary."""
        return {
            "stages": {stage_type: metrics.to_dict() for stage_type, metrics in self._stage_metrics.items()},
            "tasks": {task_name: metrics.to_dict() for task_name, metrics in self._task_metrics.items()},
        }
