from __future__ import annotations

import threading
import time

from stabilize import (
    StageExecution,
    Task,
    TaskResult,
)


# =============================================================================
# Test Task Implementations
# =============================================================================


class OutputTask(Task):
    """Task that produces configurable outputs from context."""

    def execute(self, stage: StageExecution) -> TaskResult:
        outputs = stage.context.get("outputs_to_produce", {})
        return TaskResult.success(outputs=outputs)


class SuspendTask(Task):
    """Task that suspends, waiting for a signal."""

    def execute(self, stage: StageExecution) -> TaskResult:
        signal_data = stage.context.get("_signal_data")
        if signal_data is not None:
            # Resumed after signal
            return TaskResult.success(outputs={"signal_received": signal_data})
        # First execution: suspend
        return TaskResult.suspend(context={"waiting_for": "signal"})


class TrackingTask(Task):
    """Thread-safe task that tracks execution order."""

    _lock = threading.Lock()
    execution_log: list[tuple[str, str, float]] = []

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            cls.execution_log = []

    def execute(self, stage: StageExecution) -> TaskResult:
        with TrackingTask._lock:
            TrackingTask.execution_log.append((stage.execution.id, stage.ref_id, time.time()))
        outputs = stage.context.get("outputs_to_produce", {})
        return TaskResult.success(outputs=outputs)


class SlowTask(Task):
    """Task that sleeps before succeeding."""

    def execute(self, stage: StageExecution) -> TaskResult:
        delay = stage.context.get("delay", 0.1)
        time.sleep(delay)
        outputs = stage.context.get("outputs_to_produce", {})
        return TaskResult.success(outputs=outputs)
