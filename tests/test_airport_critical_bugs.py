"""
Airport-critical bug hunting tests.

This test suite specifically targets corner cases and race conditions that could
cause failures in critical airport systems. Each test documents a specific
potential failure mode and validates the system handles it correctly.

These tests run on both SQLite and PostgreSQL backends.
"""

from __future__ import annotations

import threading
import time

from stabilize import (
    StageExecution,
    Task,
    TaskResult,
)
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize


# =============================================================================
# Test Task Implementations for Bug Hunting
# =============================================================================


class SelfJumpTask(Task):
    """Task that jumps to itself for retry loop testing."""

    _call_counts: dict[str, int] = {}

    @classmethod
    def reset(cls) -> None:
        cls._call_counts = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        key = f"{stage.execution.id}:{stage.ref_id}"
        count = self._call_counts.get(key, 0) + 1
        self._call_counts[key] = count

        target_count = stage.context.get("target_count", 3)
        if count < target_count:
            return TaskResult.jump_to(
                target_stage_ref_id=stage.ref_id,
                context={"call_count": count},
            )
        return TaskResult.success(outputs={"total_calls": count})


class JumpOnConditionTask(Task):
    """Task that jumps based on stage context."""

    def execute(self, stage: StageExecution) -> TaskResult:
        jump_target = stage.context.get("jump_target")
        jump_count = stage.context.get("_jump_count", 0)
        max_jumps = stage.context.get("max_loop_iterations", 3)

        if jump_target and jump_count < max_jumps:
            return TaskResult.jump_to(
                target_stage_ref_id=jump_target,
                context={"iteration": jump_count + 1},
                outputs={"jumped": True, "iteration": jump_count},
            )
        return TaskResult.success(outputs={"final_iteration": jump_count})


class ConcurrentIncrementTask(Task):
    """Task that increments a counter - used to detect race conditions."""

    _counters: dict[str, int] = {}
    _lock = threading.Lock()

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            cls._counters = {}

    @classmethod
    def get_count(cls, key: str) -> int:
        with cls._lock:
            return cls._counters.get(key, 0)

    def execute(self, stage: StageExecution) -> TaskResult:
        key = stage.context.get("counter_key", "default")
        with self._lock:
            current = self._counters.get(key, 0)
            time.sleep(0.001)
            self._counters[key] = current + 1
        return TaskResult.success(outputs={"counter": self._counters[key]})


class NullOutputTask(Task):
    """Task that returns null/None values in outputs."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(
            outputs={
                "null_value": None,
                "empty_string": "",
                "zero": 0,
                "false": False,
                "empty_list": [],
                "empty_dict": {},
            }
        )


class LargePayloadTask(Task):
    """Task that produces large outputs."""

    def execute(self, stage: StageExecution) -> TaskResult:
        size = stage.context.get("payload_size", 10000)
        return TaskResult.success(
            outputs={
                "large_string": "x" * size,
                "large_list": list(range(size // 10)),
            }
        )


# =============================================================================
# Bug Hunt Test: Stale CompleteStage After Jump
# =============================================================================


class TestStaleMessageAfterJump:
    """
    BUG HYPOTHESIS: When a JumpToStage resets a stage to NOT_STARTED, any
    pending CompleteStage messages from the previous iteration could trigger
    downstream stages prematurely.
    """

    def test_stale_complete_stage_ignored_after_jump(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify stale CompleteStage messages are ignored after stage reset."""
        processor, runner, task_registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"self_jump": SelfJumpTask},
        )
        SelfJumpTask.reset()

        workflow = Workflow.create(
            application="airport",
            name="stale-message-test",
            stages=[
                StageExecution(
                    ref_id="jump_stage",
                    name="Self Jump Stage",
                    context={"target_count": 3},
                    tasks=[
                        TaskExecution.create(
                            name="self_jump_task",
                            implementing_class="self_jump",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                )
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(50):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Bug Hunt Test: Null/Empty Value Handling
# =============================================================================


class TestNullAndEmptyValueHandling:
    """
    BUG HYPOTHESIS: Null values, empty strings, and falsy values in outputs
    may cause issues during serialization or status determination.
    """

    def test_null_values_in_outputs_preserved(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify null/None values in outputs are preserved through storage."""
        processor, runner, task_registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"null_output": NullOutputTask},
        )

        workflow = Workflow.create(
            application="airport",
            name="null-outputs",
            stages=[
                StageExecution(
                    ref_id="null",
                    name="Null Stage",
                    tasks=[
                        TaskExecution.create(
                            name="null_task",
                            implementing_class="null_output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                )
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(50):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        null_stage = result.stage_by_ref_id("null")
        assert null_stage.outputs.get("empty_string") == ""
        assert null_stage.outputs.get("zero") == 0
        assert null_stage.outputs.get("false") is False
        assert null_stage.outputs.get("empty_list") == []


# =============================================================================
# Bug Hunt Test: Large Payload Handling
# =============================================================================


class TestLargePayloadHandling:
    """
    BUG HYPOTHESIS: Very large context/outputs may cause issues with database
    storage or queue serialization.
    """

    def test_large_output_through_task_execution(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test large outputs flowing through task execution."""
        processor, runner, task_registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"large_payload": LargePayloadTask},
        )

        workflow = Workflow.create(
            application="airport",
            name="large-output",
            stages=[
                StageExecution(
                    ref_id="large",
                    name="Large Output Stage",
                    context={"payload_size": 50000},
                    tasks=[
                        TaskExecution.create(
                            name="large_task",
                            implementing_class="large_payload",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                )
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(50):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        large_stage = result.stage_by_ref_id("large")
        assert len(large_stage.outputs.get("large_string", "")) == 50000


# =============================================================================
# Bug Hunt Test: Concurrent Processing Race Conditions
# =============================================================================


class TestConcurrentProcessingRaces:
    """
    BUG HYPOTHESIS: Concurrent processing of messages for the same workflow
    may cause race conditions leading to lost updates.
    """

    def test_concurrent_task_completions_no_lost_updates(
        self,
        file_repository: WorkflowStore,
        file_queue: Queue,
        backend: str,
    ) -> None:
        """Verify concurrent task completions don't lose updates."""
        processor, runner, task_registry = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={"concurrent": ConcurrentIncrementTask},
        )
        ConcurrentIncrementTask.reset()

        stages = []
        for i in range(5):
            stages.append(
                StageExecution(
                    ref_id=f"parallel_{i}",
                    name=f"Parallel Stage {i}",
                    context={"counter_key": "shared"},
                    tasks=[
                        TaskExecution.create(
                            name=f"inc_task_{i}",
                            implementing_class="concurrent",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                )
            )

        stages.append(
            StageExecution(
                ref_id="final",
                name="Final Stage",
                requisite_stage_ref_ids={f"parallel_{i}" for i in range(5)},
                tasks=[
                    TaskExecution.create(
                        name="final_task",
                        implementing_class="success",
                        stage_start=True,
                        stage_end=True,
                    )
                ],
            )
        )

        workflow = Workflow.create(
            application="airport",
            name="concurrent-tasks",
            stages=stages,
        )

        file_repository.store(workflow)
        runner.start(workflow)

        for _ in range(100):
            if not processor.process_one():
                break

        result = file_repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert ConcurrentIncrementTask.get_count("shared") == 5
