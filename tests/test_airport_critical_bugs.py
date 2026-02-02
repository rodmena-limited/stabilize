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
from typing import Any

import pytest

from stabilize import (
    StageExecution,
    Task,
    TaskResult,
)
from stabilize.errors import ConcurrencyError, TransientError
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartStage, StartWorkflow
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
# Bug Hunt Test: Workflow Context Persistence (BUG #1 - CRITICAL)
# =============================================================================


class TestWorkflowContextPersistence:
    """
    CRITICAL BUG FOUND: Workflow context was not persisted to the database.
    This caused _max_jumps and other workflow-level settings to be lost after
    retrieving a workflow from the database.

    FIX: Added 'context' column to pipeline_executions table and updated
    converters in both SQLite and PostgreSQL backends.
    """

    def test_workflow_context_persisted(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify workflow context is persisted and retrieved correctly."""
        workflow = Workflow.create(
            application="airport",
            name="context-persistence-test",
            context={
                "custom_setting": "test_value",
                "nested": {"key": "value"},
                "list_data": [1, 2, 3],
            },
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        assert result.context == {
            "custom_setting": "test_value",
            "nested": {"key": "value"},
            "list_data": [1, 2, 3],
        }

    def test_max_jumps_setting_persisted(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify _max_jumps workflow context setting is preserved."""
        workflow = Workflow.create(
            application="airport",
            name="max-jumps-persistence-test",
            context={"_max_jumps": 5},
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        assert result.context.get("_max_jumps") == 5

    def test_max_jumps_zero_disables_jumps(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify _max_jumps=0 persists correctly and could disable jumps."""
        workflow = Workflow.create(
            application="airport",
            name="zero-max-jumps-test",
            context={"_max_jumps": 0},
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        # _max_jumps=0 should persist as 0, not be lost
        assert "_max_jumps" in result.context
        assert result.context.get("_max_jumps") == 0

    def test_empty_workflow_context_persisted(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify empty workflow context is handled correctly."""
        workflow = Workflow.create(
            application="airport",
            name="empty-context-test",
            context={},
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        assert result.context == {}


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


# =============================================================================
# Bug Hunt Test: Status Transition Consistency
# =============================================================================


class TestStatusTransitionConsistency:
    """
    BUG HYPOTHESIS: Invalid status transitions may not be properly prevented.
    """

    def test_completed_stage_cannot_restart(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify a completed stage cannot be restarted."""
        processor, runner, task_registry = setup_stabilize(repository, queue)

        stage = StageExecution(
            ref_id="test",
            name="Test Stage",
            status=WorkflowStatus.SUCCEEDED,
            tasks=[
                TaskExecution.create(
                    name="task",
                    implementing_class="success",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        workflow = Workflow.create(
            application="airport",
            name="restart-prevention",
            stages=[stage],
        )

        repository.store(workflow)

        queue.push(
            StartStage(
                execution_type="PIPELINE",
                execution_id=workflow.id,
                stage_id=stage.id,
            )
        )

        processor.process_one()

        result = repository.retrieve(workflow.id)
        stage_result = result.stage_by_ref_id("test")
        assert stage_result.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Bug Hunt Test: Orphaned Synthetic Stages
# =============================================================================


class TestOrphanedSyntheticStages:
    """
    BUG HYPOTHESIS: Synthetic stages with invalid parent_stage_id may cause issues.
    """

    def test_synthetic_stage_without_valid_parent(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test handling of synthetic stage with invalid parent_stage_id."""
        synthetic = StageExecution(
            ref_id="orphan",
            name="Orphaned Synthetic",
            parent_stage_id="non_existent_parent_id",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_AFTER,
        )

        workflow = Workflow.create(
            application="airport",
            name="orphaned-synthetic",
            stages=[synthetic],
        )

        try:
            parent = synthetic.parent()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "not found" in str(e).lower()


# =============================================================================
# Bug Hunt Test: DAG Cycle Detection
# =============================================================================


class TestDAGCycleDetection:
    """
    BUG HYPOTHESIS: Circular dependencies may cause infinite loops.
    """

    def test_direct_cycle_detected(self) -> None:
        """Test that direct cycles (A -> B -> A) are detected."""
        from stabilize.dag.topological import CircularDependencyError, topological_sort

        workflow = Workflow.create(
            application="airport",
            name="direct-cycle",
            stages=[
                StageExecution(
                    ref_id="a",
                    name="Stage A",
                    requisite_stage_ref_ids={"b"},
                ),
                StageExecution(
                    ref_id="b",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                ),
            ],
        )

        with pytest.raises(CircularDependencyError):
            topological_sort(workflow.stages)

    def test_indirect_cycle_detected(self) -> None:
        """Test that indirect cycles (A -> B -> C -> A) are detected."""
        from stabilize.dag.topological import CircularDependencyError, topological_sort

        workflow = Workflow.create(
            application="airport",
            name="indirect-cycle",
            stages=[
                StageExecution(
                    ref_id="a",
                    name="Stage A",
                    requisite_stage_ref_ids={"c"},
                ),
                StageExecution(
                    ref_id="b",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                ),
                StageExecution(
                    ref_id="c",
                    name="Stage C",
                    requisite_stage_ref_ids={"b"},
                ),
            ],
        )

        with pytest.raises(CircularDependencyError):
            topological_sort(workflow.stages)


# =============================================================================
# Bug Hunt Test: Empty Workflow Handling
# =============================================================================


class TestEmptyWorkflowHandling:
    """
    BUG HYPOTHESIS: Empty workflows may not complete correctly.
    """

    def test_workflow_with_no_stages_completes(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that a workflow with no stages completes immediately."""
        processor, runner, task_registry = setup_stabilize(repository, queue)

        workflow = Workflow.create(
            application="airport",
            name="empty-workflow",
            stages=[],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(20):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status.is_complete


# =============================================================================
# Bug Hunt Test: Unicode and Special Characters
# =============================================================================


class TestUnicodeAndSpecialCharacters:
    """
    BUG HYPOTHESIS: Unicode characters may cause serialization issues.
    """

    def test_unicode_in_stage_context_preserved(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that unicode characters are preserved in stage context."""
        workflow = Workflow.create(
            application="airport",
            name="unicode-test",
            stages=[
                StageExecution(
                    ref_id="unicode",
                    name="Unicode Stage",
                    context={
                        "japanese": "こんにちは",
                        "emoji": "👋🌍",
                        "greek": "αβγδ",
                    },
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        unicode_stage = result.stage_by_ref_id("unicode")
        assert unicode_stage.context.get("japanese") == "こんにちは"
        assert unicode_stage.context.get("emoji") == "👋🌍"
        assert unicode_stage.context.get("greek") == "αβγδ"


# =============================================================================
# Bug Hunt Test: Timestamp Edge Cases
# =============================================================================


class TestTimestampEdgeCases:
    """
    BUG HYPOTHESIS: Edge values of timestamps may cause issues.
    """

    def test_zero_timestamp_handled(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that zero timestamps are handled correctly."""
        stage = StageExecution(
            ref_id="test",
            name="Test Stage",
            start_time=0,
            end_time=0,
        )

        workflow = Workflow.create(
            application="airport",
            name="zero-timestamp",
            stages=[stage],
        )
        workflow.start_time = 0

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        assert result.start_time == 0
        stage_result = result.stage_by_ref_id("test")
        assert stage_result.start_time == 0

    def test_very_large_timestamp_handled(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that very large timestamps are handled."""
        future_time = 32503680000000  # Year 3000

        workflow = Workflow.create(
            application="airport",
            name="future-timestamp",
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                    scheduled_time=future_time,
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        stage_result = result.stage_by_ref_id("test")
        assert stage_result.scheduled_time == future_time


# =============================================================================
# Bug Hunt Test: Zombie Stage Detection
# =============================================================================


class TestZombieStageDetection:
    """
    BUG HYPOTHESIS: Stages may get stuck in RUNNING state indefinitely.
    """

    def test_running_stage_with_completed_tasks(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test detection of stages stuck in RUNNING without active processing."""
        task = TaskExecution.create(
            name="completed_task",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        task.status = WorkflowStatus.SUCCEEDED

        stage = StageExecution(
            ref_id="zombie",
            name="Zombie Stage",
            status=WorkflowStatus.RUNNING,
            tasks=[task],
        )

        workflow = Workflow.create(
            application="airport",
            name="zombie-test",
            stages=[stage],
        )

        repository.store(workflow)

        result = repository.retrieve(workflow.id)
        zombie_stage = result.stage_by_ref_id("zombie")
        determined_status = zombie_stage.determine_status()
        assert determined_status == WorkflowStatus.SUCCEEDED
