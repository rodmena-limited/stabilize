"""
Critical corner case tests for airport-critical systems.

This test suite focuses on edge cases and potential bugs that could cause
workflow failures in critical systems. Tests run on both SQLite and PostgreSQL
backends using testcontainers.

Categories:
1. State determination edge cases
2. Jump-to functionality edge cases
3. DAG traversal edge cases
4. Concurrency and race condition tests
5. Context merging edge cases
6. Synthetic stage edge cases
7. Task execution edge cases
8. Status transition edge cases
9. Message handling edge cases
10. Recovery and resilience tests
"""

from __future__ import annotations

from typing import Any

from stabilize import (
    StageExecution,
    TaskResult,
)
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import (
    HALT_STATUSES,
    WorkflowStatus,
)
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.corner_case_support import JumpingTask, OutputMergingTask

# =============================================================================
# 1. State Determination Edge Cases
# =============================================================================


class TestStateDeterminationEdgeCases:
    """Test edge cases in stage.determine_status()."""

    def test_empty_stage_status(self) -> None:
        """Stage with no tasks and no synthetic stages."""
        stage = StageExecution.create(
            type="empty",
            name="Empty Stage",
            ref_id="empty",
        )
        # Empty stage should return NOT_STARTED
        assert stage.determine_status() == WorkflowStatus.NOT_STARTED

    def test_stage_with_only_not_started_tasks(self) -> None:
        """Stage with tasks that are all NOT_STARTED."""
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )
        stage.tasks = [
            TaskExecution(name="task1", implementing_class="test"),
            TaskExecution(name="task2", implementing_class="test"),
        ]
        # Should be RUNNING since tasks haven't started yet
        status = stage.determine_status()
        assert status == WorkflowStatus.RUNNING

    def test_stage_with_mixed_task_statuses(self) -> None:
        """Stage with tasks in different states."""
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )
        task1 = TaskExecution(name="task1", implementing_class="test")
        task1.status = WorkflowStatus.SUCCEEDED
        task2 = TaskExecution(name="task2", implementing_class="test")
        task2.status = WorkflowStatus.RUNNING
        stage.tasks = [task1, task2]

        # Should be RUNNING since task2 is still running
        assert stage.determine_status() == WorkflowStatus.RUNNING

    def test_stage_with_terminal_and_succeeded_tasks(self) -> None:
        """Stage with one TERMINAL and one SUCCEEDED task."""
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )
        task1 = TaskExecution(name="task1", implementing_class="test")
        task1.status = WorkflowStatus.SUCCEEDED
        task2 = TaskExecution(name="task2", implementing_class="test")
        task2.status = WorkflowStatus.TERMINAL
        stage.tasks = [task1, task2]

        # TERMINAL should take priority
        status = stage.determine_status()
        assert status.is_halt

    def test_stage_with_paused_tasks(self) -> None:
        """Stage with paused tasks."""
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )
        task1 = TaskExecution(name="task1", implementing_class="test")
        task1.status = WorkflowStatus.PAUSED
        stage.tasks = [task1]

        assert stage.determine_status() == WorkflowStatus.PAUSED

    def test_stage_status_priority_order(self) -> None:
        """Verify status priority order is correct."""
        # Priority: TERMINAL > STOPPED > CANCELED > PAUSED > RUNNING > FAILED_CONTINUE > SUCCEEDED
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )

        # All halt statuses should take priority
        for halt_status in [
            WorkflowStatus.TERMINAL,
            WorkflowStatus.STOPPED,
            WorkflowStatus.CANCELED,
        ]:
            task = TaskExecution(name="task", implementing_class="test")
            task.status = halt_status
            stage.tasks = [task]
            assert stage.determine_status().is_halt

    def test_after_stages_running_prevents_success(self) -> None:
        """After-stages running should prevent stage from reporting SUCCEEDED."""
        # Create workflow with parent and after-stage
        parent = StageExecution.create(
            type="test",
            name="Parent",
            ref_id="parent",
        )
        task = TaskExecution(name="task", implementing_class="test")
        task.status = WorkflowStatus.SUCCEEDED
        parent.tasks = [task]

        after = StageExecution.create_synthetic(
            type="cleanup",
            name="Cleanup",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_AFTER,
        )
        after.status = WorkflowStatus.RUNNING

        # Create workflow to link stages (must store reference to prevent GC)
        _workflow = Workflow.create(
            application="test",
            name="Test",
            stages=[parent, after],
        )

        # Parent should still be RUNNING since after-stage is running
        assert parent.determine_status() == WorkflowStatus.RUNNING
        del _workflow  # Explicit cleanup


# =============================================================================
# 2. Jump-to Functionality Edge Cases
# =============================================================================


class TestJumpToEdgeCases:
    """Test edge cases in jump-to functionality."""

    def test_jump_to_self_basic(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test jumping to the same stage (self-loop)."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jumping": JumpingTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Self Jump Stage",
            ref_id="self_jump",
            context={
                "jump_target": "self_jump",
                "_max_jumps": 3,  # Limit iterations
            },
        )

        task = TaskExecution(
            name="JumpTask",
            implementing_class="jumping",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Self Jump Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=10.0)

        # Reload and check - should have hit max jumps
        result = repository.retrieve(workflow.id)
        assert result.status in HALT_STATUSES or result.status == WorkflowStatus.TERMINAL

    def test_jump_to_nonexistent_stage(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test jumping to a stage that doesn't exist."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jumping": JumpingTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Bad Jump Stage",
            ref_id="bad_jump",
            context={"jump_target": "nonexistent_stage"},
        )

        task = TaskExecution(
            name="JumpTask",
            implementing_class="jumping",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Bad Jump Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        # Should fail with TERMINAL status
        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.TERMINAL

    def test_jump_with_zero_max_jumps(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that _max_jumps=0 correctly disables all jumps.

        Setting _max_jumps=0 should block any jump attempts, causing the
        workflow to fail with TERMINAL status when a jump is attempted.
        """
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jumping": JumpingTask},
        )

        stage1 = StageExecution.create(
            type="test",
            name="Jump Source",
            ref_id="source",
            context={"jump_target": "target", "_max_jumps": 0},
        )
        task1 = TaskExecution(
            name="JumpTask",
            implementing_class="jumping",
            stage_start=True,
            stage_end=True,
        )
        stage1.tasks = [task1]

        stage2 = StageExecution.create(
            type="test",
            name="Jump Target",
            ref_id="target",
            requisite_stage_ref_ids={"source"},
        )
        task2 = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage2.tasks = [task2]

        workflow = Workflow.create(
            application="test",
            name="Zero Max Jumps Test",
            stages=[stage1, stage2],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        # With _max_jumps=0, the jump should be blocked and workflow fails
        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.TERMINAL

    def test_jump_context_preservation(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that jump context is properly preserved."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jumping": JumpingTask, "output": OutputMergingTask},
        )

        stage1 = StageExecution.create(
            type="test",
            name="Jump Source",
            ref_id="source",
            context={
                "original_value": "preserved",
                "jump_target": "target",
                "_max_jumps": 5,
            },
        )
        task1 = TaskExecution(
            name="JumpTask",
            implementing_class="jumping",
            stage_start=True,
            stage_end=True,
        )
        stage1.tasks = [task1]

        stage2 = StageExecution.create(
            type="test",
            name="Jump Target",
            ref_id="target",
            requisite_stage_ref_ids={"source"},
        )
        task2 = TaskExecution(
            name="OutputTask",
            implementing_class="output",
            stage_start=True,
            stage_end=True,
        )
        stage2.tasks = [task2]

        workflow = Workflow.create(
            application="test",
            name="Jump Context Test",
            stages=[stage1, stage2],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        # Check that jump context was preserved
        target_stage = result.stage_by_ref_id("target")
        assert target_stage is not None
        assert "jumped_from" in target_stage.context or target_stage.context.get("_jump_history")


# =============================================================================
# 13. Bug Documentation Tests
# =============================================================================


class TestBugDocumentation:
    """Tests that document known bugs for tracking and verification after fixes."""

    def test_bug_max_jumps_zero_treated_as_falsy(self, tmp_path: Any) -> None:
        from stabilize import Orchestrator, TaskResult, Workflow
        from stabilize.models.status import WorkflowStatus
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue
        from stabilize.tasks.interface import Task
        from tests.audit_support import drive, make_processor, stage

        class JumpBack(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.jump_to("a")

        url = f"sqlite:///{tmp_path}/jumps.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, table_name="queue_messages")
        queue._create_table()
        processor = make_processor(store, queue, {"jumpback": JumpBack})
        wf = Workflow.create(application="test", name="no-jumps", stages=[stage("a", ["jumpback"])])
        wf.context["_max_jumps"] = 0
        store.store(wf)
        Orchestrator(queue, store=store).start(wf)
        final = drive(processor, store, wf.id, timeout=15, idle=1.5)
        a = final.stages[0]
        assert a.status == WorkflowStatus.TERMINAL, f"_max_jumps=0 still allowed a jump: stage {a.status.name}"
        assert a.context.get("jump_error") == "Max jump count exceeded: 0/0", a.context.get("jump_error")
        queue.close()
        store.close()
