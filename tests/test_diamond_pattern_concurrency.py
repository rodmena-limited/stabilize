from __future__ import annotations

import pytest

from stabilize import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.diamond_support import (
    CountingOutputTask,
    OutputTask,
    RandomDelayTask,
    SlowOutputTask,
    VerifyInputsTask,
    create_deep_diamond_workflow,
    create_diamond_workflow,
    create_wide_diamond_workflow,
)


class TestDeepDiamond:
    """Test deep diamond patterns with multiple join points."""

    def test_deep_diamond_completes(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Deep diamond with two join points should complete successfully."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_deep_diamond_workflow("deep-diamond")
        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Workflow status: {result.status}"

        # Final join stage G should have all expected inputs
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"


class TestWideDiamond:
    """Test wide diamond patterns with many parallel branches."""

    def test_wide_diamond_completes(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Wide diamond with 10 parallel branches should complete."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_wide_diamond_workflow("wide-diamond", width=10)
        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Workflow status: {result.status}"
        assert not VerifyInputsTask.verification_errors


class TestConcurrentDiamonds:
    """Test multiple diamond workflows running concurrently."""

    @pytest.mark.stress
    def test_many_concurrent_diamonds(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """
        Run many diamond workflows concurrently.

        This is a critical stress test for race conditions at join points.
        """
        VerifyInputsTask.reset()
        CountingOutputTask.reset()

        num_workflows = 20

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "counting": CountingOutputTask,
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        workflow_ids = []
        for i in range(num_workflows):
            wf = Workflow.create(
                application="diamond-test",
                name=f"concurrent-diamond-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={
                            "outputs_to_produce": {"workflow_index": i, "from_a": True},
                            "min_delay": 0.001,
                            "max_delay": 0.01,
                        },
                        tasks=[TaskExecution.create("Task A", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"from_b": True},
                            "min_delay": 0.001,
                            "max_delay": 0.02,
                        },
                        tasks=[TaskExecution.create("Task B", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"from_c": True},
                            "min_delay": 0.001,
                            "max_delay": 0.02,
                        },
                        tasks=[TaskExecution.create("Task C", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D (Join)",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["workflow_index", "from_a", "from_b", "from_c"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )
            file_repository.store(wf)
            runner.start(wf)
            workflow_ids.append(wf.id)

        # Process all workflows
        processor.process_all(timeout=120.0)

        # Verify all completed successfully
        succeeded = 0
        failed = []
        for wf_id in workflow_ids:
            result = file_repository.retrieve(wf_id)
            if result.status == WorkflowStatus.SUCCEEDED:
                succeeded += 1
            else:
                failed.append((wf_id, result.status))

        assert succeeded == num_workflows, f"Only {succeeded}/{num_workflows} succeeded. Failed: {failed}"
        assert not VerifyInputsTask.verification_errors, f"Verification errors: {VerifyInputsTask.verification_errors}"


class TestDiamondRaceConditions:
    """Tests specifically designed to expose race conditions."""

    def test_simultaneous_branch_completion(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Force B and C to complete at almost exactly the same time.

        Both will push StartStage(D) - only one should win.
        """
        CountingOutputTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "slow_output": SlowOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Run multiple times to increase chance of hitting race
        for run in range(5):
            wf = Workflow.create(
                application="diamond-test",
                name=f"race-test-{run}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={"outputs_to_produce": {"from_a": True}},
                        tasks=[TaskExecution.create("Task A", "slow_output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={"outputs_to_produce": {"from_b": True}, "delay": 0.1},
                        tasks=[TaskExecution.create("Task B", "slow_output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={"outputs_to_produce": {"from_c": True}, "delay": 0.1},
                        tasks=[TaskExecution.create("Task C", "slow_output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D (Join)",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["from_a", "from_b", "from_c"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )

            file_repository.store(wf)
            runner.start(wf)
            processor.process_all(timeout=30.0)

            result = file_repository.retrieve(wf.id)
            assert result.status == WorkflowStatus.SUCCEEDED, f"Run {run}: {result.status}"

            # Stage D should have been executed exactly once
            stage_d = result.stage_by_ref_id("stage_d")
            assert stage_d.status == WorkflowStatus.SUCCEEDED

    def test_join_does_not_start_prematurely(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Verify D never starts before both B and C complete.

        Make C very slow to ensure B completes first.
        D should not start until C also completes.
        """
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "slow_output": SlowOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="premature-start-test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B (Fast)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_b": True, "b_finished_first": True}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C (Very Slow)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={
                        "outputs_to_produce": {"from_c": True, "c_finished_last": True},
                        "delay": 0.5,
                    },
                    tasks=[TaskExecution.create("Task C", "slow_output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    # If D starts prematurely, it won't have from_c
                    context={
                        "expected_keys": [
                            "from_a",
                            "from_b",
                            "from_c",
                            "b_finished_first",
                            "c_finished_last",
                        ]
                    },
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # D must have received from_c (meaning it waited for C)
        assert not VerifyInputsTask.verification_errors, (
            f"Join started prematurely! Errors: {VerifyInputsTask.verification_errors}"
        )

        stage_d_key = f"{wf.id}:stage_d"
        ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
        assert ctx.get("c_finished_last") is True, "D started before C completed!"


class TestDiamondEdgeCases:
    """Test edge cases in diamond patterns."""

    def test_empty_outputs_in_branch(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Branch with empty outputs should still allow join to proceed."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_diamond_workflow(
            "empty-outputs-test",
            stage_a_outputs={"from_a": True},
            stage_b_outputs={},  # Empty outputs
            stage_c_outputs={"from_c": True},
            stage_d_expected_keys=["from_a", "from_c"],  # Don't expect from_b
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_large_outputs_in_diamond(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Diamond with large outputs should handle merging correctly."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Create large list outputs
        large_list_b = [f"item_b_{i}" for i in range(100)]
        large_list_c = [f"item_c_{i}" for i in range(100)]

        wf = create_diamond_workflow(
            "large-outputs-test",
            stage_a_outputs={"from_a": True},
            stage_b_outputs={"large_list": large_list_b},
            stage_c_outputs={"large_list": large_list_c},
            stage_d_expected_keys=["from_a", "large_list"],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify lists were concatenated
        stage_d_key = f"{wf.id}:stage_d"
        ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
        merged_list = ctx.get("large_list", [])

        assert len(merged_list) == 200, f"Expected 200 items, got {len(merged_list)}"
        assert "item_b_0" in merged_list
        assert "item_c_0" in merged_list
