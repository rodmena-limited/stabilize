from __future__ import annotations

import pytest

from stabilize import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.diamond_support import CountingOutputTask, OutputTask, RandomDelayTask, SlowOutputTask, VerifyInputsTask


class TestAggressiveStress:
    """
    Aggressive stress tests designed to expose race conditions and edge cases.

    These tests use extreme concurrency and timing to try to break the system.
    """

    @pytest.mark.stress
    def test_interleaved_diamonds(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """
        Test interleaved diamonds - one branch of diamond 1 feeds into diamond 2.

            A1
           / \\
          B1  C1
           \\   |
            D1 |
               \\|
                E (join from C1 and D1)
               / \\
              F   G
               \\ /
                H
        """
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="interleaved-diamond",
            stages=[
                StageExecution(
                    ref_id="A1",
                    name="A1",
                    context={
                        "outputs_to_produce": {"from_A1": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task A1", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="B1",
                    name="B1",
                    requisite_stage_ref_ids={"A1"},
                    context={
                        "outputs_to_produce": {"from_B1": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task B1", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="C1",
                    name="C1",
                    requisite_stage_ref_ids={"A1"},
                    context={
                        "outputs_to_produce": {"from_C1": True},
                        "min_delay": 0.001,
                        "max_delay": 0.03,
                    },
                    tasks=[TaskExecution.create("Task C1", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="D1",
                    name="D1 (partial join)",
                    requisite_stage_ref_ids={"B1"},
                    context={
                        "outputs_to_produce": {"from_D1": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task D1", "random_delay", stage_start=True, stage_end=True)],
                ),
                # E joins from C1 (bypassing B1->D1 path) and D1
                StageExecution(
                    ref_id="E",
                    name="E (cross-diamond join)",
                    requisite_stage_ref_ids={"C1", "D1"},
                    context={
                        "outputs_to_produce": {"from_E": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task E", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="F",
                    name="F",
                    requisite_stage_ref_ids={"E"},
                    context={
                        "outputs_to_produce": {"from_F": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task F", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="G",
                    name="G",
                    requisite_stage_ref_ids={"E"},
                    context={
                        "outputs_to_produce": {"from_G": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task G", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="H",
                    name="H (final join)",
                    requisite_stage_ref_ids={"F", "G"},
                    context={
                        "expected_keys": [
                            "from_A1",
                            "from_B1",
                            "from_C1",
                            "from_D1",
                            "from_E",
                            "from_F",
                            "from_G",
                        ]
                    },
                    tasks=[TaskExecution.create("Task H", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"

    @pytest.mark.stress
    def test_diamond_with_rapid_fire_messages(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Test that the system handles rapid-fire message processing correctly.

        This is designed to stress test the message deduplication and optimistic
        locking mechanisms.
        """
        CountingOutputTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "counting": CountingOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Create many small diamond workflows
        num_workflows = 30
        workflow_ids = []

        for i in range(num_workflows):
            wf = Workflow.create(
                application="rapid-fire-test",
                name=f"rapid-{i}",
                stages=[
                    StageExecution(
                        ref_id="A",
                        name="A",
                        context={"outputs_to_produce": {"wf": i}},
                        tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="B",
                        name="B",
                        requisite_stage_ref_ids={"A"},
                        context={"outputs_to_produce": {"from_B": True}},
                        tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="C",
                        name="C",
                        requisite_stage_ref_ids={"A"},
                        context={"outputs_to_produce": {"from_C": True}},
                        tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="D",
                        name="D",
                        requisite_stage_ref_ids={"B", "C"},
                        context={"expected_keys": ["wf", "from_B", "from_C"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )
            file_repository.store(wf)
            runner.start(wf)
            workflow_ids.append(wf.id)

        # Process all at once
        processor.process_all(timeout=120.0)

        # Verify all succeeded
        failed = []
        for wf_id in workflow_ids:
            result = file_repository.retrieve(wf_id)
            if result.status != WorkflowStatus.SUCCEEDED:
                failed.append((wf_id, result.status))

        assert not failed, f"Failed workflows: {failed}"
        assert not VerifyInputsTask.verification_errors


class TestDiamondRecovery:
    """Test recovery scenarios in diamond patterns."""

    def test_diamond_resumes_after_partial_completion(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Test that a diamond can resume if processing is interrupted.

        This simulates what happens if the processor stops and restarts mid-workflow.
        """
        VerifyInputsTask.reset()

        processor1, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "slow_output": SlowOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="recovery-test",
            name="interrupt-test",
            stages=[
                StageExecution(
                    ref_id="A",
                    name="A",
                    context={"outputs_to_produce": {"from_A": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="B",
                    name="B",
                    requisite_stage_ref_ids={"A"},
                    context={"outputs_to_produce": {"from_B": True}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="C",
                    name="C",
                    requisite_stage_ref_ids={"A"},
                    context={"outputs_to_produce": {"from_C": True}, "delay": 0.5},
                    tasks=[TaskExecution.create("Task C", "slow_output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="D",
                    name="D",
                    requisite_stage_ref_ids={"B", "C"},
                    context={"expected_keys": ["from_A", "from_B", "from_C"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)

        # Process just a bit (should complete A and start B/C)
        processor1.process_one()
        processor1.process_one()
        processor1.process_one()

        # "Crash" - create new processor
        processor2, _, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "slow_output": SlowOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Continue processing
        processor2.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"
        assert not VerifyInputsTask.verification_errors
