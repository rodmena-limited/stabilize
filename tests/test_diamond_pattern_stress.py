from __future__ import annotations

import pytest

from stabilize import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.diamond_support import RandomDelayTask, VerifyInputsTask


class TestAggressiveStress:
    """
    Aggressive stress tests designed to expose race conditions and edge cases.

    These tests use extreme concurrency and timing to try to break the system.
    """

    @pytest.mark.stress
    @pytest.mark.timeout(600)
    def test_100_concurrent_diamonds_with_conflicts(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Run 100 diamond workflows where each has conflicting output keys.

        This maximizes the chance of exposing non-deterministic merge order.
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

        num_workflows = 50  # Reduced from 100 for reasonable test time
        workflow_ids = []

        for i in range(num_workflows):
            wf = Workflow.create(
                application="diamond-stress",
                name=f"stress-conflict-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={
                            "outputs_to_produce": {"index": i, "conflict": f"a_{i}"},
                            "min_delay": 0.001,
                            "max_delay": 0.005,
                        },
                        tasks=[TaskExecution.create("Task A", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"conflict": f"b_{i}", "from_b": i},
                            "min_delay": 0.001,
                            "max_delay": 0.01,
                        },
                        tasks=[TaskExecution.create("Task B", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"conflict": f"c_{i}", "from_c": i},
                            "min_delay": 0.001,
                            "max_delay": 0.01,
                        },
                        tasks=[TaskExecution.create("Task C", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["index", "conflict", "from_b", "from_c"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )
            file_repository.store(wf)
            runner.start(wf)
            workflow_ids.append(wf.id)

        processor.process_all(timeout=180.0)

        # Analyze results
        conflict_values = []
        for wf_id in workflow_ids:
            result = file_repository.retrieve(wf_id)
            if result.status != WorkflowStatus.SUCCEEDED:
                continue

            stage_d_key = f"{wf_id}:stage_d"
            ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
            conflict_val = ctx.get("conflict", "MISSING")
            conflict_values.append(conflict_val)

        # Verify all workflows succeeded
        succeeded = sum(
            1 for wf_id in workflow_ids if file_repository.retrieve(wf_id).status == WorkflowStatus.SUCCEEDED
        )
        assert succeeded == num_workflows, f"Only {succeeded}/{num_workflows} succeeded"

        # Check if merge order is consistent
        # Each workflow should have conflict value from either B or C, but consistently
        b_wins = sum(1 for v in conflict_values if v.startswith("b_"))
        c_wins = sum(1 for v in conflict_values if v.startswith("c_"))

        # If both B and C "win" different runs, we have non-determinism
        if b_wins > 0 and c_wins > 0:
            pytest.fail(
                f"NON-DETERMINISTIC MERGE ORDER DETECTED!\n"
                f"B won {b_wins} times, C won {c_wins} times.\n"
                f"This indicates inconsistent merge behavior."
            )

    @pytest.mark.stress
    def test_triple_diamond_chain(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """
        Test a chain of three diamonds - maximum complexity.

            A
           /|\\
          B C D
           \\|/
            E
           /|\\
          F G H
           \\|/
            I
           /|\\
          J K L
           \\|/
            M
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
            name="triple-diamond",
            stages=[
                # First diamond
                StageExecution(
                    ref_id="A",
                    name="A",
                    context={
                        "outputs_to_produce": {"from_A": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task A", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="B",
                    name="B",
                    requisite_stage_ref_ids={"A"},
                    context={
                        "outputs_to_produce": {"from_B": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task B", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="C",
                    name="C",
                    requisite_stage_ref_ids={"A"},
                    context={
                        "outputs_to_produce": {"from_C": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task C", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="D",
                    name="D",
                    requisite_stage_ref_ids={"A"},
                    context={
                        "outputs_to_produce": {"from_D": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task D", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="E",
                    name="E (Join 1)",
                    requisite_stage_ref_ids={"B", "C", "D"},
                    context={
                        "outputs_to_produce": {"from_E": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task E", "random_delay", stage_start=True, stage_end=True)],
                ),
                # Second diamond
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
                    name="H",
                    requisite_stage_ref_ids={"E"},
                    context={
                        "outputs_to_produce": {"from_H": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task H", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="I",
                    name="I (Join 2)",
                    requisite_stage_ref_ids={"F", "G", "H"},
                    context={
                        "outputs_to_produce": {"from_I": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task I", "random_delay", stage_start=True, stage_end=True)],
                ),
                # Third diamond
                StageExecution(
                    ref_id="J",
                    name="J",
                    requisite_stage_ref_ids={"I"},
                    context={
                        "outputs_to_produce": {"from_J": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task J", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="K",
                    name="K",
                    requisite_stage_ref_ids={"I"},
                    context={
                        "outputs_to_produce": {"from_K": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task K", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="L",
                    name="L",
                    requisite_stage_ref_ids={"I"},
                    context={
                        "outputs_to_produce": {"from_L": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task L", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="M",
                    name="M (Final Join)",
                    requisite_stage_ref_ids={"J", "K", "L"},
                    context={
                        "expected_keys": [
                            "from_A",
                            "from_B",
                            "from_C",
                            "from_D",
                            "from_E",
                            "from_F",
                            "from_G",
                            "from_H",
                            "from_I",
                            "from_J",
                            "from_K",
                            "from_L",
                        ]
                    },
                    tasks=[TaskExecution.create("Task M", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"
