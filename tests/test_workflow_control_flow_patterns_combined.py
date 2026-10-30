from __future__ import annotations

from stabilize import (
    JoinType,
    SplitType,
    StageExecution,
)
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.control_flow_support import OutputTask, SlowTask

# =============================================================================
# Section 15: Combined Pattern Tests
# =============================================================================


class TestCombinedPatterns:
    """Test combinations of multiple WCP patterns in a single workflow."""

    def test_or_split_with_or_join_end_to_end(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Full OR-split -> branches -> OR-join pattern."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputTask},
        )

        wf = Workflow.create(
            application="wcp-test",
            name="or-split-or-join",
            stages=[
                StageExecution(
                    ref_id="dispatch",
                    name="Emergency Dispatch",
                    context={
                        "outputs_to_produce": {"emergency": "fire_and_medical"},
                    },
                    split_type=SplitType.OR,
                    split_conditions={
                        "fire": "True",  # Always activate
                        "police": "False",  # Never activate
                        "ambulance": "True",  # Always activate
                    },
                    tasks=[
                        TaskExecution.create(
                            "Dispatch Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="fire",
                    name="Fire Engine",
                    requisite_stage_ref_ids={"dispatch"},
                    context={"outputs_to_produce": {"fire_on_scene": True}},
                    tasks=[
                        TaskExecution.create(
                            "Fire Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="police",
                    name="Police",
                    requisite_stage_ref_ids={"dispatch"},
                    context={"outputs_to_produce": {"police_on_scene": True}},
                    tasks=[
                        TaskExecution.create(
                            "Police Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="ambulance",
                    name="Ambulance",
                    requisite_stage_ref_ids={"dispatch"},
                    context={"outputs_to_produce": {"ambulance_on_scene": True}},
                    tasks=[
                        TaskExecution.create(
                            "Ambulance Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="patient_transfer",
                    name="Patient Transfer",
                    requisite_stage_ref_ids={"fire", "police", "ambulance"},
                    join_type=JoinType.OR,
                    context={"outputs_to_produce": {"transfer_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "Transfer Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Fire and ambulance should have run
        assert result.stage_by_ref_id("fire").status == WorkflowStatus.SUCCEEDED
        assert result.stage_by_ref_id("ambulance").status == WorkflowStatus.SUCCEEDED

        # Police should be skipped
        assert result.stage_by_ref_id("police").status == WorkflowStatus.SKIPPED

        # OR-join should have completed (waited only for fire + ambulance)
        assert result.stage_by_ref_id("patient_transfer").status == WorkflowStatus.SUCCEEDED

    def test_sequence_with_and_split_and_n_of_m_join(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Sequence -> AND-split -> 3 reviewers -> N-of-M(2) join -> result."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={
                "output": OutputTask,
                "slow": SlowTask,
            },
        )

        wf = Workflow.create(
            application="wcp-test",
            name="review-pipeline",
            stages=[
                StageExecution(
                    ref_id="prepare",
                    name="Prepare Document",
                    context={"outputs_to_produce": {"document": "ready"}},
                    tasks=[
                        TaskExecution.create(
                            "Prepare Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="r1",
                    name="Reviewer 1",
                    requisite_stage_ref_ids={"prepare"},
                    context={"outputs_to_produce": {"r1": "approved"}},
                    tasks=[
                        TaskExecution.create(
                            "R1 Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="r2",
                    name="Reviewer 2",
                    requisite_stage_ref_ids={"prepare"},
                    context={"outputs_to_produce": {"r2": "approved"}},
                    tasks=[
                        TaskExecution.create(
                            "R2 Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="r3",
                    name="Reviewer 3 (slow)",
                    requisite_stage_ref_ids={"prepare"},
                    context={
                        "outputs_to_produce": {"r3": "approved"},
                        "delay": 0.3,
                    },
                    tasks=[
                        TaskExecution.create(
                            "R3 Task",
                            "slow",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="decision",
                    name="Approval Decision",
                    requisite_stage_ref_ids={"r1", "r2", "r3"},
                    join_type=JoinType.N_OF_M,
                    join_threshold=2,
                    context={"outputs_to_produce": {"approved": True}},
                    tasks=[
                        TaskExecution.create(
                            "Decision Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="publish",
                    name="Publish",
                    requisite_stage_ref_ids={"decision"},
                    context={"outputs_to_produce": {"published": True}},
                    tasks=[
                        TaskExecution.create(
                            "Publish Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Decision should have fired after 2 reviewers
        decision = result.stage_by_ref_id("decision")
        assert decision.status == WorkflowStatus.SUCCEEDED

        # Publish should have completed
        publish = result.stage_by_ref_id("publish")
        assert publish.status == WorkflowStatus.SUCCEEDED
