from __future__ import annotations

from stabilize import (
    JoinType,
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
# Section 4: Discriminator Join (WCP-9) Tests
# =============================================================================


class TestDiscriminatorJoin:
    """Test Discriminator join (WCP-9) pattern.

    The discriminator fires when the FIRST upstream completes.
    Subsequent completions are ignored.
    """

    def test_discriminator_fires_on_first_completion(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Discriminator should fire as soon as first upstream completes."""
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
            name="discriminator-basic",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    context={"outputs_to_produce": {"started": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="fast_branch",
                    name="Fast Branch",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"from_fast": True}},
                    tasks=[
                        TaskExecution.create(
                            "Fast Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="slow_branch",
                    name="Slow Branch",
                    requisite_stage_ref_ids={"root"},
                    context={
                        "outputs_to_produce": {"from_slow": True},
                        "delay": 0.3,
                    },
                    tasks=[
                        TaskExecution.create(
                            "Slow Task",
                            "slow",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="triage",
                    name="Triage (Discriminator)",
                    requisite_stage_ref_ids={"fast_branch", "slow_branch"},
                    join_type=JoinType.DISCRIMINATOR,
                    context={"outputs_to_produce": {"triage_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "Triage Task",
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
        # Workflow should succeed
        assert result.status == WorkflowStatus.SUCCEEDED

        triage = result.stage_by_ref_id("triage")
        assert triage.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Section 5: N-of-M Join (WCP-30) Tests
# =============================================================================


class TestNofMJoin:
    """Test N-of-M join (Structured Partial Join, WCP-30) pattern.

    The join fires when N out of M upstreams have completed.
    """

    def test_n_of_m_fires_at_threshold(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """N-of-M join should fire when threshold is reached."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={
                "output": OutputTask,
                "slow": SlowTask,
            },
        )

        # 3 branches, threshold = 2 (fire when 2 of 3 complete)
        wf = Workflow.create(
            application="wcp-test",
            name="n-of-m-basic",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    context={"outputs_to_produce": {"started": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="reviewer_1",
                    name="Reviewer 1",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"review_1": "approved"}},
                    tasks=[
                        TaskExecution.create(
                            "Review 1",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="reviewer_2",
                    name="Reviewer 2",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"review_2": "approved"}},
                    tasks=[
                        TaskExecution.create(
                            "Review 2",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="reviewer_3",
                    name="Reviewer 3 (slow)",
                    requisite_stage_ref_ids={"root"},
                    context={
                        "outputs_to_produce": {"review_3": "approved"},
                        "delay": 0.3,
                    },
                    tasks=[
                        TaskExecution.create(
                            "Review 3",
                            "slow",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="decision",
                    name="Decision (2 of 3)",
                    requisite_stage_ref_ids={"reviewer_1", "reviewer_2", "reviewer_3"},
                    join_type=JoinType.N_OF_M,
                    join_threshold=2,
                    context={"outputs_to_produce": {"decision": "proceed"}},
                    tasks=[
                        TaskExecution.create(
                            "Decision Task",
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

        decision = result.stage_by_ref_id("decision")
        assert decision.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Section 6: Deferred Choice (WCP-16) Tests
# =============================================================================


class TestDeferredChoice:
    """Test Deferred Choice (WCP-16) pattern.

    Multiple branches are enabled simultaneously. The first to actually
    start execution cancels the others. It's a race condition resolved
    by the environment.
    """

    def test_deferred_choice_cancels_siblings(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """First branch to start should cancel siblings in the same group.

        In a deferred choice, the winning branch's sibling is cancelled.
        We verify the cancellation behavior at the stage level.
        """
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
            name="deferred-choice",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    context={"outputs_to_produce": {"complaint": "received"}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="agent_contact",
                    name="Agent Contact",
                    requisite_stage_ref_ids={"root"},
                    deferred_choice_group="handle_complaint",
                    context={"outputs_to_produce": {"action": "agent_handled"}},
                    tasks=[
                        TaskExecution.create(
                            "Agent Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="escalate",
                    name="Escalate to Manager",
                    requisite_stage_ref_ids={"root"},
                    deferred_choice_group="handle_complaint",
                    context={
                        "outputs_to_produce": {"action": "escalated"},
                        "delay": 0.3,
                    },
                    tasks=[
                        TaskExecution.create(
                            "Escalate Task",
                            "slow",
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

        agent = result.stage_by_ref_id("agent_contact")
        escalate = result.stage_by_ref_id("escalate")

        statuses = {agent.status, escalate.status}
        assert WorkflowStatus.SUCCEEDED in statuses, f"No SUCCEEDED stage: {statuses}"
        assert WorkflowStatus.SKIPPED in statuses, f"the loser was not pruned: {statuses}"
        loser = agent if agent.status == WorkflowStatus.SKIPPED else escalate
        assert loser.context.get("_pruned") is True
        assert result.status == WorkflowStatus.SUCCEEDED, result.status


# =============================================================================
# Section 7: Milestone Gating (WCP-18) Tests
# =============================================================================


class TestMilestoneGating:
    """Test Milestone (WCP-18) pattern.

    An activity is only enabled when a milestone stage is in a specific
    status. If the milestone has already progressed past that status,
    the activity is skipped.
    """

    def test_milestone_allows_when_in_required_status(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Stage should proceed when milestone is in the required status."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputTask},
        )

        # Milestone stage runs in parallel and is RUNNING when gated stage tries to start
        wf = Workflow.create(
            application="wcp-test",
            name="milestone-allow",
            stages=[
                StageExecution(
                    ref_id="start",
                    name="Start",
                    context={"outputs_to_produce": {"started": True}},
                    tasks=[
                        TaskExecution.create(
                            "Start Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="ticket",
                    name="Ticket Processing",
                    requisite_stage_ref_ids={"start"},
                    context={"outputs_to_produce": {"ticket": "issued"}},
                    tasks=[
                        TaskExecution.create(
                            "Ticket Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="route_change",
                    name="Route Change",
                    requisite_stage_ref_ids={"start"},
                    milestone_ref_id="ticket",
                    milestone_status="NOT_STARTED",
                    context={"outputs_to_produce": {"route_changed": True}},
                    tasks=[
                        TaskExecution.create(
                            "Route Change Task",
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
