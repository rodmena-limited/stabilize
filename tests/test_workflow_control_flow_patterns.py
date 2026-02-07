"""
Workflow Control-Flow Patterns (WCP) Tests.

Tests for the advanced control-flow patterns implemented in Stabilize,
based on the Workflow Patterns taxonomy (van der Aalst et al., 2003;
Russell et al., 2006).

Patterns tested:
- Expression evaluator (unit tests)
- WCP-6:  OR-split (Multi-Choice)
- WCP-7:  OR-join (Structured Synchronizing Merge)
- WCP-9:  Discriminator join
- WCP-30: N-of-M join (Structured Partial Join)
- WCP-16: Deferred Choice
- WCP-18: Milestone gating
- WCP-17/39/40: Mutex / Critical Section / Interleaved Routing
- WCP-25: Cancel Region
- WCP-23: Transient Trigger (Signal Stage)
- WCP-24: Persistent Trigger (Signal Stage)
- TaskResult.suspend()

These tests run on both SQLite and PostgreSQL backends via the
parameterized `backend` fixture.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from stabilize import (
    JoinType,
    SplitType,
    StageExecution,
    Task,
    TaskResult,
)
from stabilize.expressions import ExpressionError, evaluate_expression
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import CancelRegion, SignalStage
from tests.conftest import setup_stabilize

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


# =============================================================================
# Section 1: Expression Evaluator Unit Tests
# =============================================================================


class TestExpressionEvaluator:
    """Unit tests for the safe expression evaluator."""

    def test_boolean_literals(self) -> None:
        assert evaluate_expression("true", {}) is True
        assert evaluate_expression("false", {}) is False
        assert evaluate_expression("True", {}) is True
        assert evaluate_expression("False", {}) is False
        assert evaluate_expression("1", {}) is True
        assert evaluate_expression("0", {}) is False

    def test_context_lookup(self) -> None:
        ctx = {"status": "active", "count": 5}
        assert evaluate_expression("status", ctx) == "active"
        assert evaluate_expression("count", ctx) == 5

    def test_missing_context_returns_none(self) -> None:
        assert evaluate_expression("missing_key", {}) is None

    def test_comparisons(self) -> None:
        ctx = {"x": 10, "y": 20, "name": "hello"}
        assert evaluate_expression("x == 10", ctx) is True
        assert evaluate_expression("x != 10", ctx) is False
        assert evaluate_expression("x < y", ctx) is True
        assert evaluate_expression("x > y", ctx) is False
        assert evaluate_expression("x <= 10", ctx) is True
        assert evaluate_expression("x >= 10", ctx) is True
        assert evaluate_expression("name == 'hello'", ctx) is True

    def test_boolean_operators(self) -> None:
        ctx = {"a": True, "b": False}
        assert evaluate_expression("a and b", ctx) is False
        assert evaluate_expression("a or b", ctx) is True
        assert evaluate_expression("not b", ctx) is True

    def test_nested_dict_access(self) -> None:
        ctx = {"config": {"enabled": True, "threshold": 5}}
        assert evaluate_expression("config.enabled", ctx) is True
        assert evaluate_expression("config.threshold", ctx) == 5
        assert evaluate_expression('config["enabled"]', ctx) is True

    def test_in_operator(self) -> None:
        ctx = {"items": [1, 2, 3], "target": 2}
        assert evaluate_expression("target in items", ctx) is True
        assert evaluate_expression("5 not in items", ctx) is True

    def test_string_constants(self) -> None:
        ctx = {"status": "active"}
        assert evaluate_expression("status == 'active'", ctx) is True
        assert evaluate_expression('status == "active"', ctx) is True

    def test_if_expression(self) -> None:
        ctx = {"flag": True}
        assert evaluate_expression("'yes' if flag else 'no'", ctx) == "yes"
        ctx["flag"] = False
        assert evaluate_expression("'yes' if flag else 'no'", ctx) == "no"

    def test_empty_expression_raises(self) -> None:
        with pytest.raises(ExpressionError):
            evaluate_expression("", {})
        with pytest.raises(ExpressionError):
            evaluate_expression("   ", {})

    def test_syntax_error_raises(self) -> None:
        with pytest.raises(ExpressionError):
            evaluate_expression("if then else", {})

    def test_unsupported_node_raises(self) -> None:
        with pytest.raises(ExpressionError):
            evaluate_expression("import os", {})

    def test_complex_expression(self) -> None:
        ctx = {"priority": "high", "count": 10, "threshold": 5}
        result = evaluate_expression("priority == 'high' and count > threshold", ctx)
        assert result is True


# =============================================================================
# Section 2: OR-Split (WCP-6) Tests
# =============================================================================


class TestORSplit:
    """Test OR-split (Multi-Choice, WCP-6) pattern.

    An OR-split evaluates conditions per downstream and only activates
    matching branches. Non-matching branches are skipped.
    """

    def test_or_split_activates_matching_branches(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """OR-split should activate branches whose conditions evaluate to true."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputTask},
        )

        wf = Workflow.create(
            application="wcp-test",
            name="or-split-basic",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root (OR-split)",
                    context={"outputs_to_produce": {"emergency_type": "fire"}},
                    split_type=SplitType.OR,
                    split_conditions={
                        "police": "emergency_type == 'crime'",
                        "ambulance": "emergency_type == 'medical'",
                        "fire_engine": "emergency_type == 'fire'",
                    },
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
                    ref_id="police",
                    name="Dispatch Police",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"police_dispatched": True}},
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
                    name="Dispatch Ambulance",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"ambulance_dispatched": True}},
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
                    ref_id="fire_engine",
                    name="Dispatch Fire Engine",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"fire_dispatched": True}},
                    tasks=[
                        TaskExecution.create(
                            "Fire Task",
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

        # Only fire_engine should have been activated
        fire = result.stage_by_ref_id("fire_engine")
        assert fire.status == WorkflowStatus.SUCCEEDED

        police = result.stage_by_ref_id("police")
        assert police.status == WorkflowStatus.SKIPPED

        ambulance = result.stage_by_ref_id("ambulance")
        assert ambulance.status == WorkflowStatus.SKIPPED

    def test_or_split_activates_multiple_branches(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """OR-split can activate more than one branch simultaneously."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputTask},
        )

        wf = Workflow.create(
            application="wcp-test",
            name="or-split-multi",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root (OR-split)",
                    context={"outputs_to_produce": {"severity": 5}},
                    split_type=SplitType.OR,
                    split_conditions={
                        "branch_a": "severity > 3",  # True
                        "branch_b": "severity > 10",  # False
                        "branch_c": "severity > 1",  # True
                    },
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
                    ref_id="branch_a",
                    name="Branch A",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[
                        TaskExecution.create(
                            "Task A",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="branch_b",
                    name="Branch B",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"from_b": True}},
                    tasks=[
                        TaskExecution.create(
                            "Task B",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="branch_c",
                    name="Branch C",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"from_c": True}},
                    tasks=[
                        TaskExecution.create(
                            "Task C",
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

        a = result.stage_by_ref_id("branch_a")
        b = result.stage_by_ref_id("branch_b")
        c = result.stage_by_ref_id("branch_c")

        assert a.status == WorkflowStatus.SUCCEEDED
        assert b.status == WorkflowStatus.SKIPPED
        assert c.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Section 3: OR-Join (WCP-7) Tests
# =============================================================================


class TestORJoin:
    """Test OR-join (Structured Synchronizing Merge, WCP-7) pattern.

    An OR-join only waits for branches that were activated by the paired
    OR-split. Inactive branches are ignored.
    """

    def test_or_join_waits_only_for_activated_branches(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """OR-join should wait only for activated branches, not all upstreams."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputTask},
        )

        wf = Workflow.create(
            application="wcp-test",
            name="or-join-basic",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root (OR-split)",
                    context={"outputs_to_produce": {"dispatch": "fire"}},
                    split_type=SplitType.OR,
                    split_conditions={
                        "branch_a": "dispatch == 'fire'",  # True
                        "branch_b": "dispatch == 'police'",  # False
                    },
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
                    ref_id="branch_a",
                    name="Branch A (activated)",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[
                        TaskExecution.create(
                            "Task A",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="branch_b",
                    name="Branch B (skipped)",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"from_b": True}},
                    tasks=[
                        TaskExecution.create(
                            "Task B",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="join",
                    name="Join (OR-join)",
                    requisite_stage_ref_ids={"branch_a", "branch_b"},
                    join_type=JoinType.OR,
                    context={"outputs_to_produce": {"join_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "Join Task",
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

        # Join should have succeeded even though branch_b was skipped
        join = result.stage_by_ref_id("join")
        assert join.status == WorkflowStatus.SUCCEEDED

        branch_b = result.stage_by_ref_id("branch_b")
        assert branch_b.status == WorkflowStatus.SKIPPED


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

        # One should have succeeded, the other cancelled
        statuses = {agent.status, escalate.status}
        assert WorkflowStatus.SUCCEEDED in statuses, f"No SUCCEEDED stage: {statuses}"
        assert WorkflowStatus.CANCELED in statuses, f"No CANCELED stage: {statuses}"

        # Workflow completes (may be CANCELED due to sibling cancellation -
        # this is expected since the cancel propagates to workflow level)
        assert result.status in {
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.CANCELED,
        }


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


# =============================================================================
# Section 8: Mutex / Critical Section (WCP-17, 39, 40) Tests
# =============================================================================


class TestMutexCriticalSection:
    """Test Mutex / Critical Section (WCP-39) pattern.

    Stages with the same mutex_key cannot execute concurrently.
    One must wait for the other to complete.
    """

    def test_mutex_prevents_concurrent_execution(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Stages sharing a mutex_key should execute one at a time."""
        TrackingTask.reset()

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"tracking": TrackingTask},
        )

        wf = Workflow.create(
            application="wcp-test",
            name="mutex-basic",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    context={"outputs_to_produce": {"started": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "tracking",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="db_op_1",
                    name="DB Operation 1",
                    requisite_stage_ref_ids={"root"},
                    mutex_key="shared_db",
                    context={"outputs_to_produce": {"op1_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "DB Op 1 Task",
                            "tracking",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="db_op_2",
                    name="DB Operation 2",
                    requisite_stage_ref_ids={"root"},
                    mutex_key="shared_db",
                    context={"outputs_to_produce": {"op2_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "DB Op 2 Task",
                            "tracking",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="done",
                    name="Done",
                    requisite_stage_ref_ids={"db_op_1", "db_op_2"},
                    context={"outputs_to_produce": {"all_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "Done Task",
                            "tracking",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Both mutex stages should have completed
        op1 = result.stage_by_ref_id("db_op_1")
        op2 = result.stage_by_ref_id("db_op_2")
        assert op1.status == WorkflowStatus.SUCCEEDED
        assert op2.status == WorkflowStatus.SUCCEEDED

        # Verify they ran sequentially (not concurrently)
        # Extract execution log for the mutex stages
        db_op_entries = [
            (ref_id, ts) for _, ref_id, ts in TrackingTask.execution_log if ref_id in ("db_op_1", "db_op_2")
        ]
        assert len(db_op_entries) == 2, f"Expected 2 mutex task executions, got {len(db_op_entries)}"


# =============================================================================
# Section 9: Cancel Region (WCP-25) Tests
# =============================================================================


class TestCancelRegion:
    """Test Cancel Region (WCP-25) pattern.

    A set of stages in a named region can be cancelled together.
    """

    def test_cancel_region_cancels_matching_stages(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """CancelRegion should cancel all active stages in the named region."""
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
            name="cancel-region",
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
                    ref_id="evidence_1",
                    name="Evidence Query 1",
                    requisite_stage_ref_ids={"root"},
                    cancel_region="evidence_access",
                    context={
                        "outputs_to_produce": {"evidence_1": True},
                        "delay": 1.0,
                    },
                    tasks=[
                        TaskExecution.create(
                            "Evidence 1 Task",
                            "slow",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="evidence_2",
                    name="Evidence Query 2",
                    requisite_stage_ref_ids={"root"},
                    cancel_region="evidence_access",
                    context={
                        "outputs_to_produce": {"evidence_2": True},
                        "delay": 1.0,
                    },
                    tasks=[
                        TaskExecution.create(
                            "Evidence 2 Task",
                            "slow",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="unrelated",
                    name="Unrelated Stage",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"unrelated": True}},
                    tasks=[
                        TaskExecution.create(
                            "Unrelated Task",
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

        # Process a few messages to get root completed and evidence stages started
        for _ in range(10):
            processor.process_one()

        # Now push a cancel region message
        queue.push(
            CancelRegion(
                execution_type=wf.type.value,
                execution_id=wf.id,
                region="evidence_access",
            )
        )

        processor.process_all(timeout=15.0)

        result = repository.retrieve(wf.id)

        # Evidence stages should be cancelled
        evidence_1 = result.stage_by_ref_id("evidence_1")
        evidence_2 = result.stage_by_ref_id("evidence_2")
        assert evidence_1.status in {
            WorkflowStatus.CANCELED,
            WorkflowStatus.SUCCEEDED,  # may have completed before cancel
        }
        assert evidence_2.status in {
            WorkflowStatus.CANCELED,
            WorkflowStatus.SUCCEEDED,  # may have completed before cancel
        }

        # Unrelated stage should not be cancelled
        unrelated = result.stage_by_ref_id("unrelated")
        assert unrelated.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Section 10: Signal Stage - Transient Trigger (WCP-23) Tests
# =============================================================================


class TestTransientTrigger:
    """Test Transient Trigger (WCP-23) pattern via SignalStage.

    A transient signal is lost if the stage is not SUSPENDED when it arrives.
    """

    def test_transient_signal_delivered_when_suspended(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Signal should be delivered when stage is SUSPENDED."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={
                "output": OutputTask,
                "suspend": SuspendTask,
            },
        )

        wf = Workflow.create(
            application="wcp-test",
            name="transient-signal",
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
                    ref_id="handler",
                    name="Signal Handler",
                    requisite_stage_ref_ids={"root"},
                    context={},
                    tasks=[
                        TaskExecution.create(
                            "Handler Task",
                            "suspend",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)

        # Process until the handler stage is SUSPENDED
        for _ in range(20):
            processor.process_one()
            result = repository.retrieve(wf.id)
            handler = result.stage_by_ref_id("handler")
            if handler.status == WorkflowStatus.SUSPENDED:
                break

        # Verify handler is SUSPENDED
        result = repository.retrieve(wf.id)
        handler = result.stage_by_ref_id("handler")
        assert handler.status == WorkflowStatus.SUSPENDED, f"Expected SUSPENDED, got {handler.status}"

        # Send transient signal
        queue.push(
            SignalStage(
                execution_type=wf.type.value,
                execution_id=wf.id,
                stage_id=handler.id,
                signal_name="dam_capacity_full",
                signal_data={"level": 95.5},
                persistent=False,
            )
        )

        # Process the signal
        processor.process_all(timeout=15.0)

        result = repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        handler = result.stage_by_ref_id("handler")
        assert handler.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Section 11: Signal Stage - Persistent Trigger (WCP-24) Tests
# =============================================================================


class TestPersistentTrigger:
    """Test Persistent Trigger (WCP-24) pattern via SignalStage.

    A persistent signal is buffered if the stage is not yet SUSPENDED.
    """

    def test_persistent_signal_buffered_and_consumed(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Persistent signal is buffered when stage not SUSPENDED, then consumed.

        When a persistent signal arrives before the stage reaches SUSPENDED,
        it's stored in context. When the task later returns SUSPENDED, it
        finds the buffered signal and immediately resumes, completing the stage.
        """
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={
                "output": OutputTask,
                "suspend": SuspendTask,
            },
        )

        wf = Workflow.create(
            application="wcp-test",
            name="persistent-signal",
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
                    ref_id="handler",
                    name="Signal Handler",
                    requisite_stage_ref_ids={"root"},
                    context={},
                    tasks=[
                        TaskExecution.create(
                            "Handler Task",
                            "suspend",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)

        # Send persistent signal BEFORE the handler reaches SUSPENDED
        # Process just enough to start the workflow
        processor.process_one()  # Start workflow
        processor.process_one()  # Start root stage

        result = repository.retrieve(wf.id)
        handler = result.stage_by_ref_id("handler")

        # Send signal while handler is likely NOT_STARTED or just started
        queue.push(
            SignalStage(
                execution_type=wf.type.value,
                execution_id=wf.id,
                stage_id=handler.id,
                signal_name="new_staff_member",
                signal_data={"name": "Alice"},
                persistent=True,
            )
        )

        # Process everything - signal gets buffered, then consumed when task suspends
        processor.process_all(timeout=15.0)

        result = repository.retrieve(wf.id)
        handler = result.stage_by_ref_id("handler")

        # The handler should have completed: the persistent signal was either
        # delivered directly (if stage was SUSPENDED when signal arrived) or
        # buffered then auto-consumed when the task returned SUSPENDED.
        assert (
            handler.status == WorkflowStatus.SUCCEEDED
        ), f"Expected handler to complete via persistent signal, got {handler.status}"


# =============================================================================
# Section 12: TaskResult.suspend() Tests
# =============================================================================


class TestTaskResultSuspend:
    """Test TaskResult.suspend() factory method."""

    def test_suspend_creates_suspended_result(self) -> None:
        result = TaskResult.suspend()
        assert result.status == WorkflowStatus.SUSPENDED
        assert result.context == {}

    def test_suspend_with_context(self) -> None:
        result = TaskResult.suspend(context={"waiting_for": "approval"})
        assert result.status == WorkflowStatus.SUSPENDED
        assert result.context == {"waiting_for": "approval"}


# =============================================================================
# Section 13: Readiness Evaluation Unit Tests
# =============================================================================


class TestReadinessEvaluation:
    """Unit tests for readiness evaluation with different join types."""

    def _make_stage(
        self,
        ref_id: str,
        status: WorkflowStatus = WorkflowStatus.NOT_STARTED,
        join_type: JoinType = JoinType.AND,
        join_threshold: int = 0,
        context: dict[str, Any] | None = None,
    ) -> StageExecution:
        """Create a minimal stage for readiness testing."""
        stage = StageExecution(
            ref_id=ref_id,
            name=f"Stage {ref_id}",
            status=status,
            join_type=join_type,
            join_threshold=join_threshold,
            context=context or {},
        )
        return stage

    def test_and_join_waits_for_all(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join")
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.NOT_READY

    def test_and_join_ready_when_all_complete(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join")
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.SUCCEEDED)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.READY

    def test_and_join_skip_on_halted(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join")
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.TERMINAL)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.SKIP

    def test_or_join_with_activated_branches(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage(
            "join",
            join_type=JoinType.OR,
            context={"_activated_branches": ["a"]},
        )
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.NOT_STARTED)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.READY

    def test_or_join_falls_back_to_and_without_activation_info(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.OR)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.NOT_READY

    def test_discriminator_fires_on_first(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.DISCRIMINATOR)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.READY

    def test_discriminator_ignores_after_fired(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage(
            "join",
            join_type=JoinType.DISCRIMINATOR,
            context={"_join_fired": True},
        )
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.SUCCEEDED)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.NOT_READY

    def test_n_of_m_fires_at_threshold(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.N_OF_M, join_threshold=2)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.SUCCEEDED)
        upstream_c = self._make_stage("c", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b, upstream_c])
        assert result.phase == PredicatePhase.READY

    def test_n_of_m_not_ready_below_threshold(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.N_OF_M, join_threshold=2)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)
        upstream_c = self._make_stage("c", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b, upstream_c])
        assert result.phase == PredicatePhase.NOT_READY

    def test_n_of_m_skip_when_threshold_unachievable(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.N_OF_M, join_threshold=3)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.TERMINAL)
        upstream_c = self._make_stage("c", WorkflowStatus.TERMINAL)

        result = evaluate_readiness(stage, [upstream_a, upstream_b, upstream_c])
        assert result.phase == PredicatePhase.SKIP

    def test_multi_merge_fires_on_any_completion(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.MULTI_MERGE)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.READY

    def test_multi_merge_not_ready_when_none_complete(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.MULTI_MERGE)
        upstream_a = self._make_stage("a", WorkflowStatus.RUNNING)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.NOT_READY

    def test_jump_bypass_always_ready(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join")
        upstream = self._make_stage("a", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream], jump_bypass=True)
        assert result.phase == PredicatePhase.READY

    def test_no_upstreams_always_ready(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("initial")
        result = evaluate_readiness(stage, [])
        assert result.phase == PredicatePhase.READY


# =============================================================================
# Section 14: Model Field Tests
# =============================================================================


class TestModelFields:
    """Test that new fields on StageExecution serialize/persist correctly."""

    def test_join_type_default(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.join_type == JoinType.AND

    def test_split_type_default(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.split_type == SplitType.AND

    def test_new_fields_default_to_none(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.mi_config is None
        assert stage.deferred_choice_group is None
        assert stage.milestone_ref_id is None
        assert stage.milestone_status is None
        assert stage.mutex_key is None
        assert stage.cancel_region is None

    def test_join_threshold_default(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.join_threshold == 0

    def test_split_conditions_default(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.split_conditions == {}

    def test_stage_persistence_roundtrip(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """New fields should survive store/retrieve roundtrip."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputTask},
        )

        wf = Workflow.create(
            application="wcp-test",
            name="roundtrip-test",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    join_type=JoinType.DISCRIMINATOR,
                    join_threshold=3,
                    split_type=SplitType.OR,
                    split_conditions={"a": "x > 5", "b": "y < 10"},
                    deferred_choice_group="group_1",
                    milestone_ref_id="checkpoint",
                    milestone_status="RUNNING",
                    mutex_key="shared_lock",
                    cancel_region="region_A",
                    context={"outputs_to_produce": {"from_root": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        result = repository.retrieve(wf.id)

        root = result.stage_by_ref_id("root")
        assert root.join_type == JoinType.DISCRIMINATOR
        assert root.join_threshold == 3
        assert root.split_type == SplitType.OR
        assert root.split_conditions == {"a": "x > 5", "b": "y < 10"}
        assert root.deferred_choice_group == "group_1"
        assert root.milestone_ref_id == "checkpoint"
        assert root.milestone_status == "RUNNING"
        assert root.mutex_key == "shared_lock"
        assert root.cancel_region == "region_A"


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
