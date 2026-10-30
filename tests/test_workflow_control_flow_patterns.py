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

import pytest

from stabilize import (
    JoinType,
    SplitType,
    StageExecution,
)
from stabilize.expressions import ExpressionError, evaluate_expression
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.control_flow_support import OutputTask


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
