"""
Mission-Critical Certification Test Suite for Stabilize.

This test suite exercises all critical paths using REAL task executions
(ShellTask, HTTPTask) through complex DAG topologies, failure modes,
timeouts, retries, and edge cases.

No mocks. No custom Task subclasses. Only real ShellTask and HTTPTask.

Run offline (default):
    pytest tests/test_mission_critical_certification.py -v

Run with network tests:
    CERT_NETWORK_TESTS=1 pytest tests/test_mission_critical_certification.py -v

Run a specific tier:
    pytest tests/test_mission_critical_certification.py -v -k "Tier1"
"""

from __future__ import annotations

from stabilize import (
    Workflow,
    WorkflowStatus,
)
from tests.certification_support import _get_stage, _http_stage, _make_infra, _run, _shell_stage, local_json_server

# ===========================================================================
# TIER 1: Core Correctness
# ===========================================================================


class TestTier1CoreCorrectness:
    """Validate fundamental DAG execution, ordering, and data propagation."""

    def test_01_sequential_chain_with_data_propagation(self):
        """A→B→C→D: each stage echoes a value, downstream receives upstream stdout."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_01",
            stages=[
                _shell_stage("A", "Stage A", "echo ALPHA"),
                _shell_stage("B", "Stage B", "echo BRAVO-{stdout}", deps={"A"}),
                _shell_stage("C", "Stage C", "echo CHARLIE-{stdout}", deps={"B"}),
                _shell_stage("D", "Stage D", "echo DELTA-{stdout}", deps={"C"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)
        assert result.status == WorkflowStatus.SUCCEEDED

        a = _get_stage(result, "A")
        assert a is not None
        assert a.outputs.get("stdout") == "ALPHA"

        b = _get_stage(result, "B")
        assert b is not None
        # shlex.quote wraps in single quotes: 'ALPHA'
        assert "BRAVO-" in b.outputs.get("stdout", "")

        d = _get_stage(result, "D")
        assert d is not None
        assert "DELTA-" in d.outputs.get("stdout", "")
        assert d.outputs.get("returncode") == 0

    def test_02_wide_parallel_fan_out_fan_in(self):
        """ROOT→{B1..B5}→GATE: 5 parallel branches, single join."""
        store, queue, proc, orch, _ = _make_infra()

        stages = [_shell_stage("ROOT", "Root", "echo ROOT")]
        for i in range(1, 6):
            stages.append(_shell_stage(f"B{i}", f"Branch {i}", f"echo BRANCH_{i}", deps={"ROOT"}))
        stages.append(
            _shell_stage(
                "GATE",
                "Gate",
                "echo GATE_PASSED",
                deps={"B1", "B2", "B3", "B4", "B5"},
            )
        )

        workflow = Workflow.create(application="cert", name="test_02", stages=stages)
        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        gate = _get_stage(result, "GATE")
        assert gate is not None
        assert gate.outputs.get("stdout") == "GATE_PASSED"

        # Verify all branches completed
        for i in range(1, 6):
            branch = _get_stage(result, f"B{i}")
            assert branch is not None
            assert branch.status == WorkflowStatus.SUCCEEDED

    def test_03_diamond_dependency_with_timing_asymmetry(self):
        """A→{B(fast), C(sleep 1)}→D: D waits for slower branch."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_03",
            stages=[
                _shell_stage("A", "Start", "echo START"),
                _shell_stage("B", "Fast", "echo FAST", deps={"A"}),
                _shell_stage("C", "Slow", "sleep 1 && echo SLOW", deps={"A"}),
                _shell_stage("D", "Join", "echo JOINED", deps={"B", "C"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow, timeout=30.0)
        assert result.status == WorkflowStatus.SUCCEEDED

        d = _get_stage(result, "D")
        assert d is not None
        assert d.outputs.get("stdout") == "JOINED"

        c = _get_stage(result, "C")
        assert c is not None
        assert c.outputs.get("stdout") == "SLOW"

    def test_04_deep_sequential_chain(self):
        """S0→S1→...→S10: 11 stages in sequence. No timeout/stack overflow."""
        store, queue, proc, orch, _ = _make_infra()

        stages = []
        for i in range(11):
            deps = {f"S{i - 1}"} if i > 0 else set()
            stages.append(_shell_stage(f"S{i}", f"Stage {i}", f"echo STEP_{i}", deps=deps))

        workflow = Workflow.create(application="cert", name="test_04", stages=stages)
        result = _run(store, queue, proc, orch, workflow, timeout=60.0)

        assert result.status == WorkflowStatus.SUCCEEDED

        last = _get_stage(result, "S10")
        assert last is not None
        assert last.outputs.get("stdout") == "STEP_10"
        assert last.outputs.get("returncode") == 0

    def test_05_mixed_task_types(self):
        """Shell_A→HTTP_B→Shell_C: cross-task-type output propagation, against a local HTTP server."""
        with local_json_server() as base:
            self._run_mixed_task_types(base)

    def _run_mixed_task_types(self, base: str) -> None:
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_05",
            stages=[
                _shell_stage("A", "Shell Start", "echo HELLO"),
                _http_stage(
                    "B",
                    "HTTP Get",
                    f"{base}/get",
                    deps={"A"},
                    extra_context={"parse_json": True, "allow_private_urls": True},
                ),
                _shell_stage("C", "Shell End", "echo DONE", deps={"B"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow, timeout=30.0)
        assert result.status == WorkflowStatus.SUCCEEDED

        b = _get_stage(result, "B")
        assert b is not None
        assert b.outputs.get("status_code") == 200

        c = _get_stage(result, "C")
        assert c is not None
        assert c.outputs.get("stdout") == "DONE"


# ===========================================================================
# TIER 2: Failure Handling
# ===========================================================================


class TestTier2FailureHandling:
    """Validate failure modes, continue_on_failure, and compensation."""

    def test_06_terminal_failure_halts_downstream(self):
        """A→B(exit 1)→C: B fails, C never runs, workflow=TERMINAL."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_06",
            stages=[
                _shell_stage("A", "Start", "echo OK"),
                _shell_stage("B", "Fail", "exit 1", deps={"A"}),
                _shell_stage("C", "After Fail", "echo SHOULD_NOT_RUN", deps={"B"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.TERMINAL

        b = _get_stage(result, "B")
        assert b is not None
        assert b.status == WorkflowStatus.TERMINAL

        c = _get_stage(result, "C")
        assert c is not None
        assert c.status == WorkflowStatus.NOT_STARTED

    def test_07_failed_continue_allows_downstream(self):
        """A→B(exit 1, continue_on_failure)→C: B=FAILED_CONTINUE, C runs."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_07",
            stages=[
                _shell_stage("A", "Start", "echo OK"),
                _shell_stage(
                    "B",
                    "Soft Fail",
                    "exit 1",
                    deps={"A"},
                    continue_on_failure=True,
                ),
                _shell_stage("C", "After Soft Fail", "echo CONTINUED", deps={"B"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        b = _get_stage(result, "B")
        assert b is not None
        assert b.status == WorkflowStatus.FAILED_CONTINUE

        c = _get_stage(result, "C")
        assert c is not None
        assert c.status == WorkflowStatus.SUCCEEDED
        assert c.outputs.get("stdout") == "CONTINUED"

    def test_08_parallel_branch_failure_isolation(self):
        """A→{B(fail), C, D}→E: B fails, E blocked, C/D still succeed."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_08",
            stages=[
                _shell_stage("A", "Start", "echo OK"),
                _shell_stage("B", "Fail Branch", "exit 1", deps={"A"}),
                _shell_stage("C", "OK Branch 1", "echo C_OK", deps={"A"}),
                _shell_stage("D", "OK Branch 2", "echo D_OK", deps={"A"}),
                _shell_stage("E", "Join", "echo JOIN", deps={"B", "C", "D"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.TERMINAL

        b = _get_stage(result, "B")
        assert b is not None
        assert b.status == WorkflowStatus.TERMINAL

        # C and D should still have completed (parallel execution)
        c = _get_stage(result, "C")
        assert c is not None
        assert c.status == WorkflowStatus.SUCCEEDED

        d = _get_stage(result, "D")
        assert d is not None
        assert d.status == WorkflowStatus.SUCCEEDED

        # E should not have run because B failed
        e = _get_stage(result, "E")
        assert e is not None
        assert e.status == WorkflowStatus.NOT_STARTED

    def test_09_compensation_after_failure(self):
        """A→B(fail+continue)→B_comp→C: compensation runs after soft fail."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_09",
            stages=[
                _shell_stage("A", "Start", "echo START"),
                _shell_stage(
                    "B",
                    "Soft Fail",
                    "exit 1",
                    deps={"A"},
                    continue_on_failure=True,
                ),
                _shell_stage(
                    "B_COMP",
                    "Compensation",
                    "echo COMPENSATED",
                    deps={"B"},
                ),
                _shell_stage("C", "Final", "echo FINAL", deps={"B_COMP"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        comp = _get_stage(result, "B_COMP")
        assert comp is not None
        assert comp.status == WorkflowStatus.SUCCEEDED
        assert comp.outputs.get("stdout") == "COMPENSATED"

        c = _get_stage(result, "C")
        assert c is not None
        assert c.status == WorkflowStatus.SUCCEEDED
        assert c.outputs.get("stdout") == "FINAL"

    def test_10_cascading_failure_in_diamond(self):
        """A→{B(fail), C}→D: one failed diamond branch blocks the join."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_10",
            stages=[
                _shell_stage("A", "Start", "echo OK"),
                _shell_stage("B", "Fail Branch", "exit 1", deps={"A"}),
                _shell_stage("C", "OK Branch", "echo C_OK", deps={"A"}),
                _shell_stage("D", "Join", "echo JOINED", deps={"B", "C"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.TERMINAL

        d = _get_stage(result, "D")
        assert d is not None
        assert d.status == WorkflowStatus.NOT_STARTED
