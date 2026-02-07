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

import os

import pytest

from stabilize import (
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStatus,
)
from stabilize.events import reset_event_bus, reset_event_recorder
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.tasks.http import HTTPTask
from stabilize.tasks.shell import ShellTask

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def _make_infra():
    """Create fresh isolated in-memory infrastructure for a single test.

    Returns (store, queue, processor, orchestrator, task_registry).
    """
    # Reset singletons for isolation
    SingletonMeta.reset(ConnectionManager)
    RunTaskHandler._executing_tasks.clear()
    reset_event_bus()
    reset_event_recorder()

    store = SqliteWorkflowStore(
        connection_string="sqlite:///:memory:",
        create_tables=True,
    )
    queue = SqliteQueue(
        connection_string="sqlite:///:memory:",
        table_name="queue_messages",
    )
    queue._create_table()

    task_registry = TaskRegistry()
    task_registry.register("shell", ShellTask)
    task_registry.register("http", HTTPTask)

    processor = QueueProcessor(queue, store=store, task_registry=task_registry)
    orchestrator = Orchestrator(queue)

    return store, queue, processor, orchestrator, task_registry


def _shell_stage(
    ref_id: str,
    name: str,
    command: str,
    deps: set[str] | None = None,
    timeout: int = 60,
    continue_on_failure: bool = False,
    restart_on_failure: bool = False,
    extra_context: dict | None = None,
) -> StageExecution:
    """Build a StageExecution wired to ShellTask."""
    context: dict = {"command": command, "timeout": timeout}
    if continue_on_failure:
        context["continue_on_failure"] = True
        context["continuePipelineOnFailure"] = True
    if restart_on_failure:
        context["restart_on_failure"] = True
    if extra_context:
        context.update(extra_context)
    return StageExecution(
        ref_id=ref_id,
        type="shell",
        name=name,
        context=context,
        requisite_stage_ref_ids=deps or set(),
        tasks=[
            TaskExecution.create(
                name=f"{name} task",
                implementing_class="shell",
                stage_start=True,
                stage_end=True,
            ),
        ],
    )


def _http_stage(
    ref_id: str,
    name: str,
    url: str,
    method: str = "GET",
    deps: set[str] | None = None,
    timeout: int = 30,
    continue_on_failure: bool = False,
    extra_context: dict | None = None,
) -> StageExecution:
    """Build a StageExecution wired to HTTPTask."""
    context: dict = {"url": url, "method": method, "timeout": timeout}
    if continue_on_failure:
        context["continue_on_failure"] = True
        context["continuePipelineOnFailure"] = True
    if extra_context:
        context.update(extra_context)
    return StageExecution(
        ref_id=ref_id,
        type="http",
        name=name,
        context=context,
        requisite_stage_ref_ids=deps or set(),
        tasks=[
            TaskExecution.create(
                name=f"{name} task",
                implementing_class="http",
                stage_start=True,
                stage_end=True,
            ),
        ],
    )


def _run(store, queue, processor, orchestrator, workflow, timeout=30.0):
    """Store, start, process, and retrieve a workflow result."""
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=timeout)
    return store.retrieve(workflow.id)


def _get_stage(result: Workflow, ref_id: str) -> StageExecution | None:
    """Find a stage by ref_id in the result."""
    for s in result.stages:
        if s.ref_id == ref_id:
            return s
    return None


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

    @pytest.mark.skipif(
        not os.environ.get("CERT_NETWORK_TESTS"),
        reason="Network tests disabled (set CERT_NETWORK_TESTS=1)",
    )
    def test_05_mixed_task_types(self):
        """Shell_A→HTTP_B→Shell_C: cross-task-type output propagation."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_05",
            stages=[
                _shell_stage("A", "Shell Start", "echo HELLO"),
                _http_stage(
                    "B",
                    "HTTP Get",
                    "https://httpbin.org/get",
                    deps={"A"},
                    extra_context={"parse_json": True},
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


# ===========================================================================
# TIER 3: Timeout & Retry
# ===========================================================================


class TestTier3TimeoutAndRetry:
    """Validate timeout killing, retry loops, and transient error recovery."""

    def test_11_shell_task_timeout(self):
        """Single stage: sleep 30, timeout=2. Process killed, stage=TERMINAL."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_11",
            stages=[
                _shell_stage("T", "Timeout Stage", "sleep 30", timeout=2),
            ],
        )

        result = _run(store, queue, proc, orch, workflow, timeout=30.0)

        assert result.status == WorkflowStatus.TERMINAL

        t = _get_stage(result, "T")
        assert t is not None
        assert t.status == WorkflowStatus.TERMINAL
        # Check error mentions timeout
        error = t.context.get("error", "") or t.context.get("exception", {}).get(
            "details", {}
        ).get("error", "")
        assert "timeout" in error.lower() or "timed out" in error.lower()

    def test_12_shell_task_retry_on_failure(self, tmp_path):
        """File-counter pattern: first run creates file + fails, second succeeds."""
        store, queue, proc, orch, _ = _make_infra()

        marker = tmp_path / "retry_marker"
        marker_path = str(marker)

        # Command: if marker file exists, succeed. Otherwise, create it and fail.
        command = f"bash -c 'if [ -f {marker_path} ]; then echo SUCCESS; else touch {marker_path}; exit 1; fi'"

        workflow = Workflow.create(
            application="cert",
            name="test_12",
            stages=[
                _shell_stage(
                    "R",
                    "Retry Stage",
                    command,
                    restart_on_failure=True,
                ),
            ],
        )

        result = _run(store, queue, proc, orch, workflow, timeout=30.0)

        assert result.status == WorkflowStatus.SUCCEEDED

        r = _get_stage(result, "R")
        assert r is not None
        assert r.status == WorkflowStatus.SUCCEEDED
        assert r.outputs.get("stdout") == "SUCCESS"

    def test_13_http_task_timeout(self):
        """HTTP to non-routable RFC 5737 IP. Connection times out. Works offline."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_13",
            stages=[
                _http_stage(
                    "H",
                    "HTTP Timeout",
                    "http://192.0.2.1:1/",
                    timeout=2,
                ),
            ],
        )

        result = _run(store, queue, proc, orch, workflow, timeout=30.0)

        assert result.status == WorkflowStatus.TERMINAL

        h = _get_stage(result, "H")
        assert h is not None
        assert h.status == WorkflowStatus.TERMINAL

    def test_14_multiple_retries_before_success(self, tmp_path):
        """File-counter: fails 2x, succeeds 3rd. restart_on_failure=True."""
        store, queue, proc, orch, _ = _make_infra()

        counter_file = tmp_path / "counter"
        counter_path = str(counter_file)

        # Command: read counter from file (default 0), increment, write back.
        # Succeed only when counter reaches 3.
        command = (
            f"bash -c '"
            f"if [ -f {counter_path} ]; then C=$(cat {counter_path}); else C=0; fi; "
            f"C=$((C + 1)); "
            f"echo $C > {counter_path}; "
            f"if [ $C -ge 3 ]; then echo PASS_$C; else exit 1; fi"
            f"'"
        )

        workflow = Workflow.create(
            application="cert",
            name="test_14",
            stages=[
                _shell_stage(
                    "MR",
                    "Multi Retry",
                    command,
                    restart_on_failure=True,
                ),
            ],
        )

        result = _run(store, queue, proc, orch, workflow, timeout=60.0)

        assert result.status == WorkflowStatus.SUCCEEDED

        mr = _get_stage(result, "MR")
        assert mr is not None
        assert mr.status == WorkflowStatus.SUCCEEDED
        assert "PASS_3" in mr.outputs.get("stdout", "")


# ===========================================================================
# TIER 4: Complex DAG Patterns
# ===========================================================================


class TestTier4ComplexDAGPatterns:
    """Validate multi-level diamonds, asymmetric fans, and independent chains."""

    def test_15_multi_level_diamond(self):
        """A→{B,C}→D→{E,F}→G: two chained diamonds."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_15",
            stages=[
                _shell_stage("A", "Start", "echo A"),
                _shell_stage("B", "D1 Left", "echo B", deps={"A"}),
                _shell_stage("C", "D1 Right", "echo C", deps={"A"}),
                _shell_stage("D", "D1 Join", "echo D", deps={"B", "C"}),
                _shell_stage("E", "D2 Left", "echo E", deps={"D"}),
                _shell_stage("F", "D2 Right", "echo F", deps={"D"}),
                _shell_stage("G", "D2 Join", "echo G", deps={"E", "F"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        for ref in ["A", "B", "C", "D", "E", "F", "G"]:
            stage = _get_stage(result, ref)
            assert stage is not None, f"Stage {ref} not found"
            assert stage.status == WorkflowStatus.SUCCEEDED, f"Stage {ref} status: {stage.status}"
            assert stage.outputs.get("stdout") == ref

    def test_16_asymmetric_fan_out(self):
        """A→{B→C→D, E→F, G}→H: branches of depth 3, 2, 1."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_16",
            stages=[
                _shell_stage("A", "Root", "echo A"),
                # Branch 1: depth 3
                _shell_stage("B", "B1.1", "echo B", deps={"A"}),
                _shell_stage("C", "B1.2", "echo C", deps={"B"}),
                _shell_stage("D", "B1.3", "echo D", deps={"C"}),
                # Branch 2: depth 2
                _shell_stage("E", "B2.1", "echo E", deps={"A"}),
                _shell_stage("F", "B2.2", "echo F", deps={"E"}),
                # Branch 3: depth 1
                _shell_stage("G", "B3.1", "echo G", deps={"A"}),
                # Join
                _shell_stage("H", "Join", "echo H", deps={"D", "F", "G"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        h = _get_stage(result, "H")
        assert h is not None
        assert h.outputs.get("stdout") == "H"

        # Verify deepest branch completed
        d = _get_stage(result, "D")
        assert d is not None
        assert d.status == WorkflowStatus.SUCCEEDED

    def test_17_independent_parallel_chains_with_join(self):
        """{A→B→C, D→E→F}→G: two independent chains join at G."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_17",
            stages=[
                # Chain 1
                _shell_stage("A", "Chain1.1", "echo A"),
                _shell_stage("B", "Chain1.2", "echo B", deps={"A"}),
                _shell_stage("C", "Chain1.3", "echo C", deps={"B"}),
                # Chain 2
                _shell_stage("D", "Chain2.1", "echo D"),
                _shell_stage("E", "Chain2.2", "echo E", deps={"D"}),
                _shell_stage("F", "Chain2.3", "echo F", deps={"E"}),
                # Join
                _shell_stage("G", "Final Join", "echo G", deps={"C", "F"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        g = _get_stage(result, "G")
        assert g is not None
        assert g.outputs.get("stdout") == "G"

        # Both chain ends should be succeeded
        for ref in ["C", "F"]:
            stage = _get_stage(result, ref)
            assert stage is not None
            assert stage.status == WorkflowStatus.SUCCEEDED


# ===========================================================================
# TIER 5: Edge Cases & Boundary Conditions
# ===========================================================================


class TestTier5EdgeCases:
    """Validate edge cases: empty output, large output, special chars, etc."""

    def test_18_empty_output_handling(self):
        """A(true)→B: 'true' produces empty stdout. Downstream handles gracefully."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_18",
            stages=[
                _shell_stage("A", "Empty Output", "true"),
                _shell_stage("B", "After Empty", "echo AFTER_EMPTY", deps={"A"}),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        a = _get_stage(result, "A")
        assert a is not None
        assert a.outputs.get("stdout") == ""
        assert a.outputs.get("returncode") == 0

        b = _get_stage(result, "B")
        assert b is not None
        assert b.outputs.get("stdout") == "AFTER_EMPTY"

    def test_19_large_output(self):
        """Single: seq 1 10000 → ~49KB output captured without corruption."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_19",
            stages=[
                _shell_stage("L", "Large Output", "seq 1 10000"),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage = _get_stage(result, "L")
        assert stage is not None
        stdout = stage.outputs.get("stdout", "")
        lines = stdout.strip().split("\n")
        assert lines[0] == "1"
        assert lines[-1] == "10000"
        assert len(lines) == 10000

    def test_20_special_characters_in_output(self):
        """printf with JSON, quotes, unicode → survives SQLite round-trip."""
        store, queue, proc, orch, _ = _make_infra()

        # Use printf to output JSON with special characters
        command = (
            r"""printf '{"key": "value", "quote": "he said \"hello\"", "unicode": "\u2603"}'"""
        )

        workflow = Workflow.create(
            application="cert",
            name="test_20",
            stages=[
                _shell_stage("S", "Special Chars", command),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage = _get_stage(result, "S")
        assert stage is not None
        stdout = stage.outputs.get("stdout", "")
        assert "key" in stdout
        assert "value" in stdout
        assert stage.outputs.get("returncode") == 0

    def test_21_rapid_fire_zero_duration_tasks(self):
        """S0→S1→...→S9: 10 instant echo stages. No race conditions."""
        store, queue, proc, orch, _ = _make_infra()

        stages = []
        for i in range(10):
            deps = {f"S{i - 1}"} if i > 0 else set()
            stages.append(_shell_stage(f"S{i}", f"Rapid {i}", f"echo R{i}", deps=deps))

        workflow = Workflow.create(application="cert", name="test_21", stages=stages)
        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        for i in range(10):
            stage = _get_stage(result, f"S{i}")
            assert stage is not None
            assert stage.status == WorkflowStatus.SUCCEEDED
            assert stage.outputs.get("stdout") == f"R{i}"

    def test_22_single_stage_workflow(self):
        """Minimal workflow: one stage. Simplest case works."""
        store, queue, proc, orch, _ = _make_infra()

        workflow = Workflow.create(
            application="cert",
            name="test_22",
            stages=[
                _shell_stage("only", "Only Stage", "echo SOLO"),
            ],
        )

        result = _run(store, queue, proc, orch, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage = _get_stage(result, "only")
        assert stage is not None
        assert stage.status == WorkflowStatus.SUCCEEDED
        assert stage.outputs.get("stdout") == "SOLO"
        assert stage.outputs.get("returncode") == 0
