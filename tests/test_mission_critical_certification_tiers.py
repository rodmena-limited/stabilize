from __future__ import annotations

from stabilize import (
    Workflow,
    WorkflowStatus,
)
from tests.certification_support import _get_stage, _http_stage, _make_infra, _run, _shell_stage

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
        error = t.context.get("error", "") or t.context.get("exception", {}).get("details", {}).get("error", "")
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
        command = r"""printf '{"key": "value", "quote": "he said \"hello\"", "unicode": "\u2603"}'"""

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
