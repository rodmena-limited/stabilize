"""What red-proof counts as proof (B55): the test's own check, in the call phase, confirmed without the mutant."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[1]
REDPROOF = REPO / "audit" / "redproof"
sys.path.insert(0, str(REDPROOF))

from mutants import Mutant, apply, mutants_for_file  # noqa: E402
from proofs import CRASH_SIGNATURES, assert_baseline_usable, confirm_with_control, proof_kind  # noqa: E402


def _entry(
    outcome: str = "failed",
    phase: str = "call",
    where: str = "tests/test_x.py:12",
    exc: str = "AssertionError",
    kind: str = "assert 1 == 2",
) -> dict[str, str]:
    return {"outcome": outcome, "phase": phase, "where": where, "exc_type": exc, "kind": kind}


def test_only_the_tests_own_check_in_the_call_phase_counts() -> None:
    assert proof_kind(_entry()) == "AssertionError: assert 1 == 2"
    golden = _entry(where="golden_standard_tests/test_g.py:40", exc="Failed", kind="DID NOT RAISE")
    assert proof_kind(golden) is not None
    assert proof_kind(_entry(where="tests/conftest.py:99")) is not None
    assert proof_kind(_entry(phase="setup", outcome="error")) is None, "a setup error is not the test's verdict"
    assert proof_kind(_entry(phase="teardown")) is None
    engine = _entry(where="src/stabilize/handlers/base.py:401", exc="AttributeError")
    assert proof_kind(engine) is None, "an engine crash is not the test's verdict"
    assert proof_kind(_entry(where="src/stabilize/redaction.py:85", exc="ValueError")) is None
    assert proof_kind(_entry(exc="Timeout", kind="Timeout >600s")) is None
    assert proof_kind(_entry(outcome="passed")) is None
    for signature in CRASH_SIGNATURES:
        assert proof_kind(_entry(exc=signature)) is None, f"{signature} inside a test file still reads as a crash"


def test_the_plugin_records_phase_location_and_exception(tmp_path: Path) -> None:
    (tmp_path / "test_sample.py").write_text(
        textwrap.dedent(
            """
            import pytest

            def test_assert():
                assert 1 == 2

            def test_raises_in_helper():
                def helper():
                    raise ValueError("deep")
                helper()

            @pytest.fixture
            def broken():
                raise RuntimeError("fixture down")

            def test_setup_error(broken):
                pass

            def test_passes():
                pass
            """
        )
    )
    out = tmp_path / "results.json"
    env = {**os.environ, "REDPROOF_OUT": str(out), "PYTHONPATH": str(REDPROOF)}
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "test_sample.py", "-q", "-p", "no:cacheprovider", "-p", "redproof_plugin"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 1, proc.stdout + proc.stderr
    results = json.loads(out.read_text())["results"]
    asserted = results["test_sample.py::test_assert"]
    assert asserted["outcome"] == "failed" and asserted["phase"] == "call"
    assert asserted["exc_type"] == "AssertionError" and asserted["where"].startswith("test_sample.py:")
    helper = results["test_sample.py::test_raises_in_helper"]
    assert helper["exc_type"] == "ValueError" and helper["where"].startswith("test_sample.py:")
    setup = results["test_sample.py::test_setup_error"]
    assert setup["outcome"] == "error" and setup["phase"] == "setup" and setup["exc_type"] == "RuntimeError"
    assert results["test_sample.py::test_passes"]["outcome"] == "passed"


def test_a_control_run_without_the_mutant_removes_flaky_reds() -> None:
    calls: list[tuple[str, list[str]]] = []

    def runner(label: str, ids: list[str]) -> dict[str, Any]:
        calls.append((label, ids))
        if label == "control":
            return {"exitstatus": 1, "results": {"t_flaky": _entry(), "t_real": _entry(outcome="passed")}}
        results = {"t_flaky": _entry(), "t_real": _entry(), "t_green": _entry(outcome="passed")}
        return {"exitstatus": 1, "results": results}

    confirmed = confirm_with_control(["t_flaky", "t_real", "t_green"], runner)
    assert confirmed == {"t_real": "AssertionError: assert 1 == 2"}, (
        "a test that also fails without the mutant is not a proof"
    )
    assert [label for label, _ in calls] == ["mutant", "mutant", "control"]
    assert calls[1][1] == ["t_flaky", "t_real"] and calls[2][1] == ["t_flaky", "t_real"]


def test_an_empty_or_killed_baseline_is_a_named_failure() -> None:
    with pytest.raises(SystemExit, match="killed"):
        assert_baseline_usable({"exitstatus": -9, "results": {}})
    with pytest.raises(SystemExit, match="no results"):
        assert_baseline_usable({"exitstatus": 0, "results": {}})
    assert_baseline_usable({"exitstatus": 1, "results": {"t": _entry(outcome="passed")}}) is None


def test_tenant_predicate_drop_keeps_the_parameter(tmp_path: Path) -> None:
    source = textwrap.dedent(
        """
        def pg(cur, tenant):
            cur.execute("SELECT 1 FROM t WHERE id = %(id)s AND stabilize_tenant = %(tenant)s", {"tenant": tenant})

        def lite(conn, tenant):
            conn.execute("DELETE FROM t WHERE stabilize_tenant = ? AND x = ?", (tenant, 1))

        def named(conn, tenant):
            conn.execute("UPDATE t SET a = 1 WHERE s.stabilize_tenant = :tenant", {"tenant": tenant})

        def formatted(cur, table, tenant):
            cur.execute(f"SELECT 1 FROM {table} WHERE stabilize_tenant = %(tenant)s", {"tenant": tenant})
        """
    )
    path = tmp_path / "sqlmod.py"
    path.write_text(source)
    drops = [m for m in mutants_for_file(path, "src/stabilize/sqlmod.py") if m.operator == "tenant-pred-drop"]
    assert len(drops) == 4, [m.key for m in drops]
    mutated = {m.line: apply(source, m) for m in drops}
    texts = "\n".join(mutated.values())
    assert "%(tenant)s::text IS NOT NULL" in texts
    assert "? IS NOT NULL AND x = ?" in texts
    assert ":tenant IS NOT NULL" in texts
    for line, text in mutated.items():
        assert text.count("stabilize_tenant =") == source.count("stabilize_tenant =") - 1, f"line {line}"
    assert "{table} WHERE %(tenant)s::text IS NOT NULL" in mutated[max(mutated)], "the f-string predicate survived"


def test_stable_keys_survive_line_shifts_and_tell_siblings_apart(tmp_path: Path) -> None:
    body = textwrap.dedent(
        """
        class Thing:
            def go(self, a, b):
                if a == b:
                    return 1
                if a == b:
                    return 2
                return None
        """
    )
    first = tmp_path / "a.py"
    first.write_text(body)
    shifted = tmp_path / "b.py"
    shifted.write_text("import os\n\n\n" + body)
    keys_a = {m.stable_key for m in mutants_for_file(first, "src/stabilize/x.py")}
    keys_b = {m.stable_key for m in mutants_for_file(shifted, "src/stabilize/x.py")}
    assert keys_a == keys_b, "a shifted file changed the stable keys"
    assert all("Thing.go" in k for k in keys_a if "cmp-flip" in k)
    flips = sorted(k for k in keys_a if "cmp-flip" in k)
    assert len(flips) == 2 and flips[0] != flips[1], "two identical comparisons need distinct ordinals"


def test_known_proofs_rest_on_engine_mutants_with_stable_keys() -> None:
    rows = [line.split("\t") for line in (REDPROOF / "known_proofs.tsv").read_text().splitlines() if line.strip()]
    assert len(rows) > 1000
    keys = [row[1] for row in rows]
    assert not [k for k in keys if k.startswith("tests/")], "proofs resting on test-helper mutants remain"
    unstable = [k for k in keys if len(k.split(":")) != 5]
    assert unstable == [], f"line-based keys remain: {unstable[:3]}"


def test_the_runner_has_no_helper_mutants_and_measures_subprocesses() -> None:
    fallback = (REDPROOF / "fallback.py").read_text()
    assert "helper_mutants" not in fallback
    coveragerc = (REDPROOF / "coveragerc").read_text()
    assert "patch = subprocess" in coveragerc and "parallel = true" in coveragerc
    assert (REDPROOF / "sitecustomize.py").exists()
    run = (REDPROOF / "run.py").read_text()
    baseline_src = (REDPROOF / "baseline.py").read_text()
    assert "coveragerc" in baseline_src and "combine" in baseline_src
    assert "baseline.run(" in run, "run.py no longer runs the baseline through the sharded module"
    assert isinstance(Mutant, type)


def test_the_baseline_splits_files_across_workers_by_weight() -> None:
    from baseline import shard_files

    files = ["tests/a.py", "tests/b.py", "tests/c.py", "tests/d.py"]
    weight = {"tests/a.py": 10, "tests/b.py": 1, "tests/c.py": 1, "tests/d.py": 1}
    assert shard_files(files, weight, 2) == [["tests/a.py"], ["tests/b.py", "tests/c.py", "tests/d.py"]]
    assert shard_files(files, weight, 1) == [files]
    assert [f for s in shard_files(files, weight, 3) for f in s] and sum(map(len, shard_files(files, weight, 3))) == 4


def test_combining_shard_data_files_keeps_every_test_context(tmp_path: Path) -> None:
    from baseline import combine
    from coverage import CoverageData
    from run import coverage_map

    ws = tmp_path / "ws-0"
    (ws / "src" / "stabilize").mkdir(parents=True)
    measured = str(ws / "src" / "stabilize" / "x.py")
    parts: list[Path] = []
    for i, nodeid in enumerate(["tests/t1.py::test_one", "tests/t2.py::test_two"]):
        part_dir = tmp_path / f"baseline-{i}"
        part_dir.mkdir()
        data = CoverageData(basename=str(part_dir / ".coverage"))
        data.set_context(f"{nodeid}|run")
        data.add_lines({measured: [1, 2 + i]})
        data.write()
        parts.append(part_dir / ".coverage")
    (tmp_path / ".coverage").write_text("stale")
    combine(tmp_path, REDPROOF / "coveragerc", sys.executable, parts)
    cmap = coverage_map(tmp_path, ws)
    assert cmap["src/stabilize/x.py:1"] == ["tests/t1.py::test_one", "tests/t2.py::test_two"]
    assert cmap["src/stabilize/x.py:2"] == ["tests/t1.py::test_one"]
    assert cmap["src/stabilize/x.py:3"] == ["tests/t2.py::test_two"]
