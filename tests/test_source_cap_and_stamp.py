"""WI-9 slice 9f (A30, A31): no source file over 500 lines, and the gate stamp tracks what the gate runs."""

from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src" / "stabilize"
CAP = 500


def test_every_source_file_is_within_the_line_cap() -> None:
    files = sorted(p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts)
    assert len(files) > 150, f"only {len(files)} source files scanned; the glob is wrong"
    over = {str(p.relative_to(REPO)): sum(1 for _ in p.open()) for p in files if sum(1 for _ in p.open()) > CAP}
    assert not over, f"source files over {CAP} lines: {over}"


def _tree(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    for rel, text in {
        "src/stabilize/a.py": "x = 1\n",
        "tests/test_a.py": "def test() -> None: ...\n",
        "migrations/01M_one.sql": "SELECT 1;\n",
        "audit/redproof/unproven_allowed.txt": "tests/test_a.py::test\tbecause\n",
        "audit/evaluations/probe_x.py": "print('probe')\n",
        "examples/demo.py": "print('demo')\n",
        "pyproject.toml": "[project]\nname = 'x'\n",
        "Makefile": "all:\n",
    }.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return root


@pytest.mark.parametrize(
    "touched",
    [
        "migrations/01M_one.sql",
        "audit/redproof/unproven_allowed.txt",
        "audit/evaluations/probe_x.py",
        "examples/demo.py",
        "pyproject.toml",
        "src/stabilize/a.py",
    ],
)
def test_the_gate_stamp_changes_when_a_tracked_file_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, touched: str
) -> None:
    import importlib

    gate_stamp = importlib.import_module("scripts.gate_stamp")
    root = _tree(tmp_path)
    monkeypatch.setattr(gate_stamp, "PROJECT_ROOT", root)
    before = gate_stamp.fingerprint()
    path = root / touched
    path.write_text(path.read_text() + "-- changed\n")
    after = gate_stamp.fingerprint()
    assert after != before, f"editing {touched} left the gate stamp valid"
