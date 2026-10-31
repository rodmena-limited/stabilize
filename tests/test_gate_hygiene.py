"""The gate's own hygiene: databases are dropped only on a gate cluster, and an old runner imports its own stabilize."""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path
from typing import Any

import pytest

from tests.mg_support import assert_own_runner, import_location, old_stabilize, require_gate_cluster, runner_env

REPO = Path(__file__).resolve().parents[1]


class _FakeConnection:
    def __init__(self, cluster: str) -> None:
        self.cluster = cluster
        self.statements: list[str] = []

    def execute(self, sql: str, *args: Any) -> _FakeConnection:
        self.statements.append(sql)
        return self

    def fetchone(self) -> tuple[str]:
        return (self.cluster,)


def test_only_a_gate_cluster_may_have_databases_dropped() -> None:
    assert require_gate_cluster(_FakeConnection("stabilize-gate-dev")) == "stabilize-gate-dev"
    assert require_gate_cluster(_FakeConnection("stabilize-gate-7")) == "stabilize-gate-7"
    for name in ("", "stabilize", "production", "stabilize-gate", "gate-stabilize-1"):
        with pytest.raises(RuntimeError, match="stabilize-gate-"):
            require_gate_cluster(_FakeConnection(name))


def test_the_gate_container_is_a_gate_cluster(postgres_container: Any) -> None:
    import psycopg

    from tests.mg_support import server_url

    with psycopg.connect(server_url(postgres_container), autocommit=True) as conn:
        assert require_gate_cluster(conn).startswith("stabilize-gate-")


def test_the_redproof_runner_creates_databases_without_docker() -> None:
    source = (REPO / "audit" / "redproof" / "run.py").read_text()
    assert "docker" not in source, "the red-proof runner still shells out to docker"
    assert "require_gate_cluster" in source
    backends = (REPO / "audit" / "evaluations" / "_backends.py").read_text()
    assert "require_gate_cluster" in backends
    conftest = (REPO / "tests" / "conftest.py").read_text()
    assert conftest.index("require_gate_cluster(") < conftest.index("DROP DATABASE IF EXISTS")


def test_runner_env_drops_the_import_path_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("PYTHONPATH", "PYTHONHOME", "PYTHONSAFEPATH", "VIRTUAL_ENV"):
        monkeypatch.setenv(name, f"/planted/{name.lower()}")
    monkeypatch.setenv("MG_DATABASE_URL", "postgresql://planted")
    env = runner_env()
    for name in ("PYTHONPATH", "PYTHONHOME", "PYTHONSAFEPATH", "VIRTUAL_ENV", "MG_DATABASE_URL"):
        assert name not in env, f"{name} leaked into the old runner's environment"
    assert env.get("PATH") == os.environ.get("PATH")


def _plant_sentinel(tmp_path: Path) -> Path:
    planted = tmp_path / "planted"
    (planted / "stabilize").mkdir(parents=True)
    (planted / "stabilize" / "__init__.py").write_text('__version__ = "0.0.0-sentinel"\n')
    return planted


def test_a_planted_pythonpath_is_seen_raw_and_dropped_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    planted = _plant_sentinel(tmp_path)
    monkeypatch.setenv("PYTHONPATH", str(planted))
    raw = import_location(sys.executable, dict(os.environ))
    assert raw.startswith(str(planted)), f"the planted package was not imported under the raw environment: {raw}"
    clean = import_location(sys.executable, runner_env())
    assert not clean.startswith(str(planted)), "the planted package survived the cleaned environment"


def _shim(tmp_path: Path, name: str, python_prints: str, version: str = "stabilize 0.33.1") -> str:
    bindir = tmp_path / name / "bin"
    bindir.mkdir(parents=True)
    python = bindir / "python"
    python.write_text(f"#!/bin/sh\nprintf '%s\\n' '{python_prints}'\n")
    python.chmod(python.stat().st_mode | stat.S_IEXEC)
    binary = bindir / "stabilize"
    binary.write_text(f"#!/bin/sh\nprintf '%s\\n' '{version}'\n")
    binary.chmod(binary.stat().st_mode | stat.S_IEXEC)
    return str(binary)


def test_an_old_runner_must_import_its_own_stabilize(tmp_path: Path) -> None:
    own = _shim(tmp_path, "own", "/opt/old/lib/python3.12/site-packages/stabilize/__init__.py")
    assert_own_runner(own)
    result = old_stabilize(own, "--version")
    assert result.returncode == 0 and "0.33.1" in result.stdout

    checkout = _shim(tmp_path, "checkout", str(REPO / "src" / "stabilize" / "__init__.py"))
    with pytest.raises(RuntimeError, match="imports stabilize from"):
        assert_own_runner(checkout)
    with pytest.raises(RuntimeError, match="imports stabilize from"):
        old_stabilize(checkout, "--version")

    from stabilize import __version__

    same = _shim(tmp_path, "same", "/opt/old/site-packages/stabilize/__init__.py", version=f"stabilize {__version__}")
    with pytest.raises(RuntimeError, match="version"):
        assert_own_runner(same)
