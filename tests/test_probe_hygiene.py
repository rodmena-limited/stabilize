"""Every probe that needs PostgreSQL takes it from STABILIZE_PROBE_DSN; none starts a container of its own (B56)."""

from __future__ import annotations

import re
from pathlib import Path

EVALUATIONS = Path(__file__).resolve().parents[1] / "audit" / "evaluations"
POSTGRES_USE = re.compile(r"PostgresWorkflowStore|PostgresQueue|PostgresEventStore|mg_up\(")
DSN_SOURCES = re.compile(r"probe_dsn\(|fresh_database\(|postgres_dsn\(|backends\(\)|STABILIZE_PROBE_DSN")


def _probe_files() -> list[Path]:
    files = sorted(EVALUATIONS.glob("probe_*.py")) + sorted(EVALUATIONS.glob("measure_*.py"))
    assert len(files) > 60, "the probe glob found almost nothing"
    return files


def test_no_probe_starts_its_own_postgres() -> None:
    offenders = []
    for path in _probe_files():
        text = path.read_text()
        if "PostgresContainer" in text or "testcontainers" in text or re.search(r"docker (run|exec|rm)", text):
            offenders.append(path.name)
    assert offenders == [], f"probes still start or drive a container: {offenders}"


def test_every_postgres_probe_reads_the_probe_dsn() -> None:
    uses_postgres = [p for p in _probe_files() if POSTGRES_USE.search(p.read_text())]
    assert len(uses_postgres) >= 14, f"only {len(uses_postgres)} probes use PostgreSQL; the scan is too narrow"
    missing = [p.name for p in uses_postgres if not DSN_SOURCES.search(p.read_text())]
    assert missing == [], f"PostgreSQL probes that take their server from somewhere else: {missing}"


def test_the_runner_partitions_by_postgres_need_not_docker() -> None:
    runner = (EVALUATIONS / "run_all.sh").read_text()
    assert "_needs_postgres" in runner
    assert "DOCKER_PROBES" not in runner


def test_the_ssrf_guard_probe_resolves_its_public_names_itself() -> None:
    guard = (EVALUATIONS / "probe_ssrf_guard.py").read_text()
    assert "93.184.216.34" in guard and "getaddrinfo" in guard, "the guard probe depends on outbound DNS"
