"""Drive `stabilize mg-up` / `mg-status` and migretti's own CLI against real PostgreSQL databases."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

REPO = Path(__file__).resolve().parent.parent
SHIPPED = sorted((REPO / "migrations").glob("*.sql"))
SHIPPED_COUNT = len(SHIPPED)
MG_VARS = ("MG_DATABASE_URL", "MG_ENV", "MG_DB_HOST", "MG_DB_PORT", "MG_DB_USER", "MG_DB_PASSWORD", "MG_DB_NAME")


def server_url(container: Any) -> str:
    return str(container.get_connection_url()).replace("+psycopg2", "")


@contextmanager
def scratch_database(container: Any, prefix: str = "mgt") -> Iterator[str]:
    import psycopg

    base = server_url(container)
    parts = urlsplit(base)
    name = f"{prefix}_{os.environ.get('PYTEST_XDIST_WORKER', 'main')}_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    try:
        yield urlunsplit((parts.scheme, parts.netloc, f"/{name}", "", ""))
    finally:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def with_schema(url: str, schema: str) -> str:
    return f"{url}?schema={schema}"


def search_path_url(url: str, schema: str) -> str:
    return f"{url}?options={quote(f'-csearch_path={schema}')}"


def clean_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in MG_VARS}
    env.update(extra or {})
    return env


def stabilize(*args: str, env: dict[str, str] | None = None, timeout: float = 300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "stabilize.cli.main", *args],
        capture_output=True,
        text=True,
        env=env if env is not None else clean_env(),
        timeout=timeout,
    )


def old_stabilize(
    binary: str, *args: str, env: dict[str, str] | None = None, cwd: Path | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [binary, *args],
        capture_output=True,
        text=True,
        env=env if env is not None else clean_env(),
        cwd=cwd,
        timeout=300,
    )


def stage(tmp: Path, files: list[tuple[str, str]] | None = None) -> Path:
    """A migretti project directory: migrations/ with the shipped files (or *files*), no mg.yaml."""
    root = tmp / f"mgproj_{uuid.uuid4().hex[:6]}"
    (root / "migrations").mkdir(parents=True)
    if files is None:
        for path in SHIPPED:
            shutil.copyfile(path, root / "migrations" / path.name)
    else:
        for name, text in files:
            (root / "migrations" / name).write_text(text)
    return root


def mg(project: Path, url: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run migretti's own CLI in *project* against *url*."""
    return subprocess.run(
        [sys.executable, "-m", "migretti", *args],
        capture_output=True,
        text=True,
        cwd=project,
        env=clean_env({"MG_DATABASE_URL": url}),
        timeout=300,
    )


def migretti_counts(project: Path, url: str) -> tuple[int, int]:
    """(applied, pending) as `mg status` reports them for the files in *project*."""
    result = mg(project, url, "status")
    assert result.returncode == 0, f"mg status failed: {result.stdout}{result.stderr}"
    applied = re.search(r"^Applied: (\d+)$", result.stdout, re.MULTILINE)
    pending = re.search(r"^Pending: (\d+)$", result.stdout, re.MULTILINE)
    assert applied and pending, f"mg status printed no counts: {result.stdout!r}"
    return int(applied.group(1)), int(pending.group(1))


def relation_exists(url: str, relation: str) -> bool:
    import psycopg

    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,))
        row = cur.fetchone()
        return bool(row and row[0])


def legacy_rows(url: str, schema: str | None = None) -> list[tuple[str, str, str]]:
    import psycopg

    table = f'"{schema}".stabilize_migrations' if schema else "stabilize_migrations"
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT name, checksum, applied_at::text FROM {table} ORDER BY name")
        return [(str(a), str(b), str(c)) for a, b, c in cur.fetchall()]


def output(result: subprocess.CompletedProcess[str]) -> str:
    return result.stdout + result.stderr
