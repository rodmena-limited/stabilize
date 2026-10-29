"""Run migretti on stabilize's shipped migrations in an isolated working directory."""

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from stabilize.cli.legacy_history import PLACEHOLDER_SQL, MigrettiState
from stabilize.redaction import redact_against

SCRUBBED_ENV = (
    "MG_DATABASE_URL",
    "MG_ENV",
    "MG_DB_HOST",
    "MG_DB_PORT",
    "MG_DB_USER",
    "MG_DB_PASSWORD",
    "MG_DB_NAME",
    "MG_LOCK_ID",
)

_SNAPSHOT_MARKER = "STABILIZE-MIGRETTI-STATE "

_SNAPSHOT_SCRIPT = f"""
import json, sys
from migretti.db import get_connection, tracking_tables_exist
from migretti.core import get_applied_migrations_details, check_failed_migrations
try:
    conn = get_connection()
except Exception as exc:
    sys.stderr.write(f"{{type(exc).__name__}}: {{exc}}\\n")
    raise SystemExit(1)
try:
    if tracking_tables_exist(conn):
        state = {{
            "tracking": True,
            "applied": {{i: c for i, _n, c in get_applied_migrations_details(conn)}},
            "failed": [i for i, _n in check_failed_migrations(conn)],
        }}
    else:
        state = {{"tracking": False, "applied": {{}}, "failed": []}}
finally:
    conn.close()
sys.stdout.write({_SNAPSHOT_MARKER!r} + json.dumps(state) + "\\n")
"""


class MigrettiFailedError(RuntimeError):
    """migretti exited non-zero."""

    def __init__(self, command: str, returncode: int) -> None:
        super().__init__(f"migretti {command} exited with status {returncode}")
        self.command = command
        self.returncode = returncode


def migretti_installed() -> bool:
    return importlib.util.find_spec("migretti") is not None


def migretti_conninfo(params: dict[str, Any], schema: str | None) -> str:
    """Return the libpq keyword/value string migretti connects with.

    Every parameter the operator gave is kept; a target schema is added as a
    search_path option so migretti's tracking tables land beside stabilize's.
    """
    from psycopg.conninfo import make_conninfo

    merged = dict(params)
    if schema:
        option = f"-c search_path={schema}"
        existing = str(merged.get("options") or "").strip()
        merged["options"] = f"{existing} {option}".strip()
    return make_conninfo("", **{key: str(value) for key, value in merged.items()})


def migretti_env(conninfo: str) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key not in SCRUBBED_ENV}
    env["MG_DATABASE_URL"] = conninfo
    return env


def _write_tree(root: Path, migrations: Sequence[tuple[str, bytes]]) -> None:
    folder = root / "migrations"
    folder.mkdir(mode=0o700)
    for name, raw in migrations:
        (folder / name).write_bytes(raw)


@contextlib.contextmanager
def staged(migrations: Sequence[tuple[str, bytes]]) -> Iterator[Path]:
    """A private directory holding exactly *migrations* under migrations/, and no mg.yaml."""
    with tempfile.TemporaryDirectory(prefix="stabilize-mg-") as tmp:
        root = Path(tmp)
        os.chmod(root, 0o700)
        _write_tree(root, migrations)
        yield root


def placeholder_files(ids_to_names: dict[str, str]) -> list[tuple[str, bytes]]:
    return [(name, PLACEHOLDER_SQL.encode("utf-8")) for _mig_id, name in sorted(ids_to_names.items())]


def _command(args: Sequence[str]) -> list[str]:
    return [sys.executable, "-P", "-m", "migretti", *args]


def run(args: Sequence[str], workdir: Path, conninfo: str, *, echo: bool = True) -> list[str]:
    """Run one migretti command in *workdir*, print its output redacted, and raise on a non-zero exit."""
    proc = subprocess.Popen(
        _command(args),
        cwd=workdir,
        env=migretti_env(conninfo),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    lines: list[str] = []
    for raw in proc.stdout or ():
        line = raw.rstrip("\n")
        lines.append(line)
        if echo and not line.startswith(_SNAPSHOT_MARKER):
            print(f"  migretti: {redact_against(line, conninfo)}", flush=True)
    returncode = proc.wait()
    if returncode != 0:
        if not echo:
            for line in lines:
                print(f"  migretti: {redact_against(line, conninfo)}", flush=True)
        raise MigrettiFailedError(args[0], returncode)
    return lines


def snapshot(workdir: Path, conninfo: str) -> MigrettiState:
    """Read migretti's history in the target schema through migretti's own read functions."""
    proc = subprocess.run(
        [sys.executable, "-P", "-c", _SNAPSHOT_SCRIPT],
        cwd=workdir,
        env=migretti_env(conninfo),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )
    for line in proc.stdout.splitlines():
        if line.startswith(_SNAPSHOT_MARKER):
            data = json.loads(line[len(_SNAPSHOT_MARKER) :])
            return MigrettiState(
                tracking=bool(data["tracking"]),
                applied={str(k): str(v) for k, v in data["applied"].items()},
                failed=tuple(str(i) for i in data["failed"]),
            )
    for line in (proc.stdout + proc.stderr).splitlines():
        print(f"  migretti: {redact_against(line, conninfo)}", flush=True)
    raise MigrettiFailedError("state read", proc.returncode or 1)
