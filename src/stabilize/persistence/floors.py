"""The floors the stability contract names, checked where they bite."""

from __future__ import annotations

import sqlite3
import sys

SQLITE_FLOOR = (3, 35, 0)


def require_sqlite_floor() -> None:
    found = tuple(sqlite3.sqlite_version_info)
    if found < SQLITE_FLOOR:
        version = ".".join(str(part) for part in found)
        floor = ".".join(str(part) for part in SQLITE_FLOOR)
        raise RuntimeError(
            f"SQLite {version} is older than the {floor} floor; the engine needs RETURNING. "
            "Install a newer libsqlite3 or a Python built against one."
        )


def refuse_free_threaded() -> None:
    gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if gil_enabled is not None and not gil_enabled():
        raise RuntimeError(
            "stabilize does not run on a free-threaded Python build with the GIL disabled "
            f"({sys.version.split()[0]}); use a standard build, or start the interpreter with PYTHON_GIL=1"
        )
