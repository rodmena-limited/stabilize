"""The floors the contract names are checked where they bite: SQLite 3.35 on open, no free-threaded Python (B61)."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

from stabilize.persistence.floors import SQLITE_FLOOR, refuse_free_threaded, require_sqlite_floor


def test_the_sqlite_floor_is_three_thirty_five() -> None:
    assert SQLITE_FLOOR == (3, 35, 0)
    require_sqlite_floor()


def test_an_old_sqlite_is_refused_on_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.persistence.sqlite import SqliteWorkflowStore

    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 34, 1))
    with pytest.raises(RuntimeError, match=r"SQLite 3\.34\.1.*3\.35"):
        SqliteWorkflowStore(f"sqlite:///{tmp_path / 'old.db'}", create_tables=True)


def test_a_current_sqlite_opens(tmp_path: Path) -> None:
    from stabilize.persistence.sqlite import SqliteWorkflowStore

    store = SqliteWorkflowStore(f"sqlite:///{tmp_path / 'new.db'}", create_tables=True)
    store.close()


def test_a_free_threaded_interpreter_is_refused_by_the_processor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.persistence.sqlite import SqliteWorkflowStore
    from stabilize.queue.processor import QueueProcessor
    from stabilize.queue.sqlite import SqliteQueue

    url = f"sqlite:///{tmp_path / 'ft.db'}"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    queue._create_table()
    try:
        monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
        with pytest.raises(RuntimeError, match="free-threaded"):
            QueueProcessor(queue, store=store)
        monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True, raising=False)
        refuse_free_threaded()
        QueueProcessor(queue, store=store)
    finally:
        queue.close()
        store.close()
