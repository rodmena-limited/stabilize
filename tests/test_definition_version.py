"""WI-8: the definition version and hash recorded on every workflow, and the listing filter on them."""

from __future__ import annotations

import hashlib
import math
import sqlite3
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgdef") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'definitions.db'}"
        return
    yield request.getfixturevalue("db")


def _parts(url: str) -> tuple[Any, Any]:
    if url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        return SqliteWorkflowStore(url, create_tables=True), SqliteQueue(url)
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    return PostgresWorkflowStore(url), PostgresQueue(url)


class _Colour(Enum):
    RED = "red"


def _launcher(store: Any, queue: Any) -> Any:
    from stabilize.launcher import WorkflowLauncher
    from stabilize.orchestrator import Orchestrator

    return WorkflowLauncher(store, Orchestrator(queue, store=store))


def test_the_canonical_form_decides_the_hash() -> None:
    from stabilize.definition_hash import HASH_ALGORITHM, canonical_json, definition_hash

    assert HASH_ALGORITHM == "stabilize-canonical-json/1"
    assert canonical_json({"b": 1, "a": [1, 2]}) == b'{"a":[1,2],"b":1}'
    assert definition_hash({"a": 1}) == (hashlib.sha256(b'{"a":1}').hexdigest(), HASH_ALGORITHM)
    assert definition_hash({"b": 1, "a": 2}) == definition_hash({"a": 2, "b": 1})
    assert definition_hash({"x": True})[0] != definition_hash({"x": 1})[0]
    assert definition_hash({"x": 1})[0] != definition_hash({"x": 1.0})[0]
    assert definition_hash({"x": -0.0}) == definition_hash({"x": 0.0})
    assert definition_hash({"x": 2**70}) == definition_hash({"x": 2**70})
    assert canonical_json({"x": 2**70}) == b'{"x":1180591620717411303424}'
    assert definition_hash((1, 2)) == definition_hash([1, 2])
    assert definition_hash({"s": {3, 1, 2}}) == definition_hash({"s": [1, 2, 3]})
    assert definition_hash({"c": _Colour.RED}) == definition_hash({"c": "red"})
    assert canonical_json({"u": "ünïcode"}) == '{"u":"ünïcode"}'.encode()
    paris = datetime(2026, 10, 31, 14, 0, tzinfo=timezone(timedelta(hours=1)))
    assert definition_hash({"t": paris}) == definition_hash({"t": datetime(2026, 10, 31, 13, 0, tzinfo=UTC)})
    assert canonical_json({"t": paris}) == b'{"t":"2026-10-31T13:00:00+00:00"}'
    assert canonical_json({"d": Decimal("1.10")}) == b'{"d":{"$type":"decimal","$value":"1.10"}}'
    assert definition_hash({"d": Decimal("1.10")})[0] != definition_hash({"d": "1.10"})[0]
    some = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert canonical_json({"u": some}) == b'{"u":{"$type":"uuid","$value":"12345678-1234-5678-1234-567812345678"}}'
    assert canonical_json({"b": b"\x00\x01"}) == b'{"b":{"$type":"bytes","$value":"AAE="}}'
    tagged = {"$type": "decimal", "$value": "1.10"}
    assert definition_hash({"d": tagged})[0] != definition_hash({"d": Decimal("1.10")})[0]
    assert canonical_json({"d": tagged}) == b'{"d":{"$type":"dict","$value":{"$type":"decimal","$value":"1.10"}}}'
    for bad in (
        {"x": math.nan},
        {"x": math.inf},
        {1: "non-string key"},
        {"t": datetime(2026, 10, 31, 13, 0)},
        {"s": "\ud800"},
        {"o": object()},
    ):
        digest, algorithm = definition_hash(bad)
        assert digest is None, bad
        assert algorithm == HASH_ALGORITHM


def _config(version: str | None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    config: dict[str, Any] = {
        "id": "cfg-def",
        "application": "defs",
        "name": "definition",
        "stages": [
            {"refId": "s0", "type": "noop", "name": "S0", "tasks": [{"name": "t", "implementingClass": "noop"}]}
        ],
    }
    if version is not None:
        config["version"] = version
    if extra:
        config.update(extra)
    return config


def test_the_launcher_and_create_record_version_and_hash(any_db: str) -> None:
    from stabilize.definition_hash import HASH_ALGORITHM, definition_hash

    store, queue = _parts(any_db)
    try:
        launcher = _launcher(store, queue)
        config = _config("3")
        launched = launcher.start(config)
        stored = store.retrieve(launched.id)
        assert stored.pipeline_config_version == "3"
        assert stored.pipeline_config_sha256 == definition_hash(config)[0]
        assert len(stored.pipeline_config_sha256 or "") == 64
        assert stored.pipeline_config_hash_alg == HASH_ALGORITHM

        task = TaskExecution.create(name="t", implementing_class="noop", stage_start=True, stage_end=True)
        created = Workflow.create(
            application="defs",
            name="direct",
            stages=[StageExecution(ref_id="s0", type="noop", name="S0", tasks=[task])],
            pipeline_config_id="cfg-def",
            pipeline_config={"id": "cfg-def", "version": 7, "stages": []},
        )
        assert created.pipeline_config_version == "7"
        assert created.pipeline_config_sha256 == definition_hash({"id": "cfg-def", "version": 7, "stages": []})[0]
        store.store(created)
        again = store.retrieve(created.id)
        assert (again.pipeline_config_version, again.pipeline_config_sha256, again.pipeline_config_hash_alg) == (
            "7",
            created.pipeline_config_sha256,
            HASH_ALGORITHM,
        )

        unhashable = launcher.start(_config("9", {"weights": [math.nan]}))
        stored = store.retrieve(unhashable.id)
        assert stored.status is not WorkflowStatus.TERMINAL
        assert stored.pipeline_config_version == "9"
        assert stored.pipeline_config_sha256 is None
        assert stored.pipeline_config_hash_alg == HASH_ALGORITHM
    finally:
        queue.close()
        store.close()


def test_listings_filter_by_pipeline_config_version(any_db: str) -> None:
    from stabilize.persistence.store import WorkflowCriteria

    store, queue = _parts(any_db)
    try:
        launcher = _launcher(store, queue)
        one = launcher.start(_config("1"))
        two = launcher.start(_config("2"))
        wanted = WorkflowCriteria(pipeline_config_version="2")
        assert [w.id for w in store.retrieve_by_pipeline_config_id("cfg-def", wanted)] == [two.id]
        assert [w.id for w in store.retrieve_by_application("defs", wanted)] == [two.id]
        assert {w.id for w in store.get_all_pending_workflows(wanted)} == {two.id}
        assert {w.id for w in store.retrieve_by_pipeline_config_id("cfg-def")} == {one.id, two.id}
        assert "pipeline_config_version" in store.supported_workflow_criteria()
    finally:
        queue.close()
        store.close()


def test_the_columns_and_the_index_exist(any_db: str) -> None:
    if any_db.startswith("sqlite"):
        store, queue = _parts(any_db)
        store.close()
        queue.close()
        with sqlite3.connect(any_db.removeprefix("sqlite:///")) as conn:
            present = {row[1] for row in conn.execute("PRAGMA table_info(pipeline_executions)")}
            assert {"pipeline_config_version", "pipeline_config_sha256", "pipeline_config_hash_alg"} <= present
            indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
            assert "idx_executions_tenant_config_version" in indexes
        return
    with psycopg.connect(any_db) as conn:
        rows = conn.execute(
            "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns "
            "WHERE table_name = 'pipeline_executions' AND column_name LIKE 'pipeline_config_%' ORDER BY column_name"
        ).fetchall()
        assert rows == [
            ("pipeline_config_hash_alg", "character varying", 32),
            ("pipeline_config_id", "character varying", 255),
            ("pipeline_config_sha256", "character varying", 64),
            ("pipeline_config_version", "character varying", 64),
        ], rows
        definition = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_executions_tenant_config_version'"
        ).fetchone()
        assert definition is not None
        assert "(stabilize_tenant, pipeline_config_id, pipeline_config_version)" in definition[0]
