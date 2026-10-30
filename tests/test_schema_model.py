"""Units of #98 and #99: the SQL model of the migrations, the adoption level, the history layout."""

from __future__ import annotations

import pytest

from stabilize.cli.adoption import AdoptionRefusedError, Catalog, _expected, adoption_level, legacy_rows_for, models_for
from stabilize.cli.history_location import DEFAULT_HISTORY_SCHEMA, Elsewhere, Layout, resolve, survey
from stabilize.cli.legacy_history import (
    KNOWN_LEGACY_CHECKSUMS,
    PLACEHOLDER_CHECKSUM,
    PLACEHOLDER_SQL,
    MigrettiState,
    TakeoverRefusedError,
)
from stabilize.cli.migrations import get_migrations, migration_id, migretti_checksum
from stabilize.cli.schema_model import model_migration, normalize_type, split_statements

SHIPPED = dict(get_migrations())
MODELS = models_for(SHIPPED)
IDS = {migration_id(name) for name in SHIPPED}


def _catalog_at(level: int, server: int = 160000) -> Catalog:
    state = _expected(MODELS, level, server)
    return Catalog(
        tables={table: dict(columns) for table, columns in state.tables.items()},
        indexes=set(state.indexes),
        sequence_types=dict(state.sequence_types),
        server_version=server,
    )


def test_every_shipped_migration_is_fully_modelled() -> None:
    for model in MODELS:
        assert not model.unmodelled, f"{model.filename} has statements adoption cannot check: {model.unmodelled}"
        assert model.expectations, f"{model.filename} leaves nothing in the catalog"


def test_types_normalize_to_postgres_format_type_names() -> None:
    assert normalize_type("VARCHAR(26)") == "character varying(26)"
    assert normalize_type("timestamp with time zone") == normalize_type("TIMESTAMPTZ") == "timestamp with time zone"
    assert normalize_type("BIGSERIAL") == "bigint"
    with pytest.raises(ValueError):
        normalize_type("MONEY")


def test_statements_split_outside_quotes_and_dollar_blocks() -> None:
    sql = "SELECT ';'; DO $$ BEGIN PERFORM 1; END $$; -- a; comment\nSELECT 2;"
    assert split_statements(sql) == ["SELECT ';'", "DO $$ BEGIN PERFORM 1; END $$", "SELECT 2"]


def test_a_statement_adoption_cannot_check_is_reported() -> None:
    model = model_migration("x.sql", "-- migrate: up\nINSERT INTO t VALUES (1);\n-- migrate: down\nSELECT 1;\n")
    assert model.unmodelled == ["INSERT INTO t VALUES (1)"]


@pytest.mark.parametrize("level", range(1, len(MODELS) + 1))
def test_a_catalog_at_each_level_is_adopted_at_or_below_it(level: int) -> None:
    adopted = adoption_level(MODELS, _catalog_at(level))
    assert 1 <= adopted <= level
    for model in MODELS[adopted:level]:
        assert all(e.may_preexist or e.kind not in ("table", "column", "index") for e in model.expectations), (
            f"level {level} was adopted at {adopted}, skipping {model.filename}, which is not re-runnable"
        )


def test_a_wrong_column_type_is_refused() -> None:
    catalog = _catalog_at(8)
    catalog.tables["stage_executions"]["join_threshold"] = "text"
    with pytest.raises(AdoptionRefusedError, match="stage_executions.join_threshold"):
        adoption_level(MODELS, catalog)


def test_commit_xid_is_expected_only_from_postgresql_13() -> None:
    last = len(MODELS)
    assert "commit_xid" in _expected(MODELS, last, 160000).tables["events"]
    assert "commit_xid" not in _expected(MODELS, last, 120000).tables["events"]


def test_adoption_records_the_checksums_the_old_runner_would_have() -> None:
    rows = legacy_rows_for(SHIPPED, 3)
    assert len(rows) == 3
    assert all(md5 in KNOWN_LEGACY_CHECKSUMS[name] for name, md5 in rows.items())


def test_without_a_history_schema_files_and_placeholders_are_unchanged() -> None:
    layout = Layout("public")
    name, text = next(iter(SHIPPED.items()))
    assert layout.stage(text) == text
    assert layout.placeholder() == PLACEHOLDER_SQL
    assert PLACEHOLDER_CHECKSUM in layout.placeholder_checksums([])


def test_with_a_history_schema_the_up_section_is_redirected() -> None:
    layout = Layout("public", "hist")
    staged = layout.stage("-- migrate: up\nCREATE TABLE t (id int);\n-- migrate: down\nDROP TABLE t;\n")
    assert staged.split("\n")[:4] == [
        "-- migrate: up",
        'SET LOCAL search_path TO "public";',
        "CREATE TABLE t (id int);",
        'SET LOCAL search_path TO "hist";',
    ]
    assert migretti_checksum(layout.placeholder()) in layout.placeholder_checksums([])
    with pytest.raises(TakeoverRefusedError):
        layout.stage("-- migrate: up\n-- migrate: no-transaction\nSELECT 1;\n")


def _state(*ids: str, checksum: str = "x") -> MigrettiState:
    return MigrettiState(tracking=bool(ids), applied={i: checksum for i in ids}, failed=())


HOST_ID = "01M60ZZZZZZZZZZZZZZZZZZZZZ"
FIRST = next(iter(SHIPPED))
FIRST_ID = migration_id(FIRST)


def _owned_by(target: str, history: str) -> MigrettiState:
    return _state(FIRST_ID, checksum=migretti_checksum(Layout(target, history).stage(SHIPPED[FIRST])))


def _survey(target: str, **states: MigrettiState) -> Elsewhere:
    return survey(target, states, SHIPPED.values(), [m.rerun_on_adoption for m in MODELS], IDS)


def test_a_history_schema_belongs_to_the_target_its_checksums_name() -> None:
    seen = _survey("public", meta=_owned_by("public", "meta"), other=_owned_by("tenant_b", "other"))
    assert seen.owners == ("meta",)
    assert "other" in seen.taken
    placeholder = _state(FIRST_ID, checksum=migretti_checksum(Layout("public", "meta").placeholder()))
    assert _survey("public", meta=placeholder).owners == ("meta",)


def test_a_history_schema_with_a_host_s_history_or_no_rows_is_not_an_owner() -> None:
    seen = _survey("public", host=_state(HOST_ID), blank=_state())
    assert seen.owners == () and seen.empty == ("blank",) and "host" in seen.taken


@pytest.mark.parametrize(
    ("explicit", "elsewhere", "state", "legacy", "expected"),
    [
        (None, Elsewhere(), (), False, None),
        (None, Elsewhere(), (HOST_ID,), True, DEFAULT_HISTORY_SCHEMA),
        (None, Elsewhere(empty=(DEFAULT_HISTORY_SCHEMA,)), (), False, DEFAULT_HISTORY_SCHEMA),
        (None, Elsewhere(owners=("stab_meta",)), (HOST_ID,), False, "stab_meta"),
        (None, Elsewhere(taken={DEFAULT_HISTORY_SCHEMA: "x"}), (), False, None),
        ("meta", Elsewhere(), (HOST_ID,), False, "meta"),
        ("meta", Elsewhere(owners=("meta",)), (), False, "meta"),
    ],
)
def test_where_the_history_lives(
    explicit: str | None, elsewhere: Elsewhere, state: tuple[str, ...], legacy: bool, expected: str | None
) -> None:
    assert resolve("public", explicit, _state(*state), IDS, legacy, elsewhere).history == expected


@pytest.mark.parametrize(
    ("explicit", "elsewhere", "state", "legacy", "message"),
    [
        (None, Elsewhere(), (HOST_ID,), False, "--history-schema stabilize_history"),
        (None, Elsewhere(taken={DEFAULT_HISTORY_SCHEMA: "x"}), (HOST_ID,), True, "--history-schema SCHEMA"),
        ("public", Elsewhere(), (), False, "must differ"),
        ("meta", Elsewhere(), (FIRST_ID,), False, "already in public"),
        (None, Elsewhere(owners=("meta",)), (FIRST_ID,), False, "in both"),
        (None, Elsewhere(owners=("a", "b")), (), False, "several schemas"),
        ("meta", Elsewhere(owners=("stab_meta",)), (), False, "--history-schema stab_meta"),
        ("meta", Elsewhere(taken={"meta": "another project's migretti history"}), (), False, "already holds"),
    ],
)
def test_history_locations_that_are_refused(
    explicit: str | None, elsewhere: Elsewhere, state: tuple[str, ...], legacy: bool, message: str
) -> None:
    with pytest.raises(TakeoverRefusedError, match=message):
        resolve("public", explicit, _state(*state), IDS, legacy, elsewhere)
