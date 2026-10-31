"""stabilize mg-up and mg-status: stabilize's PostgreSQL migrations, applied and reported by migretti."""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from stabilize.cli import migretti_runner as runner
from stabilize.cli.adoption import (
    ENGINE_MARKER_TABLE,
    AdoptionRefusedError,
    adoption_level,
    legacy_rows_for,
    models_for,
    read_catalog,
)
from stabilize.cli.config import (
    apply_schema_override,
    connection_params,
    load_config,
    parse_db_url,
    validate_schema_name,
)
from stabilize.cli.driver import announce_driver, import_psycopg
from stabilize.cli.history_location import Layout, current_schema, history_candidates, resolve, survey
from stabilize.cli.legacy_history import (
    LEGACY_TABLE,
    RETIRED_MIGRATIONS,
    MigrettiState,
    TakeoverRefusedError,
    check_history,
    plan_takeover,
)
from stabilize.cli.migrations import get_migration_files, migration_id, migration_text, migretti_checksum
from stabilize.redaction import redact_db_url, redact_text

MG_UP_LOCK_CLASS = 0x53544D47
MG_UP_LOCK_KEY = "stabilize mg-up"
DEFAULT_LOCK_TIMEOUT_SECONDS = 900.0


@dataclass
class _Run:
    layout: Layout
    plain: dict[str, str]
    staged: dict[str, str]
    files: list[tuple[str, bytes]]
    conninfo: str
    rerun: dict[str, list[str]]
    placeholder_checksums: frozenset[str]
    legacy: dict[str, str] | None
    shipped_ids: set[str] = field(default_factory=set)


def _target(
    db_url: str | None, history_flag: str | None, config_path: Path | None = None
) -> tuple[dict[str, Any], str | None, str | None]:
    config = apply_schema_override(parse_db_url(db_url)) if db_url else load_config(config_path)
    schema = config.get("schema")
    if schema:
        schema = validate_schema_name(schema)
    history = history_flag or config.get("history_schema")
    if history:
        history = validate_schema_name(history)
    return connection_params(config), schema, history


def _describe(params: dict[str, Any]) -> str:
    from psycopg import pq

    defaults = {o.keyword.decode(): o.val.decode() for o in pq.Conninfo.get_defaults() if o.val}
    shown = {key: params.get(key) or defaults.get(key) for key in ("user", "host", "port", "dbname")}
    user = shown["user"] or "?"
    target = "{}@{}:{}/{}".format(user, shown["host"] or "local socket", shown["port"] or 5432, shown["dbname"] or user)
    return redact_db_url(target)


def _require_migretti() -> None:
    if not runner.migretti_installed():
        print("Error: migretti is not installed; stabilize's PostgreSQL migrations are applied by migretti")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)


def _acquire_lock(psycopg: Any, conn: Any, timeout: float) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s, hashtext(%s))", (MG_UP_LOCK_CLASS, MG_UP_LOCK_KEY))
        row = cur.fetchone()
        if row and row[0]:
            return
        print(f"Waiting up to {timeout:g} s for another stabilize mg-up on this database to finish", flush=True)
        cur.execute("SELECT set_config('lock_timeout', %s, false)", (f"{int(timeout * 1000)}ms",))
        try:
            cur.execute("SELECT pg_advisory_lock(%s, hashtext(%s))", (MG_UP_LOCK_CLASS, MG_UP_LOCK_KEY))
        except psycopg.errors.LockNotAvailable:
            print(
                f"Error: another stabilize mg-up held the migration lock for more than {timeout:g} s; "
                "nothing was changed"
            )
            sys.exit(1)
        cur.execute("SELECT set_config('lock_timeout', '0', false)")


def _refuse_role_shadow(conn: Any, schema: str | None) -> None:
    """A target schema named after the connecting role shadows public for every unqualified name.

    PostgreSQL's default search_path is "$user", public. With a role called X, a
    schema called X is searched first, so a host project's migretti then reads
    X._migrations instead of public._migrations and reports stabilize's ids as
    its own (B65).
    """
    if not schema:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT current_user, current_setting('search_path')")
        row = cur.fetchone()
    role, path = (str(row[0]), str(row[1])) if row else ("", "")
    if schema == role and '"$user"' in path:
        raise TakeoverRefusedError(
            f"the target schema {schema} is named after the connecting role {role}, whose search_path "
            f"({path}) lists \"$user\" first; every unqualified name, including other projects' "
            "_migrations, would then resolve into stabilize's schema. Choose another schema name, "
            "or connect as a role with a different name"
        )


def _read_legacy(conn: Any) -> dict[str, str] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (LEGACY_TABLE,))
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        cur.execute(f"SELECT name, checksum FROM {LEGACY_TABLE}")
        return {str(name): str(checksum) for name, checksum in cur.fetchall()}


def _prepare(
    conn: Any, params: dict[str, Any], schema: str | None, history: str | None, *, lenient: bool = False
) -> _Run:
    raw = get_migration_files()
    plain = {name: migration_text(content) for name, content in raw}
    shipped_ids = {migration_id(name) for name in plain}
    if schema:
        conn.execute(f'SET search_path TO "{schema}"')
    target = schema or current_schema(conn)
    legacy = _read_legacy(conn)
    rerun = {migration_id(m.filename): m.rerun_on_adoption for m in models_for(plain)}
    reader = runner.migretti_conninfo(params, target)
    with runner.staged([]) as empty:
        target_state = runner.snapshot(empty, reader)
        others = runner.snapshot_schemas(empty, reader, history_candidates(conn, target))
    elsewhere = survey(target, others, plain.values(), list(rerun.values()), shipped_ids)
    try:
        layout = resolve(target, history, target_state, shipped_ids, legacy is not None, elsewhere)
    except TakeoverRefusedError as e:
        if not lenient:
            raise
        print(f"Warning: {e}")
        layout = Layout(target)
    staged = {name: layout.stage(text) for name, text in plain.items()}
    files = raw if layout.history is None else [(name, staged[name].encode("utf-8")) for name, _ in raw]
    return _Run(
        layout=layout,
        plain=plain,
        staged=staged,
        files=files,
        conninfo=runner.migretti_conninfo(params, layout.tracking_schema),
        rerun=rerun,
        placeholder_checksums=layout.placeholder_checksums(list(rerun.values())),
        legacy=legacy,
        shipped_ids=shipped_ids,
    )


def _untracked_rows(conn: Any, run: _Run, state: MigrettiState) -> dict[str, str] | None:
    ours = set(state.applied) & run.shipped_ids
    if ours and not any(state.applied[i] in run.placeholder_checksums for i in ours):
        return None
    catalog = read_catalog(conn, run.layout.target)
    if ENGINE_MARKER_TABLE not in catalog.tables:
        return None
    models = models_for(run.plain)
    level = adoption_level(models, catalog)
    print(
        f"stabilize's tables in {run.layout.target} have no migration history; the catalog matches the first "
        f"{level} migration(s), up to {models[level - 1].filename}"
    )
    return legacy_rows_for(run.plain, level)


def _take_over(conn: Any, run: _Run, workdir: Path) -> None:
    state = runner.snapshot(workdir, run.conninfo)
    check_history(run.staged, state)
    legacy, adopting = run.legacy, False
    if legacy is None:
        legacy = _untracked_rows(conn, run, state)
        adopting = legacy is not None
    if legacy is None:
        return
    plan = plan_takeover(legacy, run.staged, state, run.placeholder_checksums)
    for name in plan.retired:
        print(f"{name} was applied by the old runner and is no longer shipped; it is not recorded in migretti")
    if not plan.needed:
        return
    names = {migration_id(name): name for name in run.staged}
    source = "the catalog shows" if adopting else f"{LEGACY_TABLE} says"
    print(
        f"Recording in migretti {len(plan.fixes)} migration(s) that {source} were applied; their SQL is not run again"
    )
    if plan.placeholders:
        files = [
            (names[i], run.layout.placeholder(run.rerun.get(i) if adopting else None).encode("utf-8"))
            for i in plan.placeholders
        ]
        with runner.staged(files) as placeholder_dir:
            runner.run(["apply", "--yes", "--allow-out-of-order"], placeholder_dir, run.conninfo, echo=False)
        print(f"  migretti recorded {len(plan.placeholders)} placeholder entries under those ids")
    for mig_id in plan.fixes:
        runner.run(["fix", mig_id, "--applied", "--yes"], workdir, run.conninfo, echo=False)
        print(f"  mg fix {mig_id} --applied: {names[mig_id]} recorded with the shipped file's checksum")
    if not adopting:
        print(f"{LEGACY_TABLE} is left unchanged as the record of what the old runner applied and when")


def mg_up(
    db_url: str | None = None,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    history_schema: str | None = None,
    config_path: Path | None = None,
) -> None:
    """Apply pending migrations with migretti, first handing over any history migretti does not hold."""
    psycopg = import_psycopg()
    params, schema, history = _target(db_url, history_schema, config_path)
    if not lock_timeout > 0:
        print("Error: --lock-timeout must be a positive number of seconds")
        sys.exit(1)
    _require_migretti()
    announce_driver(psycopg)
    print(f"Database: {_describe(params)}")

    try:
        with psycopg.connect(**params, autocommit=True) as conn:
            _acquire_lock(psycopg, conn, lock_timeout)
            _refuse_role_shadow(conn, schema)
            run = _prepare(conn, params, schema, history)
            if schema:
                conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                print(f"Target schema: {schema}")
            if run.layout.history:
                conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{run.layout.history}"')
                print(
                    f"stabilize's migretti history is kept in schema {run.layout.history}; "
                    f"its tables are in {run.layout.target}"
                )
            with runner.staged(run.files) as workdir:
                _take_over(conn, run, workdir)
                print("Checking applied migrations against the shipped files: mg verify", flush=True)
                runner.run(["verify"], workdir, run.conninfo)
                print("Applying pending migrations: mg apply", flush=True)
                runner.run(["apply", "--yes"], workdir, run.conninfo)
    except (TakeoverRefusedError, AdoptionRefusedError) as e:
        print(f"Error: {e}")
        sys.exit(1)
    except runner.MigrettiFailedError as e:
        print(f"Error: {e}; stabilize mg-up stopped there")
        sys.exit(1)
    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(1)


def _status_rows(run: _Run, state: MigrettiState, untracked: set[str]) -> list[tuple[str, str]]:
    legacy = run.legacy or {}
    rows: list[tuple[str, str]] = []
    for name, text in run.staged.items():
        mig_id = migration_id(name)
        recorded = state.applied.get(mig_id)
        if mig_id in state.failed:
            status = "failed"
        elif recorded == migretti_checksum(text):
            status = "applied"
        elif recorded in run.placeholder_checksums:
            status = "takeover"
        elif recorded is not None:
            status = "MISMATCH"
        elif name in legacy:
            status = "old-runner"
        elif name in untracked:
            status = "untracked"
        else:
            status = "pending"
        rows.append((status, name))
    return rows


def _print_status(run: _Run, state: MigrettiState, untracked: set[str]) -> None:
    layout = run.layout
    where = f"schema {layout.history} (tables in {layout.target})" if layout.history else f"schema {layout.target}"
    print(f"Tracked by migretti in {where}")
    print(f"{'Status':<11} {'Migration'}")
    print("-" * 80)
    rows = _status_rows(run, state, untracked)
    for status, name in rows:
        print(f"{status:<11} {name}")
    foreign = sorted(set(state.applied) - run.shipped_ids)
    if foreign:
        print(f"In migretti's history but not shipped by this stabilize: {', '.join(foreign)}")
    retired = sorted(name for name in (run.legacy or {}) if name in RETIRED_MIGRATIONS)
    if retired:
        print(f"Applied by the old runner and no longer shipped: {', '.join(retired)}")
    if any(status in ("old-runner", "takeover", "untracked") for status, _ in rows):
        print("Run stabilize mg-up to record in migretti what is already applied")
    applied = sum(1 for status, _ in rows if status == "applied")
    print(f"Applied {applied} of {len(rows)}")


def mg_status(
    db_url: str | None = None, history_schema: str | None = None, config_path: Path | None = None
) -> None:
    """Report migration state from migretti, without creating or altering anything."""
    psycopg = import_psycopg()
    params, schema, history = _target(db_url, history_schema, config_path)
    _require_migretti()
    announce_driver(psycopg)
    print(f"Database: {_describe(params)}")

    untracked: set[str] = set()
    try:
        with psycopg.connect(**params, autocommit=True) as conn:
            run = _prepare(conn, params, schema, history, lenient=True)
            with runner.staged([]) as empty:
                state = runner.snapshot(empty, run.conninfo)
            if run.legacy is None and not set(state.applied) & run.shipped_ids:
                catalog = read_catalog(conn, run.layout.target)
                if ENGINE_MARKER_TABLE in catalog.tables:
                    try:
                        level = adoption_level(models_for(run.plain), catalog)
                        untracked = set(sorted(run.plain)[:level])
                    except AdoptionRefusedError as e:
                        print(f"Warning: {e}")
    except (TakeoverRefusedError, AdoptionRefusedError) as e:
        print(f"Error: {e}")
        sys.exit(1)
    except runner.MigrettiFailedError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(1)
    _print_status(run, state, untracked)
