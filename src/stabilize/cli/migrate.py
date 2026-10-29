"""stabilize mg-up and mg-status: stabilize's PostgreSQL migrations, applied and reported by migretti."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from stabilize.cli import migretti_runner as runner
from stabilize.cli.config import (
    apply_schema_override,
    connection_params,
    load_config,
    parse_db_url,
    validate_schema_name,
)
from stabilize.cli.driver import announce_driver, import_psycopg
from stabilize.cli.legacy_history import (
    LEGACY_TABLE,
    PLACEHOLDER_CHECKSUM,
    RETIRED_MIGRATIONS,
    MigrettiState,
    TakeoverRefused,
    check_history,
    plan_takeover,
)
from stabilize.cli.migrations import get_migration_files, migration_id, migration_text, migretti_checksum
from stabilize.redaction import redact_text

MG_UP_LOCK_CLASS = 0x53544D47
MG_UP_LOCK_KEY = "stabilize mg-up"
DEFAULT_LOCK_TIMEOUT_SECONDS = 900.0


def _target(db_url: str | None) -> tuple[dict[str, Any], str | None]:
    config = apply_schema_override(parse_db_url(db_url)) if db_url else load_config()
    schema = config.get("schema")
    if schema:
        schema = validate_schema_name(schema)
    return connection_params(config), schema


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


def _read_legacy(conn: Any) -> dict[str, str] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (LEGACY_TABLE,))
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        cur.execute(f"SELECT name, checksum FROM {LEGACY_TABLE}")
        return {str(name): str(checksum) for name, checksum in cur.fetchall()}


def _take_over(legacy: dict[str, str] | None, shipped: dict[str, str], workdir: Path, conninfo: str) -> None:
    state = runner.snapshot(workdir, conninfo)
    check_history(shipped, state)
    if legacy is None:
        return
    plan = plan_takeover(legacy, shipped, state)
    for name in plan.retired:
        print(f"{name} was applied by the old runner and is no longer shipped; it is not recorded in migretti")
    if not plan.needed:
        return
    names = {migration_id(name): name for name in shipped}
    print(
        f"Recording in migretti {len(plan.fixes)} migration(s) that {LEGACY_TABLE} says were applied; "
        "their SQL is not run again"
    )
    if plan.placeholders:
        files = runner.placeholder_files({mig_id: names[mig_id] for mig_id in plan.placeholders})
        with runner.staged(files) as placeholder_dir:
            runner.run(["apply", "--yes", "--allow-out-of-order"], placeholder_dir, conninfo, echo=False)
        print(f"  migretti recorded {len(plan.placeholders)} placeholder entries (SELECT 1) under those ids")
    for mig_id in plan.fixes:
        runner.run(["fix", mig_id, "--applied", "--yes"], workdir, conninfo, echo=False)
        print(f"  mg fix {mig_id} --applied: {names[mig_id]} recorded with the shipped file's checksum")
    print(f"{LEGACY_TABLE} is left unchanged as the record of what the old runner applied and when")


def mg_up(db_url: str | None = None, lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS) -> None:
    """Apply pending migrations with migretti, first handing over any history the old runner recorded."""
    psycopg = import_psycopg()
    params, schema = _target(db_url)
    if not lock_timeout > 0:
        print("Error: --lock-timeout must be a positive number of seconds")
        sys.exit(1)
    _require_migretti()
    files = get_migration_files()
    shipped = {name: migration_text(raw) for name, raw in files}
    announce_driver(psycopg)

    try:
        conninfo = runner.migretti_conninfo(params, schema)
        with psycopg.connect(**params, autocommit=True) as conn:
            _acquire_lock(psycopg, conn, lock_timeout)
            if schema:
                conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                conn.execute(f'SET search_path TO "{schema}"')
                print(f"Target schema: {schema}")
            legacy = _read_legacy(conn)
            with runner.staged(files) as workdir:
                _take_over(legacy, shipped, workdir, conninfo)
                print("Checking applied migrations against the shipped files: mg verify", flush=True)
                runner.run(["verify"], workdir, conninfo)
                print("Applying pending migrations: mg apply", flush=True)
                runner.run(["apply", "--yes"], workdir, conninfo)
    except TakeoverRefused as e:
        print(f"Error: {e}")
        sys.exit(1)
    except runner.MigrettiFailed as e:
        print(f"Error: {e}; stabilize mg-up stopped there")
        sys.exit(1)
    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(1)


def _status_rows(shipped: dict[str, str], legacy: dict[str, str], state: MigrettiState) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for name, text in shipped.items():
        recorded = state.applied.get(migration_id(name))
        if migration_id(name) in state.failed:
            status = "failed"
        elif recorded == migretti_checksum(text):
            status = "applied"
        elif recorded == PLACEHOLDER_CHECKSUM:
            status = "takeover"
        elif recorded is not None:
            status = "MISMATCH"
        elif name in legacy:
            status = "old-runner"
        else:
            status = "pending"
        rows.append((status, name))
    return rows


def _print_status(shipped: dict[str, str], legacy: dict[str, str], state: MigrettiState, schema: str | None) -> None:
    where = f"schema {schema}" if schema else "the connection's default schema"
    print(f"Tracked by migretti in {where}")
    print(f"{'Status':<11} {'Migration'}")
    print("-" * 80)
    rows = _status_rows(shipped, legacy, state)
    for status, name in rows:
        print(f"{status:<11} {name}")
    shipped_ids = {migration_id(name) for name in shipped}
    foreign = sorted(set(state.applied) - shipped_ids)
    if foreign:
        print(f"In migretti's history but not shipped by this stabilize: {', '.join(foreign)}")
    retired = sorted(name for name in legacy if name in RETIRED_MIGRATIONS)
    if retired:
        print(f"Applied by the old runner and no longer shipped: {', '.join(retired)}")
    if any(status in ("old-runner", "takeover") for status, _ in rows):
        print(f"Run stabilize mg-up to record in migretti what {LEGACY_TABLE} says the old runner applied")
    applied = sum(1 for status, _ in rows if status == "applied")
    print(f"Applied {applied} of {len(rows)}")


def mg_status(db_url: str | None = None) -> None:
    """Report migration state from migretti, without creating or altering anything."""
    psycopg = import_psycopg()
    params, schema = _target(db_url)
    _require_migretti()
    files = get_migration_files()
    shipped = {name: migration_text(raw) for name, raw in files}
    announce_driver(psycopg)

    try:
        conninfo = runner.migretti_conninfo(params, schema)
        with psycopg.connect(**params, autocommit=True) as conn:
            if schema:
                conn.execute(f'SET search_path TO "{schema}"')
            legacy = _read_legacy(conn) or {}
        with runner.staged(files) as workdir:
            state = runner.snapshot(workdir, conninfo)
    except runner.MigrettiFailed as e:
        print(f"Error: {e}")
        sys.exit(1)
    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(1)
    _print_status(shipped, legacy, state, schema)
