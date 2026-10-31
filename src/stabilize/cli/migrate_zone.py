"""The TimeZone check mg-up runs before processed_at becomes TIMESTAMPTZ.

The conversion has no USING clause, so PostgreSQL reads every stored naive value in the
session's TimeZone. The values were written by the engine's processes with NOW(), under
whatever zone their role or database setting gave them. mg-up therefore refuses to convert
while a role or database setting names a zone other than its own, unless the operator
says which zone the writers used.
"""

from __future__ import annotations

from typing import Any

PROCESSED_AT_MIGRATION_ID = "01M68QC4M0YQASJV3NP98564RB"


class ZoneRefusedError(RuntimeError):
    """mg-up will not convert processed_at under an unknown writers' zone."""


def session_timezone(conn: Any) -> str:
    row = conn.execute("SHOW TimeZone").fetchone()
    return str(row[0]) if row else "UTC"


def writer_timezones(conn: Any) -> dict[str, str]:
    """Every TimeZone a role or database setting pins for this database, labelled by what pins it."""
    rows = conn.execute(
        "SELECT COALESCE(r.rolname, ''), COALESCE(d.datname, ''), s.setconfig "
        "FROM pg_db_role_setting s "
        "LEFT JOIN pg_roles r ON r.oid = s.setrole "
        "LEFT JOIN pg_database d ON d.oid = s.setdatabase "
        "WHERE s.setdatabase = 0 OR d.datname = current_database()"
    ).fetchall()
    zones: dict[str, str] = {}
    for rolname, datname, setconfig in rows:
        for entry in setconfig or []:
            key, _, value = str(entry).partition("=")
            if key.strip().lower() != "timezone":
                continue
            where = " in ".join(
                part
                for part in (f"role {rolname}" if rolname else "", f"database {datname}" if datname else "")
                if part
            )
            zones[where or "server"] = value.strip().strip("'\"")
    return zones


def check_assumed_zone(conn: Any, zone: str) -> None:
    try:
        conn.execute("SELECT now() AT TIME ZONE %s", (zone,)).fetchone()
    except Exception as exc:
        raise ZoneRefusedError(f"--assume-writer-timezone {zone!r} is not a zone PostgreSQL knows: {exc}") from exc


def conversion_timezone(conn: Any, pending: bool, assumed: str | None) -> str | None:
    """The zone the processed_at conversion runs under, or None when nothing has to change.

    Raises ZoneRefusedError when the conversion is pending and a role or database setting
    names a different zone from this session's, and no zone was assumed.
    """
    session = session_timezone(conn)
    print(f"TimeZone: {session}")
    if assumed is not None:
        check_assumed_zone(conn, assumed)
    if not pending:
        return None
    if assumed is not None:
        print(f"processed_at is converted to TIMESTAMPTZ reading the stored values as {assumed} time")
        return assumed
    foreign = {where: zone for where, zone in writer_timezones(conn).items() if zone.lower() != session.lower()}
    if foreign:
        listed = "; ".join(f"{where} is set to {zone}" for where, zone in sorted(foreign.items()))
        raise ZoneRefusedError(
            "processed_at is about to become TIMESTAMPTZ and its stored values carry no zone. "
            f"This session runs in {session}, but {listed}. The engine processes that wrote the rows "
            "may have used one of those. Pass --assume-writer-timezone <zone> naming the zone the "
            "writers used (run the processed-message retention first so few rows are converted)."
        )
    return None


def with_timezone(conninfo: str, zone: str) -> str:
    """*conninfo* with TimeZone pinned for the migrations it runs."""
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    params = conninfo_to_dict(conninfo)
    existing = str(params.get("options") or "").strip()
    params["options"] = f"{existing} -c TimeZone={zone}".strip()
    return make_conninfo("", **{key: str(value) for key, value in params.items()})
