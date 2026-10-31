"""Tenant identifiers, the tenancy-mode record, and the checks every engine part makes at construction."""

from __future__ import annotations

import re
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from stabilize.errors.base import StabilizeError

TENANT_RE = re.compile(r"^[A-Za-z0-9.:-]{1,64}$")
TENANT_SETTING_RE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
RESERVED_SETTING_CLASS = "stabilize."

MODE_TABLE = "stabilize_tenancy"
TENANT_COLUMN = "stabilize_tenant"
TENANT_CHECK = "stabilize_tenant_nonempty"
GUC = "stabilize.tenant"
ALL_TENANTS = "*"
MODE_SINGLE = "single"
MODE_TENANCY = "tenancy"
MODE_RECHECK_SECONDS = 5.0


class TenantError(StabilizeError):
    """A tenant identifier is malformed, or two engine parts carry different tenants."""


class TenancyModeError(StabilizeError):
    """The schema's tenancy mode does not admit this tenant, or the mode record is unreadable."""


class TenantMismatchError(StabilizeError):
    """A write collided with a row that belongs to another tenant, or the row is invisible to this one."""


class TenantContextError(StabilizeError):
    """The connection arrived with tenant state the engine did not set, or in a mode it cannot arm."""


class TenancyModeChanged(StabilizeError):  # noqa: N818
    """The schema's fence or mode no longer matches what this process was built for; the process must stop."""


class TenantPermissionError(StabilizeError):
    """The connection claimed every tenant without being a member of the table owner."""


def sqlstate(exc: BaseException) -> str | None:
    return getattr(exc, "sqlstate", None)


def tenant_error_from(exc: BaseException) -> StabilizeError | None:
    """The engine error a database error stands for, by SQLSTATE; None when it is not a tenancy error."""
    code = sqlstate(exc)
    if code is None and type(exc).__module__ == "sqlite3":
        text = str(exc)
        if text.startswith("ST001:"):
            return TenancyModeChanged(text)
        if text.startswith("ST002:"):
            return TenantMismatchError(text)
        if "no such function: stabilize_tenant" in text:
            return TenantContextError(
                "this SQLite connection was not opened by stabilize's ConnectionManager, so the fence cannot "
                "see a tenant on it; the file is in tenancy mode and only the engine writes to it"
            )
        return None
    if code == "ST001":
        return TenancyModeChanged(str(exc))
    if code == "ST002":
        return TenantMismatchError(str(exc))
    if code == "ST003":
        return TenantPermissionError(str(exc))
    if code in ("23502", "23514"):
        diag = getattr(exc, "diag", None)
        column = getattr(diag, "column_name", None)
        constraint = getattr(diag, "constraint_name", None)
        if column == TENANT_COLUMN or constraint == TENANT_CHECK:
            return TenancyModeChanged(str(exc))
    return None


ARM_SQL = (
    "SELECT CASE WHEN coalesce(current_setting(%(guc)s, true), '') = '' "
    "THEN set_config(%(guc)s, %(tenant)s, true) END AS armed, "
    "current_setting(%(guc)s, true) AS found, "
    f"(SELECT mode FROM {MODE_TABLE} WHERE id = 1) AS mode, "
    "(SELECT c.relrowsecurity OR c.relforcerowsecurity FROM pg_catalog.pg_class c "
    f"WHERE c.oid = to_regclass('{MODE_TABLE}')) AS policed"
)
HOST_ARM_SQL = (
    ", CASE WHEN coalesce(current_setting(%(setting)s, true), '') IN ('', %(tenant)s) "
    "THEN set_config(%(setting)s, %(tenant)s, true) END AS host_armed, "
    "current_setting(%(setting)s, true) AS host_found"
)


def _field(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[name]
    return row[index]


def check_mode_row(policed: Any, mode: Any) -> str:
    if policed is None:
        raise TenancyModeError(f"{MODE_TABLE} does not exist in this schema; run stabilize mg-up")
    if policed:
        raise TenancyModeError(
            f"{MODE_TABLE} has row-level security enabled; it must never be policed: "
            f"ALTER TABLE {MODE_TABLE} DISABLE ROW LEVEL SECURITY"
        )
    return mode_from_row((mode,) if mode is not None else None)


def arm_connection(conn: Any, tenant: str, setting: str | None, reader: ModeReader | None = None) -> str:
    """One statement: set the tenant for this transaction, read the mode record, and set the host's setting.

    Refuses a connection that already carries the tenant setting, one whose host setting holds another
    tenant, and one in autocommit mode (the settings are transaction-local). Returns the mode.
    """
    if getattr(conn, "autocommit", False):
        raise TenantContextError(
            "the connection is in autocommit mode; the engine sets the tenant per transaction and needs "
            "transactions, so remove autocommit from connect_kwargs and from on_borrow"
        )
    if reader is not None and not setting and reader.fresh() == MODE_SINGLE:
        check_mode_admits(MODE_SINGLE, tenant)
        return MODE_SINGLE
    sql = ARM_SQL + (HOST_ARM_SQL if setting else "")
    params = {"guc": GUC, "tenant": tenant, "setting": setting}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    except Exception as exc:
        if sqlstate(exc) != "42501":
            raise
        role = conn.info.user
        raise TenancyModeError(
            f"role {role!r} may not read {MODE_TABLE}, which every engine process needs: "
            f'GRANT SELECT ON {MODE_TABLE} TO "{role}"'
        ) from exc
    if row is None:
        raise TenancyModeError(f"{MODE_TABLE} could not be read on this connection")
    if _field(row, "armed", 0) is None:
        found = _field(row, "found", 1)
        raise TenantContextError(
            f"{GUC} is already {found!r} on this connection; the engine sets it per transaction and "
            "refuses a value it did not set (a host hook, ALTER ROLE or ALTER DATABASE put it there)"
        )
    if setting and _field(row, "host_armed", 4) is None:
        current = _field(row, "host_found", 5)
        raise TenantContextError(
            f"{setting} is {current!r} on this connection, which is neither '' nor the engine's tenant "
            f"{tenant!r}; with transaction pooling set server_reset_query_always = 1"
        )
    mode = check_mode_row(_field(row, "policed", 3), _field(row, "mode", 2))
    if reader is not None:
        reader.record(mode)
    check_mode_admits(mode, tenant)
    return mode


@contextmanager
def armed(conn: Any, tenant: str, setting: str | None, reader: ModeReader | None = None) -> Iterator[Any]:
    """Arm *conn* for *tenant* before its next statement and after every commit or rollback inside the block.

    Database errors raised inside the block that stand for a tenancy condition (the fence's SQLSTATEs,
    a NOT NULL or CHECK violation on the tenant column) are re-raised as the matching engine error.
    """
    conn._stabilize_arm = lambda: arm_connection(conn, tenant, setting, reader)
    conn._stabilize_pending = True
    try:
        yield conn
    except Exception as exc:
        mapped = tenant_error_from(exc)
        if mapped is not None:
            raise mapped from exc
        raise
    finally:
        conn._stabilize_arm = None
        conn._stabilize_pending = False


def refuse_foreign_row(table: str, key: str, owner: str | None, tenant: str) -> None:
    """Raise when an upsert found a conflicting row the caller does not own (rule 4)."""
    if owner is None:
        raise TenantMismatchError(
            f"{table} row {key!r} conflicts with a row this tenant cannot see; another tenant owns it"
        )
    if owner != tenant:
        raise TenantMismatchError(f"{table} row {key!r} belongs to tenant {owner!r}, not {tenant!r}")


def validate_tenant(tenant: Any) -> str:
    if tenant == "":
        return ""
    if not isinstance(tenant, str):
        raise TenantError(f"tenant must be a string, not {type(tenant).__name__}")
    if not TENANT_RE.match(tenant):
        raise TenantError(
            f"tenant {tenant!r} is not allowed: 1 to 64 characters from A-Z a-z 0-9 . : - (underscore is a LIKE "
            "wildcard and is refused)"
        )
    return tenant


def validate_tenant_setting(setting: str) -> str:
    if not TENANT_SETTING_RE.match(setting):
        raise ValueError(
            f"tenant_setting {setting!r} must be a custom GUC of the form class.name in lower case "
            "(for example app.current_tenant)"
        )
    if setting.startswith(RESERVED_SETTING_CLASS):
        raise ValueError(f"tenant_setting {setting!r} is in the class stabilize reserves for itself")
    return setting


def require_same_tenant(**parts: Any) -> str:
    """Return the tenant shared by *parts*; raise when two of them disagree."""
    seen: dict[str, str] = {}
    for name, part in parts.items():
        if part is None:
            continue
        tenant = getattr(part, "tenant", None)
        if tenant is None:
            continue
        seen[name] = tenant
    distinct = set(seen.values())
    if len(distinct) > 1:
        detail = ", ".join(f"{name}={tenant!r}" for name, tenant in seen.items())
        raise TenantError(f"these parts carry different tenants and cannot work together: {detail}")
    return next(iter(distinct), "")


def check_mode_admits(mode: str, tenant: str) -> None:
    if mode == MODE_SINGLE and tenant != "":
        raise TenancyModeError(
            f"this schema is in single-tenant mode, so tenant {tenant!r} is refused; run "
            "`stabilize mg-tenancy enable` on an empty schema first, or construct without a tenant"
        )
    if mode == MODE_TENANCY and tenant == "":
        raise TenancyModeError(
            "this schema is in tenancy mode, so a tenant is required; construct with tenant='<name>'"
        )
    if mode not in (MODE_SINGLE, MODE_TENANCY):
        raise TenancyModeError(f"{MODE_TABLE} holds an unknown mode {mode!r}")


class ModeReader:
    """The mode record of one schema as the engine last saw it; every armed statement refreshes it."""

    def __init__(self, read: Callable[[Any], str] | None = None) -> None:
        self._read = read
        self._lock = threading.Lock()
        self._mode: str | None = None
        self._read_at = 0.0

    def record(self, mode: str) -> None:
        with self._lock:
            self._mode = mode
            self._read_at = time.monotonic()

    @property
    def last(self) -> str | None:
        return self._mode

    def fresh(self) -> str | None:
        """The mode as last recorded, or None when the record is older than the re-read interval."""
        with self._lock:
            if self._mode is None or time.monotonic() - self._read_at >= MODE_RECHECK_SECONDS:
                return None
            return self._mode

    def mode(self, conn: Any, force: bool = False) -> str:
        with self._lock:
            now = time.monotonic()
            if force or self._mode is None or now - self._read_at >= MODE_RECHECK_SECONDS:
                if self._read is None:
                    raise TenancyModeError("the mode has not been read on this connection yet")
                self._mode = self._read(conn)
                self._read_at = now
            return self._mode

    def admit(self, conn: Any, tenant: str) -> None:
        check_mode_admits(self.mode(conn), tenant)


def _scalar(row: Any) -> Any:
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


def mode_from_row(row: Any) -> str:
    if row is None:
        raise TenancyModeError(
            f"{MODE_TABLE} has no row; the engine never defaults the mode. Seed it with "
            f"INSERT INTO {MODE_TABLE} (id, mode) VALUES (1, '{MODE_SINGLE}')"
        )
    return str(_scalar(row))


def postgres_mode(conn: Any) -> str:
    """Read the mode record on *conn*: a plain SELECT on an un-policed, seeded singleton."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relrowsecurity OR c.relforcerowsecurity FROM pg_catalog.pg_class c WHERE c.oid = to_regclass(%s)",
            (MODE_TABLE,),
        )
        policed = cur.fetchone()
        if policed is None:
            raise TenancyModeError(f"{MODE_TABLE} does not exist in this schema; run stabilize mg-up")
        if _scalar(policed):
            raise TenancyModeError(
                f"{MODE_TABLE} has row-level security enabled; it must never be policed: "
                f"ALTER TABLE {MODE_TABLE} DISABLE ROW LEVEL SECURITY"
            )
        try:
            cur.execute(f"SELECT mode FROM {MODE_TABLE} WHERE id = 1")
        except Exception as exc:
            if getattr(exc, "sqlstate", None) != "42501":
                raise
            role = conn.info.user
            raise TenancyModeError(
                f"role {role!r} may not read {MODE_TABLE}, which every engine process needs: "
                f'GRANT SELECT ON {MODE_TABLE} TO "{role}"'
            ) from exc
        return mode_from_row(cur.fetchone())
