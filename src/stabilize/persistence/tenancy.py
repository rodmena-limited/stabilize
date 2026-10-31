"""Tenant identifiers, the tenancy-mode record, and the checks every engine part makes at construction."""

from __future__ import annotations

import re
import threading
import time
from collections.abc import Callable
from typing import Any

from stabilize.errors.base import StabilizeError

TENANT_RE = re.compile(r"^[A-Za-z0-9.:-]{1,64}$")
TENANT_SETTING_RE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
RESERVED_SETTING_CLASS = "stabilize."

MODE_TABLE = "stabilize_tenancy"
MODE_SINGLE = "single"
MODE_TENANCY = "tenancy"
MODE_RECHECK_SECONDS = 5.0


class TenantError(StabilizeError):
    """A tenant identifier is malformed, or two engine parts carry different tenants."""


class TenancyModeError(StabilizeError):
    """The schema's tenancy mode does not admit this tenant, or the mode record is unreadable."""


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
    """The mode record of one schema, read on a borrowed connection and re-read at most every 5 s."""

    def __init__(self, read: Callable[[Any], str]) -> None:
        self._read = read
        self._lock = threading.Lock()
        self._mode: str | None = None
        self._read_at = 0.0

    def mode(self, conn: Any, force: bool = False) -> str:
        with self._lock:
            now = time.monotonic()
            if force or self._mode is None or now - self._read_at >= MODE_RECHECK_SECONDS:
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
