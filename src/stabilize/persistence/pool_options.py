"""Caller-supplied options for PostgreSQL connection pools."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

if TYPE_CHECKING:
    from collections.abc import Callable

DEFAULT_MIN_SIZE = 5
DEFAULT_MAX_SIZE = 15

DEFAULT_HEALTH_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class PoolOptions:
    """Options applied when a PostgreSQL pool is created.

    stabilize supplies no ``statement_timeout`` and no ``lock_timeout`` of its
    own: without ``connect_kwargs`` or a DSN carrying ``options=-c ...``, a
    connection inherits the server defaults, and a blocked query holds a
    backend slot for as long as the server allows.

    ``acquire_timeout`` bounds how long a borrow waits for a free connection.
    psycopg_pool's own default is 30s, which is the latency an unreachable
    database imposes on every borrow, including a health check.

    Pools are keyed by connection string AND by these options, so two callers
    asking for different options get different pools rather than silently
    sharing whichever was created first.

    ``configure`` runs once per new connection and must leave that connection
    outside a transaction: psycopg_pool discards any connection a configure
    callback leaves INTRANS, so a callback issuing ``SET`` without a commit
    destroys every connection and borrows then fail with PoolTimeout. Call
    ``conn.commit()``, or set ``conn.autocommit = True`` first.

    ``check`` and ``reset`` are psycopg_pool's: ``check`` runs each time a
    connection is handed out, ``reset`` each time one is returned.
    ``max_idle`` is the seconds an idle connection above ``min_size`` is kept
    (psycopg_pool's default is 600).

    ``on_borrow`` runs on the connection each time the engine borrows one,
    before any engine statement, for reads and transactions alike. It must
    leave the connection outside a transaction, as ``configure`` must; a
    session-level setting made there lasts for the whole borrow, and
    ``reset`` can clear it when the connection is returned. Engine threads
    and executor jobs run in a copy of the contextvars context of the code
    that started them, so ``on_borrow`` can read the caller's context.
    """

    min_size: int = DEFAULT_MIN_SIZE
    max_size: int = DEFAULT_MAX_SIZE
    acquire_timeout: float | None = None
    connect_kwargs: dict[str, Any] = field(default_factory=dict)
    configure: Callable[[Any], None] | None = None
    check: Callable[[Any], None] | None = None
    max_idle: float | None = None
    on_borrow: Callable[[Any], None] | None = None
    reset: Callable[[Any], None] | None = None
    tenant_setting: str | None = None
    audit_key: bytes | None = None

    def __post_init__(self) -> None:
        if self.max_idle is not None and (
            isinstance(self.max_idle, bool) or not isinstance(self.max_idle, (int, float)) or not self.max_idle > 0
        ):
            raise ValueError(f"max_idle must be a positive number of seconds, not {self.max_idle!r}")
        if self.tenant_setting is not None:
            from stabilize.persistence.tenancy import validate_tenant_setting

            validate_tenant_setting(self.tenant_setting)
        if self.audit_key is not None and not isinstance(self.audit_key, bytes):
            raise ValueError("audit_key must be bytes")

    def key(self) -> tuple[Any, ...]:
        """A hashable identity for pool sharing."""
        return (
            self.min_size,
            self.max_size,
            self.acquire_timeout,
            tuple(sorted(self.connect_kwargs.items())),
            _identity(self.configure),
            _identity(self.check),
            self.max_idle,
            _identity(self.on_borrow),
            _identity(self.reset),
            self.tenant_setting,
            self.audit_key,
        )


def _identity(callback: Callable[[Any], None] | None) -> int | None:
    return id(callback) if callback is not None else None


DEFAULT_POOL_OPTIONS = PoolOptions()


_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_KV_OPTIONS_RE = re.compile(r"(?i)(?:^|\s)options\s*=")


def validate_schema_name(schema: str) -> str:
    """Validate a schema name before it reaches a connection option string."""
    if not _SCHEMA_NAME_RE.match(schema) or len(schema) > 63:
        raise ValueError(
            f"Invalid schema name {schema!r}: expected a plain PostgreSQL identifier "
            "(letters, digits, underscore; max 63 chars)"
        )
    return schema


def dsn_sets_options(connection_string: str | None) -> bool:
    """Whether *connection_string* already carries a libpq ``options`` value.

    A psycopg keyword argument BEATS the conninfo, so a schema= that ignored
    this would silently replace the caller's own search_path rather than defer
    to it. Covers both DSN spellings: a URL query parameter and a
    keyword/value string.
    """
    if not connection_string:
        return False
    if "://" in connection_string:
        return "options" in parse_qs(urlsplit(connection_string).query)
    return bool(_KV_OPTIONS_RE.search(connection_string))


def with_schema(
    options: PoolOptions | None,
    schema: str | None,
    connection_string: str | None = None,
) -> PoolOptions | None:
    """Return *options* with a search_path for *schema* merged in.

    Delivered as a libpq connect option rather than by schema-qualifying every
    statement: the queries stay as written, and pools are keyed by options, so
    two schemas get two pools instead of silently sharing one.

    A caller who already set their own ``options`` keeps it, whether they set
    it in PoolOptions.connect_kwargs or in the DSN itself. Both are deliberate
    and neither is second-guessed.
    """
    if schema is None:
        return options
    validate_schema_name(schema)
    base = options or PoolOptions()
    if "options" in base.connect_kwargs or dsn_sets_options(connection_string):
        return options
    merged = dict(base.connect_kwargs)
    merged["options"] = f"-c search_path={schema}"
    return replace(base, connect_kwargs=merged)
