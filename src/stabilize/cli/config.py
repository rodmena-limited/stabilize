"""Configuration loading utilities for Stabilize CLI."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, quote, unquote

if TYPE_CHECKING:
    from typing import Any

# Migration tracking table
MIGRATION_TABLE = "stabilize_migrations"

# Plain PostgreSQL identifier: interpolated into DDL, so nothing else is legal.
_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_schema_name(schema: str) -> str:
    """Validate a target schema name as a plain PostgreSQL identifier."""
    if not _SCHEMA_NAME_RE.match(schema) or len(schema) > 63:
        print(f"Error: Invalid schema name: {schema!r}")
        print("Expected a plain PostgreSQL identifier (letters, digits, underscore; max 63 chars)")
        sys.exit(1)
    return schema


def load_config() -> dict[str, Any]:
    """Load database config from mg.yaml or environment."""
    config: dict[str, Any] | None = None

    db_url = os.environ.get("MG_DATABASE_URL")
    if db_url:
        config = parse_db_url(db_url)
    else:
        # Try to load mg.yaml
        mg_yaml = Path("mg.yaml")
        if mg_yaml.exists():
            try:
                import yaml

                with open(mg_yaml) as f:
                    raw = yaml.safe_load(f)
                    config = raw.get("database", {}) if raw else {}
            except ImportError:
                print("Warning: PyYAML not installed, cannot read mg.yaml")
                print("Set MG_DATABASE_URL environment variable instead")
                sys.exit(1)

    if config is None:
        print("Error: No database configuration found")
        print("Either create mg.yaml or set MG_DATABASE_URL environment variable")
        sys.exit(1)

    schema = os.environ.get("MG_SCHEMA")
    if schema:
        config["schema"] = schema
    return config


_KV_SECRET_RE = re.compile(
    r"(?i)\b(password|passfile|sslpassword)\s*=\s*('(?:[^'\\]|\\.)*'|\S*)"
)

_ECHO_LIMIT = 200


def _redact_userinfo(url: str) -> str:
    scheme, separator, rest = url.partition("://")
    if not separator:
        rest = url

    userinfo, at_sign, hostpart = rest.rpartition("@")
    if not at_sign:
        return url

    user, colon, _password = userinfo.partition(":")
    if not colon:
        return url

    prefix = f"{scheme}://" if separator else ""
    return f"{prefix}{user}:***@{hostpart}"


def redact_db_url(url: str) -> str:
    """Return *url* safe to echo in an error message.

    Covers both DSN forms an operator may supply. Userinfo is split on the
    LAST ``@`` so a password containing ``@`` or ``/`` -- neither of which a
    malformed URL is obliged to percent-encode -- is covered rather than
    partially echoed, and libpq keyword/value secrets are scrubbed too.
    Control characters are escaped so a crafted value cannot forge a second
    log line, and the result is length-capped.
    """
    redacted = _KV_SECRET_RE.sub(r"\1=***", _redact_userinfo(url))
    redacted = redacted.encode("unicode_escape").decode("ascii")
    if len(redacted) > _ECHO_LIMIT:
        redacted = f"{redacted[:_ECHO_LIMIT]}... (truncated)"
    return redacted


def connection_params(config: dict[str, Any]) -> dict[str, Any]:
    """Build libpq connection parameters from *config*.

    Returned as a mapping rather than a conninfo string: values are never
    quoted or concatenated, so no value can alter another parameter, and an
    absent password is expressed by omitting the key so libpq falls back to
    PGPASSWORD/.pgpass.
    """
    params: dict[str, Any] = {
        "host": config["host"],
        "port": config.get("port", 5432),
        "user": config.get("user", "postgres"),
        "dbname": config["dbname"],
    }
    password = config.get("password")
    if password:
        params["password"] = password
    return params


def build_db_url(config: dict[str, Any]) -> str:
    """Build a ``postgres://`` URL from *config*.

    Userinfo is percent-encoded, and the password is omitted entirely when
    absent rather than emitted as an empty value.
    """
    user = quote(str(config.get("user", "postgres")), safe="")
    password = config.get("password")
    userinfo = f"{user}:{quote(str(password), safe='')}" if password else user
    host = config.get("host", "localhost")
    port = config.get("port", 5432)
    dbname = quote(str(config.get("dbname", "stabilize")), safe="")
    return f"postgres://{userinfo}@{host}:{port}/{dbname}"


def parse_db_url(url: str) -> dict[str, Any]:
    """Parse a database URL into connection parameters."""
    # postgres://user:pass@host:port/dbname?schema=name
    pattern = (
        r"postgres(?:ql)?://(?:(?P<user>[^:]+)(?::(?P<password>[^@]+))?@)?"
        r"(?P<host>[^:/]+)(?::(?P<port>\d+))?/(?P<dbname>[^?]+)(?:\?(?P<query>.*))?$"
    )
    match = re.match(pattern, url)
    if not match:
        print(f"Error: Invalid database URL: {redact_db_url(url)}")
        print("Expected postgres://[user[:password]@]host[:port]/dbname[?schema=name]")
        sys.exit(1)

    user = match.group("user")
    password = match.group("password")
    config: dict[str, Any] = {
        "host": match.group("host"),
        "port": int(match.group("port") or 5432),
        "user": unquote(user) if user else "postgres",
        "password": unquote(password) if password is not None else None,
        "dbname": unquote(match.group("dbname")),
    }

    query = match.group("query")
    if query:
        schema_values = parse_qs(query).get("schema")
        if schema_values:
            config["schema"] = schema_values[-1]
    return config
