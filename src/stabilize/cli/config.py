"""Configuration loading utilities for Stabilize CLI."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, quote, unquote

from stabilize.redaction import redact_db_url as redact_db_url

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


def announce_file_source(path: Path, config: dict[str, Any] | None) -> None:
    """Say which file the target came from, and what it resolved to.

    migretti owns the ``mg.yaml`` name and reads it from the working directory
    too, so a bare ``stabilize mg-up`` in a repo configured for migretti will
    silently adopt that file and connect to whatever it names. Printing the
    source and the resolved target is what lets an operator notice before the
    connection rather than after.
    """
    if not config:
        return
    host = config.get("host", "localhost")
    port = config.get("port", 5432)
    dbname = config.get("dbname") or config.get("database") or "?"
    user = config.get("user") or "?"
    target = redact_db_url(f"{user}@{host}:{port}/{dbname}")
    print(f"Using database configuration from {path}: {target}")


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
                announce_file_source(mg_yaml, config)
            except ImportError:
                print("Warning: PyYAML not installed, cannot read mg.yaml")
                print("Set MG_DATABASE_URL environment variable instead")
                sys.exit(1)

    if config is None:
        print("Error: No database configuration found")
        print("Either create mg.yaml or set MG_DATABASE_URL environment variable")
        sys.exit(1)

    return apply_schema_override(config)


def connection_params(config: dict[str, Any]) -> dict[str, Any]:
    """Build libpq connection parameters from *config*.

    Returned as a mapping rather than a conninfo string: values are never
    quoted or concatenated, so no value can alter another parameter, and an
    absent password is expressed by omitting the key so libpq falls back to
    PGPASSWORD/.pgpass.

    Query parameters carried on the URL -- sslmode, sslrootcert, sslcert,
    sslkey, connect_timeout, application_name and the rest -- are passed
    through. The URL's own components win over a same-named query parameter.
    """
    params: dict[str, Any] = dict(config.get("connect_params") or {})
    params.update(
        {
            "host": config["host"],
            "port": config.get("port", 5432),
            "user": config.get("user", "postgres"),
            "dbname": config["dbname"],
        }
    )
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


def apply_schema_override(config: dict[str, Any]) -> dict[str, Any]:
    """Apply MG_SCHEMA to *config* when the URL did not carry a schema.

    Shared by both entry points. load_config() applied this and the --db-url
    path did not, so `MG_SCHEMA=x stabilize mg-status --db-url ...` silently
    looked in public and reported the migration table as nonexistent, which
    reads as "nothing has ever been applied" rather than "wrong schema".
    """
    schema = os.environ.get("MG_SCHEMA")
    if schema:
        config["schema"] = schema
    return config


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
        parsed = parse_qs(query, keep_blank_values=True)
        schema_values = parsed.pop("schema", None)
        if schema_values:
            config["schema"] = schema_values[-1]
        # Every remaining query parameter is a libpq connection parameter and
        # must survive. Dropping them silently contacted a TLS-mandatory
        # database with no TLS settings at all -- a security control the
        # operator had asked for, discarded without a word.
        if parsed:
            config["connect_params"] = {key: values[-1] for key, values in parsed.items()}
    return config
