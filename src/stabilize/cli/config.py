"""Configuration loading utilities for Stabilize CLI."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, quote, unquote, urlencode

from stabilize.redaction import redact_db_url as redact_db_url

if TYPE_CHECKING:
    from typing import Any

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


TARGET_VARIABLES = (
    "MG_DB_HOST",
    "MG_DB_PORT",
    "MG_DB_USER",
    "MG_DB_PASSWORD",
    "MG_DB_NAME",
    "PGHOST",
    "PGPORT",
    "PGDATABASE",
)


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    """Resolve the database target from MG_DATABASE_URL, or from the file named by --config.

    A working-directory mg.yaml is never read on its own: migretti owns that
    name, and a bare ``stabilize mg-up`` in a repo configured for migretti
    would otherwise connect to whatever that file names (B51).
    """
    db_url = os.environ.get("MG_DATABASE_URL")
    if db_url:
        present = [name for name in TARGET_VARIABLES if os.environ.get(name)]
        if present:
            print(
                "Error: MG_DATABASE_URL is set together with "
                + ", ".join(present)
                + "; the URL names the target, so unset the others"
            )
            sys.exit(1)
        return apply_schema_override(parse_db_url(db_url))

    if config_path is None:
        print("Error: No database configuration found")
        print("Set MG_DATABASE_URL, pass --db-url, or name a file with --config PATH")
        sys.exit(1)
    try:
        import yaml
    except ImportError:
        print("Error: PyYAML is not installed, so --config cannot be read")
        print("Set MG_DATABASE_URL instead")
        sys.exit(1)
    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f)
    except OSError as exc:
        print(f"Error: cannot read {config_path}: {exc.strerror}")
        sys.exit(1)
    config: dict[str, Any] = (raw or {}).get("database") or {}
    if not config:
        print(f"Error: {config_path} has no database: section")
        sys.exit(1)
    announce_file_source(config_path, config)
    return apply_schema_override(config)


def connection_params(config: dict[str, Any]) -> dict[str, Any]:
    """Build libpq connection parameters from *config*.

    Returned as a mapping rather than a conninfo string: values are never
    quoted or concatenated, so no value can alter another parameter, and an
    absent password is expressed by omitting the key so libpq falls back to
    PGPASSWORD/.pgpass.

    Query parameters carried on the URL -- sslmode, sslrootcert, sslcert,
    sslkey, connect_timeout, application_name and the rest -- are passed
    through. A query parameter wins over the same component of the URL, as
    libpq resolves it. A URL without a user or port leaves them to libpq's
    own defaults (PGUSER, PGPORT, the operating system user), as the engine
    does; a mg.yaml mapping without them keeps the user postgres and port 5432.
    """
    components = {
        "host": config.get("host"),
        "port": config.get("port", 5432),
        "user": config.get("user", "postgres"),
        "dbname": config.get("dbname"),
    }
    params: dict[str, Any] = {key: value for key, value in components.items() if value is not None}
    password = config.get("password")
    if password:
        params["password"] = password
    params.update(config.get("connect_params") or {})
    return params


def build_db_url(config: dict[str, Any]) -> str:
    """Build a ``postgres://`` URL that libpq resolves as ``connection_params(config)``.

    Every libpq parameter is kept, query parameters such as sslmode included,
    and stabilize's own settings (schema, history_schema) are left out. Userinfo
    is percent-encoded, and the password is omitted when absent rather than
    emitted as an empty value.
    """
    params = connection_params(config)
    user = params.pop("user", None)
    password = params.pop("password", None)
    host = params.pop("host", None)
    port = params.pop("port", None)
    dbname = params.pop("dbname", None)
    userinfo = quote(str(user), safe="") if user else ""
    if password:
        userinfo = f"{userinfo}:{quote(str(password), safe='')}"
    query = {key: str(value) for key, value in params.items()}
    netloc = ""
    if host and not str(host).startswith("/") and "," not in str(host):
        netloc = f"[{host}]" if ":" in str(host) else str(host)
        if port:
            netloc = f"{netloc}:{port}"
    else:
        if host:
            query["host"] = str(host)
        if port:
            query["port"] = str(port)
    if userinfo:
        netloc = f"{userinfo}@{netloc}"
    path = "/" + quote(str(dbname), safe="") if dbname else "/"
    return f"postgres://{netloc}{path}" + (f"?{urlencode(query)}" if query else "")


def apply_schema_override(config: dict[str, Any]) -> dict[str, Any]:
    """Apply MG_SCHEMA and MG_HISTORY_SCHEMA to *config* when the URL did not carry them.

    Shared by both entry points. load_config() applied this and the --db-url
    path did not, so `MG_SCHEMA=x stabilize mg-status --db-url ...` silently
    looked in public and reported the migration table as nonexistent, which
    reads as "nothing has ever been applied" rather than "wrong schema".
    """
    for key, variable in (("schema", "MG_SCHEMA"), ("history_schema", "MG_HISTORY_SCHEMA")):
        value = os.environ.get(variable)
        if value and not config.get(key):
            config[key] = value
    return config


def parse_db_url(url: str) -> dict[str, Any]:
    """Parse a database URL into connection parameters."""
    # postgres://user:pass@host:port/dbname?schema=name
    pattern = (
        r"postgres(?:ql)?://(?:(?P<user>[^:]+)(?::(?P<password>[^@]+))?@)?"
        r"(?P<host>[^:/@]+)(?::(?P<port>\d+))?/(?P<dbname>[^?]+)(?:\?(?P<query>.*))?$"
    )
    match = re.match(pattern, url)
    if not match:
        return _parse_libpq_conninfo(url)

    user = match.group("user")
    password = match.group("password")
    port = match.group("port")
    config: dict[str, Any] = {
        "host": match.group("host"),
        "port": int(port) if port else None,
        "user": unquote(user) if user else None,
        "password": unquote(password) if password is not None else None,
        "dbname": unquote(match.group("dbname")),
    }

    query = match.group("query")
    if query:
        parsed = parse_qs(query, keep_blank_values=True)
        for key in OWN_PARAMETERS:
            values = parsed.pop(key, None)
            if values:
                config[key] = values[-1]
        # Every remaining query parameter is a libpq connection parameter and
        # must survive. Dropping them silently contacted a TLS-mandatory
        # database with no TLS settings at all -- a security control the
        # operator had asked for, discarded without a word.
        if parsed:
            config["connect_params"] = {key: values[-1] for key, values in parsed.items()}
    return config


OWN_PARAMETERS = ("schema", "history_schema")


def _split_own(url: str) -> tuple[str, dict[str, str]]:
    """Remove stabilize's own settings, which libpq does not know, from a conninfo string."""
    own: dict[str, str] = {}
    if "://" in url:
        base, question, query = url.partition("?")
        if not question:
            return url, own
        parsed = parse_qs(query, keep_blank_values=True)
        for key in OWN_PARAMETERS:
            values = parsed.pop(key, None)
            if values:
                own[key] = values[-1]
        rest = urlencode([(key, value) for key, values in parsed.items() for value in values])
        return (f"{base}?{rest}" if rest else base), own
    for key in OWN_PARAMETERS:
        found = re.search(rf"(?:^|\s){key}\s*=\s*(\S+)", url)
        if found:
            own[key] = found.group(1)
            url = (url[: found.start()] + url[found.end() :]).strip()
    return url, own


def _parse_libpq_conninfo(url: str) -> dict[str, Any]:
    """Parse any connection string libpq accepts: keyword/value, or a URL without a host (socket)."""
    from psycopg.conninfo import conninfo_to_dict

    text, own = _split_own(url.strip())
    try:
        params: dict[str, Any] = dict(conninfo_to_dict(text))
    except Exception:
        params = {}
    if not params:
        print(f"Error: Invalid database URL: {redact_db_url(url)}")
        print(
            "Expected postgres://[user[:password]@]host[:port]/dbname[?schema=name], "
            "a libpq keyword/value string (host=... dbname=...), or a socket URL "
            "(postgresql:///dbname?host=/var/run/postgresql)"
        )
        sys.exit(1)
    port = params.pop("port", None)
    config: dict[str, Any] = {
        "host": params.pop("host", None),
        "port": int(port) if port and str(port).isdigit() else port,
        "user": params.pop("user", None),
        "password": params.pop("password", None),
        "dbname": params.pop("dbname", None),
        "conninfo": url,
    }
    config.update(own)
    if params:
        config["connect_params"] = params
    return config
