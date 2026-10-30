"""The CLI resolves a database URL as libpq and the runtime do (#99 R9), and explicit settings beat the environment."""

from __future__ import annotations

import pytest
from psycopg.conninfo import conninfo_to_dict

from stabilize.cli.config import apply_schema_override, connection_params, parse_db_url

URLS = [
    "postgresql://u:p@localhost/db?host=/var/run/postgresql",
    "postgresql://u:p@h1:5432/db1?dbname=db2&port=6000",
    "postgresql://u:p@h/db?user=other&sslmode=require",
]


@pytest.mark.parametrize("url", URLS, ids=["socket-host", "dbname-and-port", "user"])
def test_the_cli_connects_where_libpq_and_the_runtime_would(url: str) -> None:
    ours = connection_params(parse_db_url(url))
    libpq = conninfo_to_dict(url)
    differing = {key: (ours.get(key), value) for key, value in libpq.items() if str(ours.get(key)) != str(value)}
    assert not differing, f"the CLI and libpq resolve {url} differently: {differing}"


def test_an_explicit_schema_in_the_url_beats_mg_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MG_SCHEMA", "from_environment")
    config = apply_schema_override(parse_db_url("postgresql://u:p@h/db?schema=from_url"))
    assert config["schema"] == "from_url", f"MG_SCHEMA replaced the URL's own schema with {config['schema']!r}"


def test_mg_schema_still_applies_when_the_url_names_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MG_SCHEMA", "from_environment")
    assert apply_schema_override(parse_db_url("postgresql://u:p@h/db"))["schema"] == "from_environment"


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://u:p@h/db?history_schema=hist&sslmode=disable",
        "host=h dbname=db history_schema=hist sslmode=disable",
    ],
    ids=["url", "keyword"],
)
def test_history_schema_is_stabilize_s_own_and_never_sent_to_libpq(url: str) -> None:
    config = parse_db_url(url)
    params = connection_params(config)
    assert config.get("history_schema") == "hist", f"history_schema was not read from {url}: {config}"
    assert "history_schema" not in params and params.get("sslmode") == "disable", params
