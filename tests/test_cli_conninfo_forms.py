"""mg-up and mg-status accept keyword/value conninfo and socket URLs (#55)."""

from __future__ import annotations

from typing import Any

import pytest

from stabilize.cli import commands
from stabilize.cli.config import connection_params, parse_db_url

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def test_keyword_form_keeps_every_parameter_and_the_schema() -> None:
    config = parse_db_url(
        "host=db.example port=6432 dbname=trace user=owner sslmode=verify-full sslcert=/c.crt sslkey=/c.key schema=orch"
    )
    params = connection_params(config)
    assert config["schema"] == "orch"
    assert params == {
        "host": "db.example",
        "port": 6432,
        "user": "owner",
        "dbname": "trace",
        "sslmode": "verify-full",
        "sslcert": "/c.crt",
        "sslkey": "/c.key",
    }


def test_socket_url_with_host_parameter() -> None:
    config = parse_db_url("postgresql:///appdb?host=/var/run/postgresql&schema=stab")
    assert config["schema"] == "stab"
    assert connection_params(config) == {"host": "/var/run/postgresql", "dbname": "appdb"}


def test_socket_url_without_host_leaves_the_default_to_libpq() -> None:
    assert connection_params(parse_db_url("postgresql:///appdb")) == {"dbname": "appdb"}


def test_user_without_host_is_not_read_as_a_hostname() -> None:
    assert connection_params(parse_db_url("postgresql://u@/appdb?host=/tmp")) == {
        "host": "/tmp",
        "user": "u",
        "dbname": "appdb",
    }


def test_classic_url_is_unchanged() -> None:
    config = parse_db_url("postgres://user:pass@host:5433/mydb?schema=stabilize&sslmode=require")
    assert config["schema"] == "stabilize"
    assert connection_params(config) == {
        "sslmode": "require",
        "host": "host",
        "port": 5433,
        "user": "user",
        "dbname": "mydb",
        "password": "pass",
    }


def test_unparseable_string_still_exits_without_echoing_the_password(capsys: Any) -> None:
    secret = "Zq7SentinelPw9xK"
    with pytest.raises(SystemExit) as raised:
        parse_db_url(f"host=h password={secret} dbname")
    assert raised.value.code == 1
    assert secret not in capsys.readouterr().out


def test_mg_status_connects_with_a_keyword_form_string(postgres_url: str, capsys: Any) -> None:
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    keyword = make_conninfo(**{k: v for k, v in conninfo_to_dict(postgres_url).items() if v is not None})
    assert "://" not in keyword
    commands.mg_status(keyword)
    out = capsys.readouterr().out
    assert "Database error" not in out
    assert "01KDQ4N9QPJ6Q4MCV3V9GHWPV4_initial_schema.sql" in out
