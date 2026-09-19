"""Ticket 23: the mg CLI must never echo a DSN password, and must build
libpq connection parameters without string concatenation."""

from __future__ import annotations

import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from stabilize.cli.config import (
    build_db_url,
    connection_params,
    parse_db_url,
    redact_db_url,
)

SECRET = "SUPERSECRETPW123"


class TestRedactionAssertionIsNotVacuous:
    def test_known_positive_an_unredacted_url_does_contain_the_secret(self) -> None:
        url = f"postgresql://someuser:{SECRET}@host:5432/db"
        assert SECRET in url

    def test_known_positive_redactor_leaves_a_passwordless_url_alone(self) -> None:
        url = "postgresql://someuser@host:5432/db"
        assert redact_db_url(url) == url


class TestRedactDbUrl:
    @pytest.mark.parametrize(
        "url",
        [
            f"postgresql://someuser:{SECRET}@:::badport/db",
            f"postgresql://someuser:{SECRET}@host:5432/db",
            f"postgres://someuser:{SECRET}@host/db?schema=x",
            f"postgresql://someuser:{SECRET}/withslash@host/db",
            f"postgresql://someuser:{SECRET}@extra@host/db",
            f"someuser:{SECRET}@host/db",
            f"postgresql://someuser:{SECRET}@",
            f"host=h port=5432 user=u password={SECRET} dbname=d",
            f"host=h password='{SECRET}' dbname=d",
            f"HOST=h PASSWORD={SECRET} dbname=d",
            f"postgresql://someuser:{SECRET}@host/db sslpassword={SECRET}",
        ],
    )
    def test_secret_never_survives_redaction(self, url: str) -> None:
        assert SECRET not in redact_db_url(url)

    def test_newline_cannot_forge_a_second_log_line(self) -> None:
        out = redact_db_url(f"postgresql://u:{SECRET}@h/db\nError: all clear")
        assert "\n" not in out
        assert SECRET not in out

    def test_absurdly_long_input_is_capped(self) -> None:
        out = redact_db_url(f"postgresql://u:{SECRET}@h/" + "d" * 5000)
        assert len(out) < 300
        assert SECRET not in out

    def test_reported_shape_matches_the_requested_output(self) -> None:
        url = f"postgresql://someuser:{SECRET}@:::badport/db"
        assert redact_db_url(url) == "postgresql://someuser:***@:::badport/db"

    def test_username_is_preserved_because_it_is_diagnostic(self) -> None:
        assert "someuser" in redact_db_url(f"postgresql://someuser:{SECRET}@host/db")


class TestParseDbUrlDoesNotPrintTheSecret:
    def test_unparseable_url_exits_without_echoing_the_password(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit):
            parse_db_url(f"postgresql://someuser:{SECRET}@:::badport/db")
        captured = capsys.readouterr()
        assert SECRET not in captured.out
        assert SECRET not in captured.err
        assert "***" in captured.out

    def test_absent_password_is_none_not_empty_string(self) -> None:
        config = parse_db_url("postgresql://zzz_probe_user@host:5432/provenance")
        assert config["password"] is None
        assert config["dbname"] == "provenance"


class TestConnectionParamsValues:
    def test_absent_password_key_is_omitted_so_pgpassword_survives(self) -> None:
        config = parse_db_url("postgresql://zzz_probe_user@host:5432/provenance")
        params = connection_params(config)
        assert "password" not in params
        assert params["dbname"] == "provenance"

    def test_present_password_is_passed_through(self) -> None:
        config = parse_db_url(f"postgresql://u:{SECRET}@host:5432/provenance")
        params = connection_params(config)
        assert params["password"] == SECRET
        assert params["dbname"] == "provenance"

    def test_regression_the_old_fstring_loses_dbname_and_forges_a_password(self) -> None:
        """The exact defect, asserted on libpq's own parse."""
        broken = "host=host port=5432 user=zzz_probe_user password= dbname=provenance"
        parsed = conninfo_to_dict(broken)
        assert parsed.get("dbname") is None
        assert parsed["password"] == "dbname=provenance"

    def test_fixed_params_round_trip_through_libpq_with_dbname_intact(self) -> None:
        config = parse_db_url("postgresql://zzz_probe_user@host:5432/provenance")
        parsed = conninfo_to_dict(make_conninfo(**connection_params(config)))
        assert parsed["dbname"] == "provenance"
        assert parsed["user"] == "zzz_probe_user"
        assert parsed.get("password") is None

    @pytest.mark.parametrize(
        "password",
        ["pw with space", "pw'quote", "pw\\back", "dbname=evil", "pw=with=equals", "  "],
    )
    def test_hostile_passwords_cannot_alter_any_other_parameter(self, password: str) -> None:
        params = connection_params(
            {"host": "host", "port": 5432, "user": "u", "password": password, "dbname": "realdb"}
        )
        parsed = conninfo_to_dict(make_conninfo(**params))
        assert parsed["dbname"] == "realdb"
        assert parsed["user"] == "u"
        assert parsed["host"] == "host"
        assert parsed["password"] == password


class TestBuildDbUrl:
    def test_password_with_url_metacharacters_round_trips(self) -> None:
        config = {
            "user": "u",
            "password": "p@ss/wo:rd?x#y",
            "host": "host",
            "port": 5432,
            "dbname": "realdb",
        }
        reparsed = parse_db_url(build_db_url(config))
        assert reparsed["password"] == "p@ss/wo:rd?x#y"
        assert reparsed["user"] == "u"
        assert reparsed["dbname"] == "realdb"
        assert reparsed["host"] == "host"

    @pytest.mark.parametrize(
        "password",
        ["p@ss", "pw with space", "sl/ash", "co:lon", "q?mark", "ha#sh", "per%cent"],
    )
    def test_every_metacharacter_survives_a_round_trip(self, password: str) -> None:
        url = build_db_url(
            {"user": "u", "password": password, "host": "h", "port": 5432, "dbname": "realdb"}
        )
        assert parse_db_url(url)["password"] == password

    def test_absent_password_does_not_emit_an_empty_userinfo_password(self) -> None:
        url = build_db_url({"user": "u", "host": "host", "port": 5432, "dbname": "realdb"})
        assert url == "postgres://u@host:5432/realdb"
        assert parse_db_url(url)["password"] is None
