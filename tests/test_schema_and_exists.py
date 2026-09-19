"""Ticket #24 (reported by vellum-build-d8bbd2): exists() must not hide an
operational failure, and the runtime must accept a schema."""

from __future__ import annotations

import pytest

from stabilize.persistence.pool_options import (
    PoolOptions,
    validate_schema_name,
    with_schema,
)
from stabilize.persistence.store import WorkflowNotFoundError
from stabilize.persistence.store.interface import WORKFLOW_ABSENT_ERRORS


class _Store:
    """Minimal stand-in exercising the inherited exists()."""

    def __init__(self, error: Exception | None) -> None:
        self._error = error

    def retrieve_execution_summary(self, execution_id: str) -> object:
        if self._error is not None:
            raise self._error
        return object()

    exists = __import__(
        "stabilize.persistence.store.interface", fromlist=["WorkflowStore"]
    ).WorkflowStore.exists


class TestExistsDistinguishesAbsenceFromBreakage:
    def test_present_workflow_is_true(self) -> None:
        assert _Store(None).exists("id") is True

    def test_absent_workflow_is_false(self) -> None:
        assert _Store(WorkflowNotFoundError("id")).exists("id") is False

    @pytest.mark.parametrize(
        "error",
        [
            RuntimeError("relation \"pipeline_executions\" does not exist"),
            OSError("connection refused"),
            PermissionError("permission denied for table pipeline_executions"),
        ],
    )
    def test_operational_failure_propagates(self, error: Exception) -> None:
        """A broken deployment must not be reported as an empty one."""
        with pytest.raises(type(error)):
            _Store(error).exists("id")

    def test_absent_error_tuple_covers_both_spellings(self) -> None:
        assert len(WORKFLOW_ABSENT_ERRORS) == 2


class TestSchemaOption:
    def test_schema_becomes_a_search_path_connect_option(self) -> None:
        options = with_schema(None, "orchestration")
        assert options is not None
        assert options.connect_kwargs["options"] == "-c search_path=orchestration"

    def test_no_schema_leaves_options_untouched(self) -> None:
        assert with_schema(None, None) is None
        existing = PoolOptions(acquire_timeout=3.0)
        assert with_schema(existing, None) is existing

    def test_caller_supplied_options_are_not_overridden(self) -> None:
        explicit = PoolOptions(connect_kwargs={"options": "-c statement_timeout=5000"})
        result = with_schema(explicit, "orchestration")
        assert result is not None
        assert result.connect_kwargs["options"] == "-c statement_timeout=5000"

    def test_differing_schemas_produce_different_pool_keys(self) -> None:
        a = with_schema(None, "alpha")
        b = with_schema(None, "beta")
        assert a is not None and b is not None
        assert a.key() != b.key()

    @pytest.mark.parametrize(
        "bad",
        ["public; DROP TABLE x", "has space", "-c evil", "", "1leading", "a" * 64, "quo'te"],
    )
    def test_hostile_schema_names_are_refused(self, bad: str) -> None:
        with pytest.raises(ValueError):
            validate_schema_name(bad)

    @pytest.mark.parametrize("good", ["public", "orchestration", "_private", "s1", "A_b2"])
    def test_valid_schema_names_are_accepted(self, good: str) -> None:
        """Without this the validator could pass by refusing everything."""
        assert validate_schema_name(good) == good


class TestMgSchemaOverride:
    def test_override_applies_when_env_is_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from stabilize.cli.config import apply_schema_override

        monkeypatch.setenv("MG_SCHEMA", "orchestration")
        assert apply_schema_override({})["schema"] == "orchestration"

    def test_override_is_absent_when_env_is_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from stabilize.cli.config import apply_schema_override

        monkeypatch.delenv("MG_SCHEMA", raising=False)
        assert "schema" not in apply_schema_override({})


class TestDsnOptionsPrecedence:
    """A DSN carrying its own `options` must beat schema=.

    psycopg's keyword argument beats the conninfo, so a schema= that ignored
    the DSN would silently replace the caller's search_path. Reported by
    vellum-build-d8bbd2 against the first version of this fix, where the
    precedence guard inspected PoolOptions only.
    """

    URL = "postgresql://u:p@h/db?options=-csearch_path%3Dalpha"
    KV = "host=h dbname=db options=-csearch_path=alpha"

    def test_url_dsn_options_are_detected(self) -> None:
        from stabilize.persistence.pool_options import dsn_sets_options

        assert dsn_sets_options(self.URL) is True

    def test_keyword_value_dsn_options_are_detected(self) -> None:
        from stabilize.persistence.pool_options import dsn_sets_options

        assert dsn_sets_options(self.KV) is True

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://u:p@h/db",
            "postgresql://u:p@h/db?sslmode=require",
            "host=h dbname=db user=u",
            "",
            None,
        ],
    )
    def test_dsn_without_options_is_not_detected(self, dsn: str | None) -> None:
        """Without this, the guard could defer always and schema= would be dead."""
        from stabilize.persistence.pool_options import dsn_sets_options

        assert dsn_sets_options(dsn) is False

    def test_schema_defers_to_a_url_dsn(self) -> None:
        assert with_schema(None, "beta", self.URL) is None

    def test_schema_defers_to_a_keyword_value_dsn(self) -> None:
        assert with_schema(None, "beta", self.KV) is None

    def test_schema_still_applies_on_a_plain_dsn(self) -> None:
        result = with_schema(None, "beta", "postgresql://u:p@h/db")
        assert result is not None
        assert result.connect_kwargs["options"] == "-c search_path=beta"

    def test_pool_options_precedence_is_unchanged(self) -> None:
        explicit = PoolOptions(connect_kwargs={"options": "-c search_path=alpha"})
        assert with_schema(explicit, "beta", "postgresql://u:p@h/db") is explicit


class TestUrlQueryParametersReachLibpq:
    """Reported by trace-thinkpad-83589d: mg-up/mg-status dropped every query
    parameter, so a TLS-mandatory database was contacted with no TLS settings
    at all — a security control the operator asked for, discarded silently."""

    URL = (
        "postgresql://u:p@h:5432/db?sslmode=verify-full&sslrootcert=/etc/ca.crt"
        "&sslcert=/etc/c.crt&sslkey=/etc/c.key&application_name=mg&connect_timeout=5"
    )

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            ("sslmode", "verify-full"),
            ("sslrootcert", "/etc/ca.crt"),
            ("sslcert", "/etc/c.crt"),
            ("sslkey", "/etc/c.key"),
            ("application_name", "mg"),
            ("connect_timeout", "5"),
        ],
    )
    def test_query_parameters_survive(self, key: str, value: str) -> None:
        from stabilize.cli.config import connection_params, parse_db_url

        assert connection_params(parse_db_url(self.URL))[key] == value

    def test_url_components_still_win(self) -> None:
        """Without this, a query parameter could clobber the real target."""
        from stabilize.cli.config import connection_params, parse_db_url

        params = connection_params(parse_db_url(self.URL + "&host=evil&dbname=evil"))
        assert params["host"] == "h"
        assert params["dbname"] == "db"

    def test_schema_is_stabilize_s_own_and_not_sent_to_libpq(self) -> None:
        from stabilize.cli.config import connection_params, parse_db_url

        config = parse_db_url("postgresql://u:p@h/db?schema=orchestration&sslmode=require")
        assert config["schema"] == "orchestration"
        params = connection_params(config)
        assert "schema" not in params
        assert params["sslmode"] == "require"

    def test_plain_url_gains_no_extra_parameters(self) -> None:
        """Both directions: the passthrough must not invent parameters."""
        from stabilize.cli.config import connection_params, parse_db_url

        params = connection_params(parse_db_url("postgresql://u:p@h/db"))
        assert set(params) == {"host", "port", "user", "dbname", "password"}
