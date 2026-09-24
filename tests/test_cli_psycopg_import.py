"""psycopg import diagnosis and driver disclosure in the migration CLI (#47)."""

from __future__ import annotations

import builtins
import types
from typing import Any

import pytest

from stabilize.cli import commands
from stabilize.redaction import redact_text

_REAL_IMPORT = builtins.__import__


def _failing_import(exc: BaseException) -> Any:
    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "psycopg":
            raise exc
        return _REAL_IMPORT(name, *args, **kwargs)

    return fake_import


def test_real_psycopg_imports_through_the_helper() -> None:
    module = commands.import_psycopg()
    assert module.__name__ == "psycopg"


def test_absent_psycopg_prints_the_install_hint(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    monkeypatch.setattr(
        builtins, "__import__", _failing_import(ModuleNotFoundError("No module named 'psycopg'", name="psycopg"))
    )
    with pytest.raises(SystemExit) as raised:
        commands.import_psycopg()
    assert raised.value.code == 1
    assert "pip install stabilize[postgres]" in capsys.readouterr().out


def test_mismatched_accelerator_propagates_with_its_own_message(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    message = "cannot import name 'Deque' from 'psycopg_c._psycopg.generators'"
    monkeypatch.setattr(builtins, "__import__", _failing_import(ImportError(message)))
    with pytest.raises(ImportError, match="Deque"):
        commands.import_psycopg()
    assert "not installed" not in capsys.readouterr().out


def test_missing_submodule_is_not_reported_as_missing_psycopg(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    monkeypatch.setattr(
        builtins, "__import__", _failing_import(ModuleNotFoundError("No module named 'psycopg_c'", name="psycopg_c"))
    )
    with pytest.raises(ModuleNotFoundError, match="psycopg_c"):
        commands.import_psycopg()
    assert "not installed" not in capsys.readouterr().out


def test_mg_up_surfaces_the_real_import_error(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    message = "cannot import name 'Deque' from 'psycopg_c._psycopg.generators'"
    monkeypatch.setattr(builtins, "__import__", _failing_import(ImportError(message)))
    with pytest.raises(ImportError, match="Deque"):
        commands.mg_up("postgresql://u:p@127.0.0.1:1/db")
    assert "not installed" not in capsys.readouterr().out


def _fake_psycopg(impl: str, libpq: int) -> Any:
    pq = types.SimpleNamespace(__impl__=impl, version=lambda: libpq)
    return types.SimpleNamespace(__version__="3.9.9", pq=pq)


def test_disclosure_names_implementation_and_system_libpq() -> None:
    text = commands.describe_driver(_fake_psycopg("python", 170007))
    assert "impl=python" in text
    assert "libpq 170007" in text
    assert "system libpq" in text


def test_disclosure_flags_a_bundled_libpq() -> None:
    text = commands.describe_driver(_fake_psycopg("binary", 180000))
    assert "impl=binary" in text
    assert "bundled" in text
    assert "system libpq" not in text


def test_disclosure_of_the_installed_driver_is_real() -> None:
    import psycopg

    text = commands.describe_driver(psycopg)
    assert f"impl={psycopg.pq.__impl__}" in text
    assert f"libpq {psycopg.pq.version()}" in text


def test_undeterminable_driver_refuses_to_connect(capsys: Any) -> None:
    def broken_version() -> int:
        raise RuntimeError("libpq not loadable")

    broken = types.SimpleNamespace(__version__="3.9.9", pq=types.SimpleNamespace(__impl__="c", version=broken_version))
    with pytest.raises(SystemExit) as raised:
        commands.announce_driver(broken)
    assert raised.value.code == 1
    assert "Refusing to connect" in capsys.readouterr().out


def test_mg_up_announces_the_driver_before_connecting(capsys: Any) -> None:
    with pytest.raises(SystemExit):
        commands.mg_up("postgresql://u:p@127.0.0.1:1/db?connect_timeout=1")
    out = capsys.readouterr().out
    assert "Driver: psycopg" in out
    assert out.index("Driver: psycopg") < out.index("Database error")


def test_parse_error_text_is_redacted() -> None:
    secret = "Zq7SentinelPw9xK"
    message = f'missing "=" after "postgresql+psycopg://u:{secret}@h.example/db" in connection info string'
    assert secret in message
    redacted = redact_text(message)
    assert secret not in redacted
    assert "postgresql+psycopg://u:***@h.example/db" in redacted


def test_keyword_password_is_redacted() -> None:
    secret = "Zq7SentinelPw9xK"
    redacted = redact_text(f"invalid connection option in 'host=h password={secret} dbname=d'")
    assert secret not in redacted
    assert "password=***" in redacted


def test_a_message_without_a_dsn_is_left_intact() -> None:
    message = (
        'connection failed: connection to server at "127.0.0.1", port 5432 failed: '
        "server does not support SSL, but SSL was required"
    )
    assert redact_text(message) == message


def test_a_malformed_scheme_is_still_redacted() -> None:
    secret = "Zq7SentinelPw9xK"
    redacted = redact_text(f'missing "=" after "postgres!!!://u:{secret}@127.0.0.1:1/db" in connection info string')
    assert secret not in redacted
    assert "postgres!!!://u:***@127.0.0.1:1/db" in redacted
