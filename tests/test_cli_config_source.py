"""Where the CLI takes its database target from (B51): never a working-directory mg.yaml by accident."""

from __future__ import annotations

from pathlib import Path

import pytest

from stabilize.cli.config import load_config

MG_YAML = "database:\n  host: yaml-host\n  port: 5433\n  user: yaml_user\n  password: pw\n  dbname: yaml_db\n"
TARGET_VARS = (
    "MG_DB_HOST",
    "MG_DB_PORT",
    "MG_DB_USER",
    "MG_DB_PASSWORD",
    "MG_DB_NAME",
    "PGHOST",
    "PGPORT",
    "PGDATABASE",
)


def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("MG_DATABASE_URL", "MG_SCHEMA", "MG_HISTORY_SCHEMA", "PGUSER", "PGPASSWORD", *TARGET_VARS):
        monkeypatch.delenv(name, raising=False)


def test_a_working_directory_mg_yaml_is_not_read_without_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear(monkeypatch)
    (tmp_path / "mg.yaml").write_text(MG_YAML)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as exit_info:
        load_config()
    assert exit_info.value.code == 1
    out = capsys.readouterr().out
    assert "yaml-host" not in out, f"the CLI read the working-directory mg.yaml: {out}"
    assert "--config" in out, f"the refusal does not name --config: {out}"


def test_config_reads_the_named_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear(monkeypatch)
    path = tmp_path / "targets" / "prod.yaml"
    path.parent.mkdir()
    path.write_text(MG_YAML)
    config = load_config(config_path=path)
    assert (config["host"], config["port"], config["dbname"]) == ("yaml-host", 5433, "yaml_db")


@pytest.mark.parametrize("variable", TARGET_VARS)
def test_target_variables_beside_the_url_are_refused(
    variable: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("MG_DATABASE_URL", "postgres://u:p@h:5432/d")
    monkeypatch.setenv(variable, "other")
    with pytest.raises(SystemExit) as exit_info:
        load_config()
    assert exit_info.value.code == 1
    assert variable in capsys.readouterr().out, f"the refusal does not name {variable}"


def test_the_url_alone_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("MG_DATABASE_URL", "postgres://u:p@h:5432/d")
    assert load_config()["dbname"] == "d"
