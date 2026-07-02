"""
Repro tests for SSHTask over-quoting (audit finding A3-15).

SSHTask appended shlex.quote(command) to the ssh argv. Because subprocess
passes argv WITHOUT a local shell, the remote shell received the literal
quoted string as a single word — 'systemctl status nginx' — and tried to
exec a program by that name. Every remote command with arguments failed.
(When typing `ssh host 'cmd arg'` interactively it works only because the
LOCAL shell strips the quotes first; there is no local shell here.)
"""

import subprocess
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.tasks.ssh import SSHTask


@pytest.fixture()
def captured_ssh(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    calls: list[list[str]] = []

    def fake_run(cmd: Any, **kwargs: Any) -> subprocess.CompletedProcess:
        calls.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0, stdout="ok\n", stderr="")

    import stabilize.tasks.ssh as ssh_module

    monkeypatch.setattr(ssh_module.subprocess, "run", fake_run)
    return calls


def _stage(command: str) -> StageExecution:
    return StageExecution(
        ref_id="s",
        type="ssh",
        name="SSH",
        context={"host": "server.example.com", "user": "deploy", "command": command},
        tasks=[],
    )


class TestSSHCommandQuoting:
    def test_command_with_arguments_is_passed_verbatim(
        self, captured_ssh: list[list[str]]
    ) -> None:
        result = SSHTask().execute(_stage("systemctl status nginx"))
        assert result.status.name == "SUCCEEDED"

        ssh_argv = captured_ssh[-1]
        assert ssh_argv[-1] == "systemctl status nginx", (
            f"remote command was re-quoted to {ssh_argv[-1]!r}; the remote shell "
            "would exec a program literally named 'systemctl status nginx'"
        )

    def test_command_with_shell_syntax_preserved(self, captured_ssh: list[list[str]]) -> None:
        """Pipelines/redirection are legitimate remote-shell constructs and
        must reach the remote shell unmangled."""
        SSHTask().execute(_stage("df -h | grep /data > /tmp/report"))
        assert captured_ssh[-1][-1] == "df -h | grep /data > /tmp/report"
