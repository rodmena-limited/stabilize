"""
Repro tests for audit findings A3-19/20/21.

- FileAuditLogger claimed "Log audit events to a secure file" but never
  wrote the file (bridge to the process log only).
- TaskRegistry.register's docstring promised ValueError on duplicate
  registration but silently overwrote.
- HTTPTask retried non-idempotent methods (POST/PATCH) whenever retries
  were enabled, duplicating side effects on 5xx/timeouts, with no opt-out.
"""

import json
from typing import Any
from urllib.error import URLError

import pytest

from stabilize.audit import AuditEvent, FileAuditLogger
from stabilize.models.stage import StageExecution
from stabilize.tasks.http.task import HTTPTask
from stabilize.tasks.interface import Task
from stabilize.tasks.registry import TaskRegistry
from stabilize.tasks.result import TaskResult


class TestFileAuditLoggerWritesFile:
    def test_log_appends_json_line_to_file(self, tmp_path: Any) -> None:
        path = tmp_path / "audit.log"
        audit_logger = FileAuditLogger(str(path))
        event = AuditEvent(
            event_type="workflow",
            action="start",
            user="alice",
            resource_type="workflow",
            resource_id="wf-1",
        )
        audit_logger.log(event)

        assert path.exists(), "FileAuditLogger wrote no file despite its documented purpose"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["action"] == "start"
        assert record["user"] == "alice"

    def test_global_default_does_not_create_files(self, tmp_path: Any, monkeypatch: Any) -> None:
        """The module-level default logger must stay log-only: merely using
        stabilize must not start dropping audit.log files into the CWD."""
        monkeypatch.chdir(tmp_path)
        from stabilize.audit import audit

        audit(
            event_type="workflow",
            action="start",
            user="bob",
            resource_type="workflow",
            resource_id="wf-2",
        )
        assert not (tmp_path / "audit.log").exists()


class _DemoTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class TestRegistryStrictRegistration:
    def test_default_overwrites_with_warning(self) -> None:
        registry = TaskRegistry()
        registry.register("demo", _DemoTask)
        registry.register("demo", _DemoTask)  # documented-compatible: warns

    def test_strict_raises_on_duplicate(self) -> None:
        registry = TaskRegistry()
        registry.register("demo", _DemoTask)
        with pytest.raises(ValueError):
            registry.register("demo", _DemoTask, strict=True)


class _CountingOpener:
    def __init__(self) -> None:
        self.calls = 0

    def open(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        raise URLError("connection refused")


class TestHTTPNonIdempotentRetryOptOut:
    def _run(self, monkeypatch: Any, context: dict[str, Any]) -> int:
        import stabilize.tasks.http.task as http_task_module

        opener = _CountingOpener()
        monkeypatch.setattr(http_task_module, "build_opener", lambda *h: opener)
        stage = StageExecution(ref_id="s", type="http", name="HTTP", context=context, tasks=[])
        HTTPTask().execute(stage)
        return opener.calls

    def test_post_retries_by_default_when_enabled(self, monkeypatch: Any) -> None:
        calls = self._run(
            monkeypatch,
            {
                "url": "http://127.0.0.1:9/x",
                "allow_private_urls": True,
                "method": "POST",
                "retries": 2,
                "retry_delay": 0.01,
            },
        )
        assert calls == 3  # unchanged default behavior: 1 + 2 retries

    def test_post_retry_opt_out(self, monkeypatch: Any) -> None:
        calls = self._run(
            monkeypatch,
            {
                "url": "http://127.0.0.1:9/x",
                "allow_private_urls": True,
                "method": "POST",
                "retries": 2,
                "retry_delay": 0.01,
                "retry_non_idempotent": False,
            },
        )
        assert calls == 1, (
            "POST was retried despite retry_non_idempotent=False — duplicate "
            "side effects on the remote service"
        )

    def test_get_still_retries_with_opt_out(self, monkeypatch: Any) -> None:
        calls = self._run(
            monkeypatch,
            {
                "url": "http://127.0.0.1:9/x",
                "allow_private_urls": True,
                "method": "GET",
                "retries": 2,
                "retry_delay": 0.01,
                "retry_non_idempotent": False,
            },
        )
        assert calls == 3  # GET is idempotent: retries stay enabled
