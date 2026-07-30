"""
Tests for the A4-triage fixes (audit findings confirmed by re-inspection).
"""

from typing import Any

from stabilize.models.stage import StageExecution
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.sqlite import SqliteQueue
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult


class BigOutputTask(Task):
    """Module-level so the spawn context can pickle it."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"blob": "x" * 2_000_000})


class TestProcessExecutorLargeResult:
    def test_large_result_is_not_misreported_as_timeout(self) -> None:
        """A big output blocks the child on the pipe until the parent reads
        it; draining before join must return it, not deadlock->timeout."""
        from stabilize.resilience.process_executor import ProcessIsolatedTaskExecutor

        stage = StageExecution(ref_id="s", type="big", name="Big", tasks=[])
        result = ProcessIsolatedTaskExecutor(timeout_seconds=30).execute(BigOutputTask(), stage)

        assert result.status.name == "SUCCEEDED", (
            "large task result deadlocked the join and was misreported"
        )
        assert len(result.outputs["blob"]) == 2_000_000


class TestTransientVerificationErrorContext:
    def test_context_update_accepted_and_stored(self) -> None:
        from stabilize.errors.verification import TransientVerificationError

        err = TransientVerificationError("pending", context_update={"progress": 3})
        assert err.context_update == {"progress": 3}

    def test_defaults_to_empty_dict(self) -> None:
        from stabilize.errors.verification import TransientVerificationError

        assert TransientVerificationError("pending").context_update == {}


class TestSqliteQueuePushHonorsConnection:
    def test_push_with_external_connection_defers_commit(self, tmp_path: Any) -> None:
        SingletonMeta.reset(ConnectionManager)
        try:
            queue = SqliteQueue(f"sqlite:///{tmp_path}/push_conn.db")
            queue._create_table()
            conn = queue._get_connection()

            queue.push(
                StartWorkflow(execution_type="PIPELINE", execution_id="e1"),
                connection=conn,
            )
            # Not committed yet: rolling back must discard the message.
            conn.rollback()
            assert queue.size() == 0, "push(connection=...) committed despite the contract"

            queue.push(
                StartWorkflow(execution_type="PIPELINE", execution_id="e2"),
                connection=conn,
            )
            conn.commit()
            assert queue.size() == 1
        finally:
            SingletonMeta.reset(ConnectionManager)


class TestHTTPResponseContentLength:
    def test_malformed_content_length_does_not_crash(self) -> None:
        from stabilize.tasks.http.response import process_response

        class FakeResponse:
            status = 200
            headers = {"Content-Type": "text/plain", "Content-Length": "not-a-number"}

            def read(self, size: int = -1) -> bytes:
                return b"hello"

            def getheaders(self) -> list:
                return list(self.headers.items())

            def geturl(self) -> str:
                return "http://example.com"

        # Should not raise ValueError on the malformed header.
        result = process_response(
            FakeResponse(),
            {},  # context
            "http://example.com",
            5,  # elapsed_ms
            False,  # continue_on_failure
        )
        assert result is not None


class TestDockerRunAutoName:
    def test_run_without_name_gets_generated_name(self, monkeypatch: Any) -> None:
        import stabilize.tasks.docker as docker_module

        calls: list[list[str]] = []

        def fake_run(cmd: Any, **kwargs: Any) -> Any:
            calls.append(list(cmd))
            import subprocess as sp

            # docker version check + the run
            return sp.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(docker_module.subprocess, "run", fake_run)
        stage = StageExecution(
            ref_id="s",
            type="docker",
            name="D",
            context={"action": "run", "image": "alpine", "command": "true"},
            tasks=[],
        )
        docker_module.DockerTask().execute(stage)

        assert stage.context.get("name", "").startswith("stabilize-run-"), (
            "run container was not given an addressable name for timeout cleanup"
        )
        run_cmd = next(c for c in calls if "run" in c)
        assert "--name" in run_cmd
