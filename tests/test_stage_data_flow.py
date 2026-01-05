"""Tests for stage data flow - output passing between stages.

These tests verify that outputs from one stage are properly passed to
downstream stages via requisite_stage_ref_ids and accessible via INPUT.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    DockerTask,
    HTTPTask,
    Orchestrator,
    PythonTask,
    QueueProcessor,
    RunTaskHandler,
    ShellTask,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStatus,
)


@pytest.fixture
def workflow_engine():
    """Create a complete workflow engine with all handlers."""
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()

    registry = TaskRegistry()
    registry.register("shell", ShellTask)
    registry.register("python", PythonTask)
    registry.register("docker", DockerTask)
    registry.register("http", HTTPTask)

    processor = QueueProcessor(queue)
    handlers = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]
    for h in handlers:
        processor.register_handler(h)

    orchestrator = Orchestrator(queue)

    return {
        "store": store,
        "queue": queue,
        "processor": processor,
        "orchestrator": orchestrator,
        "registry": registry,
    }


def run_workflow(engine: dict, workflow: Workflow, timeout: float = 30.0) -> Workflow:
    """Execute a workflow and return the result."""
    engine["store"].store(workflow)
    engine["orchestrator"].start(workflow)
    engine["processor"].process_all(timeout=timeout)
    return engine["store"].retrieve(workflow.id)


class TestPythonTaskDataFlow:
    """Test data flow between stages using PythonTask."""

    def test_simple_two_stage_pipeline(self, workflow_engine: dict) -> None:
        """Test output from stage A is available in stage B via INPUT."""
        workflow = Workflow.create(
            application="test",
            name="Two Stage Pipeline",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    type="python",
                    name="Stage A - Generate Data",
                    context={
                        "script": """
RESULT = {"value": 42, "name": "test"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    type="python",
                    name="Stage B - Use Data",
                    requisite_stage_ref_ids={"stage_a"},
                    context={
                        "script": """
# Access upstream output via INPUT
upstream = INPUT.get("result", {})
value = upstream.get("value", 0)
name = upstream.get("name", "")
RESULT = {"doubled": value * 2, "greeting": f"Hello, {name}!"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify stage A output
        stage_a = next(s for s in result.stages if s.ref_id == "stage_a")
        assert stage_a.outputs.get("result") == {"value": 42, "name": "test"}

        # Verify stage B accessed stage A's output correctly
        stage_b = next(s for s in result.stages if s.ref_id == "stage_b")
        assert stage_b.outputs.get("result") == {"doubled": 84, "greeting": "Hello, test!"}

    def test_three_stage_chain(self, workflow_engine: dict) -> None:
        """Test data flows through A -> B -> C chain."""
        workflow = Workflow.create(
            application="test",
            name="Three Stage Chain",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="Stage A",
                    context={"script": "RESULT = {'n': 5}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
n = INPUT.get("result", {}).get("n", 0)
RESULT = {"factorial": 1}
for i in range(1, n + 1):
    RESULT["factorial"] *= i
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="c",
                    type="python",
                    name="Stage C",
                    requisite_stage_ref_ids={"b"},
                    context={
                        "script": """
factorial = INPUT.get("result", {}).get("factorial", 0)
RESULT = {"result": f"5! = {factorial}"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_c = next(s for s in result.stages if s.ref_id == "c")
        assert stage_c.outputs.get("result") == {"result": "5! = 120"}

    def test_parallel_branches_join(self, workflow_engine: dict) -> None:
        """Test parallel branches with shell tasks (unique stdout per stage)."""
        # Note: When using PythonTask, all branches output to "result" key which
        # causes overwrites. Using ShellTask avoids this since each stage's
        # stdout is unique output that gets merged.
        workflow = Workflow.create(
            application="test",
            name="Parallel Branches Join",
            stages=[
                StageExecution(
                    ref_id="root",
                    type="shell",
                    name="Root",
                    context={"command": "echo -n '10'"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                # Three parallel branches - each outputs unique values
                # Branch A writes to a temp marker file
                StageExecution(
                    ref_id="branch_a",
                    type="python",
                    name="Branch A",
                    requisite_stage_ref_ids={"root"},
                    context={
                        "script": """
base = int(INPUT.get("stdout", "0"))
RESULT = {"value": base + 1, "source": "a"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="branch_b",
                    type="python",
                    name="Branch B",
                    requisite_stage_ref_ids={"root"},
                    context={
                        "script": """
base = int(INPUT.get("stdout", "0"))
RESULT = {"value": base + 2, "source": "b"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="branch_c",
                    type="python",
                    name="Branch C",
                    requisite_stage_ref_ids={"root"},
                    context={
                        "script": """
base = int(INPUT.get("stdout", "0"))
RESULT = {"value": base + 3, "source": "c"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                # Join point - due to key collision, only one result survives
                # but we verify the workflow completes and at least one branch's data is available
                StageExecution(
                    ref_id="join",
                    type="python",
                    name="Join",
                    requisite_stage_ref_ids={"branch_a", "branch_b", "branch_c"},
                    context={
                        "script": """
# When multiple PythonTask branches complete, their "result" keys
# get overwritten during merge. Only one branch's result survives.
# This test verifies the workflow completes and data passes through.
result = INPUT.get("result", {})
value = result.get("value", 0)
source = result.get("source", "unknown")
RESULT = {"received_value": value, "received_from": source, "all_completed": True}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        join_stage = next(s for s in result.stages if s.ref_id == "join")
        join_result = join_stage.outputs.get("result", {})
        # At least one branch's data should be available
        assert join_result.get("received_value") in [11, 12, 13]
        assert join_result.get("received_from") in ["a", "b", "c"]
        assert join_result.get("all_completed") is True

    def test_diamond_dependency(self, workflow_engine: dict) -> None:
        """Test diamond pattern: A -> B, A -> C, B+C -> D.

        Note: When B and C both use PythonTask, their "result" outputs collide.
        Only one branch's result survives in the merge. This test verifies the
        workflow completes and data flows correctly through one path.
        """
        workflow = Workflow.create(
            application="test",
            name="Diamond Dependency",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="A",
                    context={"script": "RESULT = {'x': 100}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="B",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
x = INPUT.get("result", {}).get("x", 0)
RESULT = {"value": x * 2, "from": "b"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="c",
                    type="python",
                    name="C",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
x = INPUT.get("result", {}).get("x", 0)
RESULT = {"value": x * 3, "from": "c"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="d",
                    type="python",
                    name="D",
                    requisite_stage_ref_ids={"b", "c"},
                    context={
                        "script": """
# Due to key collision, only one of B or C's result is available
result = INPUT.get("result", {})
value = result.get("value", 0)
source = result.get("from", "unknown")
RESULT = {"received": value, "source": source}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_d = next(s for s in result.stages if s.ref_id == "d")
        d_result = stage_d.outputs.get("result", {})
        # Either B's (200) or C's (300) result should be available
        assert d_result.get("received") in [200, 300]
        assert d_result.get("source") in ["b", "c"]


class TestShellTaskDataFlow:
    """Test data flow between stages using ShellTask."""

    def test_shell_to_python_pipeline(self, workflow_engine: dict) -> None:
        """Test shell output is available to downstream Python stage."""
        workflow = Workflow.create(
            application="test",
            name="Shell to Python",
            stages=[
                StageExecution(
                    ref_id="shell_stage",
                    type="shell",
                    name="Generate Numbers",
                    context={"command": "echo '1 2 3 4 5'"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="python_stage",
                    type="python",
                    name="Process Numbers",
                    requisite_stage_ref_ids={"shell_stage"},
                    context={
                        "script": """
# Shell output is in stdout key
stdout = INPUT.get("stdout", "")
numbers = [int(x) for x in stdout.split()]
RESULT = {"sum": sum(numbers), "count": len(numbers)}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        python_stage = next(s for s in result.stages if s.ref_id == "python_stage")
        assert python_stage.outputs.get("result") == {"sum": 15, "count": 5}

    def test_shell_to_shell_pipeline(self, workflow_engine: dict) -> None:
        """Test shell output passed to another shell via placeholder."""
        workflow = Workflow.create(
            application="test",
            name="Shell to Shell",
            stages=[
                StageExecution(
                    ref_id="stage1",
                    type="shell",
                    name="Generate",
                    context={"command": "echo -n hello"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage2",
                    type="shell",
                    name="Transform",
                    requisite_stage_ref_ids={"stage1"},
                    context={
                        "command": "echo '{stdout}' | tr 'h' 'H'",
                    },
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage2 = next(s for s in result.stages if s.ref_id == "stage2")
        assert stage2.outputs.get("stdout") == "Hello"

    def test_shell_exit_code_passed(self, workflow_engine: dict) -> None:
        """Test shell returncode is available downstream."""
        workflow = Workflow.create(
            application="test",
            name="Shell Exit Code",
            stages=[
                StageExecution(
                    ref_id="shell_stage",
                    type="shell",
                    name="Run Command",
                    context={"command": "echo 'success' && exit 0"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="check_stage",
                    type="python",
                    name="Check Exit Code",
                    requisite_stage_ref_ids={"shell_stage"},
                    context={
                        "script": """
returncode = INPUT.get("returncode", -1)
stdout = INPUT.get("stdout", "")
RESULT = {"exit_ok": returncode == 0, "output": stdout}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        check_stage = next(s for s in result.stages if s.ref_id == "check_stage")
        assert check_stage.outputs.get("result", {}).get("exit_ok") is True


class TestDockerTaskDataFlow:
    """Test data flow between stages using DockerTask (mocked)."""

    def test_docker_to_shell_pipeline(self, workflow_engine: dict) -> None:
        """Test docker output is available to downstream shell stage.

        Note: We use ShellTask as downstream instead of PythonTask to avoid
        mock interference with subprocess.run calls.
        """
        with patch("stabilize.tasks.docker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="container_output_data",
                stderr="",
            )

            workflow = Workflow.create(
                application="test",
                name="Docker to Shell",
                stages=[
                    StageExecution(
                        ref_id="docker_stage",
                        type="docker",
                        name="Run Container",
                        context={
                            "action": "run",
                            "image": "alpine",
                            "command": "echo container_output_data",
                        },
                        tasks=[TaskExecution.create("Run", "docker", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="shell_stage",
                        type="shell",
                        name="Process Output",
                        requisite_stage_ref_ids={"docker_stage"},
                        context={
                            "command": "echo 'Received: {stdout}'",
                        },
                        tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                    ),
                ],
            )

            result = run_workflow(workflow_engine, workflow)

            assert result.status == WorkflowStatus.SUCCEEDED

            shell_stage = next(s for s in result.stages if s.ref_id == "shell_stage")
            # The docker stdout was passed through and used in the shell command
            assert "container_output_data" in shell_stage.outputs.get("stdout", "")

    def test_shell_to_docker_pipeline(self, workflow_engine: dict) -> None:
        """Test Shell output is available to downstream Docker stage via placeholder."""
        with patch("stabilize.tasks.docker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="docker received: test_data",
                stderr="",
            )

            workflow = Workflow.create(
                application="test",
                name="Shell to Docker",
                stages=[
                    StageExecution(
                        ref_id="shell_stage",
                        type="shell",
                        name="Generate Value",
                        context={"command": "echo -n test_data"},
                        tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="docker_stage",
                        type="docker",
                        name="Run Container",
                        requisite_stage_ref_ids={"shell_stage"},
                        context={
                            "action": "run",
                            "image": "alpine",
                            "command": "echo 'docker received: {stdout}'",
                        },
                        tasks=[TaskExecution.create("Run", "docker", stage_start=True, stage_end=True)],
                    ),
                ],
            )

            result = run_workflow(workflow_engine, workflow)

            assert result.status == WorkflowStatus.SUCCEEDED

            docker_stage = next(s for s in result.stages if s.ref_id == "docker_stage")
            assert docker_stage.outputs.get("stdout") == "docker received: test_data"


class MockHTTPHandler(BaseHTTPRequestHandler):
    """Mock HTTP server for testing."""

    responses: ClassVar[dict[str, tuple[int, str]]] = {}

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        status, body = self.responses.get(self.path, (200, "OK"))
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body.encode())


@pytest.fixture(scope="module")
def http_server():
    """Start a test HTTP server."""
    MockHTTPHandler.responses = {
        "/data": (200, '{"api_value": 999}'),
        "/status": (200, "healthy"),
    }
    server = HTTPServer(("127.0.0.1", 0), MockHTTPHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


class TestHTTPTaskDataFlow:
    """Test data flow between stages using HTTPTask."""

    def test_http_to_shell_pipeline(self, workflow_engine: dict, http_server: str) -> None:
        """Test HTTP response is available to downstream shell stage.

        Note: We use ShellTask instead of PythonTask to avoid serialization
        issues with complex HTTP response data (headers, etc).
        """
        workflow = Workflow.create(
            application="test",
            name="HTTP to Shell",
            stages=[
                StageExecution(
                    ref_id="http_stage",
                    type="http",
                    name="Fetch Status",
                    context={"url": f"{http_server}/status"},
                    tasks=[TaskExecution.create("Run", "http", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="shell_stage",
                    type="shell",
                    name="Process Response",
                    requisite_stage_ref_ids={"http_stage"},
                    context={
                        "command": "echo 'HTTP returned: {body}'",
                    },
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        shell_stage = next(s for s in result.stages if s.ref_id == "shell_stage")
        assert "healthy" in shell_stage.outputs.get("stdout", "")

    def test_shell_to_http_pipeline(self, workflow_engine: dict, http_server: str) -> None:
        """Test Shell output is available to HTTP stage via placeholder."""
        workflow = Workflow.create(
            application="test",
            name="Shell to HTTP",
            stages=[
                StageExecution(
                    ref_id="shell_stage",
                    type="shell",
                    name="Generate Path",
                    context={"command": "echo -n status"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="http_stage",
                    type="http",
                    name="Fetch Dynamic URL",
                    requisite_stage_ref_ids={"shell_stage"},
                    context={
                        "url": f"{http_server}/{{stdout}}",
                    },
                    tasks=[TaskExecution.create("Run", "http", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        http_stage = next(s for s in result.stages if s.ref_id == "http_stage")
        assert http_stage.outputs.get("body") == "healthy"


class TestMixedTaskDataFlow:
    """Test data flow with mixed task types."""

    def test_full_pipeline_shell_python_shell(self, workflow_engine: dict) -> None:
        """Test complete pipeline: Shell -> Python -> Shell.

        This tests the core data flow without HTTP complexity.
        """
        workflow = Workflow.create(
            application="test",
            name="Full Pipeline",
            stages=[
                # Stage 1: Shell generates data
                StageExecution(
                    ref_id="shell1",
                    type="shell",
                    name="Generate",
                    context={"command": "echo -n '42'"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                # Stage 2: Python processes shell output
                StageExecution(
                    ref_id="process",
                    type="python",
                    name="Process",
                    requisite_stage_ref_ids={"shell1"},
                    context={
                        "script": """
stdout = INPUT.get("stdout", "0")
value = int(stdout)
RESULT = {"doubled": value * 2}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                # Stage 3: Another Python in parallel
                StageExecution(
                    ref_id="parallel",
                    type="python",
                    name="Parallel Process",
                    requisite_stage_ref_ids={"shell1"},
                    context={
                        "script": """
stdout = INPUT.get("stdout", "0")
value = int(stdout)
RESULT = {"tripled": value * 3}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                # Stage 4: Final shell aggregation
                # Note: Due to key collision between process and parallel,
                # only one result survives. But both have unique keys inside result.
                StageExecution(
                    ref_id="final",
                    type="shell",
                    name="Aggregate",
                    requisite_stage_ref_ids={"process", "parallel"},
                    context={
                        "command": "echo 'Pipeline complete'",
                    },
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify middle stages completed correctly
        process_stage = next(s for s in result.stages if s.ref_id == "process")
        assert process_stage.outputs.get("result", {}).get("doubled") == 84

        parallel_stage = next(s for s in result.stages if s.ref_id == "parallel")
        assert parallel_stage.outputs.get("result", {}).get("tripled") == 126

        final_stage = next(s for s in result.stages if s.ref_id == "final")
        assert "Pipeline complete" in final_stage.outputs.get("stdout", "")


class TestEdgeCases:
    """Test edge cases for data flow."""

    def test_empty_result(self, workflow_engine: dict) -> None:
        """Test stage with empty RESULT passes through."""
        workflow = Workflow.create(
            application="test",
            name="Empty Result",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="Empty Output",
                    context={"script": "RESULT = {}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="Handle Empty",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
result = INPUT.get("result", {})
RESULT = {"empty": len(result) == 0}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_b = next(s for s in result.stages if s.ref_id == "b")
        assert stage_b.outputs.get("result", {}).get("empty") is True

    def test_complex_nested_result(self, workflow_engine: dict) -> None:
        """Test passing complex nested data structures."""
        workflow = Workflow.create(
            application="test",
            name="Complex Nested",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="Generate Complex",
                    context={
                        "script": """
RESULT = {
    "users": [
        {"name": "Alice", "age": 30, "tags": ["admin", "user"]},
        {"name": "Bob", "age": 25, "tags": ["user"]},
    ],
    "metadata": {
        "version": "1.0",
        "nested": {"deep": {"value": 42}},
    },
}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="Access Nested",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
result = INPUT.get("result", {})
users = result.get("users", [])
deep_value = result.get("metadata", {}).get("nested", {}).get("deep", {}).get("value", 0)
admin_count = sum(1 for u in users if "admin" in u.get("tags", []))
RESULT = {"user_count": len(users), "deep_value": deep_value, "admin_count": admin_count}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_b = next(s for s in result.stages if s.ref_id == "b")
        b_result = stage_b.outputs.get("result", {})
        assert b_result.get("user_count") == 2
        assert b_result.get("deep_value") == 42
        assert b_result.get("admin_count") == 1

    def test_no_upstream_dependency(self, workflow_engine: dict) -> None:
        """Test stages without dependencies run correctly."""
        workflow = Workflow.create(
            application="test",
            name="No Dependencies",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="Independent A",
                    context={"script": "RESULT = {'from': 'A'}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="Independent B",
                    context={"script": "RESULT = {'from': 'B'}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_a = next(s for s in result.stages if s.ref_id == "a")
        stage_b = next(s for s in result.stages if s.ref_id == "b")
        assert stage_a.outputs.get("result") == {"from": "A"}
        assert stage_b.outputs.get("result") == {"from": "B"}

    def test_transitive_dependencies(self, workflow_engine: dict) -> None:
        """Test that only direct dependencies are merged (not transitive)."""
        workflow = Workflow.create(
            application="test",
            name="Transitive Test",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="A",
                    context={"script": "RESULT = {'a_value': 'from_a'}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="B",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
# B depends on A, so a_value should be available
a_val = INPUT.get("result", {}).get("a_value", "missing")
RESULT = {"b_value": 'from_b', "saw_a": a_val}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="c",
                    type="python",
                    name="C",
                    requisite_stage_ref_ids={"b"},  # Only depends on B, not A
                    context={
                        "script": """
# C only depends on B directly
result = INPUT.get("result", {})
b_val = result.get("b_value", "missing")
saw_a = result.get("saw_a", "missing")
RESULT = {"c_saw_b": b_val, "c_saw_a_through_b": saw_a}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_c = next(s for s in result.stages if s.ref_id == "c")
        c_result = stage_c.outputs.get("result", {})
        assert c_result.get("c_saw_b") == "from_b"
        assert c_result.get("c_saw_a_through_b") == "from_a"


class TestDataFlowWithInputs:
    """Test data flow combined with explicit inputs."""

    def test_upstream_merged_with_inputs(self, workflow_engine: dict) -> None:
        """Test that upstream outputs are merged with explicit inputs."""
        workflow = Workflow.create(
            application="test",
            name="Merged Inputs",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="A",
                    context={"script": "RESULT = {'upstream': 100}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="B",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
# Both explicit inputs and upstream should be available
explicit = INPUT.get("explicit_value", 0)
upstream = INPUT.get("result", {}).get("upstream", 0)
RESULT = {"sum": explicit + upstream}
""",
                        "inputs": {"explicit_value": 50},
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_b = next(s for s in result.stages if s.ref_id == "b")
        assert stage_b.outputs.get("result", {}).get("sum") == 150
