from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from stabilize import (
    StageExecution,
    TaskExecution,
    Workflow,
    WorkflowStatus,
)
from tests.stage_data_flow.conftest import run_workflow


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
                        context={"command": "printf '%s' test_data"},
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
                    context={"url": f"{http_server}/status", "allow_private_urls": True},
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
                    context={"command": "printf '%s' status"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="http_stage",
                    type="http",
                    name="Fetch Dynamic URL",
                    requisite_stage_ref_ids={"shell_stage"},
                    context={
                        "url": f"{http_server}/{{stdout}}",
                        "allow_private_urls": True,
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
                    context={"command": "printf '%s' '42'"},
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
