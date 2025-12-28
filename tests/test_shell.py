import base64
import os
import tempfile
from unittest.mock import MagicMock
import pytest
from stabilize import ShellTask, StageExecution, WorkflowStatus

def shell_task() -> ShellTask:
    """Create a ShellTask instance."""
    return ShellTask()

def mock_stage() -> MagicMock:
    """Create a mock stage with configurable context."""
    stage = MagicMock(spec=StageExecution)
    stage.context = {}
    return stage

class TestShellTaskBasic:
    """Basic command execution tests."""

    def test_simple_command(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test executing a simple command."""
        mock_stage.context = {"command": "echo hello"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "hello"
        assert result.outputs["returncode"] == 0

    def test_missing_command(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test error when no command specified."""
        mock_stage.context = {}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "No 'command' specified" in str(result.context.get("error", ""))

    def test_command_with_args(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test command with multiple arguments."""
        mock_stage.context = {"command": "echo -n hello world"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "hello world"

    def test_command_with_pipe(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test command with pipe."""
        mock_stage.context = {"command": "echo hello | tr 'h' 'H'"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "Hello"

class TestShellTaskCwd:
    """Working directory tests."""
