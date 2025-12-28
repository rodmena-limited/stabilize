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

    def test_cwd(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test running command in specific directory."""
        mock_stage.context = {"command": "pwd", "cwd": "/tmp"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "/tmp"

    def test_cwd_with_file_operations(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test file operations in specific directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a file
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, "w") as f:
                f.write("content")

            mock_stage.context = {"command": "cat test.txt", "cwd": tmpdir}
            result = shell_task.execute(mock_stage)

            assert result.status == WorkflowStatus.SUCCEEDED
            assert result.outputs["stdout"] == "content"

    def test_invalid_cwd(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test error with invalid working directory."""
        mock_stage.context = {"command": "pwd", "cwd": "/nonexistent/directory"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.TERMINAL

class TestShellTaskEnv:
    """Environment variable tests."""

    def test_env_single_var(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test setting a single environment variable."""
        mock_stage.context = {"command": "echo $MY_VAR", "env": {"MY_VAR": "hello"}}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "hello"

    def test_env_multiple_vars(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test setting multiple environment variables."""
        mock_stage.context = {
            "command": "echo $VAR1-$VAR2",
            "env": {"VAR1": "hello", "VAR2": "world"},
        }
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "hello-world"

    def test_env_inherits_parent(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test that parent environment is inherited."""
        mock_stage.context = {"command": "echo $HOME"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == os.environ["HOME"]

class TestShellTaskShell:
    """Shell selection tests."""

    def test_bash_shell(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test using bash shell."""
        mock_stage.context = {"command": "echo $BASH_VERSION", "shell": "/bin/bash"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] != ""  # Should have bash version

    def test_sh_shell(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test using sh shell."""
        mock_stage.context = {"command": "echo hello", "shell": "/bin/sh"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "hello"

class TestShellTaskStdin:
    """Stdin input tests."""

    def test_stdin_simple(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test sending input to stdin."""
        mock_stage.context = {"command": "cat", "stdin": "hello world"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "hello world"

    def test_stdin_multiline(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test multiline stdin input."""
        mock_stage.context = {"command": "cat", "stdin": "line1\nline2\nline3"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert "line1" in result.outputs["stdout"]
        assert "line2" in result.outputs["stdout"]

    def test_stdin_with_wc(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test stdin with word count."""
        mock_stage.context = {"command": "wc -w", "stdin": "one two three four"}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["stdout"] == "4"

class TestShellTaskMaxOutput:
    """Output size limit tests."""

    def test_output_not_truncated(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test output under limit is not truncated."""
        mock_stage.context = {"command": "echo hello", "max_output_size": 1000}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["truncated"] is False

    def test_output_truncated(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test output over limit is truncated."""
        # Generate output larger than limit
        mock_stage.context = {
            "command": "python3 -c \"print('x' * 1000)\"",
            "max_output_size": 100,
        }
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["truncated"] is True
        assert len(result.outputs["stdout"]) <= 100

class TestShellTaskExpectedCodes:
    """Expected exit code tests."""

    def test_expected_code_zero(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test success with exit code 0."""
        mock_stage.context = {"command": "true", "expected_codes": [0]}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED

    def test_expected_code_nonzero(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test success with non-zero expected code."""
        mock_stage.context = {"command": "false", "expected_codes": [0, 1]}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["returncode"] == 1

    def test_unexpected_code(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test failure with unexpected exit code."""
        mock_stage.context = {"command": "exit 5", "expected_codes": [0, 1]}
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.TERMINAL

    def test_grep_no_match(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test grep with no match (exit code 1) as success."""
        mock_stage.context = {
            "command": "echo 'hello' | grep nonexistent",
            "expected_codes": [0, 1],
        }
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["returncode"] == 1

class TestShellTaskSecrets:
    """Secret masking tests (logs only, not outputs)."""

    def test_secret_in_command(self, shell_task: ShellTask, mock_stage: MagicMock) -> None:
        """Test that secrets work (masking is for logs, output still works)."""
        mock_stage.context = {
            "command": "echo {token}",
            "token": "secret123",
            "secrets": ["token"],
        }
        result = shell_task.execute(mock_stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        # Output should still contain the actual value (masking is for logs)
        assert result.outputs["stdout"] == "secret123"
