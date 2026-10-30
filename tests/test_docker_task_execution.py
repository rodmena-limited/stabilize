from unittest.mock import MagicMock, patch

from stabilize import DockerTask, StageExecution, TaskExecution, WorkflowStatus


class TestDockerTaskBuild:
    """Test docker build command building."""

    def test_build_simple(self) -> None:
        """Test simple build command."""
        task = DockerTask()
        context = {
            "tag": "myapp:latest",
        }

        cmd = task._build_build_command(context)

        assert cmd[0:2] == ["docker", "build"]
        assert "-t" in cmd
        assert "myapp:latest" in cmd
        assert "." in cmd  # Default context

    def test_build_with_dockerfile(self) -> None:
        """Test build with custom Dockerfile."""
        task = DockerTask()
        context = {
            "tag": "myapp:latest",
            "dockerfile": "Dockerfile.prod",
            "context": "./docker",
        }

        cmd = task._build_build_command(context)

        assert "-f" in cmd
        assert "Dockerfile.prod" in cmd
        assert "./docker" in cmd

    def test_build_with_args(self) -> None:
        """Test build with build arguments."""
        task = DockerTask()
        context = {
            "tag": "myapp:latest",
            "build_args": {"VERSION": "1.0", "ENV": "prod"},
        }

        cmd = task._build_build_command(context)

        assert "--build-arg" in cmd
        assert "VERSION=1.0" in cmd
        assert "ENV=prod" in cmd

    def test_build_no_cache(self) -> None:
        """Test build without cache."""
        task = DockerTask()
        context = {
            "tag": "myapp:latest",
            "no_cache": True,
        }

        cmd = task._build_build_command(context)

        assert "--no-cache" in cmd


class TestDockerTaskExecution:
    """Test DockerTask execution with mocked subprocess."""

    @patch("stabilize.tasks.docker.subprocess.run")
    def test_execute_success(self, mock_run: MagicMock) -> None:
        """Test successful execution."""
        # Mock docker version check
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="hello world\n",
            stderr="",
        )

        task = DockerTask()
        stage = StageExecution(
            ref_id="1",
            type="docker",
            name="Test",
            context={
                "action": "run",
                "image": "alpine",
                "command": "echo hello",
            },
            tasks=[TaskExecution.create(name="test", implementing_class="docker")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["exit_code"] == 0

    @patch("stabilize.tasks.docker.subprocess.run")
    def test_execute_failure(self, mock_run: MagicMock) -> None:
        """Test failed execution."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="error occurred",
        )

        task = DockerTask()
        stage = StageExecution(
            ref_id="1",
            type="docker",
            name="Test",
            context={
                "action": "run",
                "image": "alpine",
                "command": "exit 1",
            },
            tasks=[TaskExecution.create(name="test", implementing_class="docker")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL

    @patch("stabilize.tasks.docker.subprocess.run")
    def test_execute_continue_on_failure(self, mock_run: MagicMock) -> None:
        """Test continue_on_failure flag."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="error",
        )

        task = DockerTask()
        stage = StageExecution(
            ref_id="1",
            type="docker",
            name="Test",
            context={
                "action": "run",
                "image": "alpine",
                "command": "exit 1",
                "continue_on_failure": True,
            },
            tasks=[TaskExecution.create(name="test", implementing_class="docker")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.FAILED_CONTINUE

    @patch("stabilize.tasks.docker.subprocess.run")
    def test_docker_not_available(self, mock_run: MagicMock) -> None:
        """Test when Docker is not available."""
        mock_run.side_effect = FileNotFoundError("docker not found")

        task = DockerTask()
        stage = StageExecution(
            ref_id="1",
            type="docker",
            name="Test",
            context={
                "action": "run",
                "image": "alpine",
            },
            tasks=[TaskExecution.create(name="test", implementing_class="docker")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "not available" in result.context["error"].lower()

    def test_unsupported_action(self) -> None:
        """Test unsupported action."""
        task = DockerTask()
        stage = StageExecution(
            ref_id="1",
            type="docker",
            name="Test",
            context={"action": "invalid"},
            tasks=[TaskExecution.create(name="test", implementing_class="docker")],
        )

        # Mock docker version check to pass
        with patch("stabilize.tasks.docker.subprocess.run"):
            result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "unsupported" in result.context["error"].lower()

    def test_missing_image(self) -> None:
        """Test run without image."""
        task = DockerTask()
        stage = StageExecution(
            ref_id="1",
            type="docker",
            name="Test",
            context={"action": "run"},
            tasks=[TaskExecution.create(name="test", implementing_class="docker")],
        )

        # Mock docker version check to pass
        with patch("stabilize.tasks.docker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "image" in result.context["error"].lower()


class TestDockerTaskOtherActions:
    """Test other Docker actions."""

    def test_pull_command(self) -> None:
        """Test pull command."""
        task = DockerTask()
        context = {"image": "nginx:latest"}

        cmd = task._build_command("pull", context)

        assert cmd == ["docker", "pull", "nginx:latest"]

    def test_ps_command(self) -> None:
        """Test ps command."""
        task = DockerTask()
        context = {}

        cmd = task._build_command("ps", context)
        assert cmd == ["docker", "ps"]

        context = {"all": True}
        cmd = task._build_command("ps", context)
        assert cmd == ["docker", "ps", "-a"]

    def test_images_command(self) -> None:
        """Test images command."""
        task = DockerTask()
        context = {}

        cmd = task._build_command("images", context)

        assert cmd == ["docker", "images"]

    def test_logs_command(self) -> None:
        """Test logs command."""
        task = DockerTask()
        context = {"name": "my-container", "tail": 100}

        cmd = task._build_command("logs", context)

        assert "docker" in cmd
        assert "logs" in cmd
        assert "--tail" in cmd
        assert "100" in cmd
        assert "my-container" in cmd

    def test_stop_command(self) -> None:
        """Test stop command."""
        task = DockerTask()
        context = {"name": "my-container"}

        cmd = task._build_command("stop", context)

        assert cmd == ["docker", "stop", "my-container"]

    def test_rm_command(self) -> None:
        """Test rm command."""
        task = DockerTask()
        context = {"name": "my-container"}

        cmd = task._build_command("rm", context)
        assert cmd == ["docker", "rm", "my-container"]

        context = {"name": "my-container", "force": True}
        cmd = task._build_command("rm", context)
        assert cmd == ["docker", "rm", "-f", "my-container"]
