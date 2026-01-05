"""Tests for DockerTask."""

from unittest.mock import MagicMock, patch

import pytest

from stabilize import DockerTask, StageExecution, TaskExecution, WorkflowStatus


class TestDockerTaskRun:
    """Test docker run command building."""

    def test_simple_run(self) -> None:
        """Test simple container run."""
        task = DockerTask()
        stage = StageExecution(
            ref_id="1",
            type="docker",
            name="Simple Run",
            context={
                "action": "run",
                "image": "alpine:latest",
                "command": "echo hello",
            },
            tasks=[TaskExecution.create(name="test", implementing_class="docker")],
        )

        cmd = task._build_run_command(stage.context)

        assert cmd[0:2] == ["docker", "run"]
        assert "--rm" in cmd
        assert "alpine:latest" in cmd
        assert "echo" in cmd
        assert "hello" in cmd

    def test_run_with_environment(self) -> None:
        """Test run with environment variables."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "environment": {"FOO": "bar", "BAZ": "qux"},
        }

        cmd = task._build_run_command(context)

        assert "-e" in cmd
        cmd.index("-e")
        assert "FOO=bar" in cmd
        assert "BAZ=qux" in cmd

    def test_run_with_volumes(self) -> None:
        """Test run with volume mounts."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "volumes": ["/host:/container", "/data:/data:ro"],
        }

        cmd = task._build_run_command(context)

        assert "-v" in cmd
        assert "/host:/container" in cmd
        assert "/data:/data:ro" in cmd

    def test_run_with_ports(self) -> None:
        """Test run with port mappings."""
        task = DockerTask()
        context = {
            "image": "nginx",
            "ports": ["8080:80", "443:443"],
        }

        cmd = task._build_run_command(context)

        assert "-p" in cmd
        assert "8080:80" in cmd
        assert "443:443" in cmd

    def test_run_with_entrypoint(self) -> None:
        """Test run with entrypoint override."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "entrypoint": "/bin/sh",
        }

        cmd = task._build_run_command(context)

        assert "--entrypoint" in cmd
        entrypoint_idx = cmd.index("--entrypoint")
        assert cmd[entrypoint_idx + 1] == "/bin/sh"

    def test_run_with_user(self) -> None:
        """Test run as specific user."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "user": "1000:1000",
        }

        cmd = task._build_run_command(context)

        assert "--user" in cmd
        user_idx = cmd.index("--user")
        assert cmd[user_idx + 1] == "1000:1000"

    def test_run_with_memory_limit(self) -> None:
        """Test run with memory limit."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "memory": "512m",
            "memory_swap": "1g",
        }

        cmd = task._build_run_command(context)

        assert "--memory" in cmd
        assert "512m" in cmd
        assert "--memory-swap" in cmd
        assert "1g" in cmd

    def test_run_with_cpu_limit(self) -> None:
        """Test run with CPU limit."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "cpus": "0.5",
        }

        cmd = task._build_run_command(context)

        assert "--cpus" in cmd
        assert "0.5" in cmd

    def test_run_with_gpus(self) -> None:
        """Test run with GPU access."""
        task = DockerTask()
        context = {
            "image": "nvidia/cuda",
            "gpus": "all",
        }

        cmd = task._build_run_command(context)

        assert "--gpus" in cmd
        assert "all" in cmd

    def test_run_privileged(self) -> None:
        """Test run in privileged mode."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "privileged": True,
        }

        cmd = task._build_run_command(context)

        assert "--privileged" in cmd

    def test_run_with_capabilities(self) -> None:
        """Test run with Linux capabilities."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "cap_add": ["NET_ADMIN", "SYS_TIME"],
            "cap_drop": ["MKNOD"],
        }

        cmd = task._build_run_command(context)

        assert "--cap-add" in cmd
        assert "NET_ADMIN" in cmd
        assert "SYS_TIME" in cmd
        assert "--cap-drop" in cmd
        assert "MKNOD" in cmd

    def test_run_with_network(self) -> None:
        """Test run with specific network."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "network": "my-network",
        }

        cmd = task._build_run_command(context)

        assert "--network" in cmd
        assert "my-network" in cmd

    def test_run_with_hostname(self) -> None:
        """Test run with custom hostname."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "hostname": "my-container",
        }

        cmd = task._build_run_command(context)

        assert "--hostname" in cmd
        assert "my-container" in cmd

    def test_run_with_dns(self) -> None:
        """Test run with custom DNS."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "dns": ["8.8.8.8", "8.8.4.4"],
        }

        cmd = task._build_run_command(context)

        assert "--dns" in cmd
        assert "8.8.8.8" in cmd
        assert "8.8.4.4" in cmd

    def test_run_with_extra_hosts(self) -> None:
        """Test run with extra host mappings."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "extra_hosts": ["myhost:192.168.1.1"],
        }

        cmd = task._build_run_command(context)

        assert "--add-host" in cmd
        assert "myhost:192.168.1.1" in cmd

    def test_run_with_labels(self) -> None:
        """Test run with container labels."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "labels": {"app": "test", "env": "dev"},
        }

        cmd = task._build_run_command(context)

        assert "--label" in cmd
        assert "app=test" in cmd
        assert "env=dev" in cmd

    def test_run_with_ulimits(self) -> None:
        """Test run with ulimits."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "ulimit": {"nofile": "1024:2048"},
        }

        cmd = task._build_run_command(context)

        assert "--ulimit" in cmd
        assert "nofile=1024:2048" in cmd

    def test_run_with_security_opt(self) -> None:
        """Test run with security options."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "security_opt": ["no-new-privileges"],
        }

        cmd = task._build_run_command(context)

        assert "--security-opt" in cmd
        assert "no-new-privileges" in cmd

    def test_run_with_platform(self) -> None:
        """Test run with target platform."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "platform": "linux/amd64",
        }

        cmd = task._build_run_command(context)

        assert "--platform" in cmd
        assert "linux/amd64" in cmd

    def test_run_with_pull_policy(self) -> None:
        """Test run with pull policy."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "pull": "always",
        }

        cmd = task._build_run_command(context)

        assert "--pull" in cmd
        assert "always" in cmd

    def test_run_with_init(self) -> None:
        """Test run with init process."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "init": True,
        }

        cmd = task._build_run_command(context)

        assert "--init" in cmd

    def test_run_read_only(self) -> None:
        """Test run with read-only filesystem."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "read_only": True,
        }

        cmd = task._build_run_command(context)

        assert "--read-only" in cmd

    def test_run_detached(self) -> None:
        """Test run in detached mode."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "detach": True,
        }

        cmd = task._build_run_command(context)

        assert "-d" in cmd

    def test_run_no_remove(self) -> None:
        """Test run without auto-remove."""
        task = DockerTask()
        context = {
            "image": "alpine",
            "remove": False,
        }

        cmd = task._build_run_command(context)

        assert "--rm" not in cmd


class TestDockerTaskExec:
    """Test docker exec command building."""

    def test_exec_simple(self) -> None:
        """Test simple exec command."""
        task = DockerTask()
        context = {
            "name": "my-container",
            "command": "ls -la",
        }

        cmd = task._build_exec_command(context)

        assert cmd[0:2] == ["docker", "exec"]
        assert "my-container" in cmd
        assert "ls" in cmd
        assert "-la" in cmd

    def test_exec_with_workdir(self) -> None:
        """Test exec with working directory."""
        task = DockerTask()
        context = {
            "name": "my-container",
            "command": "pwd",
            "workdir": "/app",
        }

        cmd = task._build_exec_command(context)

        assert "-w" in cmd
        assert "/app" in cmd

    def test_exec_missing_name(self) -> None:
        """Test exec without container name fails."""
        task = DockerTask()
        context = {"command": "ls"}

        with pytest.raises(ValueError, match="name"):
            task._build_exec_command(context)

    def test_exec_missing_command(self) -> None:
        """Test exec without command fails."""
        task = DockerTask()
        context = {"name": "my-container"}

        with pytest.raises(ValueError, match="command"):
            task._build_exec_command(context)


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
