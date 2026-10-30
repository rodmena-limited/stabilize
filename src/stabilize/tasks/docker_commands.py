from __future__ import annotations

import os
import shlex
from typing import Any

_DANGEROUS_CAPABILITIES = frozenset(
    {
        "SYS_ADMIN",
        "SYS_PTRACE",
        "SYS_RAWIO",
        "SYS_MODULE",
        "DAC_READ_SEARCH",
        "NET_ADMIN",
        "NET_RAW",
        "SYS_BOOT",
        "ALL",
    }
)


class DockerCommandMixin:
    def _build_command(self, action: str, context: dict[str, Any]) -> list[str]:
        """Build Docker command based on action and context."""
        if action == "run":
            return self._build_run_command(context)
        elif action == "exec":
            return self._build_exec_command(context)
        elif action == "build":
            return self._build_build_command(context)
        elif action == "pull":
            image = context.get("image")
            if not image:
                raise ValueError("'image' is required for pull action")
            return ["docker", "pull", image]
        elif action == "ps":
            cmd = ["docker", "ps"]
            if context.get("all"):
                cmd.append("-a")
            return cmd
        elif action == "images":
            return ["docker", "images"]
        elif action == "logs":
            name = context.get("name")
            if not name:
                raise ValueError("'name' is required for logs action")
            cmd = ["docker", "logs"]
            if context.get("follow"):
                cmd.append("-f")
            if context.get("tail"):
                cmd.extend(["--tail", str(context["tail"])])
            cmd.append(name)
            return cmd
        elif action == "stop":
            name = context.get("name")
            if not name:
                raise ValueError("'name' is required for stop action")
            return ["docker", "stop", name]
        elif action == "rm":
            name = context.get("name")
            if not name:
                raise ValueError("'name' is required for rm action")
            cmd = ["docker", "rm"]
            if context.get("force"):
                cmd.append("-f")
            cmd.append(name)
            return cmd
        else:
            raise ValueError(f"Unknown action: {action}")

    def _build_run_command(self, context: dict[str, Any]) -> list[str]:
        """Build docker run command."""
        image = context.get("image")
        if not image:
            raise ValueError("'image' is required for run action")

        cmd = ["docker", "run"]

        # Container name
        if context.get("name"):
            cmd.extend(["--name", context["name"]])

        # Remove after exit
        if context.get("remove", True):
            cmd.append("--rm")

        # Detach mode
        if context.get("detach"):
            cmd.append("-d")

        # Interactive/TTY
        if context.get("stdin_open"):
            cmd.append("-i")
        if context.get("tty"):
            cmd.append("-t")

        # Entrypoint override
        entrypoint = context.get("entrypoint")
        if entrypoint:
            if isinstance(entrypoint, list):
                cmd.extend(["--entrypoint", entrypoint[0]])
            else:
                cmd.extend(["--entrypoint", entrypoint])

        # User
        if context.get("user"):
            cmd.extend(["--user", context["user"]])

        # Hostname
        if context.get("hostname"):
            cmd.extend(["--hostname", context["hostname"]])

        # Privileged mode - requires explicit opt-in
        if context.get("privileged"):
            if not context.get("allow_privileged", False):
                raise ValueError(
                    "Privileged mode requires 'allow_privileged: true' in context. "
                    "This grants full host access and should only be used when absolutely necessary."
                )
            cmd.append("--privileged")

        # Capabilities
        for cap in context.get("cap_add", []):
            if cap.upper() in _DANGEROUS_CAPABILITIES and not context.get("allow_dangerous_capabilities", False):
                raise ValueError(
                    f"Capability '{cap}' is dangerous and requires 'allow_dangerous_capabilities: true' in context."
                )
            cmd.extend(["--cap-add", cap])
        for cap in context.get("cap_drop", []):
            cmd.extend(["--cap-drop", cap])

        # Resource limits
        if context.get("memory"):
            cmd.extend(["--memory", context["memory"]])
        if context.get("memory_swap"):
            cmd.extend(["--memory-swap", context["memory_swap"]])
        if context.get("cpus"):
            cmd.extend(["--cpus", str(context["cpus"])])

        # GPU access
        if context.get("gpus"):
            cmd.extend(["--gpus", context["gpus"]])

        # Shared memory
        if context.get("shm_size"):
            cmd.extend(["--shm-size", context["shm_size"]])

        # tmpfs mounts
        for tmpfs in context.get("tmpfs", []):
            cmd.extend(["--tmpfs", tmpfs])

        # Read-only filesystem
        if context.get("read_only"):
            cmd.append("--read-only")

        # Security options
        for opt in context.get("security_opt", []):
            cmd.extend(["--security-opt", opt])

        # Ulimits
        for name, value in context.get("ulimit", {}).items():
            cmd.extend(["--ulimit", f"{name}={value}"])

        # Labels
        for key, value in context.get("labels", {}).items():
            cmd.extend(["--label", f"{key}={value}"])

        # DNS
        for dns in context.get("dns", []):
            cmd.extend(["--dns", dns])

        # Extra hosts
        for host in context.get("extra_hosts", []):
            cmd.extend(["--add-host", host])

        # Init process
        if context.get("init"):
            cmd.append("--init")

        # Platform
        if context.get("platform"):
            cmd.extend(["--platform", context["platform"]])

        # Pull policy
        if context.get("pull"):
            cmd.extend(["--pull", context["pull"]])

        # Restart policy
        if context.get("restart"):
            cmd.extend(["--restart", context["restart"]])

        for vol in context.get("volumes", []):
            parts = vol.split(":")
            host_path = parts[0] if parts else vol
            if ".." in host_path:
                raise ValueError(f"Volume mount path traversal blocked: {vol}")
            if not os.path.isabs(host_path):
                raise ValueError(f"Volume host path must be absolute: {vol}")
            cmd.extend(["-v", vol])

        # Ports
        for port in context.get("ports", []):
            cmd.extend(["-p", port])

        # Environment variables
        for key, value in context.get("environment", {}).items():
            cmd.extend(["-e", f"{key}={value}"])

        # Working directory
        if context.get("workdir"):
            cmd.extend(["-w", context["workdir"]])

        # Network
        if context.get("network"):
            cmd.extend(["--network", context["network"]])

        # Image
        cmd.append(image)

        # Command (must come after image)
        container_cmd = context.get("command")
        if container_cmd:
            if isinstance(container_cmd, str):
                # Use shlex to handle complex commands with quotes
                # This allows commands like: sh -c 'echo $VAR'
                cmd.extend(shlex.split(container_cmd))
            else:
                cmd.extend(container_cmd)

        return cmd

    def _build_exec_command(self, context: dict[str, Any]) -> list[str]:
        """Build docker exec command."""
        name = context.get("name")
        command = context.get("command")

        if not name:
            raise ValueError("'name' is required for exec action")
        if not command:
            raise ValueError("'command' is required for exec action")

        cmd = ["docker", "exec"]

        # Interactive/TTY
        if context.get("interactive"):
            cmd.append("-i")
        if context.get("tty"):
            cmd.append("-t")

        # Working directory
        if context.get("workdir"):
            cmd.extend(["-w", context["workdir"]])

        # Environment variables
        for key, value in context.get("environment", {}).items():
            cmd.extend(["-e", f"{key}={value}"])

        cmd.append(name)

        if isinstance(command, str):
            cmd.extend(shlex.split(command))
        else:
            cmd.extend(command)

        return cmd

    def _build_build_command(self, context: dict[str, Any]) -> list[str]:
        """Build docker build command."""
        cmd = ["docker", "build"]

        # Tag
        tag = context.get("tag") or context.get("image")
        if tag:
            cmd.extend(["-t", tag])

        # Dockerfile
        if context.get("dockerfile"):
            cmd.extend(["-f", context["dockerfile"]])

        # Build args
        for key, value in context.get("build_args", {}).items():
            cmd.extend(["--build-arg", f"{key}={value}"])

        # No cache
        if context.get("no_cache"):
            cmd.append("--no-cache")

        # Context path
        build_context = context.get("context", ".")
        cmd.append(build_context)

        return cmd
