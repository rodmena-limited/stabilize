"""Tasks and the cancel handler run by the release pipeline in scripts/release.py."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from stabilize import ShellTask, StabilizeHandler, StageExecution, Task, TaskResult, WorkflowStatus
from stabilize.queue.messages import CancelStage

# =============================================================================
# Project paths
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
INIT_PATH = PROJECT_ROOT / "src" / "stabilize" / "__init__.py"


# =============================================================================
# Custom Tasks
# =============================================================================


class VersionValidatorTask(Task):
    """
    Validate that pyproject.toml and __init__.py have matching versions.

    This task reads both files and compares the version strings.
    Fails terminally if versions don't match.

    Outputs:
        version: The validated version string
        pyproject_version: Version from pyproject.toml
        init_version: Version from __init__.py
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        pyproject_path = stage.context.get("pyproject_path", str(PYPROJECT_PATH))
        init_path = stage.context.get("init_path", str(INIT_PATH))

        # Read pyproject.toml version
        try:
            pyproject_content = Path(pyproject_path).read_text()
            pyproject_match = re.search(r'^version\s*=\s*"([^"]+)"', pyproject_content, re.MULTILINE)
            if not pyproject_match:
                return TaskResult.terminal(error=f"Could not find version in {pyproject_path}")
            pyproject_version = pyproject_match.group(1)
        except FileNotFoundError:
            return TaskResult.terminal(error=f"pyproject.toml not found at {pyproject_path}")
        except Exception as e:
            return TaskResult.terminal(error=f"Error reading pyproject.toml: {e}")

        # Read __init__.py version
        try:
            init_content = Path(init_path).read_text()
            init_match = re.search(r'^__version__\s*=\s*"([^"]+)"', init_content, re.MULTILINE)
            if not init_match:
                return TaskResult.terminal(error=f"Could not find __version__ in {init_path}")
            init_version = init_match.group(1)
        except FileNotFoundError:
            return TaskResult.terminal(error=f"__init__.py not found at {init_path}")
        except Exception as e:
            return TaskResult.terminal(error=f"Error reading __init__.py: {e}")

        # Compare versions
        if pyproject_version != init_version:
            return TaskResult.terminal(
                error=(
                    f"Version mismatch!\n"
                    f"  pyproject.toml: {pyproject_version}\n"
                    f"  __init__.py:    {init_version}\n"
                    f"Please sync versions before releasing."
                )
            )

        print(f"  [VersionValidator] Version validated: {pyproject_version}")
        return TaskResult.success(
            outputs={
                "version": pyproject_version,
                "pyproject_version": pyproject_version,
                "init_version": init_version,
            }
        )


class GitTagTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        import subprocess as sp

        version = stage.context.get("version")
        if not version:
            return TaskResult.terminal(error="No 'version' found in context (from upstream outputs)")

        cwd = stage.context.get("cwd", ".")
        tag = f"v{version}"
        cmd = f'git tag -a "{tag}" -m "Release {tag}" && git push origin "{tag}"'

        print(f"  [Create Git Tag] Running: {cmd}")
        sys.stdout.flush()

        try:
            result = sp.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print(f"  [Create Git Tag] Tagged {tag}")
                return TaskResult.success(outputs={"tag": tag})

            error = result.stderr.strip() or result.stdout.strip()
            if stage.context.get("continue_on_failure"):
                return TaskResult.failed_continue(error=f"Git tag failed: {error}", outputs={"tag": tag})
            return TaskResult.terminal(error=f"Git tag failed: {error}")
        except sp.TimeoutExpired:
            return TaskResult.terminal(error="Git tag timed out after 60s")
        except Exception as e:
            return TaskResult.terminal(error=f"Git tag error: {e}")


class SkipTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        reason = stage.context.get("reason", "Skipped by user request")
        print(f"  [Skip] {reason}")
        return TaskResult.success(outputs={"skipped": True, "reason": reason})


class StreamingShellTask(ShellTask):
    """ShellTask that streams stdout in real-time for long-running commands."""

    def execute(self, stage: StageExecution) -> TaskResult:
        command = stage.context.get("command", "")
        stage_name = stage.name

        print(f"  [{stage_name}] Running: {command[:80]}{'...' if len(command) > 80 else ''}")
        sys.stdout.flush()

        result = self._execute_streaming(stage)

        failed_statuses = {WorkflowStatus.TERMINAL, WorkflowStatus.FAILED_CONTINUE}
        if result.status in failed_statuses:
            print(f"  [{stage_name}] FAILED")
            if result.context and "error" in result.context:
                print(f"  [{stage_name}] Error: {str(result.context['error'])[:200]}")
        else:
            print(f"  [{stage_name}] Done")

        sys.stdout.flush()
        return result

    def _execute_streaming(self, stage: StageExecution) -> TaskResult:
        import subprocess as sp

        command = stage.context.get("command", "")
        timeout: int = stage.context.get("timeout", 60)
        cwd: str | None = stage.context.get("cwd")
        env_extra: dict[str, str] = stage.context.get("env", {})
        continue_on_failure: bool = stage.context.get("continue_on_failure", False)

        full_env = os.environ.copy()
        for k, v in env_extra.items():
            full_env[k] = str(v) if not isinstance(v, str) else v

        proc = sp.Popen(
            command,
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.STDOUT,
            cwd=cwd,
            env=full_env,
        )

        output_lines: list[str] = []
        try:
            assert proc.stdout is not None
            import time

            start = time.monotonic()
            for raw_line in proc.stdout:
                if time.monotonic() - start > timeout:
                    self._kill_process_tree(proc)
                    return TaskResult.terminal(
                        error=f"Command timed out after {timeout}s",
                        context={"stdout": "\n".join(output_lines)},
                    )
                line = raw_line.decode("utf-8", errors="replace").rstrip()
                output_lines.append(line)
                print(f"       {line}")
                sys.stdout.flush()
            proc.wait()
        except Exception as e:
            self._kill_process_tree(proc)
            return TaskResult.terminal(error=f"Streaming error: {e}")

        stdout_text = "\n".join(output_lines)
        outputs = {
            "stdout": stdout_text,
            "stderr": "",
            "returncode": proc.returncode,
            "truncated": False,
        }

        if proc.returncode == 0:
            return TaskResult.success(outputs=outputs)

        error_msg = f"Command exited with code {proc.returncode}"
        if continue_on_failure:
            return TaskResult.failed_continue(error=error_msg, outputs=outputs)
        return TaskResult.terminal(error=error_msg, context=outputs)


class CancelStageHandler(StabilizeHandler):
    """Handler for CancelStage messages - marks stages as canceled."""

    @property
    def message_type(self) -> type[CancelStage]:
        return CancelStage  # type: ignore[no-any-return]

    def handle(self, message: CancelStage) -> None:
        """Mark the stage as canceled."""

        def on_stage(stage: StageExecution) -> None:
            stage.status = WorkflowStatus.CANCELED
            self.repository.store_stage(stage)

        self.with_stage(message, on_stage)
