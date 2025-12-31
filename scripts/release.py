from __future__ import annotations
import argparse
import re
import sys
from pathlib import Path
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    ShellTask,
    SqliteQueue,
    SqliteWorkflowStore,
    StabilizeHandler,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.queue.messages import CancelStage
PROJECT_ROOT = Path(__file__).parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
INIT_PATH = PROJECT_ROOT / "src" / "stabilize" / "__init__.py"

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

class SkipTask(Task):
    """A task that immediately succeeds - used for skipped stages."""

    def execute(self, stage: StageExecution) -> TaskResult:
        reason = stage.context.get("reason", "Skipped by user request")
        print(f"  [Skip] {reason}")
        return TaskResult.success(outputs={"skipped": True, "reason": reason})

class VerboseShellTask(ShellTask):
    """ShellTask that prints progress before and after execution."""

    def execute(self, stage: StageExecution) -> TaskResult:
        command = stage.context.get("command", "")
        stage_name = stage.name

        # Print what we're about to do
        print(f"  [{stage_name}] Running: {command[:60]}{'...' if len(command) > 60 else ''}")
        sys.stdout.flush()

        # Run the actual command
        result = super().execute(stage)

        # Print result based on status
        failed_statuses = {WorkflowStatus.TERMINAL, WorkflowStatus.FAILED_CONTINUE}
        if result.status in failed_statuses:
            print(f"  [{stage_name}] FAILED")
            if result.context and "error" in result.context:
                error_preview = str(result.context["error"])[:200]
                print(f"  [{stage_name}] Error: {error_preview}")
        else:
            print(f"  [{stage_name}] Done")

        sys.stdout.flush()
        return result

class CancelStageHandler(StabilizeHandler):
    """Handler for CancelStage messages - marks stages as canceled."""
