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
