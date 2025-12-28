from __future__ import annotations
import base64
import logging
import os
import subprocess
from typing import TYPE_CHECKING, Any
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
logger = logging.getLogger(__name__)
RESERVED_KEYS = frozenset(
    {
        "command",
        "timeout",
        "cwd",
        "env",
        "shell",
        "stdin",
        "max_output_size",
        "expected_codes",
        "secrets",
        "binary",
        "continue_on_failure",
    }
)
