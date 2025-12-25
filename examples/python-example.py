import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

class PythonTask(Task):
    """
    Execute Python code.

    Context Parameters:
        script: Inline Python code to execute (string)
        script_file: Path to Python script file (alternative to script)
        args: Command line arguments as list (optional)
        inputs: Input variables as dict, available as INPUT in script (optional)
        python_path: Python interpreter path (default: current interpreter)
        timeout: Execution timeout in seconds (default: 60)

    Outputs:
        stdout: Standard output
        stderr: Standard error
        exit_code: Process exit code
        result: Value of RESULT variable if set in script

    Notes:
        - Scripts can access INPUT dict for inputs
        - Scripts should set RESULT variable for return value
        - RESULT must be JSON-serializable
    """
    WRAPPER_TEMPLATE = '\nimport json\nimport sys\n\n# Input data\ntry:\n    INPUT = json.loads(\'\'\'{inputs}\'\'\')\nexcept json.JSONDecodeError:\n    # Fallback for simple cases or empty\n    INPUT = {{}}\n\n# User script\n{script}\n\n# Output result if set\nif \'RESULT\' in dir():\n    print("__RESULT_START__")\n    print(json.dumps(RESULT))\n    print("__RESULT_END__")\n'
