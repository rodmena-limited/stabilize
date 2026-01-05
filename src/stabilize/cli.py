"""Stabilize CLI for database migrations and developer tools."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue


# =============================================================================
# RAG PROMPT - Comprehensive documentation for AI coding agents
# =============================================================================

PROMPT_TEXT = r'''
===============================================================================
STABILIZE WORKFLOW ENGINE - COMPLETE REFERENCE FOR CODE GENERATION
===============================================================================

Stabilize is a Python DAG-based workflow orchestration engine. Workflows consist
of Stages (nodes in the DAG) containing Tasks (atomic work units). Stages can
run sequentially or in parallel based on their dependencies.

CRITICAL RULE FOR SHELL WORKFLOWS:
For shell/command workflows, import the BUILT-IN ShellTask:
    from stabilize import ShellTask
    registry.register("shell", ShellTask)

DO NOT define your own ShellTask class - use the built-in one! It automatically
substitutes {key} placeholders with upstream outputs (e.g., {stdout} becomes actual output).

===============================================================================
1. COMPLETE WORKING EXAMPLE - COPY THIS AS YOUR STARTING TEMPLATE
===============================================================================

#!/usr/bin/env python3
"""Minimal working Stabilize workflow example."""

from stabilize import (
    # Core models
    Workflow, StageExecution, TaskExecution, WorkflowStatus,
    # Infrastructure (use SqliteWorkflowStore/SqliteQueue or Postgres variants)
    Orchestrator, QueueProcessor,
    Queue, SqliteQueue,                    # Queue abstract + SQLite impl
    WorkflowStore, SqliteWorkflowStore,    # Store abstract + SQLite impl
    # Tasks - use built-in tasks, do NOT define your own
    Task, TaskResult, TaskRegistry,
    ShellTask,      # For shell/command execution
    PythonTask,     # For Python code execution (uses script/INPUT/RESULT)
    DockerTask,     # For Docker container execution
    HTTPTask,       # For HTTP requests
    # Handlers
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler,
    CompleteWorkflowHandler,
)


# Step 1: USE BUILT-IN TASKS - Do NOT define your own Task classes!
# Available built-in tasks:
#   - ShellTask: For shell/command execution
#   - PythonTask: For Python code execution (uses script/INPUT/RESULT)
#   - DockerTask: For Docker container execution
#   - HTTPTask: For HTTP API requests
#
# Only define custom Task classes if the built-in tasks don't meet your needs.
# Example custom task (rarely needed):
#
# class MyCustomTask(Task):
#     def execute(self, stage: StageExecution) -> TaskResult:
#         value = stage.context.get("key")
#         return TaskResult.success(outputs={"result": value})


# Step 2: Setup infrastructure
def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with task registered."""
    task_registry = TaskRegistry()
    # Register built-in tasks
    task_registry.register("shell", ShellTask)
    task_registry.register("python", PythonTask)
    task_registry.register("docker", DockerTask)
    task_registry.register("http", HTTPTask)

    processor = QueueProcessor(queue)

    # Register all handlers in order
    handlers = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, task_registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]
    for handler in handlers:
        processor.register_handler(handler)

    orchestrator = Orchestrator(queue)
    return processor, orchestrator


# Step 3: Create and run workflow
def main():
    # Initialize storage (in-memory SQLite for development)
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Create workflow with stages using built-in tasks
    workflow = Workflow.create(
        application="my-app",
        name="My Pipeline",
        stages=[
            StageExecution(
                ref_id="1",
                type="shell",  # Use built-in ShellTask
                name="First Stage",
                context={"command": "echo 'Hello World'"},
                tasks=[
                    TaskExecution.create(
                        name="Run Shell",
                        implementing_class="shell",    # Must match registered name
                        stage_start=True,              # REQUIRED for first task
                        stage_end=True,                # REQUIRED for last task
                    ),
                ],
            ),
        ],
    )

    # Execute workflow
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Check result
    result = store.retrieve(workflow.id)
    print(f"Status: {result.status}")
    print(f"Output: {result.stages[0].outputs}")


if __name__ == "__main__":
    main()

===============================================================================
1.1 SHELL PIPELINE TEMPLATE - USE FOR ANY SHELL/COMMAND WORKFLOWS
===============================================================================
For shell commands, IMPORT the built-in ShellTask (do NOT define your own):

from stabilize import ShellTask, TaskRegistry

registry = TaskRegistry()
registry.register("shell", ShellTask)

ShellTask Context Parameters:
  command (str)         - The shell command to execute (required)
  timeout (int)         - Timeout in seconds (default: 60)
  cwd (str)             - Working directory
  env (dict)            - Additional environment variables
  shell (bool|str)      - True for default shell, or path like "/bin/bash"
  stdin (str)           - Input to send to command stdin
  max_output_size (int) - Max bytes for output (default: 10MB)
  expected_codes (list) - Exit codes treated as success (default: [0])
  secrets (list)        - Context keys to mask in logs
  binary (bool)         - Capture output as bytes (default: False)
  continue_on_failure   - Return failed_continue instead of terminal

ShellTask Outputs:
  stdout, stderr, returncode, truncated (bool), stdout_b64 (if binary)

# Example: Pipeline with upstream output substitution
stages=[
    StageExecution(
        ref_id="1", type="shell", name="Get Data",
        context={"command": "git status"},
        tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
    ),
    StageExecution(
        ref_id="2", type="shell", name="Save Data",
        requisite_stage_ref_ids={"1"},
        context={"command": "echo '{stdout}' > /tmp/output.txt"},  # {stdout} auto-replaced
        tasks=[TaskExecution.create("Save", "shell", stage_start=True, stage_end=True)],
    ),
]

# Example: With environment and working directory
context={"command": "npm install", "cwd": "/app", "env": {"NODE_ENV": "production"}}

# Example: With secrets masking
context={"command": "curl -H 'Auth: {token}' api.com", "token": "secret", "secrets": ["token"]}

# Example: Allow grep's exit code 1 (no match found)
context={"command": "grep pattern file.txt", "expected_codes": [0, 1]}

===============================================================================
1.2 HTTP PIPELINE TEMPLATE - USE FOR ANY HTTP/API WORKFLOWS
===============================================================================
For HTTP requests, IMPORT the built-in HTTPTask (do NOT define your own):

from stabilize import HTTPTask, TaskRegistry

registry = TaskRegistry()
registry.register("http", HTTPTask)

HTTPTask Context Parameters:
  url (str)             - Request URL (required)
  method (str)          - GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS (default: GET)

  Request Body (mutually exclusive):
    body (str|bytes)    - Raw request body
    json (dict)         - JSON body (auto-serialized, sets Content-Type)
    form (dict)         - Form-encoded body (application/x-www-form-urlencoded)

  Headers & Auth:
    headers (dict)      - Custom request headers
    auth (list)         - Basic auth as [username, password]
    bearer_token (str)  - Bearer token for Authorization header

  File Upload:
    upload_file (str)   - Path to file to upload (multipart/form-data)
    upload_field (str)  - Form field name (default: "file")
    upload_form (dict)  - Additional form fields with upload

  File Download:
    download_to (str)   - Path to save response body

  Timeouts & Retries:
    timeout (int)       - Request timeout in seconds (default: 30)
    retries (int)       - Number of retries (default: 0)
    retry_delay (float) - Delay between retries (default: 1.0)
    retry_on_status (list) - Status codes to retry (default: [502, 503, 504])

  Response Handling:
    expected_status (int|list) - Expected status code(s)
    parse_json (bool)   - Auto-parse JSON response (default: False)
    max_response_size (int) - Max bytes (default: 10MB)

  Other:
    verify_ssl (bool)   - Verify SSL certs (default: True)
    continue_on_failure - Return failed_continue instead of terminal

HTTPTask Outputs:
  status_code, headers, body, body_json (if parse_json), elapsed_ms, url, content_type, content_length

# Example: Simple GET with JSON parsing
context={"url": "https://api.example.com/users", "parse_json": True}

# Example: POST with JSON body
context={"url": "https://api.example.com/users", "method": "POST", "json": {"name": "John"}}

# Example: With Bearer token authentication
context={"url": "https://api.example.com/private", "bearer_token": "my-token"}

# Example: File upload
context={"url": "https://api.example.com/upload", "method": "POST", "upload_file": "/path/to/file.pdf"}

# Example: Download file
context={"url": "https://example.com/report.pdf", "download_to": "/tmp/report.pdf"}

# Example: With retries for unreliable endpoints
context={"url": "https://api.example.com/data", "retries": 3, "retry_delay": 2.0}

===============================================================================
1.3 PYTHON PIPELINE TEMPLATE - USE FOR PYTHON CODE EXECUTION
===============================================================================
For Python code execution, IMPORT the built-in PythonTask (do NOT define your own):

from stabilize import PythonTask, TaskRegistry

registry = TaskRegistry()
registry.register("python", PythonTask)

PythonTask Context Parameters:
  # Execution Mode (choose one):
  script (str)          - Inline Python code to execute
  script_file (str)     - Path to Python script file
  module (str)          - Module path (e.g., "myapp.tasks")
  function (str)        - Function name (required with module)

  # Inputs:
  inputs (dict)         - Input data, available as INPUT in script
  args (list)           - Command line arguments

  # Execution:
  python_path (str)     - Python interpreter (default: current)
  timeout (int)         - Timeout in seconds (default: 60)
  cwd (str)             - Working directory
  env (dict)            - Additional environment variables
  continue_on_failure   - Return failed_continue instead of terminal

PythonTask Outputs:
  stdout, stderr, exit_code, result (value of RESULT variable if set)

Script Convention:
  - Access inputs via INPUT dict (includes upstream outputs + explicit inputs)
  - Set return value via RESULT variable (must be JSON-serializable)

# Example: Inline script with INPUT and RESULT
stages=[
    StageExecution(
        ref_id="1", type="python", name="Calculate",
        context={
            "script": """
numbers = INPUT["values"]
RESULT = {"sum": sum(numbers), "avg": sum(numbers) / len(numbers)}
""",
            "inputs": {"values": [1, 2, 3, 4, 5]}
        },
        tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
    ),
]

# Example: Module + function mode (calls myapp.validators.validate(INPUT))
context={"module": "myapp.validators", "function": "validate", "inputs": {"data": {...}}}

# Example: Script file
context={"script_file": "/path/to/script.py", "inputs": {"config": {...}}}

===============================================================================
1.4 DOCKER PIPELINE TEMPLATE - USE FOR CONTAINER EXECUTION
===============================================================================
For Docker container execution, IMPORT the built-in DockerTask (do NOT define your own):

from stabilize import DockerTask, TaskRegistry

registry = TaskRegistry()
registry.register("docker", DockerTask)

DockerTask Actions:
  run     - Run a container (default)
  exec    - Execute command in running container
  build   - Build image from Dockerfile
  pull    - Pull image from registry
  ps      - List containers
  images  - List images
  logs    - Get container logs
  stop    - Stop container
  rm      - Remove container

DockerTask Context Parameters (run action):
  image (str)           - Docker image (required)
  command (str|list)    - Command to run in container
  entrypoint (str|list) - Override container entrypoint
  name (str)            - Container name
  user (str)            - Run as user (e.g., "1000:1000")
  hostname (str)        - Container hostname

  # Mounts & Network:
  volumes (list)        - Volume mounts as "host:container"
  ports (list)          - Port mappings as "host:container"
  network (str)         - Docker network name
  dns (list)            - Custom DNS servers
  extra_hosts (list)    - Add host mappings as "host:ip"

  # Environment:
  environment (dict)    - Environment variables
  workdir (str)         - Working directory

  # Resources:
  memory (str)          - Memory limit (e.g., "512m", "2g")
  memory_swap (str)     - Memory + swap limit
  cpus (str)            - CPU limit (e.g., "0.5", "2")
  gpus (str)            - GPU access (e.g., "all", "device=0")
  shm_size (str)        - Shared memory size

  # Security:
  privileged (bool)     - Privileged mode
  cap_add (list)        - Add Linux capabilities
  cap_drop (list)       - Drop Linux capabilities
  security_opt (list)   - Security options
  read_only (bool)      - Read-only root filesystem

  # Other:
  remove (bool)         - Remove after exit (default: True)
  detach (bool)         - Run in background
  init (bool)           - Run init inside container
  platform (str)        - Target platform (e.g., "linux/amd64")
  pull (str)            - Pull policy: "always", "never", "missing"
  labels (dict)         - Container labels
  timeout (int)         - Command timeout (default: 300)
  continue_on_failure   - Return failed_continue instead of terminal

DockerTask Outputs:
  stdout, stderr, exit_code, container_id (if detach), image_id (if build)

# Example: Simple container run
context={"action": "run", "image": "python:3.11", "command": "python -c 'print(1+1)'"}

# Example: With volumes and environment
context={
    "action": "run",
    "image": "node:18",
    "volumes": ["/app:/app"],
    "environment": {"NODE_ENV": "production"},
    "workdir": "/app",
    "command": "npm test"
}

# Example: GPU container with resource limits
context={
    "action": "run",
    "image": "pytorch/pytorch:latest",
    "gpus": "all",
    "memory": "8g",
    "shm_size": "2g",
    "volumes": ["/data:/data"],
    "command": "python train.py"
}

# Example: Build and tag image
context={"action": "build", "tag": "myapp:latest", "context": "./docker"}

===============================================================================
2. CORE CLASSES API
===============================================================================

2.1 Workflow
-------------
Factory: Workflow.create(application, name, stages, trigger=None, pipeline_config_id=None)

Fields:
  id: str                    - Unique ULID identifier (auto-generated)
  status: WorkflowStatus     - Current execution status
  stages: list[StageExecution] - All stages in the workflow
  application: str           - Application name
  name: str                  - Pipeline name

Methods:
  stage_by_id(stage_id) -> StageExecution    - Get stage by internal ID
  stage_by_ref_id(ref_id) -> StageExecution  - Get stage by reference ID
  get_context() -> dict                      - Get merged outputs from all stages


2.2 StageExecution
-------------------
Constructor: StageExecution(ref_id, type, name, context, tasks, requisite_stage_ref_ids=set())

Fields:
  ref_id: str                         - UNIQUE reference ID for DAG (e.g., "1", "deploy", "build")
  type: str                           - Stage type (usually matches task name)
  name: str                           - Human-readable name
  context: dict[str, Any]             - INPUT parameters for this stage
  outputs: dict[str, Any]             - OUTPUT values for downstream stages (populated by tasks)
  tasks: list[TaskExecution]          - Tasks to execute (sequentially)
  requisite_stage_ref_ids: set[str]   - Dependencies (ref_ids of upstream stages)
  status: WorkflowStatus              - Current status

DAG Dependencies:
  - Empty set: Stage runs immediately (initial stage)
  - {"A"}: Stage runs after stage with ref_id="A" completes
  - {"A", "B"}: Stage waits for BOTH A and B to complete (join point)


2.3 TaskExecution
------------------
Factory: TaskExecution.create(name, implementing_class, stage_start=False, stage_end=False)

Fields:
  name: str                  - Human-readable task name
  implementing_class: str    - MUST match the name used in TaskRegistry.register()
  stage_start: bool          - MUST be True for first task in stage
  stage_end: bool            - MUST be True for last task in stage
  status: WorkflowStatus     - Current status

CRITICAL: If a stage has only one task, set BOTH stage_start=True AND stage_end=True


2.4 WorkflowStatus
-------------------
All status values:
  NOT_STARTED     - Not yet started
  RUNNING         - Currently executing
  PAUSED          - Paused, can be resumed
  SUSPENDED       - Waiting for external trigger
  SUCCEEDED       - Completed successfully
  FAILED_CONTINUE - Failed but pipeline continues
  TERMINAL        - Failed, pipeline halts
  CANCELED        - Execution was canceled
  STOPPED         - Execution was stopped
  SKIPPED         - Stage/task was skipped
  REDIRECT        - Decision branch redirect
  BUFFERED        - Buffered, waiting

Properties:
  .is_complete: bool    - Has finished executing
  .is_halt: bool        - Blocks downstream stages
  .is_successful: bool  - SUCCEEDED, STOPPED, or SKIPPED
  .is_failure: bool     - TERMINAL, STOPPED, or FAILED_CONTINUE

===============================================================================
3. TASK IMPLEMENTATION
===============================================================================

3.1 Task Interface (Abstract Base Class)
-----------------------------------------
from stabilize import Task

class MyTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        # Read from stage.context (includes upstream outputs)
        value = stage.context.get("key")

        # Return TaskResult
        return TaskResult.success(outputs={"output_key": "value"})

    # Optional: Handle timeout (for RetryableTask)
    def on_timeout(self, stage: StageExecution) -> TaskResult | None:
        return TaskResult.terminal(error="Task timed out")

    # Optional: Handle cancellation
    def on_cancel(self, stage: StageExecution) -> TaskResult | None:
        return TaskResult.canceled()


3.2 TaskResult Factory Methods - CRITICAL REFERENCE
----------------------------------------------------
from stabilize import TaskResult

SUCCESS - Task completed successfully, pipeline continues:
    TaskResult.success(outputs=None, context=None)
    Parameters:
      outputs: dict  - Values available to downstream stages
      context: dict  - Values stored in stage.context (stage-scoped)

RUNNING - Task needs to poll again (for RetryableTask):
    TaskResult.running(context=None)
    Parameters:
      context: dict  - Updated state for next poll iteration

TERMINAL - Task failed, pipeline HALTS:
    TaskResult.terminal(error, context=None)
    Parameters:
      error: str     - Error message (REQUIRED)
      context: dict  - Additional context data
    WARNING: Does NOT accept 'outputs' parameter!

FAILED_CONTINUE - Task failed but pipeline continues:
    TaskResult.failed_continue(error, outputs=None, context=None)
    Parameters:
      error: str     - Error message (REQUIRED)
      outputs: dict  - Values still available downstream
      context: dict  - Additional context data

SKIPPED - Task was skipped:
    TaskResult.skipped()

CANCELED - Task was canceled:
    TaskResult.canceled(outputs=None)

STOPPED - Task was stopped:
    TaskResult.stopped(outputs=None)

REDIRECT - Indicates decision branch redirect:
    TaskResult.redirect(context=None)
    Parameters:
      context: dict  - Context for the redirect

Builder Pattern (for complex results):
    TaskResult.builder(status).context({...}).outputs({...}).build()

    Methods:
      .context(dict)           - Set the full context
      .outputs(dict)           - Set the full outputs
      .add_context(key, value) - Add a single context value
      .add_output(key, value)  - Add a single output value
      .build()                 - Build and return the TaskResult


3.3 RetryableTask - For Polling Operations
-------------------------------------------
from datetime import timedelta
from stabilize import RetryableTask

class PollTask(RetryableTask):
    def get_timeout(self) -> timedelta:
        """Maximum time before task times out."""
        return timedelta(minutes=30)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        """Time to wait between poll attempts."""
        return timedelta(seconds=10)

    def execute(self, stage: StageExecution) -> TaskResult:
        status = check_external_system()

        if status == "complete":
            return TaskResult.success(outputs={"status": "done"})
        elif status == "failed":
            return TaskResult.terminal(error="External system failed")
        else:
            # Keep polling - will be called again after backoff
            return TaskResult.running(context={"last_check": time.time()})


3.4 SkippableTask - Conditional Execution
------------------------------------------
from stabilize.tasks.interface import SkippableTask  # Advanced, not in main exports

class ConditionalTask(SkippableTask):
    def is_enabled(self, stage: StageExecution) -> bool:
        """Return False to skip this task."""
        return stage.context.get("should_run", True)

    def do_execute(self, stage: StageExecution) -> TaskResult:
        """Actual task logic (only called if is_enabled returns True)."""
        return TaskResult.success()


3.5 Additional Built-in Tasks
------------------------------
from stabilize.tasks.interface import CallableTask, NoOpTask, WaitTask

OverridableTimeoutRetryableTask:
    A RetryableTask that allows the stage to override timeout via 'stageTimeoutMs'
    context value. Useful when timeout should be configurable per-stage.

CallableTask:
    Wraps a callable function as a task without creating a class.

    def my_task(stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"result": "done"})

    task = CallableTask(my_task)
    registry.register("my_task", task)

NoOpTask:
    A task that does nothing and returns success immediately.
    Useful for testing, placeholder stages, or synchronization points.

    registry.register("noop", NoOpTask)

WaitTask:
    Built-in RetryableTask that waits for a specified duration.
    Reads 'waitTime' (seconds) from stage.context.

    StageExecution(
        ref_id="wait",
        type="wait",
        name="Wait 30 seconds",
        context={"waitTime": 30},
        tasks=[TaskExecution.create("Wait", "wait", stage_start=True, stage_end=True)],
    )

===============================================================================
4. TASK REGISTRY
===============================================================================

from stabilize import TaskRegistry

registry = TaskRegistry()

# Register a task class
registry.register("my_task", MyTask)

# Register with aliases
registry.register("http", HTTPTask, aliases=["http_request", "web_request"])

# The implementing_class in TaskExecution MUST match the registered name:
TaskExecution.create(
    name="Do something",
    implementing_class="my_task",  # Must match registry.register() name
    stage_start=True,
    stage_end=True,
)

===============================================================================
5. DAG PATTERNS
===============================================================================

5.1 Sequential Stages (A -> B -> C)
------------------------------------
stages=[
    StageExecution(ref_id="A", ..., requisite_stage_ref_ids=set()),      # Initial
    StageExecution(ref_id="B", ..., requisite_stage_ref_ids={"A"}),      # After A
    StageExecution(ref_id="C", ..., requisite_stage_ref_ids={"B"}),      # After B
]


5.2 Parallel Stages
--------------------
       A
      / \
     B   C    <- B and C run in parallel after A
      \ /
       D

stages=[
    StageExecution(ref_id="A", ..., requisite_stage_ref_ids=set()),
    StageExecution(ref_id="B", ..., requisite_stage_ref_ids={"A"}),    # Parallel
    StageExecution(ref_id="C", ..., requisite_stage_ref_ids={"A"}),    # Parallel
    StageExecution(ref_id="D", ..., requisite_stage_ref_ids={"B", "C"}), # Join
]


5.3 Complex DAG
----------------
     A
    /|\
   B C D     <- All parallel after A
   |/ \|
   E   F     <- E waits for B,C; F waits for C,D
    \ /
     G       <- G waits for E and F

stages=[
    StageExecution(ref_id="A", ..., requisite_stage_ref_ids=set()),
    StageExecution(ref_id="B", ..., requisite_stage_ref_ids={"A"}),
    StageExecution(ref_id="C", ..., requisite_stage_ref_ids={"A"}),
    StageExecution(ref_id="D", ..., requisite_stage_ref_ids={"A"}),
    StageExecution(ref_id="E", ..., requisite_stage_ref_ids={"B", "C"}),
    StageExecution(ref_id="F", ..., requisite_stage_ref_ids={"C", "D"}),
    StageExecution(ref_id="G", ..., requisite_stage_ref_ids={"E", "F"}),
]

===============================================================================
6. CONTEXT AND OUTPUTS DATA FLOW
===============================================================================

stage.context  - INPUT: Parameters passed when creating the stage
                 Also includes outputs from upstream stages (automatic lookup)

stage.outputs  - OUTPUT: Values produced by tasks for downstream stages
                 Set via TaskResult.success(outputs={...})

Example flow:
  Stage A context: {"input": "hello"}
  Stage A task returns: TaskResult.success(outputs={"result": "processed"})
  Stage B context: {"input": "hello", "result": "processed"}  <- Includes A's output

Accessing in tasks:
  def execute(self, stage):
      # Read from context (includes upstream outputs)
      upstream_result = stage.context.get("result")  # From upstream stage

      # Write to outputs (available downstream)
      return TaskResult.success(outputs={"my_output": "value"})

IMPORTANT - Shell Tasks with Upstream Outputs:
  Use the BUILT-IN ShellTask which automatically substitutes {key} placeholders:

  from stabilize import ShellTask
  registry.register("shell", ShellTask)

  The built-in ShellTask handles: cwd, env, stdin, timeout, expected_codes, secrets, binary mode.
  See section 1.1 for full parameter documentation.

===============================================================================
7. COMMON MISTAKES AND HOW TO FIX THEM
===============================================================================

MISTAKE 1: Using 'outputs' parameter with TaskResult.terminal()
---------------------------------------------------------------
WRONG:
    return TaskResult.terminal(error="Failed", outputs={"data": value})

RIGHT:
    return TaskResult.terminal(error="Failed", context={"data": value})

terminal() only accepts: error (required), context (optional)


MISTAKE 2: Forgetting stage_start and stage_end on tasks
---------------------------------------------------------
WRONG:
    TaskExecution.create(name="X", implementing_class="y")

RIGHT:
    TaskExecution.create(name="X", implementing_class="y", stage_start=True, stage_end=True)


MISTAKE 3: implementing_class doesn't match registered name
------------------------------------------------------------
WRONG:
    registry.register("http_task", HTTPTask)
    TaskExecution.create(..., implementing_class="HTTPTask")  # Class name, not registered name

RIGHT:
    registry.register("http_task", HTTPTask)
    TaskExecution.create(..., implementing_class="http_task")  # Matches registered name


MISTAKE 4: Duplicate ref_id values
-----------------------------------
WRONG:
    StageExecution(ref_id="1", name="Stage A", ...)
    StageExecution(ref_id="1", name="Stage B", ...)  # Same ref_id!

RIGHT:
    StageExecution(ref_id="1", name="Stage A", ...)
    StageExecution(ref_id="2", name="Stage B", ...)  # Unique ref_ids


MISTAKE 5: Missing handlers
----------------------------
All 7 handlers are REQUIRED for the engine to work:
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler, CompleteWorkflowHandler


MISTAKE 6: Forgetting requisite_stage_ref_ids for sequential stages
--------------------------------------------------------------------
WRONG - Stages may run in parallel, stage 2 won't have stage 1 outputs:
    StageExecution(ref_id="1", context={"command": "git status"}, ...),
    StageExecution(ref_id="2", context={"command": "echo {stdout}"}, ...),  # No dependency!

RIGHT - Stage 2 waits for stage 1 and receives its outputs:
    StageExecution(ref_id="1", context={"command": "git status"}, ...),
    StageExecution(ref_id="2", requisite_stage_ref_ids={"1"}, context={"command": "echo {stdout}"}, ...),

Without requisite_stage_ref_ids, stages run in parallel and upstream outputs are NOT available.


MISTAKE 7: Using $variable instead of {variable} for upstream outputs
----------------------------------------------------------------------
WRONG - Shell variable syntax doesn't work:
    context={"command": "echo $stdout > file.txt"}  # $stdout is shell variable, not context

RIGHT - Use {key} placeholders that ShellTask substitutes:
    context={"command": "echo '{stdout}' > file.txt"}  # {stdout} replaced by task


MISTAKE 8: Defining your own ShellTask instead of using built-in
-----------------------------------------------------------------
WRONG - Defining custom ShellTask that may lack features:
    class ShellTask(Task):
        def execute(self, stage):
            command = stage.context.get("command")
            result = subprocess.run(command, shell=True, ...)

RIGHT - Use the built-in ShellTask which handles everything:
    from stabilize import ShellTask
    registry.register("shell", ShellTask)

===============================================================================
8. COMPLETE EXAMPLE: SEQUENTIAL PIPELINE WITH ERROR HANDLING
===============================================================================

#!/usr/bin/env python3
from stabilize import (
    Workflow, StageExecution, TaskExecution, WorkflowStatus,
    Orchestrator, QueueProcessor, SqliteQueue, SqliteWorkflowStore,
    Task, TaskResult, TaskRegistry,
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler,
    CompleteWorkflowHandler,
)


class ValidateTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        data = stage.context.get("data")
        if not data:
            return TaskResult.terminal(error="No data provided")
        return TaskResult.success(outputs={"validated": True, "data": data})


class ProcessTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        data = stage.context.get("data")
        validated = stage.context.get("validated")
        if not validated:
            return TaskResult.terminal(error="Data not validated")
        result = data.upper()
        return TaskResult.success(outputs={"processed_data": result})


class NotifyTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        processed = stage.context.get("processed_data")
        # Even if notification fails, we don't want to fail the pipeline
        try:
            send_notification(processed)
            return TaskResult.success(outputs={"notified": True})
        except Exception as e:
            # Use failed_continue to not halt the pipeline
            return TaskResult.failed_continue(
                error=f"Notification failed: {e}",
                outputs={"notified": False}
            )


def setup_pipeline_runner(store, queue):
    registry = TaskRegistry()
    registry.register("validate", ValidateTask)
    registry.register("process", ProcessTask)
    registry.register("notify", NotifyTask)

    processor = QueueProcessor(queue)
    handlers = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]
    for h in handlers:
        processor.register_handler(h)

    return processor, Orchestrator(queue)


def main():
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="data-pipeline",
        name="Process Data",
        stages=[
            StageExecution(
                ref_id="validate",
                type="validate",
                name="Validate Input",
                context={"data": "hello world"},
                tasks=[TaskExecution.create("Validate", "validate", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="process",
                type="process",
                name="Process Data",
                requisite_stage_ref_ids={"validate"},
                context={},  # Will receive 'data' from upstream
                tasks=[TaskExecution.create("Process", "process", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="notify",
                type="notify",
                name="Send Notification",
                requisite_stage_ref_ids={"process"},
                context={},
                tasks=[TaskExecution.create("Notify", "notify", stage_start=True, stage_end=True)],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"Final status: {result.status}")
    for stage in result.stages:
        print(f"  {stage.name}: {stage.status} - {stage.outputs}")


if __name__ == "__main__":
    main()

===============================================================================
9. COMPLETE EXAMPLE: PARALLEL STAGES WITH JOIN
===============================================================================

#!/usr/bin/env python3
from stabilize import (
    Workflow, StageExecution, TaskExecution,
    Orchestrator, QueueProcessor, SqliteQueue, SqliteWorkflowStore,
    Task, TaskResult, TaskRegistry,
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler,
    CompleteWorkflowHandler,
)


class FetchDataTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        source = stage.context.get("source")
        # Simulate fetching data from different sources
        data = f"data_from_{source}"
        return TaskResult.success(outputs={f"{source}_data": data})


class AggregateTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        # Collect data from all upstream parallel stages
        api_data = stage.context.get("api_data")
        db_data = stage.context.get("db_data")
        cache_data = stage.context.get("cache_data")
        combined = f"{api_data} + {db_data} + {cache_data}"
        return TaskResult.success(outputs={"combined_data": combined})


def setup_pipeline_runner(store, queue):
    registry = TaskRegistry()
    registry.register("fetch", FetchDataTask)
    registry.register("aggregate", AggregateTask)

    processor = QueueProcessor(queue)
    for h in [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]:
        processor.register_handler(h)

    return processor, Orchestrator(queue)


def main():
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    #        Start
    #       /  |  \
    #     API  DB  Cache    <- Run in parallel
    #       \  |  /
    #      Aggregate        <- Join point

    workflow = Workflow.create(
        application="parallel-fetch",
        name="Parallel Data Fetch",
        stages=[
            StageExecution(
                ref_id="api",
                type="fetch",
                name="Fetch from API",
                context={"source": "api"},
                tasks=[TaskExecution.create("Fetch API", "fetch", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="db",
                type="fetch",
                name="Fetch from Database",
                context={"source": "db"},
                tasks=[TaskExecution.create("Fetch DB", "fetch", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="cache",
                type="fetch",
                name="Fetch from Cache",
                context={"source": "cache"},
                tasks=[TaskExecution.create("Fetch Cache", "fetch", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="aggregate",
                type="aggregate",
                name="Aggregate Results",
                requisite_stage_ref_ids={"api", "db", "cache"},  # Wait for ALL three
                context={},
                tasks=[TaskExecution.create("Aggregate", "aggregate", stage_start=True, stage_end=True)],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"Final status: {result.status}")
    print(f"Combined data: {result.stages[-1].outputs.get('combined_data')}")


if __name__ == "__main__":
    main()

===============================================================================
10. COMPLETE IMPORTS REFERENCE
===============================================================================

# RECOMMENDED: Single consolidated import (most common classes)
from stabilize import (
    # Core models
    Workflow, StageExecution, TaskExecution, WorkflowStatus,
    # Infrastructure
    Orchestrator, QueueProcessor, SqliteQueue, SqliteWorkflowStore,
    # Tasks
    Task, RetryableTask, TaskResult, TaskRegistry,
    ShellTask, HTTPTask, DockerTask, SSHTask, HighwayTask,
    # Handlers (all 7 required)
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler,
    CompleteWorkflowHandler,
)

# Advanced imports (for specialized use cases)
from stabilize.persistence.store import WorkflowStore      # Abstract base for custom stores
from stabilize.queue.queue import Queue                    # Abstract base for custom queues
from stabilize.tasks.interface import (                    # Advanced task types
    SkippableTask, OverridableTimeoutRetryableTask,
    CallableTask, NoOpTask, WaitTask,
)
from stabilize.tasks.result import TaskResultBuilder       # For complex result building

# Verification System (NEW)
from stabilize.verification import (
    VerifyResult, VerifyStatus, Verifier, OutputVerifier, CallableVerifier,
)

# Structured Conditions (NEW)
from stabilize.conditions import (
    Condition, ConditionSet, ConditionType, ConditionReason,
)

# Assertion Helpers (NEW)
from stabilize.assertions import (
    assert_context, assert_context_type, assert_context_in,
    assert_output, assert_output_type,
    assert_config, assert_verified, assert_true,
    assert_stage_ready, assert_not_none, assert_non_empty,
    ContextError, OutputError, ConfigError, VerificationError,
    PreconditionError, StageNotReadyError,
)

# Configuration Validation (NEW)
from stabilize.config_validation import (
    validate_context, validate_outputs, is_valid,
    SchemaValidator, ValidationError,
    SHELL_TASK_SCHEMA, WAIT_TASK_SCHEMA,
)

# Error Handling & Reliability (NEW)
from stabilize.errors import (
    TransientError, PermanentError, TaskTimeoutError,
    is_transient, is_permanent,
)
from stabilize.models.status import (
    can_transition, validate_transition, InvalidStateTransitionError,
)
from stabilize.recovery import WorkflowRecovery, recover_on_startup

===============================================================================
11. VERIFICATION SYSTEM (NEW)
===============================================================================

The verification system validates stage outputs after task completion,
before downstream stages start. This ensures data integrity in pipelines.

11.1 VerifyResult - Verification Result Type
---------------------------------------------
from stabilize.verification import VerifyResult, VerifyStatus

# Create results using factory methods:
VerifyResult.ok(message="All checks passed")           # Verification passed
VerifyResult.retry(message="Still waiting", details={}) # Will retry
VerifyResult.failed(message="Check failed", details={}) # Terminal failure
VerifyResult.skipped(message="Not applicable")          # Skipped

# Check result status:
result.is_ok        # True if verification passed
result.is_retry     # True if should retry
result.is_failed    # True if terminal failure
result.is_terminal  # True if OK, FAILED, or SKIPPED (won't retry)

11.2 OutputVerifier - Check Required Outputs
--------------------------------------------
from stabilize.verification import OutputVerifier

# Verify that specific outputs exist with correct types
verifier = OutputVerifier(
    required_keys=["url", "status_code"],
    type_checks={"status_code": int},
)

class MyTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        # ... task logic ...
        result = verifier.verify(stage)
        if not result.is_ok:
            return TaskResult.terminal(result.message)
        return TaskResult.success(outputs={"url": url, "status_code": 200})

11.3 Custom Verifier
---------------------
from stabilize.verification import Verifier, VerifyResult

class URLVerifier(Verifier):
    def verify(self, stage: StageExecution) -> VerifyResult:
        url = stage.outputs.get("url")
        if not url:
            return VerifyResult.failed("No URL in outputs")

        # Check if URL is reachable
        try:
            response = requests.head(url, timeout=5)
            if response.ok:
                return VerifyResult.ok(f"URL {url} is reachable")
            return VerifyResult.retry(f"URL returned {response.status_code}")
        except Exception as e:
            return VerifyResult.retry(f"URL check failed: {e}")

    @property
    def max_retries(self) -> int:
        return 5  # Override default of 3

    @property
    def retry_delay_seconds(self) -> float:
        return 2.0  # Override default of 1.0

===============================================================================
12. STRUCTURED CONDITIONS (NEW)
===============================================================================

Conditions provide detailed status information with reasons and timestamps,
inspired by Kubernetes conditions.

12.1 Condition - Status with Context
------------------------------------
from stabilize.conditions import Condition, ConditionType, ConditionReason

# Create conditions using factory methods:
Condition.ready(status=True, reason=ConditionReason.TASKS_SUCCEEDED, message="Done")
Condition.progressing(status=True, reason=ConditionReason.IN_PROGRESS)
Condition.verified(status=True, reason=ConditionReason.VERIFICATION_PASSED)
Condition.failed(reason=ConditionReason.TASK_FAILED, message="Task timed out")
Condition.config_valid(status=True)

# Update a condition (immutable - returns new instance)
updated = condition.update(status=False, reason=ConditionReason.IN_PROGRESS)

# Serialize for storage
data = condition.to_dict()  # {"type": "Ready", "status": true, ...}
condition = Condition.from_dict(data)

12.2 ConditionSet - Manage Multiple Conditions
----------------------------------------------
from stabilize.conditions import ConditionSet

conditions = ConditionSet()

# Set/update conditions
conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
conditions.set(Condition.progressing(False, ConditionReason.STAGE_COMPLETED))

# Quick status checks
conditions.is_ready       # True if Ready condition is True
conditions.is_progressing # True if Progressing condition is True
conditions.is_verified    # True if Verified condition is True
conditions.has_failed     # True if Failed condition exists
conditions.is_config_valid # True if ConfigValid is True (default: True)

# Get specific condition
ready = conditions.get(ConditionType.READY)
if ready:
    print(f"Ready: {ready.status}, Reason: {ready.reason}")

# Serialize
data_list = conditions.to_list()
conditions = ConditionSet.from_list(data_list)

===============================================================================
13. ASSERTION HELPERS (NEW)
===============================================================================

Assertion helpers provide clean error handling with descriptive exceptions.

13.1 Context Assertions
-----------------------
from stabilize.assertions import (
    assert_context, assert_context_type, assert_context_in,
    ContextError,
)

class MyTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        # Assert key exists and get value (raises ContextError if missing)
        api_key = assert_context(stage, "api_key", "API key is required")

        # Assert key exists with specific type
        timeout = assert_context_type(stage, "timeout", int, "Timeout must be int")

        # Assert value is in allowed list
        env = assert_context_in(stage, "env", ["dev", "staging", "prod"])

        # ... rest of task logic
        return TaskResult.success()

13.2 Output Assertions
----------------------
from stabilize.assertions import assert_output, assert_output_type, OutputError

# Assert output exists
result = assert_output(stage, "deployment_id")

# Assert output with type
count = assert_output_type(stage, "item_count", int)

13.3 Configuration & Verification Assertions
--------------------------------------------
from stabilize.assertions import assert_config, assert_verified, ConfigError

# Assert configuration is valid
assert_config(timeout > 0, "Timeout must be positive", field="timeout")

# Assert verification condition
assert_verified(response.ok, "API check failed", details={"status": response.status_code})

13.4 Stage Readiness Assertions
-------------------------------
from stabilize.assertions import assert_stage_ready, assert_no_upstream_failures

# Assert all upstream stages complete
assert_stage_ready(stage, "Cannot start: upstream incomplete")

# Assert no upstream failures
assert_no_upstream_failures(stage)

13.5 General Assertions
-----------------------
from stabilize.assertions import assert_true, assert_not_none, assert_non_empty

assert_true(condition, "Condition not met")
user = assert_not_none(get_user(id), f"User {id} not found")
items = assert_non_empty(stage.context.get("items", []), "Items required")

13.6 Exception Hierarchy
------------------------
StabilizeError (base)
├── StabilizeFatalError (unrecoverable - halts pipeline)
│   ├── ContextError (missing/invalid context)
│   └── ConfigError (invalid configuration)
└── StabilizeExpectedError (may allow retry)
    ├── PreconditionError (general precondition)
    ├── OutputError (missing/invalid output)
    ├── VerificationError (verification failed)
    └── StageNotReadyError (upstream incomplete)

===============================================================================
14. CONFIGURATION VALIDATION (NEW)
===============================================================================

JSON Schema-based validation for stage contexts and configurations.

14.1 Validate Context
---------------------
from stabilize.config_validation import validate_context, ValidationError

DEPLOY_SCHEMA = {
    "type": "object",
    "required": ["cluster", "image"],
    "properties": {
        "cluster": {"type": "string", "minLength": 1},
        "image": {"type": "string", "pattern": r"^[a-z0-9./-]+:[a-z0-9.-]+$"},
        "replicas": {"type": "integer", "minimum": 1, "default": 1},
        "timeout": {"type": "integer", "minimum": 0},
    },
}

class DeployTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        errors = validate_context(stage.context, DEPLOY_SCHEMA)
        if errors:
            return TaskResult.terminal(f"Invalid config: {errors[0]}")

        # Config is valid, proceed
        cluster = stage.context["cluster"]
        image = stage.context["image"]
        # ...

14.2 Built-in Schemas
---------------------
from stabilize.config_validation import SHELL_TASK_SCHEMA, WAIT_TASK_SCHEMA

# SHELL_TASK_SCHEMA validates: command (required), timeout, cwd, env, etc.
# WAIT_TASK_SCHEMA validates: waitTime (required, >= 0)

14.3 Quick Validation Check
---------------------------
from stabilize.config_validation import is_valid

if not is_valid(stage.context, DEPLOY_SCHEMA):
    return TaskResult.terminal("Invalid configuration")

14.4 Supported Validations
--------------------------
Type:         "type": "string" | "integer" | "number" | "boolean" | "array" | "object" | "null"
Union:        "type": ["string", "integer"]
Required:     "required": ["field1", "field2"]
Enum:         "enum": ["value1", "value2"]
Const:        "const": "fixed_value"

String:       "minLength", "maxLength", "pattern"
Number:       "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"
Array:        "minItems", "maxItems", "uniqueItems", "items" (schema for array elements)
Object:       "properties", "additionalProperties", "minProperties", "maxProperties"

===============================================================================
15. ERROR HANDLING & RELIABILITY (NEW)
===============================================================================

Stabilize has enterprise-grade reliability features for production deployments.

15.1 Transient vs Permanent Errors
----------------------------------
from stabilize.errors import TransientError, PermanentError, is_transient

# Transient errors are automatically retried with exponential backoff
raise TransientError("Connection timeout")  # Will retry

# Permanent errors immediately fail the task
raise PermanentError("Invalid input")  # No retry, marks task as terminal

# Classification helper - checks exception class name for keywords
is_transient(ConnectionError("timeout"))  # True - has "connection"
is_transient(TimeoutError())              # True - has "timeout"
is_transient(ValueError("bad input"))     # False - standard exception

Keywords that make an error transient:
  - "timeout", "temporary", "transient", "connection", "network"
  - "unavailable", "retry", "throttl", "rate", "limit", "5xx"

15.2 Automatic Retry with Exponential Backoff
---------------------------------------------
Transient errors are retried with exponential backoff:
  - Attempt 1: ~1 second delay
  - Attempt 2: ~2 seconds delay
  - Attempt 3: ~4 seconds delay
  - ...continues doubling up to 60 seconds max
  - ±25% jitter added to prevent thundering herd

Maximum 10 retry attempts before marking as terminal.

15.3 Message Deduplication (Idempotency)
----------------------------------------
Messages are deduplicated to prevent duplicate processing:

# Automatic - no code changes needed
# Each message has a unique ID tracked in processed_messages table
# Re-processing the same message is skipped

This ensures:
- Crash recovery doesn't cause duplicate side effects
- Network retries don't duplicate work
- At-least-once delivery becomes effectively-once processing

15.4 State Transition Validation
--------------------------------
from stabilize.models.status import can_transition, validate_transition

# Check if transition is valid
can_transition(WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING)  # True
can_transition(WorkflowStatus.SUCCEEDED, WorkflowStatus.RUNNING)    # False

# Validate with exception
validate_transition(
    WorkflowStatus.SUCCEEDED,
    WorkflowStatus.RUNNING,
    entity_type="workflow",
    entity_id="wf-123",
)  # Raises InvalidStateTransitionError

Valid transitions:
  NOT_STARTED → RUNNING, CANCELED, SKIPPED
  RUNNING     → SUCCEEDED, FAILED_CONTINUE, TERMINAL, CANCELED, PAUSED, STOPPED
  PAUSED      → RUNNING, CANCELED
  Terminal states (SUCCEEDED, TERMINAL, CANCELED, STOPPED, SKIPPED) → no transitions

15.5 Timeout Enforcement
------------------------
Tasks are executed with timeout enforcement using thread interruption:

# Default timeout: 5 minutes for regular tasks
# RetryableTask can override via get_dynamic_timeout()

class MyRetryableTask(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=30)

    def get_dynamic_timeout(self, stage: StageExecution) -> timedelta:
        # Can use stage context to determine timeout
        return timedelta(milliseconds=stage.context.get("stageTimeoutMs", 300000))

# When timeout occurs, task.on_timeout(stage) is called if defined
def on_timeout(self, stage: StageExecution) -> TaskResult | None:
    # Cleanup and return partial result, or None for default behavior
    return TaskResult.failed_continue(error="Timed out", outputs={"partial": data})

15.6 Crash Recovery
-------------------
from stabilize.recovery import WorkflowRecovery, recover_on_startup

# At application startup, recover in-progress workflows
recovery = WorkflowRecovery(store, queue)
results = recovery.recover_pending_workflows()

# Convenience function
recover_on_startup(store, queue)  # Returns list of RecoveryResult

Recovery automatically:
- Finds workflows in RUNNING/NOT_STARTED state
- Re-queues their current stages for continuation
- Uses idempotency to prevent duplicate work

===============================================================================
END OF REFERENCE
===============================================================================
'''

# Migration tracking table
MIGRATION_TABLE = "stabilize_migrations"


def load_config() -> dict[str, Any]:
    """Load database config from mg.yaml or environment."""
    db_url = os.environ.get("MG_DATABASE_URL")
    if db_url:
        return parse_db_url(db_url)

    # Try to load mg.yaml
    mg_yaml = Path("mg.yaml")
    if mg_yaml.exists():
        try:
            import yaml  # type: ignore[import-untyped]

            with open(mg_yaml) as f:
                config = yaml.safe_load(f)
                db_config: dict[str, Any] = config.get("database", {}) if config else {}
                return db_config
        except ImportError:
            print("Warning: PyYAML not installed, cannot read mg.yaml")
            print("Set MG_DATABASE_URL environment variable instead")
            sys.exit(1)

    print("Error: No database configuration found")
    print("Either create mg.yaml or set MG_DATABASE_URL environment variable")
    sys.exit(1)


def parse_db_url(url: str) -> dict[str, Any]:
    """Parse a database URL into connection parameters."""
    # postgres://user:pass@host:port/dbname
    pattern = r"postgres(?:ql)?://(?:(?P<user>[^:]+)(?::(?P<password>[^@]+))?@)?(?P<host>[^:/]+)(?::(?P<port>\d+))?/(?P<dbname>[^?]+)"
    match = re.match(pattern, url)
    if not match:
        print(f"Error: Invalid database URL: {url}")
        sys.exit(1)

    return {
        "host": match.group("host"),
        "port": int(match.group("port") or 5432),
        "user": match.group("user") or "postgres",
        "password": match.group("password") or "",
        "dbname": match.group("dbname"),
    }


def get_migrations() -> list[tuple[str, str]]:
    """Get all migration files from the package."""
    migrations_pkg = files("stabilize.migrations")
    migrations = []

    for item in migrations_pkg.iterdir():
        if item.name.endswith(".sql"):
            content = item.read_text()
            migrations.append((item.name, content))

    # Sort by filename (ULID prefix ensures chronological order)
    migrations.sort(key=lambda x: x[0])
    return migrations


def extract_up_migration(content: str) -> str:
    """Extract the UP migration from SQL content."""
    # Find content between "-- migrate: up" and "-- migrate: down"
    up_match = re.search(
        r"--\s*migrate:\s*up\s*\n(.*?)(?:--\s*migrate:\s*down|$)",
        content,
        re.DOTALL | re.IGNORECASE,
    )
    if up_match:
        return up_match.group(1).strip()
    return content


def compute_checksum(content: str) -> str:
    """Compute MD5 checksum of migration content."""
    return hashlib.md5(content.encode()).hexdigest()


def mg_up(db_url: str | None = None) -> None:
    """Apply pending migrations to PostgreSQL database."""
    try:
        import psycopg
    except ImportError:
        print("Error: psycopg not installed")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)

    # Load config
    if db_url:
        config = parse_db_url(db_url)
    else:
        config = load_config()

    # Connect to database
    conninfo = (
        f"host={config['host']} port={config.get('port', 5432)} "
        f"user={config.get('user', 'postgres')} password={config.get('password', '')} "
        f"dbname={config['dbname']}"
    )

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Ensure migration tracking table exists
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL UNIQUE,
                        checksum VARCHAR(32) NOT NULL,
                        applied_at TIMESTAMP DEFAULT NOW()
                    )
                """
                )
                conn.commit()

                # Get applied migrations
                cur.execute(f"SELECT name, checksum FROM {MIGRATION_TABLE}")
                applied = {row[0]: row[1] for row in cur.fetchall()}

                # Get available migrations
                migrations = get_migrations()

                if not migrations:
                    print("No migrations found in package")
                    return

                # Apply pending migrations
                pending = 0
                for name, content in migrations:
                    if name in applied:
                        # Verify checksum
                        expected = compute_checksum(content)
                        if applied[name] != expected:
                            print(f"Warning: Checksum mismatch for {name}")
                        continue

                    pending += 1
                    print(f"Applying: {name}")

                    up_sql = extract_up_migration(content)
                    cur.execute(up_sql)

                    checksum = compute_checksum(content)
                    cur.execute(
                        f"INSERT INTO {MIGRATION_TABLE} (name, checksum) VALUES (%s, %s)",
                        (name, checksum),
                    )
                    conn.commit()

                if pending == 0:
                    print("All migrations already applied")
                else:
                    print(f"Applied {pending} migration(s)")

    except psycopg.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


def prompt() -> None:
    """Output comprehensive documentation for RAG systems and coding agents."""
    print(PROMPT_TEXT)


def monitor(
    db_url: str | None,
    app_filter: str | None,
    refresh_interval: int,
    status_filter: str,
) -> None:
    """Launch the real-time monitoring dashboard."""
    from stabilize.monitor import run_monitor

    # Create store based on db_url
    if db_url is None:
        # Try to load from config
        try:
            config = load_config()
            db_url = (
                f"postgres://{config.get('user', 'postgres')}:"
                f"{config.get('password', '')}@"
                f"{config.get('host', 'localhost')}:"
                f"{config.get('port', 5432)}/"
                f"{config.get('dbname', 'stabilize')}"
            )
        except SystemExit:
            print("Error: No database configuration found.")
            print("Provide --db-url or set up mg.yaml / MG_DATABASE_URL")
            sys.exit(1)

    # Determine store type from URL
    store: WorkflowStore
    queue: Queue | None = None
    if db_url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite_queue import SqliteQueue

        store = SqliteWorkflowStore(db_url, create_tables=False)
        # Try to create queue for stats
        try:
            queue = SqliteQueue(db_url, table_name="queue_messages")
        except Exception:
            queue = None
    elif db_url.startswith("postgres"):
        try:
            from stabilize.persistence.postgres import PostgresWorkflowStore
            from stabilize.queue.queue import PostgresQueue

            store = PostgresWorkflowStore(db_url)
            try:
                queue = PostgresQueue(db_url)
            except Exception:
                queue = None
        except ImportError:
            print("Error: psycopg not installed")
            print("Install with: pip install stabilize[postgres]")
            sys.exit(1)
    else:
        print(f"Error: Unsupported database URL: {db_url}")
        print("Use sqlite:///path or postgres://...")
        sys.exit(1)

    print(f"Connecting to {db_url[:50]}...")
    run_monitor(
        store=store,
        queue=queue,
        app_filter=app_filter,
        refresh_interval=refresh_interval,
        status_filter=status_filter,
    )


def rag_init(
    db_url: str | None = None,
    force: bool = False,
    additional_context: list[str] | None = None,
) -> None:
    """Initialize RAG embeddings from examples, documentation, and additional context."""
    try:
        from stabilize.rag import StabilizeRAG, get_cache
    except ImportError:
        print("Error: RAG support requires: pip install stabilize[rag]")
        sys.exit(1)

    cache = get_cache(db_url)
    rag = StabilizeRAG(cache)

    print("Initializing embeddings...")
    count = rag.init(force=force, additional_paths=additional_context)
    if count > 0:
        print(f"Cached {count} embeddings")
    else:
        print("Embeddings already initialized (use --force to regenerate)")


def rag_clear(db_url: str | None = None) -> None:
    """Clear all cached embeddings."""
    try:
        from stabilize.rag import get_cache
    except ImportError:
        print("Error: RAG support requires: pip install stabilize[rag]")
        sys.exit(1)

    cache = get_cache(db_url)
    cache.clear()
    print("Embedding cache cleared")


def rag_generate(
    prompt_text: str,
    db_url: str | None = None,
    execute: bool = False,
    top_k: int = 5,
    temperature: float = 0.3,
    llm_model: str | None = None,
) -> None:
    """Generate pipeline code from natural language prompt."""
    try:
        from stabilize.rag import StabilizeRAG, get_cache
    except ImportError:
        print("Error: RAG support requires: pip install stabilize[rag]")
        sys.exit(1)

    cache = get_cache(db_url)
    rag = StabilizeRAG(cache, llm_model=llm_model)

    try:
        code = rag.generate(prompt_text, top_k=top_k, temperature=temperature)
    except RuntimeError as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(code)

    if execute:
        print("\n--- Executing generated code ---\n")
        try:
            exec(code, {"__name__": "__main__"})
        except ImportError as e:
            print("\n--- Execution failed: Import error ---")
            print(f"Error: {e}")
            print("\nThe generated code has incorrect imports.")
            print("Review the imports above and compare with examples/shell-example.py")
            sys.exit(1)
        except Exception as e:
            print("\n--- Execution failed ---")
            print(f"Error: {type(e).__name__}: {e}")
            print("\nThe generated code may need manual adjustments.")
            sys.exit(1)


def mg_status(db_url: str | None = None) -> None:
    """Show migration status."""
    try:
        import psycopg
    except ImportError:
        print("Error: psycopg not installed")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)

    # Load config
    if db_url:
        config = parse_db_url(db_url)
    else:
        config = load_config()

    conninfo = (
        f"host={config['host']} port={config.get('port', 5432)} "
        f"user={config.get('user', 'postgres')} password={config.get('password', '')} "
        f"dbname={config['dbname']}"
    )

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Check if tracking table exists
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """,
                    (MIGRATION_TABLE,),
                )
                row = cur.fetchone()
                table_exists = row[0] if row else False

                applied = {}
                if table_exists:
                    cur.execute(f"SELECT name, checksum, applied_at FROM {MIGRATION_TABLE} ORDER BY applied_at")
                    applied = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

                migrations = get_migrations()

                print(f"{'Status':<10} {'Migration':<50} {'Applied At'}")
                print("-" * 80)

                for name, content in migrations:
                    if name in applied:
                        checksum, applied_at = applied[name]
                        expected = compute_checksum(content)
                        status = "applied" if checksum == expected else "MISMATCH"
                        print(f"{status:<10} {name:<50} {applied_at}")
                    else:
                        print(f"{'pending':<10} {name:<50} -")

    except psycopg.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="stabilize",
        description="Stabilize - Workflow Engine CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # mg-up command
    up_parser = subparsers.add_parser("mg-up", help="Apply pending PostgreSQL migrations")
    up_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://user:pass@host:port/dbname)",
    )

    # mg-status command
    status_parser = subparsers.add_parser("mg-status", help="Show migration status")
    status_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://user:pass@host:port/dbname)",
    )

    # prompt command
    subparsers.add_parser(
        "prompt",
        help="Output comprehensive RAG context for pipeline code generation",
    )

    # rag command (with subcommands)
    rag_parser = subparsers.add_parser(
        "rag",
        help="RAG-powered pipeline generation",
    )
    rag_subparsers = rag_parser.add_subparsers(dest="rag_command")

    # rag init
    init_parser = rag_subparsers.add_parser(
        "init",
        help="Initialize embeddings cache from examples and documentation",
    )
    init_parser.add_argument(
        "--db-url",
        help="Database URL for caching (postgres://... or sqlite path)",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration even if cache exists",
    )
    init_parser.add_argument(
        "--additional-context",
        action="append",
        metavar="PATH",
        help="Additional file or directory to include in training context (can be specified multiple times)",
    )

    # rag generate
    gen_parser = rag_subparsers.add_parser(
        "generate",
        help="Generate pipeline code from natural language prompt",
    )
    gen_parser.add_argument(
        "prompt",
        help="Natural language description of the desired pipeline",
    )
    gen_parser.add_argument(
        "--db-url",
        help="Database URL for caching",
    )
    gen_parser.add_argument(
        "-x",
        "--execute",
        action="store_true",
        help="Execute the generated code after displaying it",
    )
    gen_parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of context chunks to retrieve (default: 10)",
    )
    gen_parser.add_argument(
        "--temperature",
        type=float,
        default=0.3,
        help="LLM temperature for generation (default: 0.3)",
    )
    gen_parser.add_argument(
        "--llm-model",
        default=None,
        help="LLM model for generation (default: qwen3-vl:235b)",
    )

    # rag clear
    clear_parser = rag_subparsers.add_parser(
        "clear",
        help="Clear all cached embeddings",
    )
    clear_parser.add_argument(
        "--db-url",
        help="Database URL for caching",
    )

    # monitor command
    monitor_parser = subparsers.add_parser(
        "monitor",
        help="Real-time workflow monitoring dashboard (htop-like)",
    )
    monitor_parser.add_argument(
        "--app",
        help="Filter by application name",
    )
    monitor_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://... or sqlite:///...)",
    )
    monitor_parser.add_argument(
        "--refresh",
        type=int,
        default=2,
        help="Refresh interval in seconds (default: 2)",
    )
    monitor_parser.add_argument(
        "--status",
        choices=["all", "running", "failed", "recent"],
        default="all",
        help="Filter workflows by status (default: all)",
    )

    args = parser.parse_args()

    if args.command == "mg-up":
        mg_up(args.db_url)
    elif args.command == "mg-status":
        mg_status(args.db_url)
    elif args.command == "prompt":
        prompt()
    elif args.command == "rag":
        if args.rag_command == "init":
            rag_init(args.db_url, args.force, args.additional_context)
        elif args.rag_command == "generate":
            rag_generate(
                args.prompt,
                args.db_url,
                args.execute,
                args.top_k,
                args.temperature,
                args.llm_model,
            )
        elif args.rag_command == "clear":
            rag_clear(args.db_url)
        else:
            rag_parser.print_help()
            sys.exit(1)
    elif args.command == "monitor":
        monitor(args.db_url, args.app, args.refresh, args.status)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
