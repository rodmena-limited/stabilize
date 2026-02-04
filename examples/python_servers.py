#!/usr/bin/env python3
"""3 parallel long-running tasks with automatic restart on failure.

This example demonstrates:
- 3 parallel long-running stages (2 HTTP servers, 1 monitor)
- Automatic restart when a server crashes (restart_on_failure=True)
- Workflow recovery after interruption (Ctrl+C)

Usage:
    python examples/python_servers.py    # Start or recover workflow
    # Press Ctrl+C to stop
    python examples/python_servers.py    # Automatically recovers

To start fresh:
    rm ~/workflow.db && python examples/python_servers.py
"""

import os

from stabilize import (
    Orchestrator,
    QueueProcessor,
    ShellTask,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)
from stabilize.queue.processor import QueueProcessorConfig
from stabilize.recovery import recover_on_startup

# Fixed directories (not tempfile so they persist across restarts)
DIR1 = "/tmp/server1"
DIR2 = "/tmp/server2"
os.makedirs(DIR1, exist_ok=True)
os.makedirs(DIR2, exist_ok=True)
open(f"{DIR1}/index.html", "w").write("<h1>Server 1</h1>")
open(f"{DIR2}/index.html", "w").write("<h1>Server 2</h1>")

WORKFLOW_ID = "my-servers-workflow"


def create_workflow_stages():
    """Create the workflow stage definitions."""
    return [
        StageExecution(
            ref_id="server1",
            type="shell",
            name="Server 1",
            context={
                "command": f"cd {DIR1} && python3 -m http.server 19001",
                "timeout": 3600,
                "restart_on_failure": True,
            },
            requisite_stage_ref_ids=set(),
            tasks=[TaskExecution.create(name="S1", implementing_class="shell", stage_start=True, stage_end=True)],
        ),
        StageExecution(
            ref_id="server2",
            type="shell",
            name="Server 2",
            context={
                "command": f"cd {DIR2} && python3 -m http.server 19002",
                "timeout": 3600,
                "restart_on_failure": True,
            },
            requisite_stage_ref_ids=set(),
            tasks=[TaskExecution.create(name="S2", implementing_class="shell", stage_start=True, stage_end=True)],
        ),
        StageExecution(
            ref_id="monitor",
            type="shell",
            name="Monitor",
            context={
                "command": "while true; do curl -s localhost:19001 && curl -s localhost:19002; sleep 30; done",
                "timeout": 3600,
                "restart_on_failure": True,
            },
            requisite_stage_ref_ids=set(),
            tasks=[TaskExecution.create(name="Mon", implementing_class="shell", stage_start=True, stage_end=True)],
        ),
    ]


def main():
    import pathlib

    db = pathlib.Path("/tmp/workflow_test.db")
    store = SqliteWorkflowStore(f"sqlite:///{db}", create_tables=True)
    queue = SqliteQueue(f"sqlite:///{db}", table_name="queue_messages")
    queue._create_table()

    reg = TaskRegistry()
    reg.register("shell", ShellTask)

    processor = QueueProcessor(
        queue,
        config=QueueProcessorConfig(max_workers=4, poll_frequency_ms=100),
        store=store,
        task_registry=reg,
    )

    # Check if our workflow already exists
    existing = None
    if store.exists(WORKFLOW_ID):
        existing = store.retrieve(WORKFLOW_ID)
        print(f"Found existing workflow: {existing.status.name}")

        # Handle different states
        if existing.status.is_complete:
            print(f"Previous workflow completed with status: {existing.status.name}")
            print(f"Delete {db} to start a fresh workflow.")
            return

        # Workflow is in progress - try to recover
        print("\nRecovering workflow...")
        results = recover_on_startup(store, queue, application="demo")
        if results:
            for r in results:
                print(f"  {r.workflow_id}: {r.status} - {r.message}")
        else:
            print("  No recovery actions needed")
    else:
        print("No existing workflow, creating new one...")

        stages = create_workflow_stages()
        workflow = Workflow(
            id=WORKFLOW_ID,
            application="demo",
            name="3 Servers",
            stages=stages,
        )
        store.store(workflow)
        Orchestrator(queue).start(workflow)

    print(f"\nServer 1: http://localhost:19001 ({DIR1})")
    print(f"Server 2: http://localhost:19002 ({DIR2})")
    print("Starting... Ctrl+C to stop\n")

    try:
        processor.start()  # Start async processing with thread pool
        import time

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
        processor.stop()
        print("Stopped. Run again to recover the workflow.")


if __name__ == "__main__":
    main()
