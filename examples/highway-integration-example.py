#!/usr/bin/env python3
"""
Highway Integration Example - Demonstrates Stabilize + Highway Workflow Engine.

This example shows how to:
1. Define a Highway workflow that fetches multiple URLs in parallel
2. Use HighwayTask to submit and monitor the workflow from Stabilize
3. Get results when the Highway workflow completes

Architecture:
    Stabilize (Control Plane) -> Highway (Execution Plane)

    - Execution: Black Box - Stabilize sends `start`, waits for `completed`
    - State: Black Box - Stabilize stores only `run_id`
    - Observability: Glass Box - Stabilize proxies logs/current_step for UI

Environment Variables:
    HIGHWAY_API_KEY: Your Highway API key (hw_k1_... format)
    HIGHWAY_API_ENDPOINT: Highway API URL (default: https://highway.solutions)

Run with:
    python examples/highway-integration-example.py

    Or set environment variables manually:
    export HIGHWAY_API_KEY="hw_k1_your_key_here"
    python examples/highway-integration-example.py
"""

import json
import logging
import os
from pathlib import Path
from typing import Any

# Load .env file from project root
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    HighwayTask,
    Orchestrator,
    Queue,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStore,
)

# =============================================================================
# Highway Workflow Definition: Parallel URL Fetcher
# =============================================================================

# This workflow runs ENTIRELY on Highway - Stabilize just submits and monitors.
# Highway handles: parallel execution, retries, transactions, checkpoints.
PARALLEL_URL_FETCHER_WORKFLOW = {
    "name": "parallel_url_fetcher",
    "version": "1.0.0",
    "description": "Fetch multiple URLs in parallel and return results",
    "start_task": "fork_fetches",
    "tasks": {
        # Step 1: Fork into parallel branches (returns immediately)
        "fork_fetches": {
            "task_id": "fork_fetches",
            "operator_type": "parallel",
            "dependencies": [],
            "trigger_rule": "all_success",
            "result_key": "fork_data",
            "branches": {
                "fetch_httpbin": ["fetch_url_1"],
                "fetch_jsonplaceholder": ["fetch_url_2"],
                "fetch_postman": ["fetch_url_3"],
            },
            "branch_workflows": {
                "fetch_httpbin": {
                    "name": "fork_fetches_fetch_httpbin",
                    "version": "1.0.0",
                    "tasks": {
                        "fetch_url_1": {
                            "task_id": "fetch_url_1",
                            "operator_type": "task",
                            "dependencies": [],
                            "function": "tools.http.request",
                            "kwargs": {
                                "url": "https://httpbin.org/get",
                                "method": "GET",
                                "timeout": 30,
                            },
                            "result_key": "httpbin_response",
                        }
                    },
                    "start_task": None,
                },
                "fetch_jsonplaceholder": {
                    "name": "fork_fetches_fetch_jsonplaceholder",
                    "version": "1.0.0",
                    "tasks": {
                        "fetch_url_2": {
                            "task_id": "fetch_url_2",
                            "operator_type": "task",
                            "dependencies": [],
                            "function": "tools.http.request",
                            "kwargs": {
                                "url": "https://jsonplaceholder.typicode.com/posts/1",
                                "method": "GET",
                                "timeout": 30,
                            },
                            "result_key": "jsonplaceholder_response",
                        }
                    },
                    "start_task": None,
                },
                "fetch_postman": {
                    "name": "fork_fetches_fetch_postman",
                    "version": "1.0.0",
                    "tasks": {
                        "fetch_url_3": {
                            "task_id": "fetch_url_3",
                            "operator_type": "task",
                            "dependencies": [],
                            "function": "tools.http.request",
                            "kwargs": {
                                "url": "https://postman-echo.com/get",
                                "method": "GET",
                                "timeout": 30,
                            },
                            "result_key": "postman_response",
                        }
                    },
                    "start_task": None,
                },
            },
            "timeout": 120,
        },
        # Step 2: Wait for all branches to complete
        "wait_for_branches": {
            "task_id": "wait_for_branches",
            "operator_type": "task",
            "dependencies": ["fork_fetches"],
            "trigger_rule": "all_success",
            "function": "tools.workflow.wait_for_parallel_branches",
            "args": ["{{fork_data}}"],
            "kwargs": {"timeout_seconds": 120},
            "result_key": "branch_results",
        },
        # Step 3: Aggregate results (using shell to create JSON output)
        "aggregate_results": {
            "task_id": "aggregate_results",
            "operator_type": "task",
            "dependencies": ["wait_for_branches"],
            "trigger_rule": "all_success",
            "function": "tools.shell.run",
            "args": ["echo 'All URLs fetched successfully'"],
            "kwargs": {},
            "result_key": "final_message",
        },
    },
    "variables": {},
    "max_active_runs": 1,
}

# Simpler workflow for quick testing - single HTTP request
SIMPLE_HTTP_WORKFLOW = {
    "name": "simple_http_test",
    "version": "1.0.0",
    "description": "Simple HTTP request for integration testing",
    "start_task": "fetch_test",
    "tasks": {
        "fetch_test": {
            "task_id": "fetch_test",
            "operator_type": "task",
            "dependencies": [],
            "trigger_rule": "all_success",
            "function": "tools.http.request",
            "kwargs": {
                "url": "https://httpbin.org/get",
                "method": "GET",
                "timeout": 30,
            },
            "result_key": "test_response",
        },
    },
    "variables": {},
    "max_active_runs": 1,
}


# =============================================================================
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with HighwayTask registered."""
    task_registry = TaskRegistry()
    task_registry.register("highway", HighwayTask)

    processor = QueueProcessor(queue)

    handlers: list[Any] = [
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


# =============================================================================
# Example 1: Simple Highway Workflow
# =============================================================================


def example_simple_highway() -> None:
    """Submit a simple Highway workflow and wait for completion."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Highway HTTP Request")
    print("=" * 60)

    # Check for API key
    api_key = os.environ.get("HIGHWAY_API_KEY")
    if not api_key:
        print("ERROR: HIGHWAY_API_KEY environment variable not set")
        print("Set it with: export HIGHWAY_API_KEY='hw_k1_your_key_here'")
        return

    api_endpoint = os.environ.get("HIGHWAY_API_ENDPOINT", "https://highway.solutions")
    print(f"Using Highway API: {api_endpoint}")

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Create Stabilize workflow that triggers Highway
    workflow = Workflow.create(
        application="highway-integration",
        name="Simple Highway Test",
        stages=[
            StageExecution(
                ref_id="highway_stage",
                type="highway",
                name="Run Highway Workflow",
                context={
                    # Highway workflow definition (Black Box - Highway does everything)
                    "highway_workflow_definition": SIMPLE_HTTP_WORKFLOW,
                    # Optional inputs for the Highway workflow
                    "highway_inputs": {},
                    # Optional overrides (could also use env vars)
                    # "highway_api_endpoint": api_endpoint,
                    # "highway_api_key": api_key,
                },
                tasks=[
                    TaskExecution.create(
                        name="Highway Task",
                        implementing_class="highway",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)

    print("Workflow submitted. Polling for completion...")
    print("(Highway handles all HTTP requests internally)")

    # Process with longer timeout for Highway
    processor.process_all(timeout=120.0)

    result = store.retrieve(workflow.id)
    print(f"\nStabilize Workflow Status: {result.status}")

    if result.stages[0].outputs:
        outputs = result.stages[0].outputs
        print("Highway Run ID: {}".format(outputs.get("highway_run_id")))
        print("Highway Status: {}".format(outputs.get("highway_status")))
        if outputs.get("highway_result"):
            print("Highway Result: {}".format(json.dumps(outputs.get("highway_result"), indent=2)[:500]))
    else:
        print("No outputs received (check Highway logs)")


# =============================================================================
# Example 2: Parallel URL Fetcher (Full Demo)
# =============================================================================


def example_parallel_fetcher() -> None:
    """Submit a parallel URL fetcher workflow to Highway."""
    print("\n" + "=" * 60)
    print("Example 2: Parallel URL Fetcher via Highway")
    print("=" * 60)

    # Check for API key
    api_key = os.environ.get("HIGHWAY_API_KEY")
    if not api_key:
        print("ERROR: HIGHWAY_API_KEY environment variable not set")
        print("Set it with: export HIGHWAY_API_KEY='hw_k1_your_key_here'")
        return

    api_endpoint = os.environ.get("HIGHWAY_API_ENDPOINT", "https://highway.solutions")
    print(f"Using Highway API: {api_endpoint}")
    print("\nThis workflow will:")
    print("  1. Fork into 3 parallel branches")
    print("  2. Each branch fetches a different URL")
    print("  3. Wait for all branches to complete")
    print("  4. Return aggregated results")
    print("\nAll execution happens on Highway (Black Box).")
    print("Stabilize only monitors completion (Glass Box observability).")

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="highway-integration",
        name="Parallel URL Fetcher",
        stages=[
            StageExecution(
                ref_id="highway_parallel",
                type="highway",
                name="Highway Parallel Fetch",
                context={
                    "highway_workflow_definition": PARALLEL_URL_FETCHER_WORKFLOW,
                    "highway_inputs": {},
                },
                tasks=[
                    TaskExecution.create(
                        name="Highway Parallel Task",
                        implementing_class="highway",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)

    print("\nWorkflow submitted. Polling for completion...")

    # Process with longer timeout for parallel execution
    processor.process_all(timeout=180.0)

    result = store.retrieve(workflow.id)
    print(f"\nStabilize Workflow Status: {result.status}")

    if result.stages[0].outputs:
        outputs = result.stages[0].outputs
        print("Highway Run ID: {}".format(outputs.get("highway_run_id")))
        print("Highway Status: {}".format(outputs.get("highway_status")))
        if outputs.get("highway_result"):
            print("Highway Result Preview: {}...".format(str(outputs.get("highway_result"))[:200]))
    else:
        print("No outputs received (check Highway logs)")


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    """Run all examples."""
    print("=" * 60)
    print("Highway Integration Examples")
    print("=" * 60)
    print("\nArchitecture:")
    print("  Stabilize (Control Plane) -> Highway (Execution Plane)")
    print("\n  - Execution: Black Box")
    print("  - State: Black Box (only run_id stored)")
    print("  - Observability: Glass Box (current_step for UI)")

    # Run examples
    example_simple_highway()
    example_parallel_fetcher()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
