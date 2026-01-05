#!/usr/bin/env python3
"""
HTTP Example - Demonstrates making HTTP requests with Stabilize.

This example shows how to use the built-in HTTPTask for:
1. Simple GET requests
2. POST with JSON bodies
3. Sequential API workflows
4. Parallel requests
5. All HTTP methods (GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH)

Requirements:
    None (uses urllib from standard library)

Run with:
    python examples/http-example.py
"""

import json
import logging
from typing import Any

logging.basicConfig(level=logging.ERROR)

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    HTTPTask,
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
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with HTTPTask registered."""
    task_registry = TaskRegistry()
    task_registry.register("http", HTTPTask)

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
# Example 1: Simple GET Request
# =============================================================================


def example_simple_get() -> None:
    """Make a simple GET request to a public API."""
    print("\n" + "=" * 60)
    print("Example 1: Simple GET Request")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="http-example",
        name="Simple GET",
        stages=[
            StageExecution(
                ref_id="1",
                type="http",
                name="Get IP Info",
                context={
                    "url": "https://httpbin.org/ip",
                    "method": "GET",
                },
                tasks=[
                    TaskExecution.create(
                        name="HTTP GET",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print(f"Response Status: {result.stages[0].outputs.get('status_code')}")
    print(f"Response Body: {result.stages[0].outputs.get('body', '')[:200]}")


# =============================================================================
# Example 2: POST with JSON Body
# =============================================================================


def example_post_json() -> None:
    """Make a POST request with JSON payload."""
    print("\n" + "=" * 60)
    print("Example 2: POST with JSON Body")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="http-example",
        name="POST JSON",
        stages=[
            StageExecution(
                ref_id="1",
                type="http",
                name="Create Resource",
                context={
                    "url": "https://httpbin.org/post",
                    "method": "POST",
                    "json": {
                        "name": "Stabilize",
                        "version": "0.9.0",
                        "features": ["DAG", "parallel", "retry"],
                    },
                },
                tasks=[
                    TaskExecution.create(
                        name="HTTP POST",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print(f"Response Status: {result.stages[0].outputs.get('status_code')}")

    body = result.stages[0].outputs.get("body", "")
    if body:
        try:
            data = json.loads(body)
            print(f"Echoed JSON: {json.dumps(data.get('json', {}), indent=2)}")
        except json.JSONDecodeError:
            print(f"Response: {body[:200]}")


# =============================================================================
# Example 3: Sequential API Workflow
# =============================================================================


def example_sequential_api() -> None:
    """Sequential API calls: GET -> POST -> GET to verify."""
    print("\n" + "=" * 60)
    print("Example 3: Sequential API Workflow")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="http-example",
        name="Sequential API",
        stages=[
            StageExecution(
                ref_id="1",
                type="http",
                name="Step 1: Get Headers",
                context={
                    "url": "https://httpbin.org/headers",
                    "method": "GET",
                    "headers": {"X-Request-ID": "step-1"},
                },
                tasks=[
                    TaskExecution.create(
                        name="GET headers",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="2",
                type="http",
                name="Step 2: Post Data",
                requisite_stage_ref_ids={"1"},
                context={
                    "url": "https://httpbin.org/post",
                    "method": "POST",
                    "json": {"step": 2, "data": "from step 1"},
                    "headers": {"X-Request-ID": "step-2"},
                },
                tasks=[
                    TaskExecution.create(
                        name="POST data",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="3",
                type="http",
                name="Step 3: Verify",
                requisite_stage_ref_ids={"2"},
                context={
                    "url": "https://httpbin.org/get",
                    "method": "GET",
                    "headers": {"X-Request-ID": "step-3-verify"},
                },
                tasks=[
                    TaskExecution.create(
                        name="GET verify",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    for stage in result.stages:
        status = stage.outputs.get("status_code", "N/A")
        elapsed = stage.outputs.get("elapsed_ms", "N/A")
        print(f"  {stage.name}: {status} ({elapsed}ms)")


# =============================================================================
# Example 4: Parallel Requests
# =============================================================================


def example_parallel_requests() -> None:
    """Make parallel requests to multiple endpoints."""
    print("\n" + "=" * 60)
    print("Example 4: Parallel Requests")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    #     Start
    #    /  |  \
    # EP1  EP2  EP3
    #    \  |  /
    #    Collect

    workflow = Workflow.create(
        application="http-example",
        name="Parallel Requests",
        stages=[
            StageExecution(
                ref_id="start",
                type="http",
                name="Start",
                context={
                    "url": "https://httpbin.org/get?stage=start",
                    "method": "GET",
                },
                tasks=[
                    TaskExecution.create(
                        name="Start request",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Parallel branches
            StageExecution(
                ref_id="ep1",
                type="http",
                name="Endpoint 1 (IP)",
                requisite_stage_ref_ids={"start"},
                context={
                    "url": "https://httpbin.org/ip",
                    "method": "GET",
                },
                tasks=[
                    TaskExecution.create(
                        name="Get IP",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="ep2",
                type="http",
                name="Endpoint 2 (Headers)",
                requisite_stage_ref_ids={"start"},
                context={
                    "url": "https://httpbin.org/headers",
                    "method": "GET",
                },
                tasks=[
                    TaskExecution.create(
                        name="Get Headers",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="ep3",
                type="http",
                name="Endpoint 3 (User-Agent)",
                requisite_stage_ref_ids={"start"},
                context={
                    "url": "https://httpbin.org/user-agent",
                    "method": "GET",
                },
                tasks=[
                    TaskExecution.create(
                        name="Get User-Agent",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Join
            StageExecution(
                ref_id="collect",
                type="http",
                name="Collect Results",
                requisite_stage_ref_ids={"ep1", "ep2", "ep3"},
                context={
                    "url": "https://httpbin.org/get?stage=complete",
                    "method": "GET",
                },
                tasks=[
                    TaskExecution.create(
                        name="Final request",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    for stage in result.stages:
        status = stage.outputs.get("status_code", "N/A")
        elapsed = stage.outputs.get("elapsed_ms", "N/A")
        print(f"  {stage.name}: {status} ({elapsed}ms)")


# =============================================================================
# Example 5: All HTTP Methods
# =============================================================================


def example_all_methods() -> None:
    """Demonstrate all supported HTTP methods."""
    print("\n" + "=" * 60)
    print("Example 5: All HTTP Methods")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    methods = [
        ("GET", "https://httpbin.org/get", None),
        ("POST", "https://httpbin.org/post", {"action": "create"}),
        ("PUT", "https://httpbin.org/put", {"action": "update"}),
        ("PATCH", "https://httpbin.org/patch", {"action": "partial"}),
        ("DELETE", "https://httpbin.org/delete", None),
        ("HEAD", "https://httpbin.org/get", None),
        ("OPTIONS", "https://httpbin.org/get", None),
    ]

    stages = []
    prev_ref = None
    for i, (method, url, body) in enumerate(methods, 1):
        ref_id = str(i)
        context: dict[str, Any] = {"url": url, "method": method}
        if body:
            context["json"] = body

        stage = StageExecution(
            ref_id=ref_id,
            type="http",
            name=f"{method} Request",
            requisite_stage_ref_ids={prev_ref} if prev_ref else set(),
            context=context,
            tasks=[
                TaskExecution.create(
                    name=f"HTTP {method}",
                    implementing_class="http",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
        )
        stages.append(stage)
        prev_ref = ref_id

    workflow = Workflow.create(
        application="http-example",
        name="All HTTP Methods",
        stages=stages,
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=120.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    for stage in result.stages:
        status = stage.outputs.get("status_code", "N/A")
        elapsed = stage.outputs.get("elapsed_ms", "N/A")
        print(f"  {stage.name}: {status} ({elapsed}ms)")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("Stabilize HTTP Examples")
    print("=" * 60)
    print("Using httpbin.org for testing")

    example_simple_get()
    example_post_json()
    example_sequential_api()
    example_parallel_requests()
    example_all_methods()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
