import json
import logging
import os
from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    HighwayTask,
    Orchestrator,
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
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue
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
