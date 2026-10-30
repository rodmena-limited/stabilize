#!/usr/bin/env python3
"""
Ollama LLM Example - Demonstrates interacting with local LLM via Ollama with Stabilize.

This example shows how to:
1. Create a custom Task that calls Ollama API
2. Generate text completions with local LLMs
3. Build LLM-powered processing pipelines

Requirements:
    Ollama installed and running (https://ollama.ai)
    At least one model pulled (e.g., ollama pull qwen3-coder-next:cloud)

Run with:
    python examples/llama-example.py
"""

import json
import logging

logging.basicConfig(level=logging.ERROR)

from llama_example_pipelines import example_parallel_generation, example_processing_pipeline
from llama_example_support import setup_pipeline_runner

from stabilize import (
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    Workflow,
    WorkflowStatus,
)


# =============================================================================
# Example 1: Simple Text Generation
# =============================================================================


def example_simple_generation() -> None:
    """Generate text with a simple prompt."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Text Generation")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="llama-example",
        name="Simple Generation",
        stages=[
            StageExecution(
                ref_id="1",
                type="ollama",
                name="Generate Text",
                context={
                    "prompt": "Explain what a workflow engine is in 2-3 sentences.",
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.7,
                    "max_tokens": 150,
                },
                tasks=[
                    TaskExecution.create(
                        name="Generate",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=180.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    if result.status == WorkflowStatus.SUCCEEDED:
        response = result.stages[0].outputs.get("response", "")
        tokens = result.stages[0].outputs.get("eval_count", 0)
        duration = result.stages[0].outputs.get("total_duration_ms", 0)
        print(f"\nResponse ({tokens} tokens, {duration}ms):")
        print("-" * 40)
        print(response)


# =============================================================================
# Example 2: Text with System Prompt
# =============================================================================


def example_with_system_prompt() -> None:
    """Generate text with a system prompt for role/context."""
    print("\n" + "=" * 60)
    print("Example 2: Text with System Prompt")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="llama-example",
        name="System Prompt",
        stages=[
            StageExecution(
                ref_id="1",
                type="ollama",
                name="Technical Explanation",
                context={
                    "system": "You are a senior software architect. Provide clear, technical explanations. Be concise.",
                    "prompt": "What are the benefits of using a DAG (Directed Acyclic Graph) for workflow orchestration?",
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.5,
                    "max_tokens": 200,
                },
                tasks=[
                    TaskExecution.create(
                        name="Generate",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=180.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    if result.status == WorkflowStatus.SUCCEEDED:
        response = result.stages[0].outputs.get("response", "")
        print("\nResponse:")
        print("-" * 40)
        print(response)


# =============================================================================
# Example 3: JSON Structured Output
# =============================================================================


def example_json_output() -> None:
    """Generate structured JSON output."""
    print("\n" + "=" * 60)
    print("Example 3: JSON Structured Output")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="llama-example",
        name="JSON Output",
        stages=[
            StageExecution(
                ref_id="1",
                type="ollama",
                name="Generate JSON",
                context={
                    "system": "You are a JSON generator. Output only valid JSON, no markdown or explanation.",
                    "prompt": """Generate a JSON object describing a software project with these fields:
- name: string
- version: string (semver)
- description: string (1 sentence)
- features: array of 3 strings
- status: one of "alpha", "beta", "stable"
""",
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.3,
                    "format": "json",
                },
                tasks=[
                    TaskExecution.create(
                        name="Generate JSON",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=180.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    if result.status == WorkflowStatus.SUCCEEDED:
        json_output = result.stages[0].outputs.get("json")
        if json_output:
            print("\nParsed JSON:")
            print("-" * 40)
            print(json.dumps(json_output, indent=2))
        else:
            print("\nRaw Response:")
            print(result.stages[0].outputs.get("response", ""))


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("Stabilize Ollama LLM Examples")
    print("=" * 60)
    print("Requires: Ollama running with qwen3-coder-next:cloud model")
    print("Install: ollama pull qwen3-coder-next:cloud")

    example_simple_generation()
    example_with_system_prompt()
    example_json_output()
    example_processing_pipeline()
    example_parallel_generation()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
