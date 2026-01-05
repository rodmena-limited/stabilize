#!/usr/bin/env python3
"""
Ollama LLM Example - Demonstrates interacting with local LLM via Ollama with Stabilize.

This example shows how to:
1. Create a custom Task that calls Ollama API
2. Generate text completions with local LLMs
3. Build LLM-powered processing pipelines

Requirements:
    Ollama installed and running (https://ollama.ai)
    At least one model pulled (e.g., ollama pull deepseek-v3.1:671b-cloud)

Run with:
    python examples/llama-example.py
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
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
    WorkflowStore,
)

# =============================================================================
# Custom Task: OllamaTask
# =============================================================================


class OllamaTask(Task):
    """
    Generate text using Ollama local LLM.

    Uses HTTPTask internally for all HTTP operations.

    Context Parameters:
        prompt: The prompt to send to the model (required)
        model: Model name (default: deepseek-v3.1:671b-cloud)
        system: System prompt to set context (optional)
        host: Ollama API host (default: http://localhost:11434)
        temperature: Sampling temperature 0.0-2.0 (default: 0.7)
        max_tokens: Maximum tokens to generate (optional)
        timeout: Request timeout in seconds (default: 120)
        format: Response format - "json" for JSON mode (optional)

    Outputs:
        response: Generated text response
        model: Model used
        total_duration_ms: Total generation time in milliseconds
        prompt_eval_count: Number of tokens in prompt
        eval_count: Number of tokens generated

    Notes:
        - Requires Ollama running locally
        - Supports any model available in Ollama
        - Use format="json" for structured JSON output
    """

    DEFAULT_HOST = "http://localhost:11434"
    DEFAULT_MODEL = "deepseek-v3.1:671b-cloud"

    def __init__(self) -> None:
        self._http_task = HTTPTask()

    def _make_http_request(self, stage: StageExecution, context: dict[str, Any]) -> TaskResult:
        """Execute HTTP request using HTTPTask with a temporary stage."""
        # Create a temporary stage with the HTTP context
        temp_stage = StageExecution(
            ref_id=stage.ref_id,
            type="http",
            name=f"{stage.name} (HTTP)",
            context=context,
            tasks=[],
        )
        return self._http_task.execute(temp_stage)

    def execute(self, stage: StageExecution) -> TaskResult:
        prompt = stage.context.get("prompt")
        model = stage.context.get("model", self.DEFAULT_MODEL)
        system = stage.context.get("system")
        host = stage.context.get("host", self.DEFAULT_HOST)
        temperature = stage.context.get("temperature", 0.7)
        max_tokens = stage.context.get("max_tokens")
        timeout = stage.context.get("timeout", 120)
        response_format = stage.context.get("format")

        if not prompt:
            return TaskResult.terminal(error="No 'prompt' specified in context")

        # Check Ollama availability using HTTPTask
        health_result = self._make_http_request(
            stage,
            {
                "url": f"{host}/api/tags",
                "method": "GET",
                "timeout": 5,
                "continue_on_failure": True,
            },
        )

        if health_result.status.is_halt or health_result.status.is_failure:
            return TaskResult.terminal(error=f"Ollama not available at {host}. Ensure Ollama is running.")

        # Build request payload
        payload: dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
            },
        }

        if system:
            payload["system"] = system

        if max_tokens:
            payload["options"]["num_predict"] = max_tokens

        if response_format == "json":
            payload["format"] = "json"

        print(f"  [OllamaTask] Generating with {model}...")

        # Make generation request using HTTPTask
        gen_result = self._make_http_request(
            stage,
            {
                "url": f"{host}/api/generate",
                "method": "POST",
                "json": payload,
                "timeout": timeout,
                "parse_json": True,
            },
        )

        if gen_result.status.is_halt:
            return TaskResult.terminal(error=f"Ollama API error: {gen_result.context.get('error', 'Unknown')}")

        if gen_result.status.is_failure:
            error_body = gen_result.outputs.get("body", "Unknown error")
            status = gen_result.outputs.get("status_code", "?")
            return TaskResult.terminal(error=f"Ollama API error ({status}): {error_body}")

        # Parse response from HTTPTask outputs
        result = gen_result.outputs.get("body_json", {})
        elapsed_ms = gen_result.outputs.get("elapsed_ms", 0)

        generated_text = result.get("response", "")
        total_duration = result.get("total_duration", 0) // 1_000_000  # ns to ms
        prompt_eval_count = result.get("prompt_eval_count", 0)
        eval_count = result.get("eval_count", 0)

        outputs = {
            "response": generated_text,
            "model": model,
            "total_duration_ms": total_duration or elapsed_ms,
            "prompt_eval_count": prompt_eval_count,
            "eval_count": eval_count,
        }

        # Try to parse JSON if format was json
        if response_format == "json":
            try:
                outputs["json"] = json.loads(generated_text)
            except json.JSONDecodeError:
                outputs["json"] = None
                outputs["json_error"] = "Failed to parse JSON response"

        print(f"  [OllamaTask] Generated {eval_count} tokens in {total_duration}ms")
        return TaskResult.success(outputs=outputs)


# =============================================================================
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with OllamaTask registered."""
    task_registry = TaskRegistry()
    task_registry.register("ollama", OllamaTask)

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
                    "model": "deepseek-v3.1:671b-cloud",
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
                    "model": "deepseek-v3.1:671b-cloud",
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
                    "model": "deepseek-v3.1:671b-cloud",
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
# Example 4: Sequential Processing Pipeline
# =============================================================================


def example_processing_pipeline() -> None:
    """Chain LLM calls: summarize -> translate -> format."""
    print("\n" + "=" * 60)
    print("Example 4: Processing Pipeline")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    original_text = """
    Stabilize is a lightweight Python workflow execution engine with DAG-based
    stage orchestration. It supports message-driven execution, parallel and
    sequential stages, synthetic stages for lifecycle hooks, multiple persistence
    backends including PostgreSQL and SQLite, and a pluggable task system with
    retry and timeout support.
    """

    workflow = Workflow.create(
        application="llama-example",
        name="Processing Pipeline",
        stages=[
            # Step 1: Summarize
            StageExecution(
                ref_id="1",
                type="ollama",
                name="Summarize",
                context={
                    "system": "You are a technical writer. Summarize text into bullet points.",
                    "prompt": f"Summarize this into 3 key bullet points:\n\n{original_text}",
                    "model": "deepseek-v3.1:671b-cloud",
                    "temperature": 0.3,
                    "max_tokens": 150,
                },
                tasks=[
                    TaskExecution.create(
                        name="Summarize",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Step 2: Extract keywords
            StageExecution(
                ref_id="2",
                type="ollama",
                name="Extract Keywords",
                requisite_stage_ref_ids={"1"},
                context={
                    "system": "Extract technical keywords. Output only a comma-separated list.",
                    "prompt": f"Extract 5 technical keywords from:\n\n{original_text}",
                    "model": "deepseek-v3.1:671b-cloud",
                    "temperature": 0.2,
                    "max_tokens": 50,
                },
                tasks=[
                    TaskExecution.create(
                        name="Keywords",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Step 3: Generate tagline
            StageExecution(
                ref_id="3",
                type="ollama",
                name="Generate Tagline",
                requisite_stage_ref_ids={"2"},
                context={
                    "system": "You are a marketing copywriter. Be concise and catchy.",
                    "prompt": f"Create a one-line tagline (max 10 words) for this product:\n\n{original_text}",
                    "model": "deepseek-v3.1:671b-cloud",
                    "temperature": 0.8,
                    "max_tokens": 30,
                },
                tasks=[
                    TaskExecution.create(
                        name="Tagline",
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
    processor.process_all(timeout=300.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print("\nPipeline Results:")
    print("-" * 40)
    for stage in result.stages:
        print(f"\n{stage.name}:")
        response = stage.outputs.get("response", "N/A")
        print(f"  {response[:200]}")


# =============================================================================
# Example 5: Parallel Generation
# =============================================================================


def example_parallel_generation() -> None:
    """Generate multiple variations in parallel."""
    print("\n" + "=" * 60)
    print("Example 5: Parallel Generation")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    base_prompt = "Write a one-sentence description of a workflow engine"

    #      Start
    #     /  |  \
    # Formal Casual Technical
    #     \  |  /
    #     Compare

    workflow = Workflow.create(
        application="llama-example",
        name="Parallel Generation",
        stages=[
            StageExecution(
                ref_id="start",
                type="ollama",
                name="Setup",
                context={
                    "prompt": "Say 'Starting parallel generation' in exactly 3 words.",
                    "model": "deepseek-v3.1:671b-cloud",
                    "max_tokens": 10,
                },
                tasks=[
                    TaskExecution.create(
                        name="Setup",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Parallel variations
            StageExecution(
                ref_id="formal",
                type="ollama",
                name="Formal Style",
                requisite_stage_ref_ids={"start"},
                context={
                    "system": "Write in a formal, professional tone.",
                    "prompt": base_prompt,
                    "model": "deepseek-v3.1:671b-cloud",
                    "temperature": 0.3,
                    "max_tokens": 50,
                },
                tasks=[
                    TaskExecution.create(
                        name="Formal",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="casual",
                type="ollama",
                name="Casual Style",
                requisite_stage_ref_ids={"start"},
                context={
                    "system": "Write in a casual, friendly tone.",
                    "prompt": base_prompt,
                    "model": "deepseek-v3.1:671b-cloud",
                    "temperature": 0.7,
                    "max_tokens": 50,
                },
                tasks=[
                    TaskExecution.create(
                        name="Casual",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="technical",
                type="ollama",
                name="Technical Style",
                requisite_stage_ref_ids={"start"},
                context={
                    "system": "Write in a technical, precise tone for developers.",
                    "prompt": base_prompt,
                    "model": "deepseek-v3.1:671b-cloud",
                    "temperature": 0.2,
                    "max_tokens": 50,
                },
                tasks=[
                    TaskExecution.create(
                        name="Technical",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Compare
            StageExecution(
                ref_id="compare",
                type="ollama",
                name="Compare Results",
                requisite_stage_ref_ids={"formal", "casual", "technical"},
                context={
                    "prompt": "Say 'Generation complete' in exactly 2 words.",
                    "model": "deepseek-v3.1:671b-cloud",
                    "max_tokens": 10,
                },
                tasks=[
                    TaskExecution.create(
                        name="Compare",
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
    processor.process_all(timeout=300.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print("\nGenerated Variations:")
    print("-" * 40)

    for stage in result.stages:
        if stage.ref_id in ["formal", "casual", "technical"]:
            response = stage.outputs.get("response", "N/A")
            print(f"\n{stage.name}:")
            print(f"  {response[:150]}")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("Stabilize Ollama LLM Examples")
    print("=" * 60)
    print("Requires: Ollama running with deepseek-v3.1:671b-cloud model")
    print("Install: ollama pull deepseek-v3.1:671b-cloud")

    example_simple_generation()
    example_with_system_prompt()
    example_json_output()
    example_processing_pipeline()
    example_parallel_generation()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
