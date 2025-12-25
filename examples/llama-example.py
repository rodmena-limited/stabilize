import json
import logging
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    HTTPTask,
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
    WorkflowStatus,
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

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
    DEFAULT_HOST = 'http://localhost:11434'
    DEFAULT_MODEL = 'deepseek-v3.1:671b-cloud'
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
