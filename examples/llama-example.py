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
