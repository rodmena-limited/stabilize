"""
LLM tasks: a plain chat task and a ReAct tool-calling agent loop.

Built on the engine's existing primitives so they inherit its durability:
- :class:`LLMTask` calls the model once and returns the completion.
- :class:`AgentLoopTask` runs a bounded ReAct loop (model -> tool calls ->
  results -> model ...) inside a single task execution, so the whole agent
  turn is one durable, retryable unit of work.

The model client is injectable (constructor or the ``LLMTask.build_client``
hook) so these stay dependency-light and target Ollama or OpenAI-compatible
endpoints without hard-coding anything.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.llm.client import ChatMessage, LLMClient
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.llm.tools import ToolRegistry
    from stabilize.models.stage import StageExecution


class LLMTask(Task):
    """Call an LLM once with a prompt/messages and return the completion.

    Context keys (all optional unless noted):
    - ``prompt`` (str): user prompt. Required unless ``messages`` is given.
    - ``system`` (str): system prompt.
    - ``messages`` (list): full message list (overrides prompt/system).
    - ``model`` (str): model id (else the injected client's model).
    - ``temperature`` (float): sampling temperature.
    - ``output_key`` (str): outputs key for the completion (default
      ``"completion"``).

    Outputs: ``{output_key: <text>, "llm_raw": <provider response>}``.
    """

    aliases = ["llm", "chat"]

    def __init__(self, client: LLMClient | None = None) -> None:
        self._client = client

    def build_client(self, stage: StageExecution) -> LLMClient:
        """Return the LLM client. Override to construct one from context, or
        pass a client to the constructor."""
        if self._client is not None:
            return self._client
        model = stage.context.get("model")
        if not model:
            raise ValueError("LLMTask needs a client or a 'model' in context")
        return LLMClient(
            model=model,
            base_url=stage.context.get("base_url"),
            api_key=stage.context.get("api_key"),
            api=stage.context.get("api", "openai"),
        )

    def _messages(self, stage: StageExecution) -> list[ChatMessage]:
        raw = stage.context.get("messages")
        if raw:
            return [ChatMessage(**m) if isinstance(m, dict) else m for m in raw]
        messages: list[ChatMessage] = []
        system = stage.context.get("system")
        if system:
            messages.append(ChatMessage(role="system", content=system))
        prompt = stage.context.get("prompt")
        if not prompt:
            raise ValueError("LLMTask needs 'prompt' or 'messages' in context")
        messages.append(ChatMessage(role="user", content=prompt))
        return messages

    def execute(self, stage: StageExecution) -> TaskResult:
        try:
            client = self.build_client(stage)
            response = client.chat(
                self._messages(stage),
                temperature=stage.context.get("temperature"),
            )
        except Exception as e:
            return TaskResult.terminal(error=f"LLM call failed: {e}")

        output_key = stage.context.get("output_key", "completion")
        return TaskResult.success(
            outputs={output_key: response.content, "llm_raw": response.raw}
        )


class AgentLoopTask(LLMTask):
    """A bounded ReAct tool-calling loop inside one task execution.

    Each iteration: send the conversation (plus tool schemas) to the model;
    if it returns tool calls, dispatch them via the :class:`ToolRegistry`,
    append the results, and loop; otherwise return the final answer. Bounded
    by ``max_iterations`` so a misbehaving model cannot loop forever.

    Provide the tools by passing a ``ToolRegistry`` to the constructor or by
    overriding :meth:`build_tools`.

    Context keys add to :class:`LLMTask`:
    - ``max_iterations`` (int): loop cap (default 8).
    - ``output_key`` (str): outputs key for the final answer (default
      ``"answer"``).

    Outputs: ``{output_key: <final text>, "tool_invocations": [...],
    "iterations": <n>}``.
    """

    aliases = ["agent", "react_agent", "agent_loop"]

    def __init__(self, client: LLMClient | None = None, tools: ToolRegistry | None = None) -> None:
        super().__init__(client)
        self._tools = tools

    def build_tools(self, stage: StageExecution) -> ToolRegistry:
        """Return the tool registry for this agent. Override or pass to ctor."""
        if self._tools is not None:
            return self._tools
        from stabilize.llm.tools import ToolRegistry

        return ToolRegistry()

    def execute(self, stage: StageExecution) -> TaskResult:
        try:
            client = self.build_client(stage)
            registry = self.build_tools(stage)
        except Exception as e:
            return TaskResult.terminal(error=f"Agent setup failed: {e}")

        messages = self._messages(stage)
        tool_schemas = registry.schemas() or None
        max_iterations = int(stage.context.get("max_iterations", 8))
        output_key = stage.context.get("output_key", "answer")
        invocations: list[dict[str, Any]] = []

        iteration = 0
        try:
            while iteration < max_iterations:
                iteration += 1
                response = client.chat(
                    messages,
                    tools=tool_schemas,
                    temperature=stage.context.get("temperature"),
                )
                if not response.has_tool_calls:
                    return TaskResult.success(
                        outputs={
                            output_key: response.content,
                            "tool_invocations": invocations,
                            "iterations": iteration,
                        }
                    )

                # Record the assistant turn (with its tool calls) then run them.
                messages.append(
                    ChatMessage(role="assistant", content=response.content, tool_calls=response.tool_calls)
                )
                for call in response.tool_calls:
                    call_id = call.get("id", "")
                    fn = call.get("function", call)
                    name = fn.get("name", "")
                    try:
                        result = registry.dispatch(call)
                        result_text = result if isinstance(result, str) else _to_text(result)
                    except Exception as tool_error:
                        result_text = f"ERROR: {tool_error}"
                    invocations.append({"tool": name, "result": result_text})
                    messages.append(
                        ChatMessage(role="tool", content=result_text, tool_call_id=call_id, name=name)
                    )
        except Exception as e:
            return TaskResult.terminal(error=f"Agent loop failed: {e}")

        # Hit the iteration cap without a final answer.
        return TaskResult.failed_continue(
            error=f"Agent did not converge within {max_iterations} iterations",
            outputs={output_key: "", "tool_invocations": invocations, "iterations": iteration},
        )


def _to_text(value: Any) -> str:
    import json

    try:
        return json.dumps(value, default=str)
    except Exception:
        return str(value)
