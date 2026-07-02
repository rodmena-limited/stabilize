"""
LLM toolkit for building agentic workflows on stabilize (agentic ergonomics).

Optional, dependency-light (stdlib-only) building blocks for LLM agents:

- :class:`LLMClient` — OpenAI-compatible / Ollama chat client.
- :class:`LLMTask` — call a model once as a workflow task.
- :class:`AgentLoopTask` — a bounded ReAct tool-calling loop as one task.
- :func:`tool` / :class:`ToolRegistry` — define and dispatch tools.

Nothing here is imported by the core engine, so it adds no runtime cost to
workflows that do not use it.
"""

from stabilize.llm.client import ChatMessage, ChatResponse, LLMClient, LLMError
from stabilize.llm.tasks import AgentLoopTask, LLMTask
from stabilize.llm.tools import ToolRegistry, ToolSpec, tool

__all__ = [
    "LLMClient",
    "ChatMessage",
    "ChatResponse",
    "LLMError",
    "LLMTask",
    "AgentLoopTask",
    "tool",
    "ToolRegistry",
    "ToolSpec",
]
