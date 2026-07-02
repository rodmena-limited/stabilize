"""
Dependency-light chat client for OpenAI-compatible / Ollama endpoints.

Uses only the standard library (urllib) so the LLM toolkit adds no runtime
dependency and runs on a laptop. The endpoint and credentials are injectable
so the same code targets Ollama (local or ollama.com cloud), OpenAI, or any
OpenAI-compatible gateway.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ChatMessage:
    """One chat message. ``tool_calls`` / ``tool_call_id`` support tool loops."""

    role: str
    content: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None
    name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"role": self.role}
        if self.content is not None:
            out["content"] = self.content
        if self.tool_calls:
            out["tool_calls"] = self.tool_calls
        if self.tool_call_id:
            out["tool_call_id"] = self.tool_call_id
        if self.name:
            out["name"] = self.name
        return out


@dataclass
class ChatResponse:
    """A model response: assistant text and any requested tool calls."""

    content: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def has_tool_calls(self) -> bool:
        return bool(self.tool_calls)


class LLMClient:
    """Minimal OpenAI-compatible / Ollama chat client (stdlib only).

    Two wire formats are supported:
    - ``api="openai"`` (default): POST ``{base_url}/chat/completions`` with
      the OpenAI schema. Works for OpenAI and OpenAI-compatible gateways,
      including ollama.com cloud's OpenAI endpoint.
    - ``api="ollama"``: POST ``{base_url}/api/chat`` with the native Ollama
      schema (for a local ``ollama serve``).

    Credentials default to the ``OLLAMA_API_KEY`` / ``OPENAI_API_KEY`` env
    vars; never hard-code keys.
    """

    def __init__(
        self,
        model: str,
        base_url: str | None = None,
        api_key: str | None = None,
        api: str = "openai",
        timeout: float = 120.0,
        default_options: dict[str, Any] | None = None,
    ) -> None:
        self.model = model
        self.api = api
        self.timeout = timeout
        self.default_options = default_options or {}
        self.api_key = api_key or os.getenv("OLLAMA_API_KEY") or os.getenv("OPENAI_API_KEY")
        if base_url is not None:
            self.base_url = base_url.rstrip("/")
        elif api == "ollama":
            self.base_url = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
        else:
            self.base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")

    def chat(
        self,
        messages: list[ChatMessage],
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        **options: Any,
    ) -> ChatResponse:
        """Send a chat request and return the assistant response.

        Args:
            messages: Conversation so far.
            tools: Optional OpenAI-style tool/function schemas to expose.
            temperature: Sampling temperature (falls back to default_options).
            **options: Extra provider options merged into the request.
        """
        if self.api == "ollama":
            return self._chat_ollama(messages, tools, temperature, options)
        return self._chat_openai(messages, tools, temperature, options)

    # ---- OpenAI-compatible ----

    def _chat_openai(
        self,
        messages: list[ChatMessage],
        tools: list[dict[str, Any]] | None,
        temperature: float | None,
        options: dict[str, Any],
    ) -> ChatResponse:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [m.to_dict() for m in messages],
        }
        if tools:
            payload["tools"] = tools
        if temperature is not None:
            payload["temperature"] = temperature
        payload.update(self.default_options)
        payload.update(options)

        data = self._post(f"{self.base_url}/chat/completions", payload)
        choice = (data.get("choices") or [{}])[0]
        msg = choice.get("message", {})
        return ChatResponse(
            content=msg.get("content") or "",
            tool_calls=msg.get("tool_calls") or [],
            raw=data,
        )

    # ---- Native Ollama ----

    def _chat_ollama(
        self,
        messages: list[ChatMessage],
        tools: list[dict[str, Any]] | None,
        temperature: float | None,
        options: dict[str, Any],
    ) -> ChatResponse:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [m.to_dict() for m in messages],
            "stream": False,
        }
        if tools:
            payload["tools"] = tools
        opts = dict(self.default_options)
        if temperature is not None:
            opts["temperature"] = temperature
        if opts:
            payload["options"] = opts
        payload.update(options)

        data = self._post(f"{self.base_url}/api/chat", payload)
        msg = data.get("message", {})
        return ChatResponse(
            content=msg.get("content") or "",
            tool_calls=msg.get("tool_calls") or [],
            raw=data,
        )

    def _post(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")
            raise LLMError(f"LLM request failed ({e.code}): {detail}") from e
        except urllib.error.URLError as e:
            raise LLMError(f"LLM request failed: {e.reason}") from e


class LLMError(Exception):
    """Raised when an LLM chat request fails."""
