"""
Tool definitions for LLM tool-calling loops.

The ``@tool`` decorator turns a plain Python function into a callable tool with
an auto-generated OpenAI-style JSON schema (derived from its signature and
type hints). A :class:`ToolRegistry` collects tools, exposes their schemas to
the model, and dispatches the model's tool calls back to the functions.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from typing import Any, get_type_hints


def _json_type(annotation: Any) -> str:
    mapping = {str: "string", int: "integer", float: "number", bool: "boolean", list: "array", dict: "object"}
    return mapping.get(annotation, "string")


class ToolSpec:
    """A registered tool: the callable plus its generated JSON schema."""

    def __init__(self, func: Callable[..., Any], name: str, description: str) -> None:
        self.func = func
        self.name = name
        self.description = description
        self.schema = self._build_schema(func, name, description)

    @staticmethod
    def _build_schema(func: Callable[..., Any], name: str, description: str) -> dict[str, Any]:
        sig = inspect.signature(func)
        try:
            hints = get_type_hints(func)
        except Exception:
            hints = {}
        properties: dict[str, Any] = {}
        required: list[str] = []
        for param_name, param in sig.parameters.items():
            if param_name in {"self", "cls"}:
                continue
            annotation = hints.get(param_name, str)
            properties[param_name] = {"type": _json_type(annotation)}
            if param.default is inspect.Parameter.empty:
                required.append(param_name)
        return {
            "type": "function",
            "function": {
                "name": name,
                "description": description,
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                },
            },
        }

    def call(self, arguments: dict[str, Any]) -> Any:
        return self.func(**arguments)


def tool(
    _func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
) -> Any:
    """Decorate a function as an LLM tool.

    The schema is derived from the signature and docstring::

        @tool
        def get_weather(city: str) -> str:
            "Return the weather for a city."
            ...
    """

    def wrap(func: Callable[..., Any]) -> Callable[..., Any]:
        spec = ToolSpec(
            func,
            name or func.__name__,
            description or (inspect.getdoc(func) or "").strip() or func.__name__,
        )
        func._tool_spec = spec  # type: ignore[attr-defined]
        return func

    if _func is not None:
        return wrap(_func)
    return wrap


class ToolRegistry:
    """Collects tools and dispatches model tool calls to them."""

    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def add(self, func: Callable[..., Any]) -> ToolRegistry:
        """Register a @tool-decorated function (or a plain callable)."""
        spec: ToolSpec = getattr(func, "_tool_spec", None) or ToolSpec(
            func, func.__name__, (inspect.getdoc(func) or "").strip() or func.__name__
        )
        self._tools[spec.name] = spec
        return self

    def schemas(self) -> list[dict[str, Any]]:
        """OpenAI-style tool schemas for every registered tool."""
        return [spec.schema for spec in self._tools.values()]

    def dispatch(self, tool_call: dict[str, Any]) -> Any:
        """Execute one model tool call and return its result.

        Accepts both OpenAI (``{"function": {"name","arguments"}}``) and
        Ollama (``{"function": {"name","arguments": {...}}}``) shapes.
        """
        function = tool_call.get("function", tool_call)
        name = function.get("name", "")
        raw_args = function.get("arguments", {})
        if isinstance(raw_args, str):
            arguments = json.loads(raw_args) if raw_args.strip() else {}
        else:
            arguments = raw_args or {}
        if name not in self._tools:
            raise KeyError(f"Unknown tool: {name}")
        return self._tools[name].call(arguments)

    def __contains__(self, name: str) -> bool:
        return name in self._tools

    def __len__(self) -> int:
        return len(self._tools)
