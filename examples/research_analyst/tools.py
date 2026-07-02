"""
Deterministic local tools for the research agents' ReAct loops.

These are real tools the LLM calls via stabilize's tool-calling AgentLoopTask
(``@tool`` + ToolRegistry). They are deterministic and offline so the demo is
reproducible: the agents must actually call them to get the numbers they
reason over — this is genuine tool use, not a simulation.

Theme: LLM-serving capacity planning. A small knowledge base holds hardware
and model specs; a calculator does the arithmetic.
"""

from __future__ import annotations

import ast
import json
import operator
from typing import Any

from stabilize.llm import tool

# ---- A tiny, curated knowledge base the agents can look things up in ----

_KNOWLEDGE_BASE: dict[str, dict[str, Any]] = {
    # Models: memory footprint at fp16 (2 bytes/param).
    "atlas-70b": {"kind": "model", "params_billion": 70, "bytes_per_param": 2, "weights_gb": 140},
    "atlas-13b": {"kind": "model", "params_billion": 13, "bytes_per_param": 2, "weights_gb": 26},
    # GPUs: VRAM, sustained decode throughput, on-demand hourly price.
    "h100": {"kind": "gpu", "vram_gb": 80, "tokens_per_sec": 2500, "hourly_usd": 3.50},
    "a100": {"kind": "gpu", "vram_gb": 80, "tokens_per_sec": 1500, "hourly_usd": 2.00},
    "l40s": {"kind": "gpu", "vram_gb": 48, "tokens_per_sec": 900, "hourly_usd": 1.10},
    # Workload target for this study.
    "workload": {"kind": "workload", "requests_per_sec": 50, "tokens_per_request": 400},
}

_ALLOWED_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def _safe_eval(node: ast.AST) -> float:
    if isinstance(node, ast.Expression):
        return _safe_eval(node.body)
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return float(node.value)
    if isinstance(node, ast.BinOp) and type(node.op) in _ALLOWED_OPS:
        return _ALLOWED_OPS[type(node.op)](_safe_eval(node.left), _safe_eval(node.right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _ALLOWED_OPS:
        return _ALLOWED_OPS[type(node.op)](_safe_eval(node.operand))
    raise ValueError("unsupported expression")


@tool
def calculate(expression: str) -> str:
    """Evaluate an arithmetic expression (e.g. '140 / 80' or '50 * 400').

    Supports + - * / // % ** and parentheses. Returns the numeric result.
    """
    try:
        value = _safe_eval(ast.parse(expression, mode="eval"))
        # Present integers without a trailing .0 for readability.
        if value == int(value):
            return str(int(value))
        return f"{value:.4f}"
    except Exception as e:
        return f"ERROR: could not evaluate '{expression}': {e}"


@tool
def knowledge_base(name: str) -> str:
    """Look up specs for a model, GPU, or the workload by name.

    Known names: models 'atlas-70b', 'atlas-13b'; GPUs 'h100', 'a100', 'l40s';
    and 'workload' (the target requests/sec and tokens/request). Returns a JSON
    object of specs, or a list of known names if the name is unknown.
    """
    key = (name or "").strip().lower()
    entry = _KNOWLEDGE_BASE.get(key)
    if entry is None:
        return json.dumps({"error": f"unknown '{name}'", "known": sorted(_KNOWLEDGE_BASE)})
    return json.dumps({"name": key, **entry})


@tool
def list_catalog() -> str:
    """List every name available in the knowledge base, grouped by kind."""
    grouped: dict[str, list[str]] = {}
    for name, spec in _KNOWLEDGE_BASE.items():
        grouped.setdefault(spec["kind"], []).append(name)
    return json.dumps(grouped)


def research_tools() -> Any:
    """Build a ToolRegistry with all research tools registered."""
    from stabilize.llm import ToolRegistry

    return ToolRegistry().add(calculate).add(knowledge_base).add(list_catalog)
