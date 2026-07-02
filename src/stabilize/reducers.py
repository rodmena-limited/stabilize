"""
Declarative fan-in reducers for parallel branches (agentic ergonomics).

LangGraph channels take a reducer (e.g. ``operator.add``) so parallel nodes
APPEND to a shared list instead of overwriting it. Stabilize's default
ancestor-output merge is last-write-wins for scalar keys (list-valued keys
already concatenate), so N parallel branches each writing ``{"result": ...}``
silently lose all but one.

This module adds opt-in, declarative reducers applied when a join stage
collects its upstream branches' outputs. A stage sets ``output_reducers``:

    StageExecution(
        ref_id="gather",
        join_type=JoinType.AND,
        requisite_stage_ref_ids={"gen_1", "gen_2", "gen_3"},
        output_reducers={"candidate": "collect", "score": "sum"},
    )

Each upstream branch that produced ``candidate`` contributes to a list under
that key in the join stage's context; ``score`` values are summed. Keys
without a reducer keep the existing last-write-wins behavior, so this is
fully backward compatible.

Built-in reducers (name -> behavior):
- ``collect`` / ``append``: gather each branch's value into a list (a
  branch's list value is extended, a scalar is appended).
- ``extend``: flatten list values from all branches into one list.
- ``sum``: numeric sum across branches.
- ``max`` / ``min``: extremum across branches.
- ``merge``: shallow-merge dict values across branches.
- ``first`` / ``last``: first/last branch's value (last == current default).

Custom reducers can be registered with :func:`register_reducer`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

# A reducer folds a list of per-branch values into one combined value.
Reducer = Callable[[list[Any]], Any]


def _collect(values: list[Any]) -> list[Any]:
    out: list[Any] = []
    for v in values:
        if isinstance(v, list):
            out.extend(v)
        else:
            out.append(v)
    return out


def _extend(values: list[Any]) -> list[Any]:
    out: list[Any] = []
    for v in values:
        if isinstance(v, list):
            out.extend(v)
        elif v is not None:
            out.append(v)
    return out


def _sum(values: list[Any]) -> Any:
    total: Any = 0
    for v in values:
        if v is not None:
            total = total + v
    return total


def _merge(values: list[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for v in values:
        if isinstance(v, dict):
            out.update(v)
    return out


_BUILTIN_REDUCERS: dict[str, Reducer] = {
    "collect": _collect,
    "append": _collect,
    "extend": _extend,
    "sum": _sum,
    "max": lambda values: max(v for v in values if v is not None),
    "min": lambda values: min(v for v in values if v is not None),
    "merge": _merge,
    "first": lambda values: values[0] if values else None,
    "last": lambda values: values[-1] if values else None,
}

_CUSTOM_REDUCERS: dict[str, Reducer] = {}


def register_reducer(name: str, reducer: Reducer) -> None:
    """Register a custom named reducer usable in ``output_reducers``."""
    _CUSTOM_REDUCERS[name] = reducer


def get_reducer(name: str) -> Reducer | None:
    """Look up a reducer by name (custom overrides built-in), or None."""
    return _CUSTOM_REDUCERS.get(name) or _BUILTIN_REDUCERS.get(name)


def apply_output_reducers(
    reducers: dict[str, str],
    branch_outputs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Combine per-branch outputs for the reducer-controlled keys.

    Args:
        reducers: Mapping of output key -> reducer name.
        branch_outputs: Each upstream branch's outputs dict.

    Returns:
        A dict of reduced values for exactly the keys named in ``reducers``
        that at least one branch produced. Keys not named here are left to
        the caller's normal merge.
    """
    result: dict[str, Any] = {}
    for key, reducer_name in reducers.items():
        reducer = get_reducer(reducer_name)
        if reducer is None:
            raise ValueError(f"Unknown output reducer '{reducer_name}' for key '{key}'")
        values = [outputs[key] for outputs in branch_outputs if key in outputs]
        if values:
            result[key] = reducer(values)
    return result
