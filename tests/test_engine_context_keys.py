"""Engine-owned context keys are enumerated, and a new one cannot land unannounced.

The 0.27.0 defect was not that `_hydrated_keys` leaked. It was that context has
no published boundary between caller data and engine bookkeeping, so EVERY
engine addition is a silent behaviour change that only a broadcast can make
safe — and the release that added it announced five changes and shipped six.

This file is the boundary's enforcement. It fails when the engine writes a
top-level underscore key that `engine_keys.py` does not register, so the next
addition is caught here rather than in a consumer's forensic diff.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from stabilize.models.stage.engine_keys import (
    ENGINE_CONTEXT_KEYS,
    HIDDEN_FROM_TASK_INPUT,
    TASK_VISIBLE_KEYS,
    caller_context,
)

SRC_ROOT = Path(__file__).resolve().parent.parent / "src" / "stabilize"

_SUBSCRIPT_WRITE = re.compile(r"""context\[\s*["'](_[a-z0-9_]+)["']\s*\]""")
_GET_READ = re.compile(r"""context\.(?:get|pop|setdefault)\(\s*["'](_[a-z0-9_]+)["']""")
# A key can also be written through a module constant, which is how _hydrated_keys
# stayed invisible to the first version of this scanner. Catch the definition too.
_CONSTANT_DEF = re.compile(r"""^[A-Z][A-Z0-9_]*\s*=\s*["'](_[a-z0-9_]+)["']""", re.MULTILINE)


def _keys_referenced_in_source() -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if path.name == "engine_keys.py":
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in (_SUBSCRIPT_WRITE, _GET_READ, _CONSTANT_DEF):
            for match in pattern.finditer(text):
                found.setdefault(match.group(1), set()).add(
                    str(path.relative_to(SRC_ROOT.parent.parent))
                )
    return found


def test_the_scanner_can_see_a_known_positive() -> None:
    """If this cannot find a key that is definitely written, its silence means nothing."""
    found = _keys_referenced_in_source()

    assert "_completed_branches" in found, (
        "the scanner found no _completed_branches reference, so it cannot be "
        "trusted to report the absence of any other engine key"
    )
    assert any("split_logic.py" in where for where in found["_completed_branches"])


def test_every_engine_written_key_is_registered() -> None:
    found = _keys_referenced_in_source()

    unregistered = sorted(key for key in found if key not in ENGINE_CONTEXT_KEYS)
    detail = "; ".join(f"{k} ({', '.join(sorted(found[k])[:2])})" for k in unregistered)

    assert not unregistered, (
        "these top-level context keys are written or read by the engine but are "
        "not registered in models/stage/engine_keys.py, so they would reach a "
        "consumer's SELECT and a task's INPUT unannounced: " + detail
    )


def test_registered_keys_that_no_longer_exist_are_removed() -> None:
    """A stale entry widens the published surface and hides the next real key."""
    found = _keys_referenced_in_source()

    stale = sorted(key for key in ENGINE_CONTEXT_KEYS if key not in found)

    assert not stale, (
        "registered in engine_keys.py but referenced nowhere in src/, so the "
        "published inventory overstates what the engine writes: " + ", ".join(stale)
    )


def test_caller_context_removes_exactly_the_engine_keys() -> None:
    context = {
        "script": "print(1)",
        "user_value": 42,
        "_hydrated_keys": ["a"],
        "_jump_count": 3,
        "_signal_name": "resume",
    }

    assert caller_context(context) == {"script": "print(1)", "user_value": 42}


def test_signal_keys_stay_visible_to_tasks() -> None:
    """WCP-24 delivers a signal to a suspended task THROUGH context, on purpose.

    A blanket underscore filter at the INPUT boundary would silently break that,
    so the split between hidden and visible is asserted rather than assumed.
    """
    assert TASK_VISIBLE_KEYS == {"_signal_name", "_signal_data"}
    assert not (TASK_VISIBLE_KEYS & HIDDEN_FROM_TASK_INPUT)
    assert TASK_VISIBLE_KEYS < ENGINE_CONTEXT_KEYS


@pytest.mark.parametrize("key", sorted(HIDDEN_FROM_TASK_INPUT))
def test_hidden_keys_are_excluded_from_task_input(key: str) -> None:
    """The exclusion set PythonTask applies must cover every hidden key."""
    source = (SRC_ROOT / "tasks" / "python.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    uses_inventory = any(
        isinstance(node, ast.Name) and node.id == "HIDDEN_FROM_TASK_INPUT"
        for node in ast.walk(tree)
    )
    assert uses_inventory, "PythonTask no longer filters INPUT by the published inventory"
    assert key in HIDDEN_FROM_TASK_INPUT
