"""What counts as a proof: the test's own check failed in the call phase, and did not fail without the mutant."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

CRASH_SIGNATURES = frozenset(
    {
        "ImportError",
        "ModuleNotFoundError",
        "SyntaxError",
        "NameError",
        "RecursionError",
        "MemoryError",
        "SystemError",
        "SystemExit",
        "KeyboardInterrupt",
        "Timeout",
        "TimeoutError",
        "Failed: Timeout",
    }
)
ENGINE_PREFIX = "src/"


def proof_kind(entry: dict[str, Any]) -> str | None:
    """The failure a proof rests on, or None when the entry is not the test's own verdict."""
    if entry.get("outcome") != "failed" or entry.get("phase") != "call":
        return None
    exc = str(entry.get("exc_type") or "")
    where = str(entry.get("where") or "")
    kind = str(entry.get("kind") or "")
    if not exc or exc in CRASH_SIGNATURES or "Timeout" in kind:
        return None
    if not where or where.startswith(ENGINE_PREFIX) or where.startswith("?"):
        return None
    return f"{exc}: {kind}" if kind else exc


def confirm_with_control(ids: list[str], runner: Callable[[str, list[str]], dict[str, Any]]) -> dict[str, str] | None:
    """Run the ids under the mutant twice, then once without it; red both times and green without it is a proof.

    None means the run itself was unusable (pytest crashed, timed out, or lost tests) and the mutant is discarded.
    """
    first = runner("mutant", ids)
    results = first.get("results") or {}
    if first.get("exitstatus") not in (0, 1) or any(t not in results for t in ids):
        return None
    red = [t for t in ids if proof_kind(results[t]) is not None]
    if not red:
        return {}
    second = runner("mutant", red).get("results") or {}
    red = [t for t in red if proof_kind(second.get(t, {})) is not None]
    if not red:
        return {}
    control = runner("control", red).get("results") or {}
    confirmed: dict[str, str] = {}
    for t in red:
        if control.get(t, {}).get("outcome") == "passed":
            kind = proof_kind(results[t])
            if kind is not None:
                confirmed[t] = kind
    return confirmed


def assert_baseline_usable(res: dict[str, Any]) -> None:
    if res.get("exitstatus") == -9:
        raise SystemExit("baseline was killed before it finished; no results to build a proof run on")
    if not res.get("results"):
        raise SystemExit(f"baseline produced no results (pytest exit {res.get('exitstatus')}); refusing to continue")
