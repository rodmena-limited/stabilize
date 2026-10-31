from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

_results: dict[str, dict[str, str]] = {}
_rootdir: Path | None = None


def pytest_configure(config: Any) -> None:
    global _rootdir
    _rootdir = Path(str(config.rootpath))


def _relative(path: str) -> str:
    try:
        return str(Path(path).resolve().relative_to(_rootdir)) if _rootdir else path
    except ValueError:
        return path


def _where(excinfo: Any) -> str:
    """The innermost frame inside the checkout: the suite's own check, or the engine line that raised."""
    try:
        entries = list(excinfo.traceback)
    except Exception:
        return "?"
    for entry in reversed(entries):
        path = str(entry.path)
        rel = _relative(path)
        if rel != path:
            return f"{rel}:{entry.lineno + 1}"
    return "?"


def _message(excinfo: Any) -> str:
    try:
        text = excinfo.exconly()
    except Exception:
        return "unknown"
    first = text.splitlines()[0] if text else ""
    return first.split(": ", 1)[1][:200] if ": " in first else first[:200]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: Any, call: Any) -> Any:
    outcome = yield
    report = outcome.get_result()
    if call.excinfo is not None and not report.passed:
        report._redproof = {
            "exc_type": call.excinfo.type.__name__,
            "where": _where(call.excinfo),
            "kind": _message(call.excinfo),
        }


def pytest_runtest_setup(item: Any) -> None:
    os.environ["REDPROOF_TEST_ID"] = item.nodeid


def pytest_runtest_logreport(report: Any) -> None:
    entry = _results.setdefault(
        report.nodeid, {"outcome": "passed", "phase": "", "where": "", "exc_type": "", "kind": ""}
    )
    if report.failed and entry["outcome"] != "failed":
        evidence = getattr(report, "_redproof", {}) or {}
        text = str(getattr(report, "longreprtext", "") or "")
        timed_out = "Timeout >" in text or "+++++ Timeout" in text
        entry.update(
            outcome="failed" if report.when == "call" else "error",
            phase=str(report.when),
            where=str(evidence.get("where", "?")),
            exc_type="Timeout" if timed_out else str(evidence.get("exc_type", "")),
            kind=str(evidence.get("kind", "")),
        )
    elif report.skipped and report.when in ("setup", "call") and entry["outcome"] == "passed":
        entry["outcome"] = "skipped"


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    out = os.environ.get("REDPROOF_OUT")
    if not out:
        return
    with open(out, "w") as fh:
        json.dump({"exitstatus": int(exitstatus), "results": _results}, fh)
