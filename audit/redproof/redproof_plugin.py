from __future__ import annotations

import json
import os
from typing import Any

_results: dict[str, dict[str, str]] = {}


def _kind(report: Any) -> str:
    text = str(getattr(report, "longreprtext", "") or "")
    if "Timeout >" in text or "+++++ Timeout" in text:
        return "timeout"
    where = ""
    crash = getattr(getattr(report, "longrepr", None), "reprcrash", None)
    if crash is not None:
        where = f"{crash.path.rsplit('/', 2)[-1] if crash.path else '?'}:{crash.lineno} "
        return (where + str(crash.message).splitlines()[0])[:200]
    for line in text.splitlines():
        if line.startswith("E "):
            return line[2:].strip()[:200]
    return "unknown"


def pytest_runtest_logreport(report: Any) -> None:
    entry = _results.setdefault(report.nodeid, {"outcome": "passed", "kind": ""})
    if report.failed and entry["outcome"] != "failed":
        entry["outcome"] = "failed" if report.when == "call" else "error"
        entry["kind"] = f"{report.when}: {_kind(report)}"
    elif report.skipped and report.when in ("setup", "call") and entry["outcome"] == "passed":
        entry["outcome"] = "skipped"


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    out = os.environ.get("REDPROOF_OUT")
    if not out:
        return
    with open(out, "w") as fh:
        json.dump({"exitstatus": int(exitstatus), "results": _results}, fh)
