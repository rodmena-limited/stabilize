"""SQLSTATE rules shared by task classification, deadlock retry and the store-outage check."""

from __future__ import annotations

import os

TRANSIENT_SQLSTATES = frozenset({"40001", "40P01", "55P03", "53300", "57P03", "08001", "08004"})

SHUTDOWN_SQLSTATES = frozenset({"57P01", "57P02", "57014"})

RETRY_ON_SHUTDOWN_ENV = "STABILIZE_RETRY_ON_SHUTDOWN"

STORE_UNAVAILABLE_SQLSTATES = frozenset({"53300", "57P01", "57P02", "57P03", "25006", "08007", "40003", "0A000"})

STORE_UNAVAILABLE_CLASSES = frozenset({"08", "58"})


def sqlstate(error: BaseException) -> str | None:
    code = getattr(error, "sqlstate", None)
    return code if isinstance(code, str) and len(code) == 5 else None


def is_transient_sqlstate(code: str | None) -> bool:
    if code is None:
        return False
    if code in TRANSIENT_SQLSTATES:
        return True
    return code in SHUTDOWN_SQLSTATES and os.environ.get(RETRY_ON_SHUTDOWN_ENV, "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def is_store_unavailable_sqlstate(code: str | None) -> bool:
    if code is None:
        return False
    return code in STORE_UNAVAILABLE_SQLSTATES or code[:2] in STORE_UNAVAILABLE_CLASSES
