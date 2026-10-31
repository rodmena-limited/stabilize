"""Audit probe: do URL query parameters reach libpq?

Reported by trace-thinkpad-83589d: mg-up/mg-status dropped every query
parameter, so `sslmode=verify-full` on a TLS-mandatory database was discarded
and the connection proceeded with no TLS settings at all.

The live half uses a server with SSL OFF and asserts that `sslmode=require`
FAILS. That is decisive without needing certificates: if the parameter is
dropped the connection succeeds, and if it is honoured libpq refuses.

Run:  python audit/evaluations/probe_dsn_ssl_params.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _backends import probe_dsn, skip  # noqa: E402

import os
import subprocess
import sys

TLS_KEYS = ("sslmode", "sslrootcert", "sslcert", "sslkey")


def _mg_status(dsn: str) -> str:
    out = subprocess.run(
        [sys.executable, "-m", "stabilize.cli.main", "mg-status", "--db-url", dsn],
        capture_output=True, text=True, check=False,
    )
    return (out.stdout + out.stderr).strip()


def main() -> int:
    from stabilize.cli.config import connection_params, parse_db_url

    failures = 0

    print("=" * 72)
    print("STATIC — every query parameter must reach the connection mapping")
    url = (
        "postgresql://u:p@h:5432/db?sslmode=verify-full&sslrootcert=/ca.crt"
        "&sslcert=/c.crt&sslkey=/c.key&application_name=mg"
    )
    params = connection_params(parse_db_url(url))
    for key in TLS_KEYS:
        present = key in params
        print(f"  {key:14} -> {'present' if present else 'DROPPED'}")
        if not present:
            failures += 1
    print(f"  application_name -> {'present' if 'application_name' in params else 'DROPPED'}")
    if "application_name" not in params:
        failures += 1

    print()
    print("  CONTROL: a plain URL must not gain parameters it never had")
    plain = connection_params(parse_db_url("postgresql://u:p@h/db"))
    extra = set(plain) - {"host", "port", "user", "dbname", "password"}
    print(f"    unexpected keys: {sorted(extra) or 'none'}")
    if extra:
        failures += 1

    if not os.environ.get("STABILIZE_PROBE_DSN"):
        print(f"RESULT: {'STATIC CHECKS PASSED' if failures == 0 else f'{failures} FAILED'}")
        if failures:
            return 1
        skip("live half: no PostgreSQL available (set STABILIZE_PROBE_DSN)")
    dsn = probe_dsn("ssl")

    try:
        print()
        print("=" * 72)
        print("LIVE — server has SSL OFF, so sslmode=require must be REFUSED")
        print()
        print("  CONTROL A: no sslmode -> must connect")
        text = _mg_status(dsn)
        reachable = "Status" in text or "applied" in text
        print(f"    reachable: {reachable}")
        if not reachable:
            print("    >>> CONTROL FAILED: server unreachable; the live half proves nothing")
            return 1

        print()
        print("  sslmode=require -> must FAIL")
        text = _mg_status(dsn + "?sslmode=require")
        refused = "does not support SSL" in text
        print(f"    {text.splitlines()[0][:90] if text else '(no output)'}")
        if refused:
            print("    >>> PASS: the parameter reached libpq")
        else:
            print("    >>> FAIL: connection succeeded, so sslmode was dropped")
            failures += 1

        print()
        print("  CONTROL B: sslmode=disable -> must connect again")
        text = _mg_status(dsn + "?sslmode=disable")
        ok = "Status" in text or "applied" in text
        print(f"    reachable: {ok}")
        if not ok:
            print("    >>> FAIL: the guard is refusing everything, not honouring sslmode")
            failures += 1
    finally:
        pass

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
