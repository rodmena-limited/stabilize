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

import os
import subprocess
import sys
import time

CONTAINER = "stabilize-probe-ssl"
PORT = 55438

TLS_KEYS = ("sslmode", "sslrootcert", "sslcert", "sslkey")


def _start() -> str | None:
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
    if subprocess.run(
        ["docker", "run", "-d", "--name", CONTAINER,
         "-e", "POSTGRES_PASSWORD=pw", "-e", "POSTGRES_USER=vu", "-e", "POSTGRES_DB=vdb",
         "-p", f"{PORT}:5432", "postgres:16"],
        capture_output=True, check=False,
    ).returncode != 0:
        return None
    for _ in range(60):
        if subprocess.run(
            ["docker", "exec", CONTAINER, "pg_isready", "-U", "vu", "-d", "vdb"],
            capture_output=True, check=False,
        ).returncode == 0:
            time.sleep(2)
            return f"postgresql://vu:pw@127.0.0.1:{PORT}/vdb"
        time.sleep(1)
    return None


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

    dsn = os.environ.get("STABILIZE_PROBE_DSN") or _start()
    owns = "STABILIZE_PROBE_DSN" not in os.environ
    if dsn is None:
        print()
        print("SKIP (live half): no PostgreSQL available")
        print(f"RESULT: {'STATIC CHECKS PASSED' if failures == 0 else f'{failures} FAILED'}")
        return 1 if failures else 0

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
        if owns:
            subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
