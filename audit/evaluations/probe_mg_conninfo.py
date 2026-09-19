"""Ticket 23 live probe: what does stabilize actually put on the wire?

Stands up a minimal PostgreSQL wire-protocol listener and reports the
startup parameters libpq sends -- the real counterparty's view of the
database name and password stabilize asked for. Answers the two halves of
the reported defect 2 without needing a strict-pg_hba server.

Run:  python audit/evaluations/probe_mg_conninfo.py
"""

from __future__ import annotations

import os
import socket
import struct
import sys
import threading
from typing import Any

SSL_REQUEST_CODE = 80877103


def _read_exactly(conn: socket.socket, count: int) -> bytes:
    buffer = b""
    while len(buffer) < count:
        chunk = conn.recv(count - len(buffer))
        if not chunk:
            break
        buffer += chunk
    return buffer


def _parse_startup_params(payload: bytes) -> dict[str, str]:
    fields = payload.split(b"\x00")
    pairs: dict[str, str] = {}
    for index in range(0, len(fields) - 1, 2):
        key = fields[index].decode("utf-8", "replace")
        if not key:
            break
        pairs[key] = fields[index + 1].decode("utf-8", "replace")
    return pairs


def _serve_once(listener: socket.socket, result: dict[str, Any]) -> None:
    conn, _ = listener.accept()
    try:
        length_bytes = _read_exactly(conn, 4)
        if len(length_bytes) < 4:
            return
        length = struct.unpack("!I", length_bytes)[0]
        body = _read_exactly(conn, length - 4)

        if len(body) >= 4 and struct.unpack("!I", body[:4])[0] == SSL_REQUEST_CODE:
            conn.sendall(b"N")
            length_bytes = _read_exactly(conn, 4)
            length = struct.unpack("!I", length_bytes)[0]
            body = _read_exactly(conn, length - 4)

        result["startup"] = _parse_startup_params(body[4:])

        conn.sendall(b"R" + struct.pack("!II", 8, 3))

        tag = _read_exactly(conn, 1)
        if tag == b"p":
            msg_length = struct.unpack("!I", _read_exactly(conn, 4))[0]
            secret = _read_exactly(conn, msg_length - 4)
            result["password_sent"] = secret.rstrip(b"\x00").decode("utf-8", "replace")

        error = b"SFATAL\x00C28000\x00Mprobe\x00\x00"
        conn.sendall(b"E" + struct.pack("!I", len(error) + 4) + error)
    finally:
        conn.close()


def capture(db_url: str, environment: dict[str, str]) -> dict[str, Any]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    result: dict[str, Any] = {"startup": {}, "password_sent": None}
    thread = threading.Thread(target=_serve_once, args=(listener, result), daemon=True)
    thread.start()

    saved = dict(os.environ)
    for key in ("PGDATABASE", "PGPASSWORD", "PGUSER", "PGHOST", "PGPORT"):
        os.environ.pop(key, None)
    os.environ.update(environment)
    try:
        import psycopg

        from stabilize.cli.config import connection_params, parse_db_url

        params = connection_params(parse_db_url(db_url.replace("PORT", str(port))))
        try:
            psycopg.connect(**params)
        except Exception as exc:
            result["client_error"] = type(exc).__name__
    finally:
        os.environ.clear()
        os.environ.update(saved)
        thread.join(timeout=5)
        listener.close()
    return result


def main() -> int:
    failures = 0

    print("=" * 72)
    print("CONTROL: password present in the URL is the one that reaches the server")
    control = capture("postgresql://zzz_probe_user:URLPW@127.0.0.1:PORT/provenance", {})
    print(f"  database requested : {control['startup'].get('database')!r}")
    print(f"  user requested     : {control['startup'].get('user')!r}")
    print(f"  password sent      : {control['password_sent']!r}")
    if control["password_sent"] != "URLPW":
        print("  >>> CONTROL FAILED: probe cannot observe a password; results are vacuous")
        return 1
    print("  >>> control green: the probe CAN see database and password")

    print()
    print("=" * 72)
    print("DEFECT 2a: no password in URL -> which DATABASE is requested?")
    case = capture(
        "postgresql://zzz_probe_user@127.0.0.1:PORT/provenance", {"PGPASSWORD": "FROM_ENV"}
    )
    database = case["startup"].get("database")
    print(f"  URL says /provenance, server was asked for: {database!r}")
    if database == "provenance":
        print("  >>> PASS: database resolved correctly")
    else:
        print(f"  >>> FAIL: user-as-database defect present (got {database!r})")
        failures += 1

    print()
    print("DEFECT 2b: no password in URL -> is PGPASSWORD honoured?")
    print(f"  PGPASSWORD=FROM_ENV, password actually sent: {case['password_sent']!r}")
    if case["password_sent"] == "FROM_ENV":
        print("  >>> PASS: PGPASSWORD honoured, not overridden")
    else:
        print(f"  >>> FAIL: PGPASSWORD overridden (sent {case['password_sent']!r})")
        failures += 1

    print()
    print("DEFECT 3: password containing a space must connect, not crash")
    spaced = capture(
        "postgresql://zzz_probe_user:pw%20with%20space@127.0.0.1:PORT/provenance", {}
    )
    print(f"  database requested : {spaced['startup'].get('database')!r}")
    print(f"  password sent      : {spaced['password_sent']!r}")
    if spaced["startup"].get("database") == "provenance":
        print("  >>> PASS: spaced password did not corrupt the connection")
    else:
        print("  >>> FAIL: spaced password corrupted the connection")
        failures += 1

    print()
    print("INJECTION: a password containing 'dbname=evil' must not redirect")
    evil = capture(
        "postgresql://zzz_probe_user:dbname%3Devil@127.0.0.1:PORT/provenance", {}
    )
    print(f"  database requested : {evil['startup'].get('database')!r}")
    if evil["startup"].get("database") == "provenance":
        print("  >>> PASS: no conninfo injection")
    else:
        print(f"  >>> FAIL: connection redirected to {evil['startup'].get('database')!r}")
        failures += 1

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
