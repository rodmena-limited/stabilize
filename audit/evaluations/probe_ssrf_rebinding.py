"""Audit probe (#3): does HTTPTask still connect when the name resolves to a
public address at validation time and a private one at connect time?

Simulates DNS rebinding by patching the resolver used by the SSRF guard to
answer 'public', while the socket layer actually reaches loopback. A guard
that only validates hostnames passes this; one that checks the live peer does
not.

Run:  python audit/evaluations/probe_ssrf_rebinding.py
"""

from __future__ import annotations

import http.server
import socket
import sys
import threading
from unittest import mock

PUBLIC_ANSWER = "93.184.216.34"


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = b"INTERNAL SECRET"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


def _serve() -> tuple[http.server.HTTPServer, int]:
    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port


def _run_task(url: str) -> tuple[str, str]:
    from stabilize.models import StageExecution
    from stabilize.tasks.http.task import HTTPTask

    stage = StageExecution(ref_id="s", name="s", context={"url": url, "timeout": 5})
    result = HTTPTask().execute(stage)
    ctx = result.context or {}
    return str(result.status), str(ctx.get("error") or ctx.get("body", ""))[:160]


def main() -> int:
    server, port = _serve()
    url = f"http://rebind.probe.invalid:{port}/secret"
    failures = 0

    try:
        print("=" * 72)
        print("CONTROL: with no rebinding, a loopback URL must be BLOCKED")
        status, detail = _run_task(f"http://127.0.0.1:{port}/secret")
        print(f"  status: {status}")
        print(f"  detail: {detail}")
        if "SSRF blocked" not in detail:
            print("  >>> CONTROL FAILED: plain loopback was not blocked; probe is meaningless")
            return 1
        print("  >>> control green: the guard blocks loopback it can see")

        print()
        print("=" * 72)
        print("REBINDING: validation resolves PUBLIC, the socket reaches LOOPBACK")
        real_getaddrinfo = socket.getaddrinfo
        lookups: list[str] = []

        def rebinding_getaddrinfo(host, port_, *args, **kwargs):
            """First lookup answers PUBLIC, every later one answers LOOPBACK.

            One patched function, because task.socket and socket are the same
            module object: patching both only applies the second. A call
            counter is also the faithful simulation — rebinding is exactly the
            record changing between the validation lookup and the connect
            lookup.
            """
            if host != "rebind.probe.invalid":
                return real_getaddrinfo(host, port_, *args, **kwargs)
            lookups.append(host)
            # HTTPTask resolves TWICE before connecting: once in the pre-flight
            # validation and once in the revalidation immediately before open().
            # Both must be answered PUBLIC for this to test the connect-time
            # guard rather than either validator. The CONNECT lookup then
            # answers loopback -- which is exactly what a rebinding attacker
            # with a short TTL does.
            if len(lookups) <= 2:
                return [
                    (socket.AF_INET, socket.SOCK_STREAM, 6, "", (PUBLIC_ANSWER, port_ or 0))
                ]
            return real_getaddrinfo("127.0.0.1", port_, *args, **kwargs)

        with mock.patch.object(socket, "getaddrinfo", rebinding_getaddrinfo):
            status, detail = _run_task(url)

        print(f"  DNS lookups made          : {len(lookups)}")
        if len(lookups) < 3:
            print("  >>> PROBE INVALID: fewer than 3 lookups, so the connect path")
            print("      never re-resolved and the connect-time guard was not reached.")
            return 1
        print("  lookups 1-2 answered      : " + PUBLIC_ANSWER + " (passed both validators)")
        print("  connect lookup answered   : 127.0.0.1 (the rebind)")
        print(f"  status: {status}")
        print(f"  detail: {detail}")
        leaked = "INTERNAL SECRET" in detail
        blocked = "SSRF blocked" in detail
        by_preflight = "resolves to blocked address" in detail
        by_connect = "connected to blocked address" in detail
        print(f"  body of the internal service reached the task : {leaked}")
        if leaked:
            print("  >>> FAIL: rebinding succeeded, the private service was read")
            failures += 1
        elif by_connect:
            print("  >>> PASS: refused at CONNECT time on the real peer address")
        elif by_preflight:
            print("  >>> PROBE INVALID: blocked by the pre-flight validator, which means")
            print("      the first lookup did not return the public answer. The")
            print("      connect-time guard was never exercised.")
            failures += 1
        elif blocked:
            print("  >>> PASS (blocked, but not by the connect-time path)")
        else:
            print("  >>> FAIL: connection did not succeed, but not via the SSRF guard")
            failures += 1

        print()
        print("=" * 72)
        print("ALLOW-LIST ESCAPE: allow_private_urls=True must still reach loopback")
        from stabilize.models import StageExecution
        from stabilize.tasks.http.task import HTTPTask

        stage = StageExecution(
            ref_id="s",
            name="s",
            context={
                "url": f"http://127.0.0.1:{port}/secret",
                "allow_private_urls": True,
                "timeout": 5,
            },
        )
        allowed = HTTPTask().execute(stage)
        body = str((allowed.outputs or {}).get("body", ""))
        print(f"  body: {body[:40]!r}")
        if "INTERNAL SECRET" in body:
            print("  >>> PASS: the opt-in still works (guard is not blocking everything)")
        else:
            print("  >>> FAIL: allow_private_urls no longer reaches a private host")
            failures += 1
    finally:
        server.shutdown()

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
