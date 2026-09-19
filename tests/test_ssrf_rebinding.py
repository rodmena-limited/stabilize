"""Ticket #3: HTTPTask must refuse a connection whose real peer address is
blocked, even when every DNS lookup before connect answered public."""

from __future__ import annotations

import http.server
import socket
import threading
from collections.abc import Iterator
from unittest import mock

import pytest

from stabilize.models import StageExecution
from stabilize.tasks.http.guarded_opener import BlockedPeerError, build_guarded_handlers
from stabilize.tasks.http.task import HTTPTask

PUBLIC_ANSWER = "93.184.216.34"
HOSTNAME = "rebind.test.invalid"
SECRET_BODY = b"INTERNAL SECRET"


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Length", str(len(SECRET_BODY)))
        self.end_headers()
        self.wfile.write(SECRET_BODY)

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def private_server() -> Iterator[int]:
    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield port
    server.shutdown()


def _execute(url: str, **extra: object) -> tuple[str, dict]:
    stage = StageExecution(ref_id="s", name="s", context={"url": url, "timeout": 5, **extra})
    result = HTTPTask().execute(stage)
    return str(result.status), dict(result.context or {})


class TestConnectTimeGuard:
    def test_known_positive_plain_loopback_is_blocked(self, private_server: int) -> None:
        """Without this, the rebinding test could pass because nothing works."""
        _status, ctx = _execute(f"http://127.0.0.1:{private_server}/x")
        assert "SSRF blocked" in str(ctx.get("error"))

    def test_known_positive_allow_private_still_reaches_the_server(
        self, private_server: int
    ) -> None:
        stage = StageExecution(
            ref_id="s",
            name="s",
            context={
                "url": f"http://127.0.0.1:{private_server}/x",
                "allow_private_urls": True,
                "timeout": 5,
            },
        )
        result = HTTPTask().execute(stage)
        assert "INTERNAL SECRET" in str((result.outputs or {}).get("body", ""))

    def test_rebinding_at_connect_time_is_refused(self, private_server: int) -> None:
        """Both validations answer public; the connect lookup answers loopback."""
        real = socket.getaddrinfo
        lookups: list[str] = []

        def rebinding(host, port, *args, **kwargs):  # type: ignore[no-untyped-def]
            if host != HOSTNAME:
                return real(host, port, *args, **kwargs)
            lookups.append(host)
            if len(lookups) <= 2:
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (PUBLIC_ANSWER, port or 0))]
            return real("127.0.0.1", port, *args, **kwargs)

        with mock.patch.object(socket, "getaddrinfo", rebinding):
            _status, ctx = _execute(f"http://{HOSTNAME}:{private_server}/x")

        assert len(lookups) >= 3, (
            f"only {len(lookups)} lookups: the connect path never re-resolved, so this "
            "test did not exercise the connect-time guard"
        )
        detail = str(ctx.get("error"))
        assert "INTERNAL SECRET" not in detail
        assert "connected to blocked address" in detail, (
            f"expected the connect-time guard to refuse, got: {detail}"
        )

    def test_refusal_returns_a_result_rather_than_raising(self, private_server: int) -> None:
        """A blocked request must not escape execute() as an exception."""
        real = socket.getaddrinfo
        lookups: list[str] = []

        def rebinding(host, port, *args, **kwargs):  # type: ignore[no-untyped-def]
            if host != HOSTNAME:
                return real(host, port, *args, **kwargs)
            lookups.append(host)
            if len(lookups) <= 2:
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (PUBLIC_ANSWER, port or 0))]
            return real("127.0.0.1", port, *args, **kwargs)

        with mock.patch.object(socket, "getaddrinfo", rebinding):
            status, _ctx = _execute(f"http://{HOSTNAME}:{private_server}/x", retries=2)
        assert status  # reached here without an unhandled exception


class TestGuardedHandlerUnit:
    def test_blocked_peer_error_is_an_oserror(self) -> None:
        """urllib's own handling treats connection problems as OSError."""
        assert issubclass(BlockedPeerError, OSError)

    def test_handlers_are_built_for_both_schemes(self) -> None:
        handlers = build_guarded_handlers(lambda _addr: False, None)
        assert len(handlers) == 2
