"""Connect-time peer-address enforcement for HTTPTask.

Validating a hostname and then letting urllib resolve it again is a TOCTOU:
an attacker controlling DNS can answer the validation lookup with a public
address and the connection lookup with a private one. These handlers check the
address the socket is ACTUALLY connected to, after connect and before any
request bytes are written, so whatever DNS returned is irrelevant.

Checking the live peer rather than pinning a pre-resolved IP keeps TLS
hostname verification intact: the connection is still made by hostname.
"""

from __future__ import annotations

import http.client
import socket
from typing import TYPE_CHECKING, Any
from urllib.request import HTTPHandler, HTTPSHandler

if TYPE_CHECKING:
    from collections.abc import Callable


class BlockedPeerError(OSError):
    """Raised when a socket connected to an address that is not permitted."""


def _peer_address(sock: socket.socket | None) -> str | None:
    if sock is None:
        return None
    try:
        peer = sock.getpeername()
    except OSError:
        return None
    if isinstance(peer, tuple) and peer:
        return str(peer[0])
    return None


def _enforce(sock: socket.socket | None, host: str, is_blocked: Callable[[Any], bool]) -> None:
    import ipaddress

    address = _peer_address(sock)
    if address is None:
        raise BlockedPeerError(f"SSRF blocked: could not determine the peer address for {host!r}")
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError as exc:
        raise BlockedPeerError(
            f"SSRF blocked: peer address {address!r} for {host!r} is not an IP address"
        ) from exc
    if is_blocked(parsed):
        raise BlockedPeerError(
            f"SSRF blocked: {host!r} connected to blocked address {address} "
            "(the name resolved differently at connect time than at validation)"
        )


def build_guarded_handlers(
    is_blocked: Callable[[Any], bool],
    ssl_context: Any | None,
) -> list[HTTPHandler | HTTPSHandler]:
    """Handlers that refuse a connection whose peer address is blocked."""

    class GuardedHTTPConnection(http.client.HTTPConnection):
        def connect(self) -> None:
            super().connect()
            try:
                _enforce(self.sock, self.host, is_blocked)
            except BlockedPeerError:
                self.close()
                raise

    class GuardedHTTPSConnection(http.client.HTTPSConnection):
        def connect(self) -> None:
            super().connect()
            try:
                _enforce(self.sock, self.host, is_blocked)
            except BlockedPeerError:
                self.close()
                raise

    class GuardedHTTPHandler(HTTPHandler):
        def http_open(self, req: Any) -> Any:
            return self.do_open(GuardedHTTPConnection, req)

    class GuardedHTTPSHandler(HTTPSHandler):
        def https_open(self, req: Any) -> Any:
            if ssl_context is not None:
                return self.do_open(GuardedHTTPSConnection, req, context=ssl_context)
            return self.do_open(GuardedHTTPSConnection, req)

    return [GuardedHTTPHandler(), GuardedHTTPSHandler(context=ssl_context)]
