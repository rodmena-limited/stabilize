"""Redaction helpers for values that must never reach logs or stored state."""

from __future__ import annotations

import re

_KV_SECRET_RE = re.compile(r"(?i)\b(password|passfile|sslpassword)\s*=\s*('(?:[^'\\]|\\.)*'|\S*)")

_ECHO_LIMIT = 200


def redact_userinfo(url: str) -> str:
    """Return *url* with any userinfo password replaced by ``***``.

    Splits on the LAST ``@`` so that a password containing ``@`` or ``/`` --
    neither of which a malformed URL is obliged to percent-encode -- is
    covered rather than partially echoed.
    """
    scheme, separator, rest = url.partition("://")
    if not separator:
        rest = url

    userinfo, at_sign, hostpart = rest.rpartition("@")
    if not at_sign:
        return url

    user, colon, _password = userinfo.partition(":")
    if not colon:
        return url

    prefix = f"{scheme}://" if separator else ""
    return f"{prefix}{user}:***@{hostpart}"


def _redact_bare_userinfo(url: str) -> str:
    """Redact ``scheme://user:secret`` where the ``@host`` is missing.

    A DSN typed without its host still carries a password, and the echo path
    exists precisely for DSNs that failed to parse. A colon whose right-hand
    side is not a port number is treated as a password.
    """
    scheme, separator, rest = url.partition("://")
    if not separator:
        rest = url
    if "@" in rest:
        return url

    authority, slash, tail = rest.partition("/")
    head, colon, candidate = authority.partition(":")
    if not colon or not candidate or candidate.isdigit():
        return url

    prefix = f"{scheme}://" if separator else ""
    return f"{prefix}{head}:***{slash}{tail}"


def redact_db_url(url: str) -> str:
    """Return *url* safe to echo in an error message.

    Covers every DSN form an operator may supply: ``postgres://`` userinfo,
    a host-less ``user:password``, and libpq keyword/value secrets. Control
    characters are escaped so a crafted value cannot forge a second log line,
    and the result is length-capped.
    """
    redacted = _KV_SECRET_RE.sub(r"\1=***", _redact_bare_userinfo(redact_userinfo(url)))
    redacted = redacted.encode("unicode_escape").decode("ascii")
    if len(redacted) > _ECHO_LIMIT:
        redacted = f"{redacted[:_ECHO_LIMIT]}... (truncated)"
    return redacted


_URL_TOKEN_RE = re.compile(r"[^\s\"']*://[^\s\"']+")


def redact_text(text: str) -> str:
    """Return *text* with the password of every DSN it quotes, and every libpq keyword secret, replaced by ``***``.

    For messages that may quote a DSN back, such as a psycopg error raised
    while parsing a connection string. Only the URLs inside the text are
    rewritten; the rest of the message is kept. Control characters are
    escaped; the length is not capped.
    """
    redacted = _URL_TOKEN_RE.sub(lambda m: _redact_bare_userinfo(redact_userinfo(m.group(0))), text)
    redacted = _KV_SECRET_RE.sub(r"\1=***", redacted)
    return redacted.encode("unicode_escape").decode("ascii")
