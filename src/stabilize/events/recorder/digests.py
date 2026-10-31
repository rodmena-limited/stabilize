"""What a context or outputs event carries about changed values: their names, and under an audit key their HMACs."""

from __future__ import annotations

import hashlib
import hmac
import os
from typing import Any

from stabilize.definition_hash import UnhashableError, canonical_json

DIGEST_ALG = "hmac-sha256/stabilize-canonical-json/1"
AUDIT_KEY_ENV = "STABILIZE_AUDIT_KEY"


def resolve_audit_key(explicit: bytes | None, event_store: Any) -> bytes | None:
    """The audit key in force: the one given, else the event store's, else the environment's UTF-8 bytes."""
    if explicit is not None:
        return explicit
    from_store = getattr(event_store, "audit_key", None)
    if isinstance(from_store, bytes) and from_store:
        return from_store
    raw = os.environ.get(AUDIT_KEY_ENV, "")
    return raw.encode("utf-8") if raw else None


def digest_values(key: bytes, values: dict[str, Any]) -> dict[str, str | None]:
    digests: dict[str, str | None] = {}
    for name in sorted(values):
        try:
            encoded = canonical_json(values[name])
        except UnhashableError:
            digests[name] = None
            continue
        digests[name] = hmac.new(key, encoded, hashlib.sha256).hexdigest()
    return digests


def change_payload(changed: dict[str, Any], key: bytes | None) -> dict[str, Any]:
    """The event data for a change: sorted key names, plus digests when an audit key is set."""
    payload: dict[str, Any] = {"changed_keys": sorted(str(name) for name in changed)}
    if key is not None:
        payload["digest_alg"] = DIGEST_ALG
        payload["digests"] = digest_values(key, {str(name): value for name, value in changed.items()})
    return payload
