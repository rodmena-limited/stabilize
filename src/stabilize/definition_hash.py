"""The definition hash: a SHA-256 over one canonical JSON form of a pipeline configuration.

Algorithm ``stabilize-canonical-json/1``. Keys are strings sorted by code point; separators
are compact; booleans are written before integers are considered; integers are exact;
floats are ``repr`` with ``-0.0`` written as ``0.0``; strings keep their characters
(``ensure_ascii=False``) and a lone surrogate makes the value unhashable; an ``Enum`` is its
value; an aware ``datetime`` is ISO 8601 in UTC and a naive one is unhashable; ``Decimal``,
``UUID`` and ``bytes`` are tagged ``{"$type": ..., "$value": ...}`` and a real dict carrying a
``$type`` key is wrapped the same way; tuples are lists; sets are sorted by their canonical
encoding. ``NaN``, infinities, non-string keys and unknown types are unhashable. An
unhashable configuration yields no hash and a warning, never a refusal.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

HASH_ALGORITHM = "stabilize-canonical-json/1"


class UnhashableError(ValueError):
    """The configuration holds a value the canonical form does not define."""


def _canonical(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, Enum):
        return _canonical(value.value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise UnhashableError(f"float {value!r} has no canonical form")
        return 0.0 if value == 0.0 else value
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise UnhashableError("string holds a lone surrogate") from exc
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise UnhashableError("naive datetime has no canonical instant")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        return {"$type": "decimal", "$value": str(value)}
    if isinstance(value, uuid.UUID):
        return {"$type": "uuid", "$value": str(value)}
    if isinstance(value, bytes | bytearray | memoryview):
        return {"$type": "bytes", "$value": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, dict):
        items: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise UnhashableError(f"key {key!r} is not a string")
            items[key] = _canonical(item)
        if "$type" in items:
            return {"$type": "dict", "$value": items}
        return items
    if isinstance(value, list | tuple):
        return [_canonical(item) for item in value]
    if isinstance(value, set | frozenset):
        encoded = sorted(_dumps(_canonical(item)) for item in value)
        return [json.loads(text) for text in encoded]
    raise UnhashableError(f"{type(value).__name__} has no canonical form")


def _dumps(canonical: Any) -> str:
    return json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def canonical_json(config: Any) -> bytes:
    """The canonical form of *config* as UTF-8 bytes; raises UnhashableError when there is none."""
    return _dumps(_canonical(config)).encode("utf-8")


def definition_hash(config: Any) -> tuple[str | None, str]:
    """The SHA-256 hex digest of the canonical form and the algorithm name; the digest is None when there is none."""
    try:
        digest = hashlib.sha256(canonical_json(config)).hexdigest()
    except UnhashableError as exc:
        logger.warning("Pipeline configuration not hashed (%s): %s", HASH_ALGORITHM, exc)
        return None, HASH_ALGORITHM
    return digest, HASH_ALGORITHM
