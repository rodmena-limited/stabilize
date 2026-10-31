"""Attribute a subprocess's coverage to the test that spawned it, named by REDPROOF_TEST_ID."""

from __future__ import annotations

import os

try:
    import coverage
except ImportError:
    coverage = None

if coverage is not None:
    _test_id = os.environ.get("REDPROOF_TEST_ID")
    _cov = coverage.Coverage.current()
    if _cov is not None and _test_id:
        _cov.switch_context(_test_id)
