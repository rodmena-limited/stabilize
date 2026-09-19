"""Audit probe: HTTPTask SSRF guard, tested in BOTH directions.

A guard that blocks everything passes a block-only test while breaking the
product. This probe asserts the deny cases AND the allow cases, so it cannot
go green vacuously.

Run:  python audit/evaluations/probe_ssrf_guard.py
"""

from __future__ import annotations

import sys

from stabilize.tasks.http.task import _validate_url_safety

MUST_BLOCK = [
    ("http://127.0.0.1/x", "IPv4 loopback"),
    ("http://localhost/x", "loopback by name"),
    ("http://169.254.169.254/latest/meta-data/", "cloud metadata"),
    ("http://10.0.0.1/x", "RFC1918 10/8"),
    ("http://192.168.1.1/x", "RFC1918 192.168/16"),
    ("http://172.16.0.1/x", "RFC1918 172.16/12"),
    ("http://[::1]/x", "IPv6 loopback"),
    ("http://0.0.0.0/x", "unspecified, routes to local host"),
    ("http://[::]/x", "IPv6 unspecified"),
    ("http://[::ffff:127.0.0.1]/x", "IPv4-mapped IPv6 loopback"),
    ("http://[::ffff:169.254.169.254]/x", "IPv4-mapped IPv6 metadata"),
    ("http://[::ffff:10.0.0.1]/x", "IPv4-mapped IPv6 RFC1918"),
    ("http://2130706433/x", "decimal-encoded loopback"),
    ("http://0x7f000001/x", "hex-encoded loopback"),
    ("http://127.1/x", "short-form loopback"),
    ("http://100.64.0.1/x", "carrier-grade NAT"),
    ("http://224.0.0.1/x", "multicast"),
    ("http://240.0.0.1/x", "reserved"),
    ("http://[fe80::1]/x", "IPv6 link-local"),
    ("http://[fc00::1]/x", "IPv6 unique-local"),
    ("file:///etc/passwd", "file scheme"),
    ("ftp://example.com/x", "ftp scheme"),
    ("gopher://example.com/x", "gopher scheme"),
    ("http://no-such-host.invalid/x", "unresolvable host must fail closed"),
]

MUST_ALLOW = [
    ("https://example.com/x", "ordinary public host"),
    ("http://example.com/x", "plain http public host"),
    ("https://api.github.com/x", "public API"),
    ("http://93.184.216.34/x", "public literal IPv4"),
]


def main() -> int:
    failures = 0

    print("=" * 72)
    print("DIRECTION 1 — must BLOCK")
    for url, why in MUST_BLOCK:
        try:
            _validate_url_safety(url)
            print(f"  FAIL  not blocked : {url:45} ({why})")
            failures += 1
        except ValueError:
            print(f"  ok    blocked     : {url:45} ({why})")

    print()
    print("DIRECTION 2 — must ALLOW (proves the guard is not blocking everything)")
    allowed_any = False
    for url, why in MUST_ALLOW:
        try:
            _validate_url_safety(url)
            print(f"  ok    allowed     : {url:45} ({why})")
            allowed_any = True
        except ValueError as exc:
            print(f"  FAIL  blocked     : {url:45} ({why}) -> {exc}")
            failures += 1

    if not allowed_any:
        print()
        print("  >>> CONTROL FAILED: nothing was allowed; the block results are vacuous")
        failures += 1

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
