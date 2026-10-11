# Spec: HTTPTask SSRF DNS-rebinding TOCTOU (#3)

## EARS requirements

- While allow_private_urls is False, HTTPTask shall refuse any connection whose
  connect-time peer address is private, loopback, link-local, or otherwise
  blocked, regardless of what DNS answered during validation.
- A refusal shall return a terminal TaskResult, not escape execute() as an
  unhandled exception.
- An SSRF refusal shall not be retried, since a retry re-resolves the name.
- The fix shall include a regression test that fails against hostname-only
  revalidation.

## Design

Checking the LIVE PEER rather than pinning a pre-resolved IP. Guarded
HTTPConnection/HTTPSConnection subclasses call getpeername() after connect and
before any request bytes are written, and close + raise BlockedPeerError when
the address is blocked. The connection is still made by hostname, so TLS
hostname verification and SNI are unaffected — which naive IP pinning breaks.

BlockedPeerError subclasses OSError so urllib's existing error handling applies.

## Second defect found while building the probe

HTTPTask revalidates the URL immediately before _opener.open() (two sites: the
retry path and the single-attempt path). That revalidation raises ValueError,
and neither except clause caught ValueError — so a rebinding attempt escaped
execute() as an UNHANDLED EXCEPTION rather than returning a terminal result. A
blocked request crashed the task. Both sites now catch it, and should_retry()
refuses to retry an SSRF refusal.

## Verified live (audit/evaluations/probe_ssrf_rebinding.py)

A real local HTTP server on loopback, with a stateful resolver:

    lookups 1-2 -> 93.184.216.34  (passes the pre-flight AND the revalidation)
    lookup 3    -> 127.0.0.1      (the connect, i.e. the rebind)

    result: TERMINAL, "connected to blocked address 127.0.0.1
            (the name resolved differently at connect time than at validation)"
    the private server's body never reached the task

Controls that make the above non-vacuous: plain loopback is blocked, and
allow_private_urls=True still reads the same server.

The probe asserts it made at least 3 lookups and fails itself as PROBE INVALID
otherwise. Two earlier drafts passed vacuously — the first patched one module
object twice so the rebinding never happened, the second rebound at lookup 2 so
the pre-connect revalidator caught it and the connect-time guard was never
reached. Both reported success before the assertion was added.
