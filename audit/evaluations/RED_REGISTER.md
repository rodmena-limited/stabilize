# Which probes have been OBSERVED going red

A probe that has only ever been green is not evidence. It is indistinguishable
from a probe that asserts nothing. This file records, per probe, whether anyone
has actually watched it FAIL on a known-positive — and it exists because that
claim was once made for the whole suite on the strength of part of it.

The overclaim: a run against `stabilize==0.26.0` produced 8 red and 10 green,
and was reported as "the instruments detect their defects and are not vacuous".
Every number was right. The inference was not. What the run established is that
THOSE EIGHT can go red. It established nothing about the other ten, which
passed on 0.26.0 and passed on 0.27.0 — exactly what a probe asserting nothing
would also do.

A correct measurement supporting a claim wider than itself. The red is sound, so
asking "why did it go red?" does not catch it; only asking "what does this
license me to say?" does.

## RED OBSERVED — failed on 0.26.0, passed on 0.27.0

    probe_stage_message_ownership
    probe_stage_context_rehydration
    probe_event_read_forward_compat
    probe_event_commit_watermark
    probe_event_coverage
    probe_branch_pruning
    probe_multi_merge
    probe_structured_loops

These eight are the suite's only probes with a recorded red-then-green
transition across two published artifacts.

## NEVER OBSERVED RED — passed on both 0.26.0 and 0.27.0

    probe_mg_conninfo
    probe_ssrf_guard
    probe_http_credential_persistence
    probe_circuit_storage_honesty
    probe_pool_options
    probe_ssrf_rebinding
    probe_schema_and_exists
    probe_secret_redaction
    probe_dsn_ssl_params

Their fixes predate 0.26.0, so passing there is correct and expected. That is
the point: for these nine, "green" and "asserts nothing" have produced identical
output on every artifact anyone has run them against. Each needs a known-positive
— an artifact old enough to carry its defect, or a deliberate local mutation —
before its green means anything.

## RED CLAIMED BUT NOT RECORDED HERE

    probe_event_store_no_ddl_by_default
    probe_event_store_ddl_on_construction
    probe_schema_namespace_resolution
    probe_split_namespace
    probe_signal_buffer_storage
    probe_task_lease_fails_closed
    probe_message_contract
    probe_dynamic_multi_instance
    probe_engine_key_census
    probe_multitenant_rls
    probe_multi_instance_cancel_remaining
    probe_buffered_signal_growth
    probe_runtime_role_needs_no_create

Written red-first during the 0.28.0 / 0.28.1 work, and each was seen failing
before its fix landed. That evidence lives in session output rather than in the
repository, so it is recorded here as CLAIMED, not CONFIRMED. Re-establishing it
against a published artifact is the outstanding work.

One of these already demonstrates why the distinction is kept:
`probe_schema_namespace_resolution` was watched going red on the previous
release and read as proof — while an earlier case in the same probe had dropped
`commit_xid` and never restored it, so the case under test failed on a stale
column. A sound-looking red for an unrelated reason. The probe now restores the
column before that case runs.

## The failure modes this register guards against

    1  the control never goes red             no negative case at all
    2  it goes red for the WRONG reason       unrelated; dies to "why?"
    3  it cannot be OBSERVED by the test      e.g. uncommitted DDL read from
                                              a separate connection
    4  it goes red for a NARROWER reason      sound red, oversized claim; dies
                                              only to "what does this license
                                              me to say?"

Modes 2 and 3 were found in this suite. Mode 4 is what this file corrects.
