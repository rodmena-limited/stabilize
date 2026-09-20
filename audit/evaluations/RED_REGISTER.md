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

Their fixes predate 0.26.0, so passing there is correct and expected. That is
the point: for these nine, "green" and "asserts nothing" have produced identical
output on every artifact anyone has run them against. Each needs a known-positive
— an artifact old enough to carry its defect, or a deliberate local mutation —
before its green means anything.

## RED OBSERVED — a full transition on one artifact

    probe_signal_storage_degrades

Recorded 2026-09-20. This is the first probe in this suite whose red was
observed against a defect **this repo shipped**, rather than against an older
published release.

`supports_signal_storage()` latched False permanently (0.28.2). Case E was added
to ask the question cases A-D never asked: A-D use two DIFFERENT roles, so they
test the BLOCK direction twice and call it "both directions". Case E takes ONE
store through the whole cycle.

    before=False        blocked
    CONTROL             the role demonstrably regained read access
    immediate=False     the cooldown holds (no re-probe storm)
    after=True          the cooldown expires and storage resumes

    against the latching code   FAIL, 1 of 7
    against the fix             PASS, 8 checks

The control is what makes the red mean anything: without it, `after=False` is
equally consistent with a GRANT that never landed.

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

## RED OBSERVED — probe_dsn_ssl_params, on 0.25.2

Recorded 2026-09-20, prompted by trace-thinkpad-83589d.

This probe sat in NEVER OBSERVED RED and I inferred from that entry that it had
no negative case. It has one. The entry describes the RUNS, not the CASES — it
had only ever been pointed at 0.26.0 and 0.27.0, and the fix landed in 0.26.0,
so both were supposed to pass. Green on two artifacts that both contain the fix
is the same output a probe asserting nothing produces.

Pointed at 0.25.2, which carries the defect:

    STATIC   sslmode, sslrootcert, sslcert, sslkey, application_name -> ALL DROPPED
             CONTROL: a plain URL gains no parameters it never had  -> none
    LIVE     CONTROL A: no sslmode          -> reachable: True
             sslmode=require (must FAIL)    -> CONNECTED  >>> the defect
             CONTROL B: sslmode=disable     -> reachable: True
    RESULT   6 CHECK(S) FAILED, exit 1

Both live controls held, so the red is the defect and not an unreachable server.

Two things the run settled that neither party knew:

- The peer's fallback hypothesis -- that `sslmode` might survive while only the
  file-path parameters were lost -- is **falsified**. The whole set goes,
  `sslmode` included. Their original report stands exactly as filed.
- `application_name` is dropped too, so the defect was never TLS-specific: 0.25.2
  discarded **every** query parameter. The TLS ones were simply the ones that
  hurt.

The construction worth copying, in the peer's words: this negative case does not
detect "TLS was dropped" directly, it detects that **a refusal it demanded did
not happen**. It tests the consequence rather than the mechanism, which is why
it is portable to servers that do not mandate TLS.

## The mode upstream of all four

    A CHECK THAT EXISTS BUT IS NOT EXECUTED REPORTS NOTHING WHILE LOOKING LIKE
    COVERAGE.

The four modes above all describe a check that RUNS and whose result misleads.
This one is upstream of them: there is no result to mislead, and the only trace
is a filename in a directory listing that a human counts as reassurance.

Measured, two codebases in one night:

    this suite     4 of 30 probes sat outside run_all.sh, including the #16
                   probe and the only one that runs as a least-privilege role
                   -- which is what caught the 0.28.1 regression once it ran
    a peer         33 of 55 probes returned 401 on their first call, because a
                   tenant-membership fixture was never created

In both cases the artefact that made it invisible was a HAND-MAINTAINED LIST
standing beside a directory that already stated the truth. The remedy is the
same in both: derive the list, and require a written reason to exclude.

## An assertion the failure mode itself skips

Contributed by trace-thinkpad-83589d, who hit it in their own repo while
auditing it:

    if response.status_code < 400:
        ...the assertion...

A 500 is not < 400, so the assertion was **skipped for every server error** --
precisely the case it existed to catch. The route was answering 500 from an
unhandled constraint violation and the probe reported PASS on every run. It was
found in an API log, because an operator could not sign in; the suite was green
throughout.

    AN ASSERTION GUARDED BY A CONDITION THAT THE FAILURE MAKES FALSE
    IS AN ASSERTION THAT NEVER RUNS WHEN IT MATTERS.

This shape is greppable and is present in this repo:
`tests/test_failure_scenarios.py:275-282` nests its assertion under
`if moved == 0:` and then `if row:`. That one is not vacuous overall -- an
unconditional `assert moved == 1` follows it -- but the inner assertion is dead
unless the code is already broken, which is the same construction one step from
harm.

Distinct from "never goes red": the check *can* go red, and does on the happy
path. It is the failure path that silently skips it.

## The general remedy the four share

    A ZERO IS EVIDENCE ONLY WHEN SOMETHING IN THE SAME QUERY IS NON-ZERO.

`workflow_signals` reading 0 rows is indistinguishable from "the table is
unreachable and every write has been silently degrading" -- which is a real
defect this repo shipped. Pairing it with `processed_messages` at 8,482 in the
same run rules that out: the machinery is writing, and this table specifically
has never been asked for anything.

This is not a fifth failure mode. It is how to catch three of the four, so it is
filed with them rather than among them.

## When two checks agree

Distinct from the four, because it is about the relationship between two
instruments rather than a fault in either:

    AGREEMENT BETWEEN TWO CHECKS IS EVIDENCE ONLY WHEN SOMETHING ESTABLISHES
    THEY READ THE SAME ARTIFACT.

Two parties confirmed that 0.27.0 carries no PostgreSQL signal store -- one
against the wheel downloaded from PyPI, one against the copy installed in a
running venv. The answers matched. Nothing in either check established that an
installed tree and a published wheel are the same bytes, which is the assumption
the "verify the served artifact" rule exists to forbid.

Closed by a recursive diff of the two trees, with a known-positive in the same
run: appending one comment line made it exit 1; restoring the line made it exit
0. Without that, a diff comparing nothing -- wrong root, an exclude that
swallowed the tree -- also exits 0.

Agreement is treated as the end of an investigation. It is the start of one.

### It recurred in the same session, with a control attached

Four hours after writing the rule above, this repo told a peer not to file a
defect against migretti, citing a measurement with a known-positive control:

    grep ImportError across migretti  ->  NONE
    control: the same grep finds `import psycopg`  ->  the tree is visible

The measurement was sound and the artifact was wrong. It read migretti 0.9.4,
which happened to be in this repo's dev venv. PyPI ships 0.10.0, and
`pip install stabilize==0.28.2` pulls 0.10.0. The peer had independently read
the 0.10.0 source, so between the two the shipped version was covered -- by
accident, because one of us cited the wrong version and the other happened to
read the right one.

Re-run against the published 0.10.0 wheel: also clean. The conclusion held; the
evidence for it did not.

The lesson is narrower than the rule and worse:

    A MEASUREMENT WITH A CONTROL ATTACHED IS MORE PERSUASIVE THAN AN ASSERTION,
    SO BEING WRONG ABOUT ITS SUBJECT PROPAGATES FURTHER.

The control proves the instrument works. It says nothing about what the
instrument was pointed at. Name the artifact and its version in the finding, or
the control is decoration.
