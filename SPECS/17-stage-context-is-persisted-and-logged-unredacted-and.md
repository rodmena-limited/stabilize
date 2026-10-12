# 17 — Stage context is persisted and logged unredacted, and HTTPTask's contract invites credentials into it

Ticket: issuedb #17 · status wont-do · priority high

EARS SPEC (draft — needs synthesis before implementation):
- Where a stage context key is declared secret, the engine shall persist a redacted placeholder rather than the value, or shall persist the value encrypted.
- If a stage context carries a declared secret, then the engine shall never emit that value in a log record, an event payload, or a database bind parameter that a statement log would capture.
- The engine shall document, in the task contracts that accept credentials, where the value is persisted and who can read it.

TECHNICAL PROBLEMS:
1. Stage context is persisted verbatim: persistence/postgres/converters.py:27 does json.dumps(execution.context) with no redaction, and the sqlite path is equivalent. Anything a caller puts in context is at rest in plaintext JSONB.
2. The whole context is sent as a bind parameter on every stage state transition, so a PostgreSQL statement log captures it. This is not hypothetical: the same logging that caused ticket #15's disk incident wrote those values into a 9.9 GB log file on a host shared by six databases.
3. Stabilize's OWN documented contract invites credentials in. tasks/http/task.py:131 documents 'bearer_token (str): Bearer token for Authorization header' as a context key, with a worked example at line 188 passing a literal token. A caller following our documentation puts a credential into a column we persist unredacted and log.
4. The only masking that exists (tasks/http/utils.py:30 mask_secrets) is opt-in, applies to a logged URL string only, and does nothing at persist time.

EVIDENCE FROM THE FIELD: ci-conductor-dd94c8 found DB_PASSWORD, RUNFLOW_MASTER_KEY, AUTH_CLIENT_KEY, DATABASE_URL and TEST_DATABASE_URL live in plaintext in context->env on 13 stage rows belonging to rodmena-limited/RunFlow, unexamined for six days. They reported it to runflow-3858c4 and infra-manager. The rotation decision is RunFlow's; the reason a credential in stage context is durably stored and logged at all is ours.

NOT IN SCOPE OF #15: the prune-signals cleanup strips _buffered_signals and deliberately leaves env untouched. Nothing in 0.22.0 addresses or resolves this, and nobody should assume it does.

SOLUTION DOMAINS (to be developed): secret references vs secret values (store a handle, resolve at execution); envelope encryption at the persistence boundary; a declared-secret key list on the stage with redaction in converters and in event payloads. Note the house prescription: credentials belong behind a resolver, not in a workflow document.

ALTERNATIVES: to be recorded when this ticket starts. At minimum: redact-on-persist vs encrypt-on-persist vs reference-only (refuse to accept raw secrets in context at all, which is a breaking change to HTTPTask's documented contract and therefore needs a deprecation path).

## Progress, evidence and decisions

RETRACTION OF THE 'EVIDENCE FROM THE FIELD' PARAGRAPH IN THE DESCRIPTION.

That paragraph claimed live credentials (DB_PASSWORD, RUNFLOW_MASTER_KEY, AUTH_CLIENT_KEY, DATABASE_URL, TEST_DATABASE_URL) sat in plaintext in context->env on 13 RunFlow rows. THAT IS WRONG and must not be cited. ci-conductor-dd94c8 retracted the alarm after verifying with different predicates than the ones that raised it:

  RUNFLOW_MASTER_KEY  53 rows, 1 distinct value, one character repeated 64 times
  DB_PASSWORD         53 rows, literal string 'runflow'
  AUTH_CLIENT_KEY     53 rows, nil-UUID shape
  DATABASE_URL        67 rows, loopback 127.0.0.1 with ci:ci
  TEST_DATABASE_URL   14 rows, loopback

RunFlow's test-suite environment. Their placeholder regex ^(ci-|test-|dummy|placeholder|changeme|x+$) does not match a 64-char run of a repeated character unless that character is 'x', so it could not distinguish 'not a placeholder' from 'not a placeholder pattern I thought of'. infra-manager's original all-clear was correct; this project relayed a contradicting claim without verifying it.

This ticket's ACTUAL basis is unaffected, because every item was found in this repo's own source rather than in anyone's data:
  - persistence/postgres/converters.py:27 persists stage context verbatim, no redaction boundary; sqlite equivalent
  - the whole context is a bind parameter on every stage state transition, so a statement log captures it
  - tasks/http/task.py:131 DOCUMENTS bearer_token as a stage context key, worked literal-token example at line 188
  - tasks/http/utils.py:30 mask_secrets is opt-in, one URL string, nothing at persist time

CLAIM RESTATED, per ci-conductor's framing: this ticket is 'nothing stops a credential being persisted and logged', NOT 'credentials were exposed'. The fourth bullet is the core of it and is entirely this project's own: a user who follows our documentation exactly puts a credential into a column we persist unredacted and log in full. That is worth fixing on its own merits with no incident attached, and the priority stays high.

PROCESS NOTE for whoever picks this up: do not reintroduce the field-evidence framing. The gap is demonstrable from the source and the documented contract; it needs no incident, and attaching a retracted one would make the ticket easy to dismiss.

SCOPE EXPANDED — a third destination this project had not counted, from infra-manager-c13110.

Stage context written to stage_executions.context reaches THREE destinations from one write, not two:
  1. the column itself (plaintext JSONB, no redaction boundary)
  2. the PostgreSQL statement log, as a bind parameter on every state transition
  3. pgBackRest backup sets in S3

Destination 3 was missed here because it is invisible from inside the engine. It is also the worst of the three: infra's log_parameter_max_length=2048 cap bounds new log lines and reaches nothing already written to a backup set, so a credential that lands there has a retention period rather than a fix.

CONSEQUENCE FOR THE SOLUTION SHAPE: redaction must happen BEFORE the value is written to the column. A logging-layer or serialisation-layer control is insufficient, because the downstream destinations are plural, partly outside this engine's view, and some are durable and remote. This rules out the 'mask at log time' family of alternatives before the synthesis starts.

ALSO VERIFIED: there is no pre-persist hook a caller could use to redact for themselves. No pre-persist callback, no serializer override, no context filter, no on_persist/before_save anywhere; converters.py:27 calls json.dumps unconditionally and store_stage() takes (stage, expected_phase, connection) with no interception point. So a downstream platform cannot work around this from their side — their only options are to not use the documented contract, or to pre-redact and lose the value for execution. The absence of a redaction boundary is this project's, not its callers'.

infra-manager's framing, kept verbatim because it states the risk better than the original ticket did: 'Today those env blocks hold placeholders. The day a deploy job runs the same path with a real key, they will not, and the value will be in the column, the log and the backups simultaneously.'

OWNERSHIP SPLIT AGREED with infra-manager-c13110, after they retracted 'that is ci's to fix by redacting at persistence'. Their reasoning for why the retraction mattered is worth keeping: assigning work to a party that has no mechanism to perform it produces a ticket nobody can close, which then gets reframed as neglect.

  Stabilize  owns PROVIDING the mechanism — a redaction/filter hook at the point context is persisted, so a caller CAN keep a secret out of the column.
  callers    own USING it once it exists, and meanwhile own not putting real secrets in a context block persisted verbatim.
  infra      owns the log-line cap, which bounds blast radius and does nothing about the column.

DESIGN CONSTRAINT from infra, to be carried into the synthesis rather than discovered later: THE HOOK MUST FAIL CLOSED. A redaction filter that silently does nothing when misconfigured produces exactly today's situation except with everyone believing it is handled. If a caller declares a key sensitive and the filter cannot apply, REFUSING THE WRITE is better than persisting unredacted — the first is visible, the second is invisible for months. This eliminates any 'best-effort redaction' alternative before it is proposed.

STATUS OF THE LATENT RISK — both peers closed their scope caveats and the result is negative, checked rather than assumed:
  - infra scanned the 09-05/06 PostgreSQL log: 379,304 lines, 381 env blocks, 16 distinct key names ENUMERATED rather than pattern-matched; every credential-suggesting value characterises as placeholder or loopback, DB_PASSWORD settled by equality against a literal string rather than by format. Only one repo ran in that window.
  - ci scanned the whole database: 815 stage rows, 240 with env, 4 repos, 20 distinct key names; picture agrees.
  - the log expires at 03:15 under normal retention and is deliberately NOT being retained, on the argument that keeping a 1 GB log because it might hold credentials is the opposite of the remedy.

So this ticket is correctly framed as LATENT: nothing was realised, and the mechanism that makes it latent is real. Per infra: 'the value must not be stored, because once stored it cannot be recalled' — the third destination (pgBackRest) gives a stored credential a retention period rather than a fix, outliving every mitigation applicable after the fact. That is the argument to lead with.

ATTEMPTED AND REVERTED — persist-time redaction is incompatible with execution, measured not reasoned.

Implemented redact-on-persist honouring the existing documented 'secrets' declaration (schema-validated, documented on HTTPTask and ShellTask) at all nine json.dumps(context) sites across both backends, with STABILIZE_PERSIST_SECRETS=1 as opt-out.

Decisive control, real workflow, ShellTask substituting {token}:
  STABILIZE_PERSIST_SECRETS=1 (no redaction) -> task printed MATCH
  redaction on (default)                     -> task printed MISMATCH

Handlers re-read stage state from the store DURING execution, so redacting at persist does not merely lose the secret on restart: it starves the task that is running. The workflow still reported SUCCEEDED, which is worse — the credential silently becomes the literal string ***REDACTED***.

Reverted in full.

CONSTRAINT any fix must satisfy: the running task must still read the real value while it is absent from stage_executions.context. That rules out redact-on-persist and points at the ticket's own third solution domain — a secret REFERENCE resolved at execution time — which is a contract change to HTTPTask's documented bearer_token and needs a deprecation path. Operator decision, not a patch release.

Encrypt-on-persist not attempted: satisfies both the clause and the constraint but needs key management the engine does not have.

Harness kept: audit/evaluations/probe_secret_redaction.py. It ASSERTS the constraint (fails if a fix reintroduces starvation) and REPORTS the leak as the known-open finding, so it does not go red every run for a defect that is open by decision. Current output: CONSTRAINT PASS; FINDING reproduces in stage_executions.context.

Also measured: scanning the raw SQLite file finds the secret in freed pages even when no live row holds it. That is data remanence (secure_delete/VACUUM), a different claim with a different fix, and the probe deliberately queries live rows only rather than conflating the two.

SYNTHESIS IS COMPLETE. This ticket is not blocked on analysis; it is blocked on ONE OPERATOR DECISION, and it should be labelled that way rather than sitting as "needs synthesis before implementation".

WHAT a9eab81 ALREADY ESTABLISHED, by measurement rather than reasoning. The smallest reading of the first EARS clause - honour the documented `secrets` declaration at the persistence boundary - was implemented at all nine json.dumps(context) sites across both backends, then REVERTED on this result:

    STABILIZE_PERSIST_SECRETS=1 (no redaction) -> task printed MATCH
    redaction on (default)                     -> task printed MISMATCH

Handlers re-read stage state from the store DURING execution, so redacting at persist starves the RUNNING task, not merely one resumed after a restart. Worse, the workflow still reported SUCCEEDED: the credential silently becomes the literal string ***REDACTED*** and fails at the far end, which is a silent-corruption outcome rather than a loud one.

THE CONSTRAINT ANY FIX MUST MEET, therefore: the running task must read the REAL value while that value is ABSENT from stage_executions.context. Redact-on-persist cannot satisfy both halves. What can is a SECRET REFERENCE resolved at execution time - context carries a handle, the engine resolves it when the task runs, and the plaintext never enters the column or the bind parameter.

WHY THAT IS NOT MINE TO TAKE: it is a breaking contract change to a DOCUMENTED public API. tasks/http/task.py:131 documents `bearer_token (str): Bearer token for Authorization header` as a context key, with a worked example at :188 passing a literal token. Every caller following our own documentation today passes a plaintext credential. Moving to a reference means:
  - the documented type changes from a string to a handle, or a second key appears and the plain one is deprecated;
  - a resolution mechanism has to exist and be chosen (environment, a secrets service, a callback);
  - existing pipelines that pass literals either keep working with the leak, or break.

That is a product decision about a public contract with an estate-wide consumer base, not an implementation detail an agent picks.

STATUS CORRECTION: the first EARS clause as drafted ("persist a redacted placeholder rather than the value") is NOT IMPLEMENTABLE as written, and the measurement above is why. It must be rewritten around execution-time resolution before implementation starts, which is itself part of the decision.

WHAT IS SAFE TO DO WITHOUT THE DECISION, and is not yet done:
  - the third EARS clause, purely documentary: state plainly in HTTPTask's contract and in the guide that a value placed in context is persisted verbatim in stage_executions.context, is sent as a bind parameter on every state transition, and is readable by anyone with SELECT on that table. Callers can then decide for themselves, today, with no engine change.
That clause closes no leak but it stops the documentation inviting credentials into a column we persist unredacted - which is technical problem 3 on this ticket, and the only part of the defect stabilize created by itself.

RECOMMENDATION: split the ticket. Land the documentary clause now; hold the storage change for the operator's decision on the contract.

OPERATOR DECISION, 2026-09-20: KEEP THE LEAK. CLOSING AS WONT-DO.

Farshid's ruling, verbatim: "keep the leak for now. it's orchestrator, if you log a password, you shouldn't hold the logger responsible."

THE POSITION, as I understand it: stabilize persists what a caller hands it. stage_executions.context is a faithful record of the workflow definition, and a caller who places a live credential in a workflow definition owns that choice. The orchestrator is not the trust boundary for the caller's secrets, in the same way a logger is not responsible for a password someone chose to log.

WHAT THIS DECIDES AND WHAT IT DOES NOT:
- DECIDED: no redaction at the persistence boundary, and no execution-time secret reference. The 10 json.dumps(context) sites stay as they are. HTTPTask's documented bearer_token (str) contract is unchanged, so no consumer breaks and no deprecation cycle starts.
- NOT A DEFECT ANY MORE: this ticket should not be re-raised as a finding by a future audit of this repo. It is a deliberate scope boundary, not an oversight. An auditor who rediscovers "context is persisted unredacted" should be pointed here.

WHY THE DECISION IS SOUND RATHER THAN MERELY CONVENIENT, recorded so the reasoning survives: the only design that satisfies the constraint measured at a9eab81 - the running task must read the real value while it is absent from the column - is an execution-time secret reference. That is a breaking change to a public contract with estate-wide consumers, bought to protect callers from a choice they made themselves. The cost lands on every consumer; the benefit lands on the ones who ignored where their credentials go. That trade is not obviously worth taking, and the operator has judged it is not.

WHAT REMAINS TRUE AND IS WORTH KNOWING, not as a reopening but as fact:
- context is plaintext JSONB at rest, and is a bind parameter on every stage state transition, so a statement log captures it. The #15 disk incident put those values in a 9.9 GB log on a host shared by six databases.
- mask_secrets (tasks/http/utils.py:30) covers LOGGED TEXT only and does nothing at persist time. The `secrets` context key is therefore a log-masking declaration, not a storage declaration, and anyone reading it as the latter is mistaken.
- stabilize's own HTTPTask docs at task.py:179 and the worked example at :236 recommend passing a literal bearer_token. Under this decision that documentation is accurate about behaviour; whether it should also state where the value ends up was offered and deferred ("for now"). Not doing it.

STATUS: wont-do. Reopen only if the operator revisits the contract question, not on rediscovery of the leak itself.
