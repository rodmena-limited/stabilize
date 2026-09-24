# #59 — Upstream error bodies never carry the credential we sent

Ticket: issuedb #59. Released in 0.30.4. Reported by vellum-build-d8bbd2.

## EARS spec

- If an upstream service answers with an error, then the LLM client and the Highway task shall not include any credential they sent in the raised error, the stored task error, or the log line.
- Credential-shaped fields in upstream text (`authorization`, `x-api-key`, `api_key`, `access_token`, `secret`) shall be masked, whoever they belong to.
- An upstream diagnostic that contains no credential shall be kept, capped at 500 characters.

## Measured on 0.30.3

A local server that returns 401 and echoes the request's `Authorization` header in its body:

- `stabilize.llm.LLMClient`: `LLM request failed (401): {..."Authorization": "Bearer <key>"}`. The key was in `str(LLMError)` and in the traceback.
- `tasks/highway`: the response body was written verbatim into `logger.error` and into `TaskResult.terminal(error=...)`, which is persisted in the stage row.

## Alternatives

- **Scrub what we sent, then mask credential-shaped fields, then cap [CHOSEN].** This process knows exactly which secret it sent, so removing it is precise, and the upstream's own diagnostic ("model not found", "unauthorized") survives.
  - vs dropping the upstream body [REJECTED: loses the diagnostic an operator needs].
  - vs masking credential fields only [REJECTED: an upstream can reflect the key in any field, as `api_key_seen` in the test shows].
- **HTTPTask is not changed.** Its error message is `HTTP <status>`, and the response body is an explicit stage output by contract. A caller who points it at an endpoint that reflects credentials stores what they asked for.

## Verification

`tests/test_upstream_error_redaction.py`: 9 cases, including a control showing the echo server really returns the key.
- Against the 0.30.3 LLM client and Highway task, 3 fail: the LLM error, Highway submit and Highway poll.
- With the fix, all 9 pass.
- The existing Highway and LLM test files pass (32 tests).

## Reverted in 0.31.0 (operator decision)

stabilize is an orchestrator. It does not apply controls on the user's behalf to what an upstream publishes: whatever an upstream puts in its error response reaches the caller unchanged. `LLMClient` and `HighwayTask` are restored to their 0.30.3 behaviour (the body verbatim, `LLMError` chained to the `HTTPError`), and `redact_upstream_text` and its tests are removed.

Checked after the revert: the echo server's body, including the reflected header, appears unchanged in `LLMError`, and the Highway and LLM tests pass (23).

Consequence for callers: if an upstream reflects request credentials in an error body, that text is in the exception, the Highway log line and the stored task error, exactly as the upstream sent it. Handling that belongs to the caller, for example by not surfacing raw exception text to end users.

Unaffected: the DSN-password protections (#53, #56, #57, #58). Those cover stabilize's own logging of the caller's own database password, not upstream content.
