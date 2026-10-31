# #106 — 0.35: HTTPTask response outputs get their own names; request keys are not inherited from upstream responses (planned, tests first)

Ticket: issuedb #106. Reported by Provenance on 2026-10-31 (thread 01M69C1HSRKE32H5JBCMT04X6C): a fan-in HTTP stage after two HTTP stages sent the upstream response's `content-length` of 231552 as its own request header and the server reset the connection ten times. The same happens on 0.30.4, so it is not a regression. An interim note went into `docs/guide/tasks.rst` the same day.

## EARS SPEC
- When an HTTPTask completes, the task shall write its response under `response_status`, `response_headers`, `response_body` and `response_body_json`, and shall keep writing `status_code`, `headers`, `body` and `body_json` until 2.0.0 (deprecated, documented).
- When an HTTPTask builds its request, the task shall take `headers` and `body` only from keys the stage declared itself, never from keys hydrated from ancestor outputs, so a downstream HTTP stage does not send an upstream response's headers as its own.
- The engine shall document in `docs/guide/tasks.rst` and `docs/guide/data_flow.rst` which HTTPTask keys are request inputs and which are response outputs.
- A test shall run two HTTP stages in sequence against the local HTTP server used by `tests/test_http_task.py` and assert the second request carries none of the first response's headers; it shall be seen failing before the change.

## TECHNICAL PROBLEMS
1. Name collision between a task's inputs and outputs under context hydration (`StartStageHandler` merges ancestor outputs into the stage context).
2. Telling declared keys from hydrated keys inside the task: the stage's own declared context exists on `StageExecution` before hydration, or the merge can record provenance.

## SOLUTION DOMAINS
- The codebase's data-flow design (`get_merged_ancestor_outputs`, `docs/guide/data_flow.rst`) and HTTPTask (`src/stabilize/tasks/http/request.py::build_request`, `response.py` outputs).

## ALTERNATIVES
- Distinct response output names plus request keys read from declared context only [CHOSEN: additive for one minor, closes the trap for every consumer].
- Docs only [REJECTED for 0.35: the trap stays; done now as the interim note].
- Stop hydrating ancestor outputs into context [REJECTED: the documented data-flow contract that every consumer relies on].
