# 60 — An undecodable queue message is quarantined, never raised to the poller

Ticket: #60 (released in 0.31.0)

## EARS spec

- If a polled queue row cannot be decoded (unknown message type, unknown enum name, field contract failure, payload that is not a JSON object), then `poll_one` shall move it to the DLQ with `error` = `Deserialization failed: <type>: <cause>` and return `None`, on PostgreSQL and SQLite.
- `poll_one` shall not raise to its caller for a decode failure, and polling shall continue with the next message.
- The DLQ entry shall keep the payload exactly as stored, and `replay_dlq` shall put it back on the queue whatever JSON it holds.
- The move to the DLQ shall not borrow a second pool connection while `poll_one` holds one.
- stabilize shall not retry, drop or rewrite such a message on the operator's behalf; inspection and replay stay with the operator.

## Synthesis

- Technical problem: poison-message handling at the consumer boundary.
- Solution domain: messaging error handling. NServiceBus moves messages that fail deserialization to the error queue without retries; Spring Kafka's `DefaultErrorHandler` treats `DeserializationException` as not retryable and dead-letters it; MassTransit routes messages it cannot consume to `_error` / `_skipped`. Shared concept: a deterministic decode failure is not retried, the consumer keeps running, the message is quarantined losslessly with its reason, and an operator replays or discards it.
- Alternatives:
  - Quarantine immediately, lossless, replayable [CHOSEN]: neutral; the consumer survives and the decision stays with the operator.
  - Release for another worker and count attempts [REJECTED]: retries a deterministic failure `max_attempts` times and still dead-letters it; helps only in a mixed-version window, which `replay_dlq` covers explicitly.
  - Keep raising [REJECTED]: crashes synchronous embedders on data they did not write.
  - Drop [REJECTED]: data loss.

## Changes

- `stabilize/queue/decode.py`: one decoder for both backends; any failure becomes `MessageDecodeError` with a cause naming the type and error, no payload content.
- PostgreSQL poll returns `payload::text`, so both backends decode JSON text identically.
- `dlq.move_to_dlq` and `dlq.replay_dlq` (PostgreSQL) move the row server-side in one statement (`DELETE ... RETURNING` feeding `INSERT ... SELECT`). Before, the payload made a round trip through Python, and a non-object payload (an array became `smallint[]`) could not be dead-lettered or replayed.
- `stabilize.queue.sqlite.deserialize_message` returns `None` for every decode failure, as documented.

## Evidence

`tests/test_poison_message_quarantine.py`, both backends, 13 cases: five poison shapes, each followed by a valid message that must still be delivered, then replayed and quarantined again; `process_all` over a poison row; a type unknown to this version that replays once it is registered.

- 0.30.4 code: 13 failed (`poll_one` raises).
- 0.30.4 `dlq.py` with the new poll: 2 failed, the array and string payloads, `DatatypeMismatch ... smallint[]`.
- 0.31.0: all pass.

Invalid JSON cannot be stored on either backend (jsonb on PostgreSQL; the SQLite queue's `json_extract` expression index rejects it on insert), so that case is not reachable.
