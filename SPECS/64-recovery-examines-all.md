# 64 — Recovery examines every pending workflow in its window

Ticket: #64 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- When recovery runs, it shall examine every NOT_STARTED and RUNNING workflow inside the recovery window, not only the newest batch_size.
- `retrieve_by_application` and `retrieve_by_pipeline_config_id` shall honour `start_time_after` / `start_time_before` (NULL start_time counts as inside).

## Synthesis

- Solution domain: keyset/unbounded streaming of candidate ids (ids only, workflows loaded one at a time).
- Alternatives: uncapped id query + streaming [CHOSEN] vs OFFSET pagination [REJECTED: rows move between pages as recovery changes their state] vs raising batch_size [REJECTED: still a cap].

## Evidence

`probe_recovery_examines_all.py`: 0.31.0 recovered 99-100 of 150 on both backends, and the sqlite application path examined a 48 h-old workflow; fixed 150/150, window honoured. `tests/test_recovery_examines_all.py`.
