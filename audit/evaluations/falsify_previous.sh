#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
PYTHON="${PYTHON:?set PYTHON to the interpreter that has the OLD stabilize installed}"

"$PYTHON" -c 'import stabilize, sys; print("stabilize", stabilize.__version__, "from", stabilize.__file__)'

EXPECT=(
    "probe_transient_retry_bounded.py|[FAIL] sqlite always_transient"
    "probe_result_persist_failure.py|[FAIL] sqlite unavailable"
    "probe_recovery_examines_all.py|[FAIL] sqlite all-applications"
    "probe_orchestrator_start_store_failure.py|[FAIL] sqlite A store failure"
    "probe_processor_stop_bounded.py|[FAIL] C LifecycleManager"
    "probe_delete_removes_owned_rows.py|[FAIL] sqlite A X removed"
    "probe_sqlite_dlq_exists.py|[FAIL] A/B docstring setup"
    "probe_sqlite_queue_clock.py|[FAIL] E 1 h retention"
    "probe_requeue_marks_source.py|[FAIL] sqlite poller mark-failure"
    "probe_pg_queue_session_timezone.py|[FAIL] A UTC push"
    "probe_queue_id_capacity.py|[FAIL] A pushes past 2^31"
)

bad=0
for entry in "${EXPECT[@]}"; do
    probe="${entry%%|*}"
    marker="${entry#*|}"
    if [ "$probe" = "probe_queue_id_capacity.py" ] && [ "${AUDIT_ALLOW_DESTRUCTIVE:-}" != "1" ]; then
        echo "--- $probe: NOT RUN (needs AUDIT_ALLOW_DESTRUCTIVE=1); counted as a failure of this check"
        bad=$((bad + 1))
        continue
    fi
    out="$("$PYTHON" "audit/evaluations/$probe" 2>&1)"
    rc=$?
    echo "=================================================================== $probe (exit $rc)"
    printf '%s\n' "$out" | grep -E '^\[(PASS|FAIL|SKIP)\]|VERDICT' || true
    if [ "$rc" -eq 0 ]; then
        echo ">>> NOT RED: $probe passed against the old release"
        bad=$((bad + 1))
    elif ! printf '%s\n' "$out" | grep -qF -- "$marker"; then
        echo ">>> RED FOR ANOTHER REASON: expected a line starting '$marker'"
        bad=$((bad + 1))
    else
        echo ">>> red for the stated reason: $marker"
    fi
done

echo
if [ "$bad" -ne 0 ]; then
    echo "FALSIFICATION CHECK FAILED: $bad probe(s) did not go red for their stated reason"
    exit 1
fi
echo "FALSIFICATION CHECK PASSED: all ${#EXPECT[@]} probes go red on the old release for their stated reason"
