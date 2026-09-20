#!/usr/bin/env bash
# Runs the safe audit probe set. Exits non-zero if any probe FAILs.
#
#   ./audit/evaluations/run_all.sh
#
# PYTHON may override the interpreter. No probe here is destructive and none
# contacts a production host; probe_ssrf_guard.py performs outbound DNS
# resolution for public hostnames only.
#
# The probe list is DERIVED from the directory, not hand-maintained. A
# hand-maintained list is a second description of the same fact, and it drifts
# silently: four probes sat on disk outside the runner and were reported as
# coverage. Anything deliberately not run must be named in EXCLUDED with a
# reason, so an omission is a decision rather than an oversight.
set -uo pipefail

cd "$(dirname "$0")/../.." || exit 1
PYTHON="${PYTHON:-.venv/bin/python}"

if [ ! -x "$PYTHON" ]; then
    PYTHON="python3"
fi

# Probes that need a live PostgreSQL container via testcontainers. A machine
# with no Docker cannot run these, and "could not run" is NOT "failed" -- a
# runner that conflates them reports a red that means nothing, which is the
# defect this harness exists to catch. Select with PROBE_SET.
DOCKER_PROBES="probe_event_store_ddl_on_construction.py
probe_event_store_no_ddl_by_default.py
probe_multitenant_rls.py
probe_runtime_role_needs_no_create.py
probe_schema_namespace_resolution.py
probe_signal_storage_degrades.py
probe_split_namespace.py"

# PROBE_SET: all (default) | sqlite (skip Docker probes) | postgres (only those)
PROBE_SET="${PROBE_SET:-all}"

_needs_docker() {
    printf '%s\n' "$DOCKER_PROBES" | grep -qxF "$1"
}

declare -A EXCLUDED=(
    [probe_event_store_ddl_on_construction.py]="asserts the PRE-39 contract (constructor issues DDL by default), which 0.28.0 reversed; kept as the historical reproduction, superseded by probe_event_store_no_ddl_by_default.py"
)

mapfile -t ALL < <(cd audit/evaluations && ls probe_*.py | sort)

PROBES=()
SKIPPED=()
for name in "${ALL[@]}"; do
    if [ -n "${EXCLUDED[$name]:-}" ]; then
        continue
    fi
    if [ "$PROBE_SET" = "sqlite" ] && _needs_docker "$name"; then
        SKIPPED+=("$name")
        continue
    fi
    if [ "$PROBE_SET" = "postgres" ] && ! _needs_docker "$name"; then
        continue
    fi
    PROBES+=("audit/evaluations/$name")
done

echo "==================================================================="
echo "${#ALL[@]} probe(s) on disk; ${#PROBES[@]} to run; ${#EXCLUDED[@]} excluded; ${#SKIPPED[@]} skipped (PROBE_SET=$PROBE_SET)"
for name in "${SKIPPED[@]:-}"; do
    [ -n "$name" ] && echo "  SKIPPED $name  (needs a PostgreSQL container; not a failure)"
done
for name in "${!EXCLUDED[@]}"; do
    echo "  EXCLUDED $name"
    echo "           ${EXCLUDED[$name]}"
done
echo "==================================================================="
echo

failures=0
declare -a failed_names=()
for probe in "${PROBES[@]}"; do
    echo "==================================================================="
    echo "RUNNING $probe"
    echo "==================================================================="
    if "$PYTHON" "$probe"; then
        echo "--- $probe: PASS"
    else
        echo "--- $probe: FAIL"
        failures=$((failures + 1))
        failed_names+=("$probe")
    fi
    echo
done

echo "==================================================================="
if [ "$failures" -eq 0 ]; then
    echo "ALL PROBES PASSED (${#PROBES[@]} ran, ${#SKIPPED[@]} skipped)"
else
    echo "$failures of ${#PROBES[@]} PROBE(S) FAILED"
    for n in "${failed_names[@]}"; do
        echo "  FAILED $n"
    done
fi
exit "$failures"
