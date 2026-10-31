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

# Probes that need PostgreSQL take it from STABILIZE_PROBE_DSN and exit SKIP (77)
# without it; "could not run" is NOT "failed" -- a
# runner that conflates them reports a red that means nothing, which is the
# defect this harness exists to catch. PROBE_REQUIRE_PG=1 fails a SKIP. Select with PROBE_SET.
POSTGRES_PROBES="$(cd audit/evaluations && grep -lE "probe_dsn\(|fresh_database\(|postgres_dsn\(|backends\(\)|STABILIZE_PROBE_DSN" probe_*.py | sort)"

# PROBE_SET: all (default) | sqlite (skip PostgreSQL probes) | postgres (only those)
PROBE_SET="${PROBE_SET:-all}"

_needs_postgres() {
    grep -qxF "$1" <<<"$POSTGRES_PROBES"
}

declare -A EXCLUDED=()

mapfile -t ALL < <(cd audit/evaluations && ls probe_*.py | sort)

declare -A EXCLUDED_BY_FILE=()
if [ -n "${PROBE_EXCLUDE_FILE:-}" ]; then
    while IFS= read -r line; do
        [ -z "${line// /}" ] && continue
        EXCLUDED_BY_FILE["${line%%|*}"]=1
    done < "$PROBE_EXCLUDE_FILE"
fi

PROBES=()
SKIPPED=()
for name in "${ALL[@]}"; do
    if [ -n "${EXCLUDED[$name]:-}" ]; then
        continue
    fi
    if [ -n "${EXCLUDED_BY_FILE[$name]:-}" ]; then
        EXCLUDED["$name"]="listed in $PROBE_EXCLUDE_FILE: expected red on the release under test"
        continue
    fi
    if [ "$PROBE_SET" = "sqlite" ] && _needs_postgres "$name"; then
        SKIPPED+=("$name")
        continue
    fi
    if [ "$PROBE_SET" = "postgres" ] && ! _needs_postgres "$name"; then
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
declare -a skipped_names=()
for probe in "${PROBES[@]}"; do
    echo "==================================================================="
    echo "RUNNING $probe"
    echo "==================================================================="
    "$PYTHON" "$probe"; rc=$?
    if [ "$rc" -eq 77 ]; then
        if [ "${PROBE_REQUIRE_PG:-0}" = "1" ]; then
            echo "--- $probe: SKIP counted as FAIL (PROBE_REQUIRE_PG=1)"
            failures=$((failures + 1))
            failed_names+=("$probe")
        else
            echo "--- $probe: SKIP (exit 77)"
            skipped_names+=("$probe")
        fi
    elif [ "$rc" -eq 0 ]; then
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
