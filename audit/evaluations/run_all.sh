#!/usr/bin/env bash
# Runs the safe audit probe set. Exits non-zero if any probe FAILs.
#
#   ./audit/evaluations/run_all.sh
#
# PYTHON may override the interpreter. No probe here is destructive and none
# contacts a production host; probe_ssrf_guard.py performs outbound DNS
# resolution for public hostnames only.
set -uo pipefail

cd "$(dirname "$0")/../.." || exit 1
PYTHON="${PYTHON:-.venv/bin/python}"

if [ ! -x "$PYTHON" ]; then
    PYTHON="python3"
fi

PROBES=(
    audit/evaluations/probe_mg_conninfo.py
    audit/evaluations/probe_ssrf_guard.py
    audit/evaluations/probe_http_credential_persistence.py
    audit/evaluations/probe_circuit_storage_honesty.py
    audit/evaluations/probe_pool_options.py
)

failures=0
for probe in "${PROBES[@]}"; do
    echo "==================================================================="
    echo "RUNNING $probe"
    echo "==================================================================="
    if "$PYTHON" "$probe"; then
        echo "--- $probe: PASS"
    else
        echo "--- $probe: FAIL"
        failures=$((failures + 1))
    fi
    echo
done

echo "==================================================================="
if [ "$failures" -eq 0 ]; then
    echo "ALL PROBES PASSED (${#PROBES[@]} probes)"
else
    echo "$failures of ${#PROBES[@]} PROBE(S) FAILED"
fi
exit "$failures"
