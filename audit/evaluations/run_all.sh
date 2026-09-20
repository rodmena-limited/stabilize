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
    audit/evaluations/probe_ssrf_rebinding.py
    audit/evaluations/probe_schema_and_exists.py
    audit/evaluations/probe_secret_redaction.py
    audit/evaluations/probe_dsn_ssl_params.py
    audit/evaluations/probe_stage_message_ownership.py
    audit/evaluations/probe_stage_context_rehydration.py
    audit/evaluations/probe_event_read_forward_compat.py
    audit/evaluations/probe_event_commit_watermark.py
    audit/evaluations/probe_event_coverage.py
    audit/evaluations/probe_branch_pruning.py
    audit/evaluations/probe_multi_merge.py
    audit/evaluations/probe_structured_loops.py
    audit/evaluations/probe_task_lease_fails_closed.py
    audit/evaluations/probe_event_store_no_ddl_by_default.py
    audit/evaluations/probe_multi_instance_cancel_remaining.py
    audit/evaluations/probe_message_contract.py
    audit/evaluations/probe_engine_key_census.py
    audit/evaluations/probe_dynamic_multi_instance.py
    audit/evaluations/probe_multitenant_rls.py
    audit/evaluations/probe_schema_namespace_resolution.py
    audit/evaluations/probe_split_namespace.py
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
