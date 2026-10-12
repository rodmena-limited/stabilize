#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
PYTHON="${PYTHON:?set PYTHON to the interpreter that has the OLD stabilize installed}"

"$PYTHON" -c 'import stabilize, sys; print("stabilize", stabilize.__version__, "from", stabilize.__file__)'

export OLD_VERSION="${OLD_VERSION:?set OLD_VERSION to the release the probes must fail on}"
EXPECT_FILE="${EXPECT_FILE:-audit/evaluations/expect/${OLD_VERSION}.probes}"
mapfile -t EXPECT < <(grep -v '^[[:space:]]*$' "$EXPECT_FILE")
if [ "${#EXPECT[@]}" -eq 0 ]; then
    echo "FALSIFICATION CHECK FAILED: $EXPECT_FILE lists no probes; an empty check proves nothing"
    exit 2
fi
"$PYTHON" -c 'import stabilize, sys, os; v = os.environ["OLD_VERSION"]; sys.exit(0 if stabilize.__version__ == v else f"installed stabilize {stabilize.__version__} is not {v}")' || exit 2

bad=0
for entry in "${EXPECT[@]}"; do
    probe="${entry%%|*}"
    rest="${entry#*|}"
    marker="${rest%%|*}"
    control=""
    [ "$rest" != "$marker" ] && control="${rest#*|}"
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
    elif ! grep -qF -- "$marker" <<<"$out"; then
        echo ">>> RED FOR ANOTHER REASON: expected a line starting '$marker'"
        printf '%s\n' "$out" | tail -25
        bad=$((bad + 1))
    elif [ -n "$control" ] && ! grep -qF -- "$control" <<<"$out"; then
        echo ">>> CONTROL NOT GREEN: expected a line starting '$control' on the old release"
        bad=$((bad + 1))
    else
        echo ">>> red for the stated reason: $marker${control:+ (control green: $control)}"
    fi
done

echo
if [ "$bad" -ne 0 ]; then
    echo "FALSIFICATION CHECK FAILED: $bad probe(s) did not go red for their stated reason"
    exit 1
fi
echo "FALSIFICATION CHECK PASSED: all ${#EXPECT[@]} probes go red on the old release for their stated reason"
