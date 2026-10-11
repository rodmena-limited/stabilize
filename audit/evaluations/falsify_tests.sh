#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
PYTHON="${PYTHON:?set PYTHON to the interpreter that has the OLD stabilize installed}"
export OLD_VERSION="${OLD_VERSION:?set OLD_VERSION to the release the tests must fail on}"
EXPECT_FILE="${EXPECT_FILE:-audit/evaluations/expect/${OLD_VERSION}.tests}"

"$PYTHON" -c 'import stabilize, sys, os; print("stabilize", stabilize.__version__, "from", stabilize.__file__); v = os.environ["OLD_VERSION"]; sys.exit(0 if stabilize.__version__ == v and "site-packages" in stabilize.__file__ else f"need published {v} from site-packages")' || exit 2

mapfile -t EXPECT < <(grep -v '^[[:space:]]*$' "$EXPECT_FILE")
if [ "${#EXPECT[@]}" -eq 0 ]; then
    echo "FALSIFICATION CHECK FAILED: $EXPECT_FILE lists no tests; an empty check proves nothing"
    exit 2
fi

bad=0
for entry in "${EXPECT[@]}"; do
    nodeid="${entry%%|*}"
    marker="${entry#*|}"
    out="$("$PYTHON" -m pytest "$nodeid" -q -p no:cacheprovider --timeout 600 -rfE 2>&1)"
    rc=$?
    echo "=================================================================== $nodeid (pytest exit $rc)"
    printf '%s\n' "$out" | grep -E '^(FAILED|ERROR|PASSED)|passed|failed|error' | tail -5 || true
    if [ "$rc" -eq 0 ]; then
        echo ">>> NOT RED: $nodeid passed against $OLD_VERSION"
        bad=$((bad + 1))
    elif [ "$rc" -ne 1 ]; then
        echo ">>> RED FOR ANOTHER REASON: pytest exit $rc (collection, usage or no tests), not an assertion failure"
        printf '%s\n' "$out" | tail -20
        bad=$((bad + 1))
    elif ! printf '%s\n' "$out" | grep -qF -- "$marker"; then
        echo ">>> RED FOR ANOTHER REASON: output lacks the stated marker '$marker'"
        printf '%s\n' "$out" | tail -30
        bad=$((bad + 1))
    else
        echo ">>> red for the stated reason: $marker"
    fi
done

echo
if [ "$bad" -ne 0 ]; then
    echo "TEST FALSIFICATION FAILED: $bad test(s) did not go red on $OLD_VERSION for their stated reason"
    exit 1
fi
echo "TEST FALSIFICATION PASSED: all ${#EXPECT[@]} tests go red on $OLD_VERSION for their stated reason"
