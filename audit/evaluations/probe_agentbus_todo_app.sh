#!/usr/bin/env bash
#
# AgentBus multi-role todo-app pipeline probe — the hard one.
#
# Five agent containers (architect, auditor, frontend, backend, tester) each
# with its OWN agentbus identity + bound key, cross-talking through the bus
# while writing artifacts to a SHARED /data volume, cooperatively building a
# Flask todo app:
#
#   architect ──(design.md)──▶ auditor ──(audit.md)──▶ frontend
#                                                    └──▶ backend
#                                                        (app.py)
#   frontend (index.html) ──▶ tester ◀── backend
#        tester runs smoke test ──(test_results.txt)──▶ architect  (loop closes)
#
# Bus surface exercised: 1:1 send, 1:N fan-out (auditor->frontend+backend),
# N:1 merge (tester waits for BOTH), thread continuity, ack, all on the live
# bus from separate containers. /data is the shared team memory.
#
# This is a SPECS/0030 "test that can fail": asserts on LIVE outcomes (real
# bus, real identities, real built artifacts) and exits non-zero on any miss.
#
# REQUIREMENTS
#   - docker + docker compose on PATH
#   - unbound OPERATOR key at ~/.config/agentbus/operator.env
#   - outbound net to pypi.org + the agentbus service
#
# USAGE
#   ./probe_agentbus_todo_app.sh            # full run + verify + cleanup
#   ./probe_agentbus_todo_app.sh --keep     # leave containers/identities/volume
#   ./probe_agentbus_todo_app.sh --clean    # tidy a previous --keep run
#
# EXIT CODES
#   0  pipeline completed, all 5 artifacts present, smoke test green
#   1  a role failed, an artifact is missing, or the smoke test failed
#   2  environment problem
#
set -uo pipefail

BASE_IMAGE="ab-todo-base"
VOL_NAME="ab_todo_data_$(date +%s)"
NET_NAME="abtodo_net"
ROLES=(architect auditor frontend backend tester)
KEY_DIR="$HOME/.config/agentbus/keys"
OP_KEY="$HOME/.config/agentbus/operator.env"
WORKDIR="$(mktemp -d /tmp/abtodo.XXXXXX)"
RUN_ID="$(date +%s)"

log() { printf '\n== %s ==\n' "$*"; }
pass() { printf '  [PASS] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; }

cleanup() {
  log "cleanup"
  set -a; . "$OP_KEY" 2>/dev/null; set +a
  for r in "${ROLES[@]}"; do
    AGENTBUS_AGENT="todo-$r" agentbus retire >/dev/null 2>&1 || true
  done
  ( cd "$WORKDIR" && docker compose down -v >/dev/null 2>&1 ) || true
  docker volume rm -f "$VOL_NAME" >/dev/null 2>&1 || true
  docker rmi "$BASE_IMAGE" >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}

# --------------------------------------------------------------------------
# Environment checks
# --------------------------------------------------------------------------
if ! command -v docker >/dev/null; then echo "FATAL: docker not on PATH" >&2; exit 2; fi
if ! docker compose version >/dev/null 2>&1; then echo "FATAL: docker compose unavailable" >&2; exit 2; fi
if [ ! -f "$OP_KEY" ]; then echo "FATAL: operator key not found at $OP_KEY" >&2; exit 2; fi

[ "${1:-}" = "--clean" ] && { cleanup; echo "cleaned"; exit 0; }
KEEP=0; [ "${1:-}" = "--keep" ] && KEEP=1

# --------------------------------------------------------------------------
# 1. Mint five identities via the operator key
# --------------------------------------------------------------------------
log "minting identities (5 roles)"
set -a; . "$OP_KEY"; set +a
for r in "${ROLES[@]}"; do
  if ! AGENTBUS_AGENT="todo-$r" agentbus whoami >/dev/null 2>&1; then
    AGENTBUS_AGENT="todo-$r" agentbus register "todo-$r" --ephemeral >/dev/null 2>&1
  fi
  [ -f "$KEY_DIR/todo-$r.env" ] || { fail "todo-$r key not minted"; cleanup; exit 1; }
  pass "todo-$r identity + key ready"
done

# --------------------------------------------------------------------------
# 2. Build the base image (python + rodmena-agentbus + flask, all from PyPI)
# --------------------------------------------------------------------------
log "building base image (fresh deps from PyPI)"
cat > "$WORKDIR/Dockerfile" <<'DOCKER'
FROM python:3.11-slim
RUN pip install --quiet rodmena-agentbus flask
ENV PATH="/usr/local/bin:$PATH"
DOCKER
if ! docker build -q -t "$BASE_IMAGE" "$WORKDIR" >/dev/null 2>&1; then
  fail "docker build"; cleanup; exit 1
fi
pass "base image built"

# --------------------------------------------------------------------------
# 3. Role scripts. Each embeds a self-contained `wait_for` (drains stale
#    messages, waits for the right one from the right sender), writes its
#    artifact to /data, notifies the next role.
# --------------------------------------------------------------------------
# Shared helper text (quoted so nothing interpolates on the host).
read -r -d '' WAIT_FN <<'WAITEOF' || true
wait_for() {
  local sender="$1" subject="$2"
  for _ in $(seq 1 40); do
    local IN WANT ID ROW FROM SUBJ
    WANT=""
    IN=$(agentbus inbox 2>&1)
    for ID in $(echo "$IN" | grep -oE '^[[:space:]]+01[0-9A-Z]{24,}' | grep -oE '01[0-9A-Z]{24,}'); do
      ROW=$(agentbus show "$ID" 2>&1 | grep -E "From:|Subject:" | tr '\n' ' ')
      FROM=$(echo "$ROW" | sed -n "s/.*From:    *\([^ ]*\).*/\1/p")
      SUBJ=$(echo "$ROW" | sed -n "s/.*Subject: *\(.*\)/\1/p")
      if [ "$FROM" = "$sender" ] && echo "$SUBJ" | grep -q "$subject"; then
        WANT="$ID"
      fi
    done
    [ -n "$WANT" ] && { echo "$WANT"; return 0; }
    sleep 2
  done
  return 1
}
WAITEOF

# --- architect -------------------------------------------------------------
cat > "$WORKDIR/architect.sh" <<'AEOF'
#!/bin/sh
set -e
set -a; . /root/.config/agentbus/keys/todo-architect.env; set +a
mkdir -p /data
cat > /data/design.md <<'MD'
# Flask Todo App Design
- Backend: Flask REST API at /todos (GET list, POST create)
- Frontend: single-page index.html calling the API
- Data: in-memory list on the backend
- Roles: architect(author) auditor(review) backend(api) frontend(ui) tester(verify)
MD
echo "architect wrote /data/design.md"
agentbus send -s "design-ready-$RUN_ID" -b "design complete for review (run $RUN_ID)" todo-auditor 2>&1 | tail -1
echo "ARCHITECT-DONE"
AEOF

# --- auditor ---------------------------------------------------------------
cat > "$WORKDIR/auditor.sh" <<'AEOF'
#!/bin/sh
set -e
set -a; . /root/.config/agentbus/keys/todo-auditor.env; set +a
wait_for() {
  local sender="$1" subject="$2"
  for _ in $(seq 1 40); do
    local IN WANT ID ROW FROM SUBJ
    WANT=""
    IN=$(agentbus inbox 2>&1)
    for ID in $(echo "$IN" | grep -oE '^[[:space:]]+01[0-9A-Z]{24,}' | grep -oE '01[0-9A-Z]{24,}'); do
      ROW=$(agentbus show "$ID" 2>&1 | grep -E "From:|Subject:" | tr '\n' ' ')
      FROM=$(echo "$ROW" | sed -n "s/.*From:    *\([^ ]*\).*/\1/p")
      SUBJ=$(echo "$ROW" | sed -n "s/.*Subject: *\(.*\)/\1/p")
      if [ "$FROM" = "$sender" ] && echo "$SUBJ" | grep -q "$subject"; then
        WANT="$ID"
      fi
    done
    [ -n "$WANT" ] && { echo "$WANT"; return 0; }
    sleep 2
  done
  return 1
}
DELIV=$(wait_for todo-architect "design-ready-$RUN_ID")
[ -z "$DELIV" ] && { echo "AUDITOR-TIMEOUT"; exit 1; }
agentbus ack "$DELIV" >/dev/null 2>&1
mkdir -p /data
cat > /data/audit.md <<'MD'
# Design Audit
Reviewed /data/design.md. APPROVED.
- API shape: GET/POST /todos with title+done - OK
- Frontend must call /todos and render list + add form - OK
- Tester will verify GET + POST round-trip
MD
echo "auditor wrote /data/audit.md"
agentbus send -s "audit-approved-$RUN_ID" -b "design approved (run $RUN_ID)" todo-frontend todo-backend 2>&1 | tail -1
echo "AUDITOR-DONE"
AEOF

# --- backend ---------------------------------------------------------------
cat > "$WORKDIR/backend.sh" <<'AEOF'
#!/bin/sh
set -e
set -a; . /root/.config/agentbus/keys/todo-backend.env; set +a
wait_for() {
  local sender="$1" subject="$2"
  for _ in $(seq 1 40); do
    local IN WANT ID ROW FROM SUBJ
    WANT=""
    IN=$(agentbus inbox 2>&1)
    for ID in $(echo "$IN" | grep -oE '^[[:space:]]+01[0-9A-Z]{24,}' | grep -oE '01[0-9A-Z]{24,}'); do
      ROW=$(agentbus show "$ID" 2>&1 | grep -E "From:|Subject:" | tr '\n' ' ')
      FROM=$(echo "$ROW" | sed -n "s/.*From:    *\([^ ]*\).*/\1/p")
      SUBJ=$(echo "$ROW" | sed -n "s/.*Subject: *\(.*\)/\1/p")
      if [ "$FROM" = "$sender" ] && echo "$SUBJ" | grep -q "$subject"; then
        WANT="$ID"
      fi
    done
    [ -n "$WANT" ] && { echo "$WANT"; return 0; }
    sleep 2
  done
  return 1
}
DELIV=$(wait_for todo-auditor "audit-approved-$RUN_ID")
[ -z "$DELIV" ] && { echo "BACKEND-TIMEOUT"; exit 1; }
agentbus ack "$DELIV" >/dev/null 2>&1
[ -f /data/design.md ] && [ -f /data/audit.md ] || { echo "BACKEND-NO-DESIGN"; exit 1; }
mkdir -p /data
cat > /data/app.py <<'PY'
from flask import Flask, request, jsonify

app = Flask(__name__)
_todos: list[dict] = []


@app.get("/todos")
def list_todos():
    return jsonify(_todos)


@app.post("/todos")
def add_todo():
    body = request.get_json(silent=True) or {}
    item = {"id": len(_todos) + 1, "title": body.get("title", ""), "done": False}
    _todos.append(item)
    return jsonify(item), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
PY
echo "backend wrote /data/app.py"
agentbus send -s "backend-ready-$RUN_ID" -b "flask api written (run $RUN_ID)" todo-tester 2>&1 | tail -1
echo "BACKEND-DONE"
AEOF

# --- frontend --------------------------------------------------------------
cat > "$WORKDIR/frontend.sh" <<'AEOF'
#!/bin/sh
set -e
set -a; . /root/.config/agentbus/keys/todo-frontend.env; set +a
wait_for() {
  local sender="$1" subject="$2"
  for _ in $(seq 1 40); do
    local IN WANT ID ROW FROM SUBJ
    WANT=""
    IN=$(agentbus inbox 2>&1)
    for ID in $(echo "$IN" | grep -oE '^[[:space:]]+01[0-9A-Z]{24,}' | grep -oE '01[0-9A-Z]{24,}'); do
      ROW=$(agentbus show "$ID" 2>&1 | grep -E "From:|Subject:" | tr '\n' ' ')
      FROM=$(echo "$ROW" | sed -n "s/.*From:    *\([^ ]*\).*/\1/p")
      SUBJ=$(echo "$ROW" | sed -n "s/.*Subject: *\(.*\)/\1/p")
      if [ "$FROM" = "$sender" ] && echo "$SUBJ" | grep -q "$subject"; then
        WANT="$ID"
      fi
    done
    [ -n "$WANT" ] && { echo "$WANT"; return 0; }
    sleep 2
  done
  return 1
}
DELIV=$(wait_for todo-auditor "audit-approved-$RUN_ID")
[ -z "$DELIV" ] && { echo "FRONTEND-TIMEOUT"; exit 1; }
agentbus ack "$DELIV" >/dev/null 2>&1
[ -f /data/design.md ] || { echo "FRONTEND-NO-DESIGN"; exit 1; }
mkdir -p /data
cat > /data/index.html <<'HTML'
<!doctype html>
<html><head><title>Todo</title></head><body>
<h1>Todo</h1>
<input id="t" placeholder="New todo"><button onclick="add()">Add</button>
<ul id="l"></ul>
<script>
async function load(){ const r=await fetch('/todos'); const d=await r.json();
  document.getElementById('l').innerHTML=d.map(x=>`<li>${x.title}</li>`).join(''); }
async function add(){ const t=document.getElementById('t').value;
  await fetch('/todos',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({title:t})});
  document.getElementById('t').value=''; load(); }
load();
</script>
</body></html>
HTML
echo "frontend wrote /data/index.html"
agentbus send -s "frontend-ready-$RUN_ID" -b "todo ui written (run $RUN_ID)" todo-tester 2>&1 | tail -1
echo "FRONTEND-DONE"
AEOF

# --- tester ----------------------------------------------------------------
cat > "$WORKDIR/tester.sh" <<'AEOF'
#!/bin/sh
set -e
set -a; . /root/.config/agentbus/keys/todo-tester.env; set +a
wait_for() {
  local sender="$1" subject="$2"
  for _ in $(seq 1 40); do
    local IN WANT ID ROW FROM SUBJ
    WANT=""
    IN=$(agentbus inbox 2>&1)
    for ID in $(echo "$IN" | grep -oE '^[[:space:]]+01[0-9A-Z]{24,}' | grep -oE '01[0-9A-Z]{24,}'); do
      ROW=$(agentbus show "$ID" 2>&1 | grep -E "From:|Subject:" | tr '\n' ' ')
      FROM=$(echo "$ROW" | sed -n "s/.*From:    *\([^ ]*\).*/\1/p")
      SUBJ=$(echo "$ROW" | sed -n "s/.*Subject: *\(.*\)/\1/p")
      if [ "$FROM" = "$sender" ] && echo "$SUBJ" | grep -q "$subject"; then
        WANT="$ID"
      fi
    done
    [ -n "$WANT" ] && { echo "$WANT"; return 0; }
    sleep 2
  done
  return 1
}
B=$(wait_for todo-backend "backend-ready-$RUN_ID")
[ -z "$B" ] && { echo "TESTER-BACKEND-TIMEOUT"; exit 1; }
agentbus ack "$B" >/dev/null 2>&1
F=$(wait_for todo-frontend "frontend-ready-$RUN_ID")
[ -z "$F" ] && { echo "TESTER-FRONTEND-TIMEOUT"; exit 1; }
agentbus ack "$F" >/dev/null 2>&1
[ -f /data/app.py ] && [ -f /data/index.html ] || { echo "TESTER-MISSING-ARTIFACT"; exit 1; }
mkdir -p /data
cd /data && python3 - <<'PY'
import json
from app import app
c = app.test_client()
r0 = c.get("/todos")
assert r0.status_code == 200, f"GET failed: {r0.status_code}"
r1 = c.post("/todos", json={"title": "buy milk"})
assert r1.status_code == 201, f"POST failed: {r1.status_code}"
item = r1.get_json()
assert item["title"] == "buy milk" and item["done"] is False
r2 = c.get("/todos")
assert len(r2.get_json()) == 1
print("SMOKE-OK")
PY
cat > /data/test_results.txt <<'RES'
Smoke test: PASS
- GET /todos -> 200
- POST /todos {title} -> 201, item echoes
- GET after POST -> 1 item
RES
agentbus send -s "tests-passed-$RUN_ID" -b "all green (run $RUN_ID)" todo-architect 2>&1 | tail -1
echo "TESTER-DONE"
AEOF
for r in architect auditor backend frontend tester; do chmod +x "$WORKDIR/$r.sh"; done

# --------------------------------------------------------------------------
# 4. Compose: 5 services, shared /data volume, each with its own key
# --------------------------------------------------------------------------
cat > "$WORKDIR/docker-compose.yml" <<CEOF
services:
  architect:
    image: $BASE_IMAGE
    container_name: todo-architect
    environment: [AGENTBUS_AGENT=todo-architect, RUN_ID=$RUN_ID]
    volumes:
      - $KEY_DIR/todo-architect.env:/root/.config/agentbus/keys/todo-architect.env:ro
      - $WORKDIR/architect.sh:/run.sh:ro
      - $VOL_NAME:/data
    command: ["sh", "/run.sh"]
    networks: [$NET_NAME]
  auditor:
    image: $BASE_IMAGE
    container_name: todo-auditor
    environment: [AGENTBUS_AGENT=todo-auditor, RUN_ID=$RUN_ID]
    volumes:
      - $KEY_DIR/todo-auditor.env:/root/.config/agentbus/keys/todo-auditor.env:ro
      - $WORKDIR/auditor.sh:/run.sh:ro
      - $VOL_NAME:/data
    command: ["sh", "/run.sh"]
    networks: [$NET_NAME]
    depends_on: [architect]
  frontend:
    image: $BASE_IMAGE
    container_name: todo-frontend
    environment: [AGENTBUS_AGENT=todo-frontend, RUN_ID=$RUN_ID]
    volumes:
      - $KEY_DIR/todo-frontend.env:/root/.config/agentbus/keys/todo-frontend.env:ro
      - $WORKDIR/frontend.sh:/run.sh:ro
      - $VOL_NAME:/data
    command: ["sh", "/run.sh"]
    networks: [$NET_NAME]
    depends_on: [auditor]
  backend:
    image: $BASE_IMAGE
    container_name: todo-backend
    environment: [AGENTBUS_AGENT=todo-backend, RUN_ID=$RUN_ID]
    volumes:
      - $KEY_DIR/todo-backend.env:/root/.config/agentbus/keys/todo-backend.env:ro
      - $WORKDIR/backend.sh:/run.sh:ro
      - $VOL_NAME:/data
    command: ["sh", "/run.sh"]
    networks: [$NET_NAME]
    depends_on: [auditor]
  tester:
    image: $BASE_IMAGE
    container_name: todo-tester
    environment: [AGENTBUS_AGENT=todo-tester, RUN_ID=$RUN_ID]
    volumes:
      - $KEY_DIR/todo-tester.env:/root/.config/agentbus/keys/todo-tester.env:ro
      - $WORKDIR/tester.sh:/run.sh:ro
      - $VOL_NAME:/data
    command: ["sh", "/run.sh"]
    networks: [$NET_NAME]
    depends_on: [frontend, backend]
volumes:
  $VOL_NAME:
networks:
  $NET_NAME:
    driver: bridge
CEOF

# --------------------------------------------------------------------------
# 5. Run + verify
# --------------------------------------------------------------------------
log "running the 5-role pipeline (cross-talk via agentbus, shared /data)"
OUTPUT="$(cd "$WORKDIR" && docker compose up --build 2>&1)"
STATUS=$?

echo "$OUTPUT" | grep -E "ARCHITECT-DONE|AUDITOR-DONE|BACKEND-DONE|FRONTEND-DONE|TESTER-DONE|TIMEOUT|MISSING|SMOKE-OK|wrote|ready|passed" | tail -30

ALL_OK=0
[ "$STATUS" -eq 0 ] && [ "$(echo "$OUTPUT" | grep -cE 'ARCHITECT-DONE')" -ge 1 ] \
  && [ "$(echo "$OUTPUT" | grep -cE 'AUDITOR-DONE')" -ge 1 ] \
  && [ "$(echo "$OUTPUT" | grep -cE 'BACKEND-DONE')" -ge 1 ] \
  && [ "$(echo "$OUTPUT" | grep -cE 'FRONTEND-DONE')" -ge 1 ] \
  && [ "$(echo "$OUTPUT" | grep -cE 'TESTER-DONE')" -ge 1 ] \
  && ALL_OK=1

if [ "$ALL_OK" -eq 1 ]; then
  pass "all 5 roles completed and cross-talked on the bus"
  log "verifying shared /data volume contents"
  # Resolve the ACTUAL volume the containers mounted (compose project-prefixes it)
  ACTUAL_VOL=$(docker inspect todo-tester --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' 2>/dev/null)
  if [ -z "$ACTUAL_VOL" ]; then
    fail "could not resolve the shared volume from todo-tester"
    log "RESULT: FAILURES PRESENT"
    [ "$KEEP" -eq 1 ] || cleanup; exit 1
  fi
  pass "shared volume resolved: $ACTUAL_VOL"
  MISSING=0
  for f in design.md audit.md app.py index.html test_results.txt; do
    if docker run --rm -v "$ACTUAL_VOL:/data" "$BASE_IMAGE" test -f "/data/$f"; then
      pass "artifact /data/$f present"
    else
      fail "artifact /data/$f MISSING"; MISSING=1
    fi
  done
  if [ "$MISSING" -eq 1 ]; then
    log "RESULT: FAILURES PRESENT (missing artifacts)"
    [ "$KEEP" -eq 1 ] || cleanup; exit 1
  fi
  log "RESULT: ALL GREEN"
  [ "$KEEP" -eq 1 ] || cleanup
  exit 0
else
  fail "pipeline incomplete (status=$STATUS)"
  echo "$OUTPUT" | tail -30
  log "RESULT: FAILURES PRESENT"
  [ "$KEEP" -eq 1 ] || cleanup
  exit 1
fi
