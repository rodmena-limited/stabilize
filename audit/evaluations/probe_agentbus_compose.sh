#!/usr/bin/env bash
#
# AgentBus multi-agent Docker Compose probe — "test that can fail".
#
# Reproduces the 2026-08-10 multi-agent verification (stabilize-c495f9 ->
# agentbus-dev, thread 01KZPM1W4AHB56900483KWVA7W): two agent containers on a
# compose bridge network, each with its OWN identity + bound key, communicate
# through the central bus:
#
#   A send -> B receive -> B read -> B in-thread reply -> B ack
#     -> A receives the reply -> A read -> A ack
#
# This is the SPECS/0030 "test that can fail" bar: it asserts on LIVE outcomes
# (via the real bus, real identities, real keys) and exits non-zero on any
# failure. A mock proves nothing.
#
# REQUIREMENTS
#   - `docker` + `docker compose` on PATH.
#   - An unbound OPERATOR key at ~/.config/agentbus/operator.env, used to mint
#     two ephemeral test identities (dc-probe-a / dc-probe-b).
#   - Outbound network to pypi.org (to pip-install rodmena-agentbus in the
#     container) and to the agentbus service.
#
# USAGE
#   ./probe_agentbus_compose.sh            # full run (mint, compose up, verify, cleanup)
#   ./probe_agentbus_compose.sh --keep     # keep containers/identities after (debug)
#   ./probe_agentbus_compose.sh --clean    # only clean up from a previous run
#
# EXIT CODES
#   0  all checks green
#   1  at least one check failed
#   2  environment problem (docker/operator key missing)
#
# The ephemeral identities are retired at the end; the compose network and the
# built image are removed. A --keep run leaves them behind for inspection.
set -uo pipefail

BASE_IMAGE="ab-probe-base"
NET_NAME="abprobe_net"
AGENT_A="dc-probe-a"
AGENT_B="dc-probe-b"
KEY_DIR="$HOME/.config/agentbus/keys"
OP_KEY="$HOME/.config/agentbus/operator.env"
WORKDIR="$(mktemp -d /tmp/abprobe.XXXXXX)"

log() { printf '\n== %s ==\n' "$*"; }
pass() { printf '  [PASS] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; }

cleanup() {
  log "cleanup"
  # Retire the ephemeral identities (reversible)
  set -a; . "$OP_KEY" 2>/dev/null; set +a
  AGENTBUS_AGENT="$AGENT_A" agentbus retire >/dev/null 2>&1 || true
  AGENTBUS_AGENT="$AGENT_B" agentbus retire >/dev/null 2>&1 || true
  # Tear down compose + image + network
  ( cd "$WORKDIR" && docker compose down -v >/dev/null 2>&1 ) || true
  docker rmi "$BASE_IMAGE" >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}

# ---------------------------------------------------------------------------
# Environment checks
# ---------------------------------------------------------------------------
if ! command -v docker >/dev/null; then echo "FATAL: docker not on PATH" >&2; exit 2; fi
if ! docker compose version >/dev/null 2>&1; then echo "FATAL: docker compose unavailable" >&2; exit 2; fi
if [ ! -f "$OP_KEY" ]; then echo "FATAL: operator key not found at $OP_KEY" >&2; exit 2; fi

[ "${1:-}" = "--clean" ] && { cleanup; echo "cleaned"; exit 0; }
KEEP=0; [ "${1:-}" = "--keep" ] && KEEP=1

# ---------------------------------------------------------------------------
# 1. Mint two ephemeral identities via the operator key
# ---------------------------------------------------------------------------
log "minting identities"
set -a; . "$OP_KEY"; set +a
for A in "$AGENT_A" "$AGENT_B"; do
  if ! AGENTBUS_AGENT="$A" agentbus whoami >/dev/null 2>&1; then
    AGENTBUS_AGENT="$A" agentbus register "$A" --ephemeral >/dev/null 2>&1
  fi
  [ -f "$KEY_DIR/$A.env" ] || { fail "$A key not minted"; cleanup; exit 1; }
  pass "$A identity + bound key ready"
done

# ---------------------------------------------------------------------------
# 2. Build the base image (node + rodmena-agentbus from PyPI)
# ---------------------------------------------------------------------------
log "building base image (fresh rodmena-agentbus from PyPI)"
cat > "$WORKDIR/Dockerfile" <<'DOCKER'
FROM node:20-slim
RUN apt-get update -qq && apt-get install -y -qq python3 python3-venv >/dev/null 2>&1
RUN python3 -m venv /opt/ab && /opt/ab/bin/pip install --quiet rodmena-agentbus
ENV PATH="/opt/ab/bin:$PATH"
DOCKER
if ! docker build -q -t "$BASE_IMAGE" "$WORKDIR" >/dev/null 2>&1; then
  fail "docker build"; cleanup; exit 1
fi
pass "base image built"

# ---------------------------------------------------------------------------
# 3. Write the compose file + agent scripts
# ---------------------------------------------------------------------------
cat > "$WORKDIR/agent-a.sh" <<AEOF
#!/bin/sh
set -e
set -a; . /root/.config/agentbus/keys/$AGENT_A.env; set +a
echo "=== A starting ($AGENTBUS_AGENT) ==="
agentbus whoami 2>&1 | head -2
agentbus send -s "compose-probe: A->B" -b "hello from $AGENT_A in compose network" $AGENT_B 2>&1 | tail -1
for i in \$(seq 1 15); do
  sleep 2
  OUT=\$(agentbus inbox --unread 2>&1 | head -6)
  if echo "\$OUT" | grep -q "Re: compose-probe"; then
    echo "A received B's reply"
    DELIV=\$(echo "\$OUT" | grep -oE '^[[:space:]]+01[0-9A-Z]{24,}' | grep -oE '01[0-9A-Z]{24,}' | tail -1)
    agentbus show "\$DELIV" 2>&1 | head -6
    agentbus ack "\$DELIV" >/dev/null 2>&1
    echo "A-DONE"
    exit 0
  fi
done
echo "A-TIMEOUT"
exit 1
AEOF

cat > "$WORKDIR/agent-b.sh" <<BEOF
#!/bin/sh
set -e
set -a; . /root/.config/agentbus/keys/$AGENT_B.env; set +a
echo "=== B starting ($AGENTBUS_AGENT) ==="
agentbus whoami 2>&1 | head -2
for i in \$(seq 1 18); do
  sleep 2
  OUT=\$(agentbus inbox --unread 2>&1 | head -6)
  if echo "\$OUT" | grep -q "compose-probe: A->B"; then
    echo "B received A's message"
    DELIV=\$(echo "\$OUT" | grep -oE '^[[:space:]]+01[0-9A-Z]{24,}' | grep -oE '01[0-9A-Z]{24,}' | tail -1)
    agentbus show "\$DELIV" 2>&1 | head -6
    agentbus reply -b "reply from $AGENT_B in compose" "\$DELIV" 2>&1 | tail -1
    agentbus ack "\$DELIV" >/dev/null 2>&1
    echo "B-DONE"
    exit 0
  fi
done
echo "B-TIMEOUT"
exit 1
BEOF
chmod +x "$WORKDIR"/agent-*.sh

cat > "$WORKDIR/docker-compose.yml" <<CEOF
services:
  agent-a:
    image: $BASE_IMAGE
    container_name: $AGENT_A
    environment:
      - AGENTBUS_AGENT=$AGENT_A
    volumes:
      - $KEY_DIR/$AGENT_A.env:/root/.config/agentbus/keys/$AGENT_A.env:ro
      - $WORKDIR/agent-a.sh:/agent.sh:ro
    command: ["sh", "/agent.sh"]
    networks: [$NET_NAME]
  agent-b:
    image: $BASE_IMAGE
    container_name: $AGENT_B
    environment:
      - AGENTBUS_AGENT=$AGENT_B
    volumes:
      - $KEY_DIR/$AGENT_B.env:/root/.config/agentbus/keys/$AGENT_B.env:ro
      - $WORKDIR/agent-b.sh:/agent.sh:ro
    command: ["sh", "/agent.sh"]
    networks: [$NET_NAME]
networks:
  $NET_NAME:
    driver: bridge
CEOF

# ---------------------------------------------------------------------------
# 4. Run compose and assert both agents finish green
# ---------------------------------------------------------------------------
log "running compose (both agents, full loop)"
OUTPUT="$(cd "$WORKDIR" && docker compose up --build 2>&1)"
STATUS=$?

echo "$OUTPUT" | grep -E "A-DONE|B-DONE|A-TIMEOUT|B-TIMEOUT|A received|B received|replied|acked" | tail -20

A_OK=$(echo "$OUTPUT" | grep -c "A-DONE")
B_OK=$(echo "$OUTPUT" | grep -c "B-DONE")
ALL_OK=0
[ "$A_OK" -ge 1 ] && [ "$B_OK" -ge 1 ] && [ "$STATUS" -eq 0 ] && ALL_OK=1

if [ "$ALL_OK" -eq 1 ]; then
  pass "A sent, B received+replied+acked, A received the reply (full loop)"
  log "RESULT: ALL GREEN"
  [ "$KEEP" -eq 1 ] || cleanup
  exit 0
else
  fail "loop incomplete (A=$A_OK B=$B_OK status=$STATUS)"
  echo "--- tail of compose output ---"
  echo "$OUTPUT" | tail -25
  log "RESULT: FAILURES PRESENT"
  [ "$KEEP" -eq 1 ] || cleanup
  exit 1
fi
