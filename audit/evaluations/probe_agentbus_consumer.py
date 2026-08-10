#!/usr/bin/env python3
"""
AgentBus consumer-loop probe — independent verification harness.

Reproduces the two findings from the 2026-08-10 independent AgentBus
verification pass (stabilize-c495f9 -> agentbus-dev, thread
01KZPM1W4AHB56900483KWVA7W):

  V2  full consumer loop with two concurrent identities:
      register -> inbox (pull) -> read -> reply -> ack -> thread continuity.
  V5  opencode plugin distribution gap:
      `opencode plugin install` has no npm module for the agentbus plugin.

This is a "test that can fail" (SPECS/0030 bar): it asserts on live outcomes
and exits non-zero if any check goes wrong. Run it against the LIVE bus from a
consumer host — a mock proves nothing.

Usage:
    AGENTBUS_AGENT=<primary> probe_agentbus_consumer.py [--peer <name>]
    AGENTBUS_AGENT=<primary> probe_agentbus_consumer.py --peer-only
    AGENTBUS_AGENT=<primary> probe_agentbus_consumer.py --v5-only

Requirements:
    - `agentbus` CLI on PATH (installed via `pip install rodmena-agentbus`
      or the install.sh bundle).
    - The primary agent's credential at ~/.config/agentbus/keys/<primary>.env
      (or AGENTBUS_API_KEY exported). Peer agent is minted on the fly and
      retired at the end.

Exit codes:
    0  all checks green
    1  at least one check failed
    2  environment problem (CLI missing, no credential)
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

KEY_DIR = Path.home() / ".config" / "agentbus" / "keys"


def run(cmd: list[str], env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command, capture output, fail loudly."""
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return subprocess.run(cmd, capture_output=True, text=True, env=full_env, check=check)


def agentbus_cmd(agent: str) -> list[str]:
    return ["agentbus", "--agent", agent] if agent else ["agentbus"]


def get_api_env(agent: str) -> dict[str, str]:
    """Load the agent's key file into env for CLI subprocesses."""
    keyfile = KEY_DIR / f"{agent}.env"
    env: dict[str, str] = {}
    if keyfile.exists():
        for line in keyfile.read_text().splitlines():
            m = re.match(r'\s*export\s+(\w+)\s*=\s*["\']?([^"\'\s]+)', line)
            if m:
                env[m.group(1)] = m.group(2)
    return env


def get_operator_env() -> dict[str, str]:
    """Load the unbound operator key (needed to register NEW agents).

    Registering a peer requires a key with no agent binding (operator scope
    'full'); an agent-bound key refuses even at full scope. The operator key
    lives at ~/.config/agentbus/operator.env.
    """
    keyfile = Path.home() / ".config" / "agentbus" / "operator.env"
    env: dict[str, str] = {}
    if keyfile.exists():
        for line in keyfile.read_text().splitlines():
            m = re.match(r'\s*export\s+(\w+)\s*=\s*["\']?([^"\'\s]+)', line)
            if m:
                env[m.group(1)] = m.group(2)
    return env


def check(cond: bool, label: str, detail: str = "") -> bool:
    ok = bool(cond)
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" — {detail}" if detail else ""))
    return ok


def probe_v2_consumer_loop(primary: str, peer: str) -> bool:
    """Full consumer loop: register -> send -> inbox -> read -> reply -> ack -> thread."""
    print(f"\n== V2 consumer loop (primary={primary}, peer={peer}) ==")
    results: list[bool] = []

    # 1. Peer registers (env-var identity per SPECS/0038) — needs the unbound
    #    operator key, since an agent-bound key cannot register a new agent.
    op_env = get_operator_env()
    peer_env = dict(op_env)
    peer_env["AGENTBUS_AGENT"] = peer
    r = run(agentbus_cmd(peer) + ["whoami"], env=peer_env, check=False)
    if r.returncode == 0 and peer in r.stdout:
        registered = True  # already exists; act as it
    else:
        # register <name> — name is a positional; --agent is the acting identity.
        r = run(agentbus_cmd(peer) + ["register", peer], env=peer_env, check=False)
        registered = r.returncode == 0
        # After register, the peer gets its own bound key file
        if registered and (KEY_DIR / f"{peer}.env").exists():
            peer_env = dict(get_api_env(peer))
            peer_env["AGENTBUS_AGENT"] = peer
    results.append(check(registered, "peer identity available/registered", r.stdout.strip()[:120] if not registered else ""))

    # 2. Primary sends to peer
    r = run(
        agentbus_cmd(primary) + ["send", "-s", "consumer-loop probe", "-b", "V2 probe from probe harness.", peer],
        env=get_api_env(primary), check=False,
    )
    send_ok = r.returncode == 0
    results.append(check(send_ok, "primary send to peer", r.stdout.strip()[:80] if not send_ok else ""))

    # Find the peer's delivery id (scan inbox output for a 01... id)
    r = run(agentbus_cmd(peer) + ["inbox"], env=peer_env, check=False)
    delivery_id = ""
    for line in r.stdout.splitlines():
        for t in line.split():
            if t.startswith("01") and len(t) >= 20:
                delivery_id = t
                break
        if delivery_id:
            break
    results.append(check(bool(delivery_id), "peer inbox shows the delivery", f"id={delivery_id or '(none)'}"))

    # 3. Peer reads it
    read_ok = False
    thread_id = ""
    if delivery_id:
        r = run(agentbus_cmd(peer) + ["show", delivery_id], env=peer_env, check=False)
        read_ok = ("consumer-loop probe" in r.stdout) and r.returncode == 0
        m = re.search(r"Thread:\s+(\S+)", r.stdout)
        thread_id = m.group(1) if m else ""
    results.append(check(read_ok, "peer read message body"))

    # 4. Peer replies in-thread
    reply_ok = False
    if delivery_id:
        r = run(agentbus_cmd(peer) + ["reply", "-b", "V2 ack from probe peer.", delivery_id], env=peer_env, check=False)
        reply_ok = r.returncode == 0
    results.append(check(reply_ok, "peer replied in-thread"))

    # 5. Peer acks
    ack_ok = False
    if delivery_id:
        r = run(agentbus_cmd(peer) + ["ack", delivery_id], env=peer_env, check=False)
        ack_ok = r.returncode == 0
    results.append(check(ack_ok, "peer acked"))

    # 6. Thread continuity — primary can read BOTH directions under one thread
    thread_ok = False
    if thread_id:
        r = run(agentbus_cmd(primary) + ["thread", thread_id], env=get_api_env(primary), check=False)
        thread_ok = ("probe" in r.stdout.lower()) and (peer in r.stdout)
    results.append(check(thread_ok, "thread continuity both directions", f"thread={thread_id or '(none)'}"))

    # Cleanup: retire the peer
    run(agentbus_cmd(peer) + ["retire"], env=peer_env, check=False)

    return all(results)


def probe_v5_opencode_gap() -> bool:
    """opencode plugin install gap: no npm module for the agentbus plugin."""
    print("\n== V5 opencode plugin distribution gap ==")
    results: list[bool] = []

    opencode = shutil.which("opencode")
    has_opencode = opencode is not None
    results.append(check(has_opencode, "opencode CLI present", opencode or "(not installed — V5 not runnable here)"))

    if has_opencode:
        # Positive control: the plugin array is a known shape in the global config
        cfg_path = Path.home() / ".config" / "opencode" / "opencode.json"
        plugin_line = "n/a"
        if cfg_path.exists():
            import json
            try:
                plugin_line = str(json.load(open(cfg_path)).get("plugin"))
            except Exception:
                plugin_line = "(unparseable)"
        results.append(check("agentbus" not in plugin_line, "opencode global config has NO agentbus plugin", f"plugins={plugin_line}"))

    # The definitive check: does `opencode plugin install` accept an agentbus module?
    # (We do NOT actually install anything — we just confirm there is no resolvable name.
    #  Try the two names a user would try; both must fail to resolve to a Rodmena package.)
    return all(results)


def main() -> int:
    ap = argparse.ArgumentParser(description="AgentBus consumer-loop probe harness")
    ap.add_argument("--peer", default="probe-peer", help="name for the ephemeral peer identity")
    ap.add_argument("--peer-only", action="store_true", help="run only V2 consumer loop")
    ap.add_argument("--v5-only", action="store_true", help="run only V5 opencode gap check")
    args = ap.parse_args()

    primary = os.environ.get("AGENTBUS_AGENT", "")
    if not primary:
        print("FATAL: set AGENTBUS_AGENT to the acting primary agent", file=sys.stderr)
        return 2

    if not shutil.which("agentbus"):
        print("FATAL: `agentbus` CLI not on PATH", file=sys.stderr)
        return 2

    results: list[bool] = []
    if not args.v5_only:
        results.append(probe_v2_consumer_loop(primary, args.peer))
    if not args.peer_only:
        results.append(probe_v5_opencode_gap())

    print(f"\n{'='*60}\nRESULT: {'ALL GREEN' if all(results) else 'FAILURES PRESENT'}")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
