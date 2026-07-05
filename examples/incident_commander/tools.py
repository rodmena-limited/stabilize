"""
A simulated production environment for the Incident Commander showcase.

The diagnostic agents call these read-only tools (via stabilize's tool-calling
AgentLoopTask) to investigate a real-ish incident: the `checkout` service is
failing because of a bad deploy. The tools are deterministic so the demo is
reproducible, and the environment is *stateful* — a remediation actually
changes what the health check and metrics report, so verification and the
self-healing loop are driven by the environment, not by faked control flow.

`recovery_level` models how far recovery has progressed: 0 = broken, and the
service only reports healthy once two remediations have been applied (a rollback
and a connection-pool restart), which is what drives one self-heal cycle.
"""

from __future__ import annotations

from typing import Any

from stabilize.llm import ToolRegistry, tool

INCIDENT: dict[str, Any] = {
    "service": "checkout",
    "recovery_level": 0,
    "bad_deploy": "v2.4.1",
    "prev_deploy": "v2.4.0",
    "deployed_min_ago": 12,
    "actions": [],
}


def reset_environment() -> None:
    INCIDENT["recovery_level"] = 0
    INCIDENT["actions"] = []


def _healthy() -> bool:
    return INCIDENT["recovery_level"] >= 2


@tool
def query_logs(service: str) -> str:
    """Return recent error log lines for a service."""
    if _healthy():
        return f"{service}: no errors in the last 5 minutes."
    return (
        f"{service} error logs (last 5m): 42x ERROR PaymentTimeout in PaymentClient.call(); "
        f"first occurrence coincides with deploy {INCIDENT['bad_deploy']} "
        f"{INCIDENT['deployed_min_ago']}m ago."
    )


@tool
def query_metrics(service: str) -> str:
    """Return current latency and error-rate metrics for a service."""
    if _healthy():
        return f"{service}: p99=210ms, error_rate=0.1%, saturation=nominal."
    error_rate = {0: "8.2%", 1: "2.4%"}.get(INCIDENT["recovery_level"], "0.1%")
    return f"{service}: p99=3400ms, error_rate={error_rate}, pool_saturation=high."


@tool
def recent_deploys(service: str) -> str:
    """Return the recent deploy history for a service."""
    return (
        f"{service}: {INCIDENT['bad_deploy']} deployed {INCIDENT['deployed_min_ago']}m ago by ci-bot; "
        f"previous stable release {INCIDENT['prev_deploy']}."
    )


@tool
def check_health(service: str) -> str:
    """Return the current health status of a service."""
    return f"{service}: HEALTHY" if _healthy() else f"{service}: DEGRADED (elevated error rate)"


def apply_remediation(service: str) -> str:
    """Privileged action (NOT an agent tool): advance recovery by one step.

    Step 1 rolls back the bad deploy; step 2 restarts the payment connection
    pool. Health is restored only after both.
    """
    INCIDENT["recovery_level"] += 1
    level = INCIDENT["recovery_level"]
    action = {
        1: f"rolled back {INCIDENT['bad_deploy']} -> {INCIDENT['prev_deploy']}",
        2: "drained and restarted the payment connection pool",
    }.get(level, "applied additional mitigation")
    INCIDENT["actions"].append(action)
    return f"Applied: {action}."


def diagnostic_tools() -> ToolRegistry:
    """Read-only tools available to the diagnostic agents."""
    return (
        ToolRegistry()
        .add(query_logs)
        .add(query_metrics)
        .add(recent_deploys)
        .add(check_health)
    )
