"""
Agent tasks for the Autonomous Incident Commander.

The engine owns control flow, durability, and the audit trail; each task is a
small unit of agent or operator work. Diagnostic agents are genuine ReAct
tool-calling loops over the simulated environment in tools.py. Model access is
ollama.com cloud glm-5.2 via OLLAMA_API_KEY, with deterministic offline stubs
so the whole incident still runs end to end without a key.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from tools import (
    INCIDENT,
    apply_remediation,
    check_health,
    diagnostic_tools,
    query_logs,
    query_metrics,
    recent_deploys,
)

from stabilize.llm import AgentLoopTask, ChatMessage, LLMClient
from stabilize.models.stage import StageExecution
from stabilize.streaming import emit_progress
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

MODEL = os.getenv("INCIDENT_MODEL", "glm-5.2")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "https://ollama.com")
OUT_DIR = Path(os.getenv("INCIDENT_DIR", "/tmp/stabilize-incident"))
MAX_REMEDIATIONS = int(os.getenv("INCIDENT_MAX_REMEDIATIONS", "3"))

# Each diagnostic agent focuses on one angle of the investigation.
DIAG_FOCUS = [
    ("error logs", "Investigate the ERROR LOGS. Call query_logs and check_health."),
    ("metrics", "Investigate the METRICS. Call query_metrics and check_health."),
    ("recent deploys", "Investigate RECENT DEPLOYS as a possible cause. Call recent_deploys and check_health."),
]


def _client() -> LLMClient | None:
    api_key = os.getenv("OLLAMA_API_KEY")
    if not api_key:
        return None
    return LLMClient(model=MODEL, base_url=OLLAMA_BASE_URL, api_key=api_key, api="ollama", timeout=180.0)


class TriageAgent(AgentLoopTask):
    """First responder: confirm the alert and set severity, using live tools."""

    def build_client(self, stage: StageExecution) -> Any:
        client = _client()
        if client is None:
            raise ValueError("no LLM client")
        return client

    def build_tools(self, stage: StageExecution) -> Any:
        from stabilize.llm import ToolRegistry

        return ToolRegistry().add(check_health).add(query_metrics)

    def _messages(self, stage: StageExecution) -> list[ChatMessage]:
        service = stage.context["service"]
        alert = stage.context["alert"]
        return [
            ChatMessage(role="system", content=(
                "You are the on-call incident commander. Confirm the alert with the tools "
                "(check_health, query_metrics) and state the severity (SEV1/SEV2/SEV3) and the "
                "affected service in one short line.")),
            ChatMessage(role="user", content=f"Alert: {alert}\nAffected service: {service}"),
        ]

    def execute(self, stage: StageExecution) -> TaskResult:
        emit_progress(stage, f"triaging alert for '{stage.context['service']}'", agent="commander")
        if _client() is None:
            summary = f"SEV2 confirmed on {stage.context['service']}: health DEGRADED, error_rate elevated."
            return TaskResult.success(outputs={"triage": summary})
        stage.context.setdefault("output_key", "triage")
        stage.context.setdefault("max_iterations", 3)
        result = super().execute(stage)
        emit_progress(stage, "triage complete", agent="commander")
        return result


class DiagnosticAgent(AgentLoopTask):
    """One tool-using investigator focused on a single angle."""

    def build_client(self, stage: StageExecution) -> Any:
        client = _client()
        if client is None:
            raise ValueError("no LLM client")
        return client

    def build_tools(self, stage: StageExecution) -> Any:
        return diagnostic_tools()

    def _messages(self, stage: StageExecution) -> list[ChatMessage]:
        idx = int(stage.context.get("instance_index", 0))
        focus, instruction = DIAG_FOCUS[idx % len(DIAG_FOCUS)]
        service = stage.context["service"]
        return [
            ChatMessage(role="system", content=(
                "You are an SRE diagnosing a production incident. Use ONLY the tools to gather "
                "evidence — do not guess. In one or two sentences, report what you found and "
                "whether it points to a root cause.")),
            ChatMessage(role="user", content=f"Service: {service}. Focus: {focus}. {instruction}"),
        ]

    def execute(self, stage: StageExecution) -> TaskResult:
        idx = int(stage.context.get("instance_index", 0))
        focus = DIAG_FOCUS[idx % len(DIAG_FOCUS)][0]
        emit_progress(stage, f"investigating {focus}", agent=f"diag:{focus}")
        if _client() is None:
            service = stage.context["service"]
            evidence = [query_logs(service), query_metrics(service), recent_deploys(service)][idx % 3]
            return TaskResult.success(outputs={"finding": f"[{focus}] {evidence}", "focus": focus})
        stage.context.setdefault("output_key", "finding")
        stage.context.setdefault("max_iterations", 3)
        result = super().execute(stage)
        n = len(result.outputs.get("tool_invocations", [])) if result.outputs else 0
        emit_progress(stage, f"{focus}: reported ({n} tool calls)", agent=f"diag:{focus}")
        if result.outputs is not None:
            result.outputs.setdefault("focus", focus)
        return result


class RootCauseTask(Task):
    """Reach consensus: synthesize findings into a root cause + proposed fix + risk."""

    def execute(self, stage: StageExecution) -> TaskResult:
        findings = stage.context.get("finding", [])
        if isinstance(findings, str):
            findings = [findings]
        emit_progress(stage, f"correlating {len(findings)} findings", agent="commander")

        client = _client()
        if client is None:
            return TaskResult.success(outputs={
                "root_cause": f"Bad deploy {INCIDENT['bad_deploy']} introduced payment timeouts.",
                "proposed_action": "Roll back the deploy, then restart the payment connection pool.",
                "risk": "medium",
                "n_findings": len(findings),
            })

        joined = "\n".join(f"- {f}" for f in findings)
        prompt = (
            "You are the incident commander. From these diagnostic findings, state in JSON the "
            "most likely ROOT CAUSE, a concrete PROPOSED_ACTION to remediate it, and a RISK level "
            '(low/medium/high). Findings:\n' + joined + '\n\n'
            'Return ONLY: {"root_cause": "...", "proposed_action": "...", "risk": "low|medium|high"}'
        )
        try:
            import json
            import re
            resp = client.chat([ChatMessage(role="user", content=prompt)], temperature=0.2)
            m = re.search(r"\{.*\}", resp.content, re.S)
            data = json.loads(m.group(0)) if m else {}
        except Exception:
            data = {}
        emit_progress(stage, f"root cause identified (risk={data.get('risk', 'medium')})", agent="commander")
        return TaskResult.success(outputs={
            "root_cause": data.get("root_cause", f"Bad deploy {INCIDENT['bad_deploy']}."),
            "proposed_action": data.get("proposed_action", "Roll back the deploy."),
            "risk": data.get("risk", "medium"),
            "n_findings": len(findings),
        })


class RemediateTask(Task):
    """Privileged operator action: apply one remediation step to the environment."""

    def execute(self, stage: StageExecution) -> TaskResult:
        service = stage.context["service"]
        proposed = stage.context.get("proposed_action", "")
        emit_progress(stage, f"applying remediation: {proposed[:80]}", agent="operator")
        result = apply_remediation(service)
        emit_progress(stage, result, agent="operator")
        return TaskResult.success(outputs={"remediation": result})


class VerifyTask(Task):
    """Verify recovery; if the service is still degraded, loop back to re-investigate."""

    def execute(self, stage: StageExecution) -> TaskResult:
        service = stage.context["service"]
        health = check_health(service)
        healthy = "HEALTHY" in health
        emit_progress(stage, f"post-remediation health: {health}", agent="commander", healthy=healthy)

        if healthy:
            return TaskResult.success(outputs={"healed": True, "final_health": health})

        if INCIDENT["recovery_level"] < MAX_REMEDIATIONS:
            emit_progress(stage, "still degraded — re-investigating", agent="commander")
            return TaskResult.jump_to("triage", context={
                "reinvestigate": "prior remediation did not fully restore health",
            })

        return TaskResult.terminal(
            error="Automated remediation exhausted; escalating to a human on-call.",
            context={"healed": False, "final_health": health},
        )


class PostmortemTask(Task):
    """Write a short incident post-mortem report to disk."""

    def execute(self, stage: StageExecution) -> TaskResult:
        service = stage.context["service"]
        alert = stage.context.get("alert", "")
        root_cause = stage.context.get("root_cause", "")
        emit_progress(stage, "writing post-mortem", agent="commander")

        actions = "\n".join(f"- {a}" for a in INCIDENT["actions"]) or "- (none)"
        client = _client()
        narrative = ""
        if client is not None:
            try:
                narrative = client.chat([ChatMessage(role="user", content=(
                    f"Write a 3-sentence incident post-mortem summary. Service: {service}. "
                    f"Root cause: {root_cause}. Remediations taken:\n{actions}"))],
                    temperature=0.3).content
            except Exception:
                pass

        OUT_DIR.mkdir(parents=True, exist_ok=True)
        report = (
            f"# Incident Post-Mortem: {service}\n\n"
            f"**Alert:** {alert}\n\n"
            f"**Root cause:** {root_cause}\n\n"
            f"## Remediations applied\n{actions}\n\n"
            f"## Summary\n{narrative or 'Service recovered after remediation.'}\n"
        )
        path = OUT_DIR / "postmortem.md"
        path.write_text(report)
        emit_progress(stage, f"post-mortem written to {path}", agent="commander")
        return TaskResult.success(outputs={"postmortem_path": str(path)})
