"""
Agent tasks for the Autonomous Research Analyst workflow.

Each task is a small unit of agent work; the stabilize DAG owns control flow,
durability, and recovery. Tasks call a real LLM (ollama.com cloud glm-5.2 by
default) via stabilize's LLM toolkit; researchers are genuine ReAct
tool-calling loops (AgentLoopTask) over the deterministic tools in tools.py.

Without OLLAMA_API_KEY the tasks fall back to deterministic offline stubs so
the whole DAG still runs end to end (proving the engine, not the model).
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from tools import knowledge_base, research_tools  # local module

from stabilize.llm import AgentLoopTask, ChatMessage, LLMClient
from stabilize.models.stage import StageExecution
from stabilize.streaming import emit_progress
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from stabilize.tasks.sub_workflow import SubWorkflowTask

MODEL = os.getenv("ANALYST_MODEL", "glm-5.2")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "https://ollama.com")
OUT_DIR = Path(os.getenv("ANALYST_DIR", "/tmp/stabilize-research-analyst"))
NUM_RESEARCHERS = 3


def _client() -> LLMClient | None:
    api_key = os.getenv("OLLAMA_API_KEY")
    if not api_key:
        return None
    return LLMClient(model=MODEL, base_url=OLLAMA_BASE_URL, api_key=api_key, api="ollama", timeout=180.0)


def _extract_json(text: str) -> Any:
    match = re.search(r"(\{.*\}|\[.*\])", text or "", re.S)
    if not match:
        raise ValueError("no JSON found")
    return json.loads(match.group(1))


def _first_float(text: str, default: float) -> float:
    match = re.search(r"(\d+(?:\.\d+)?)", text or "")
    return float(match.group(1)) if match else default


class PlannerTask(Task):
    """Decompose the research question into sub-questions and a strategy.

    On a refinement pass (the router jumped back here) it incorporates the
    reviewer's feedback to sharpen the plan — this is the workflow-level
    agent loop.

    Outputs: sub_questions (list), strategy, plan_pass.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        question = stage.context["question"]
        feedback = stage.context.get("refine_feedback", "")
        plan_pass = int(stage.context.get("_jump_count", 0))
        emit_progress(stage, f"planning (pass {plan_pass})", agent="planner")

        client = _client()
        if client is None:
            subs = [
                "How much GPU memory does the model need, and how many GPUs per replica?",
                "What decode throughput does each GPU option provide for the target load?",
                "What is the monthly cost of the cheapest option that meets the load?",
            ]
            return TaskResult.success(
                outputs={"sub_questions": subs, "strategy": "specs-then-arithmetic", "plan_pass": plan_pass}
            )

        extra = f"\n\nA reviewer asked for a deeper pass. Address this feedback:\n{feedback}" if feedback else ""
        prompt = (
            "You are a research planner. Decompose this question into exactly 3 concrete, "
            "independently-researchable sub-questions that can be answered with a specs "
            f"knowledge base and a calculator.\n\nQUESTION: {question}{extra}\n\n"
            'Return ONLY JSON: {"sub_questions": ["...","...","..."], "strategy": "one sentence"}'
        )
        try:
            resp = client.chat([ChatMessage(role="user", content=prompt)], temperature=0.2)
            data = _extract_json(resp.content)
            subs = [str(s) for s in data["sub_questions"]][:NUM_RESEARCHERS]
            while len(subs) < NUM_RESEARCHERS:
                subs.append(question)
            strategy = str(data.get("strategy", ""))
        except Exception as e:
            emit_progress(stage, f"planner fallback: {e}", agent="planner")
            subs = [f"{question} (aspect {i + 1})" for i in range(NUM_RESEARCHERS)]
            strategy = "fallback"

        emit_progress(stage, f"planned {len(subs)} sub-questions", agent="planner")
        return TaskResult.success(
            outputs={"sub_questions": subs, "strategy": strategy, "plan_pass": plan_pass}
        )


class ResearchAgentTask(AgentLoopTask):
    """A ReAct tool-calling agent researching one sub-question.

    Extends the engine's AgentLoopTask so the whole model -> tool-calls ->
    results -> model loop runs inside one durable task. Tools are the
    deterministic calculator + knowledge base.

    Outputs (under 'finding'): the agent's answer plus its tool trace.
    """

    def build_client(self, stage: StageExecution) -> Any:
        # The registry instantiates this task with no client, so supply the
        # module client here (AgentLoopTask.execute calls build_client).
        client = _client()
        if client is None:
            raise ValueError("no LLM client (OLLAMA_API_KEY unset)")
        return client

    def build_tools(self, stage: StageExecution) -> Any:
        return research_tools()

    def _messages(self, stage: StageExecution) -> list[ChatMessage]:
        subs = stage.context.get("sub_questions", [])
        idx = int(stage.context.get("instance_index", 0))
        idx = max(0, min(idx, len(subs) - 1)) if subs else 0
        question = subs[idx] if subs else stage.context.get("question", "")
        system = (
            "You are a meticulous research analyst. You MUST use the tools: call "
            "knowledge_base(name) for any spec (models: atlas-70b/atlas-13b; GPUs: "
            "h100/a100/l40s; and 'workload'), and calculate(expression) for any "
            "arithmetic. Do not guess numbers. When done, give a concise final answer "
            "that states the key figures you derived."
        )
        return [ChatMessage(role="system", content=system), ChatMessage(role="user", content=question)]

    def execute(self, stage: StageExecution) -> TaskResult:
        idx = int(stage.context.get("instance_index", 0))
        emit_progress(stage, f"researching sub-question {idx}", agent=f"researcher:{idx}")

        client = _client()
        if client is None:
            subs = stage.context.get("sub_questions", [])
            q = subs[idx] if idx < len(subs) else "?"
            # Offline: actually call the tools deterministically so the demo is real.
            wl = json.loads(knowledge_base("workload"))
            gpu = json.loads(knowledge_base("h100"))
            model = json.loads(knowledge_base("atlas-70b"))
            answer = (
                f"[offline] {q} — model weights {model['weights_gb']}GB need "
                f"{model['weights_gb'] // gpu['vram_gb'] + 1} x {gpu['name']} for memory; "
                f"load {wl['requests_per_sec'] * wl['tokens_per_request']} tok/s."
            )
            return TaskResult.success(outputs={"finding": answer, "researcher": idx})

        # Real ReAct loop via AgentLoopTask.
        stage.context.setdefault("output_key", "finding")
        stage.context.setdefault("max_iterations", 5)
        result = super().execute(stage)
        n_tools = len(result.outputs.get("tool_invocations", [])) if result.outputs else 0
        emit_progress(
            stage, f"researcher {idx} done ({n_tools} tool calls)", agent=f"researcher:{idx}", tool_calls=n_tools
        )
        # Tag the finding with the researcher index for the reducer gather.
        if result.outputs is not None:
            result.outputs.setdefault("researcher", idx)
        return result


class SynthesizerTask(Task):
    """Combine the researchers' findings (gathered by a fan-in reducer) into a
    single analysis. Reads 'finding' as a list produced by output_reducers.

    Outputs: synthesis.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        findings = stage.context.get("finding", [])
        if isinstance(findings, str):
            findings = [findings]
        emit_progress(stage, f"synthesizing {len(findings)} findings", agent="synthesizer")

        client = _client()
        if client is None:
            synthesis = "Offline synthesis of findings:\n- " + "\n- ".join(str(f)[:200] for f in findings)
            return TaskResult.success(outputs={"synthesis": synthesis, "n_findings": len(findings)})

        joined = "\n\n".join(f"Finding {i + 1}: {f}" for i, f in enumerate(findings))
        prompt = (
            "You are a lead analyst. Synthesize these research findings into a single, "
            "concrete recommendation with the key numbers. Be decisive.\n\n" + joined
        )
        try:
            resp = client.chat([ChatMessage(role="user", content=prompt)], temperature=0.2)
            synthesis = resp.content
        except Exception as e:
            synthesis = f"[synthesis error: {e}] " + joined[:500]
        emit_progress(stage, "synthesis complete", agent="synthesizer")
        return TaskResult.success(outputs={"synthesis": synthesis, "n_findings": len(findings)})


class VerifierTask(Task):
    """Adversarially review the synthesis and emit a confidence score.

    Two of these run in parallel with different lenses; a DISCRIMINATOR join
    downstream proceeds on whichever verdict lands first.

    Outputs: verdict, confidence (0-1), lens.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        lens = stage.context.get("lens", "correctness")
        synthesis = stage.context.get("synthesis", "")
        emit_progress(stage, f"verifying ({lens})", agent=f"verifier:{lens}")

        client = _client()
        if client is None:
            # Both above the default 0.85 threshold so the offline run accepts
            # after the single mandatory refine pass.
            conf = 0.92 if lens == "correctness" else 0.88
            return TaskResult.success(outputs={"verdict": "plausible", "confidence": conf, "lens": lens})

        prompt = (
            f"You are a skeptical reviewer focused on {lens}. Review this analysis for "
            "arithmetic and specs errors. Reply with ONLY JSON: "
            '{"verdict": "one sentence", "confidence": 0.0-1.0}\n\n'
            f"ANALYSIS:\n{synthesis}"
        )
        try:
            resp = client.chat([ChatMessage(role="user", content=prompt)], temperature=0.1)
            data = _extract_json(resp.content)
            verdict = str(data.get("verdict", ""))
            confidence = float(data.get("confidence", _first_float(resp.content, 0.8)))
        except Exception:
            verdict, confidence = "unparaseable", 0.8
        confidence = max(0.0, min(1.0, confidence))
        emit_progress(stage, f"verdict conf={confidence:.2f}", agent=f"verifier:{lens}", confidence=confidence)
        return TaskResult.success(outputs={"verdict": verdict, "confidence": confidence, "lens": lens})


class RouterTask(Task):
    """Decide whether to refine (loop back to the planner) or proceed.

    Refines while under the pass budget: a low reviewer confidence OR a
    mandatory first refinement pass sends control back to 'plan' with
    feedback (a bounded, workflow-level agent cycle). Otherwise it proceeds.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        confidence = float(stage.context.get("confidence", 1.0))
        verdict = stage.context.get("verdict", "")
        # Read the pass count from a FRESH store read of the plan stage. The
        # merged context absorbs the ancestor output on the first pass and
        # (because reset keeps context keys) shadows the fresh value after a
        # jump-reset; the in-memory execution snapshot is likewise stale in a
        # cycle. The orchestrator gives an authoritative current read.
        attempt = 0
        try:
            from stabilize.orchestrator import Orchestrator

            orch = Orchestrator.get_instance()
            fresh = orch.get_execution(stage.execution.id) if orch and stage.execution else None
            if fresh is not None:
                plan_stage = next((s for s in fresh.stages if s.ref_id == "plan"), None)
                if plan_stage is not None and plan_stage.outputs:
                    attempt = int(plan_stage.outputs.get("plan_pass", 0))
        except Exception:
            pass
        threshold = float(stage.context.get("confidence_threshold", 0.85))
        min_passes = int(stage.context.get("min_passes", 1))
        max_passes = int(stage.context.get("max_passes", 3))

        emit_progress(
            stage, f"routing: conf={confidence:.2f} attempt={attempt}", agent="router", confidence=confidence
        )

        needs_refine = (attempt < min_passes or confidence < threshold) and attempt < max_passes
        if needs_refine:
            feedback = f"Reviewer confidence was {confidence:.2f}. Concern: {verdict}. Tighten the numbers."
            emit_progress(stage, f"refining (pass {attempt + 1})", agent="router")
            return TaskResult.jump_to("plan", context={"refine_feedback": feedback})

        emit_progress(stage, "accepted — proceeding to approval", agent="router")
        return TaskResult.success(outputs={"accepted": True, "final_confidence": confidence})


# ---- Report sub-workflow tasks (the child DAG) ----


class DraftReportTask(Task):
    """Child-workflow task: draft the report body from the synthesis."""

    def execute(self, stage: StageExecution) -> TaskResult:
        synthesis = stage.context.get("synthesis", "")
        client = _client()
        if client is None or not synthesis:
            body = f"# Findings\n\n{synthesis or '(no synthesis)'}\n"
            return TaskResult.success(outputs={"draft": body})
        prompt = "Turn this analysis into a short markdown report body with a heading and bullet points:\n\n" + synthesis
        try:
            body = client.chat([ChatMessage(role="user", content=prompt)], temperature=0.3).content
        except Exception:
            body = f"# Findings\n\n{synthesis}\n"
        return TaskResult.success(outputs={"draft": body})


class PolishReportTask(Task):
    """Child-workflow task: add an executive summary and write the report file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        draft = stage.context.get("draft", "")
        question = stage.context.get("question", "")
        client = _client()
        summary = "Executive summary unavailable offline."
        if client is not None and draft:
            try:
                summary = client.chat(
                    [ChatMessage(role="user", content="Write a 2-sentence executive summary of:\n\n" + draft)],
                    temperature=0.3,
                ).content
            except Exception:
                pass
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        report = f"# Research Report\n\n**Question:** {question}\n\n## Executive Summary\n\n{summary}\n\n{draft}\n"
        path = OUT_DIR / "report.md"
        path.write_text(report)
        return TaskResult.success(outputs={"report_path": str(path), "summary": summary})


def _build_report_child_config(synthesis: str, question: str) -> dict[str, Any]:
    """Fresh child DAG (draft -> polish) for the report sub-workflow.

    Built with real StageExecution objects at task time — NOT passed through
    stage context, which is JSON-serialized (objects would come back as
    strings).
    """
    from stabilize.models.task import TaskExecution

    def _t(name: str, impl: str) -> Any:
        return TaskExecution.create(name=name, implementing_class=impl, stage_start=True, stage_end=True)

    draft = StageExecution(
        ref_id="draft", type="draft_report", name="Draft Report", tasks=[_t("draft", "draft_report")]
    )
    polish = StageExecution(
        ref_id="polish",
        type="polish_report",
        name="Polish Report",
        requisite_stage_ref_ids={"draft"},
        tasks=[_t("polish", "polish_report")],
    )
    return {
        "application": "research-report",
        "name": "Report Sub-Workflow",
        "context": {"synthesis": synthesis, "question": question},
        "stages": [draft, polish],
    }


class ReportSubWorkflowTask(SubWorkflowTask):
    """Run report generation as a real child workflow (a subgraph).

    On the first execution it builds the child DAG fresh (with the runtime
    synthesis/question) and hands it to SubWorkflowTask to start and poll.
    """

    @property
    def aliases(self) -> list[str]:
        return ["report_subworkflow"]

    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.context.get("_sub_workflow_id") is None:
            stage.context["_sub_workflow_config"] = _build_report_child_config(
                stage.context.get("synthesis", ""),
                stage.context.get("question", ""),
            )
            emit_progress(stage, "spawning report sub-workflow", agent="reporter")
        return super().execute(stage)
