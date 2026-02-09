"""
Stabilize Overhead Benchmark
=============================
Measures overhead of running 32 parallel HTTP tasks through Stabilize
vs raw ThreadPoolExecutor.

Key: Uses HTTP/1.0 to avoid keepalive connection blocking artifacts.
"""

import json
import os
import statistics
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.request import urlopen

# ──────────────────────────────────────────────────
# 1. LOCAL HTTP SERVER (threaded, HTTP/1.0)
# ──────────────────────────────────────────────────


class BenchmarkHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def do_GET(self):
        response = json.dumps({"ok": True, "t": time.time()}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format, *args):
        pass


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    request_queue_size = 128


NUM_TASKS = 32

server = ThreadedHTTPServer(("127.0.0.1", 0), BenchmarkHandler)
port = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE_URL = f"http://127.0.0.1:{port}"

print("=" * 70)
print("STABILIZE OVERHEAD BENCHMARK")
print("=" * 70)
print(f"  Server:  {BASE_URL} (threaded, HTTP/1.0)")
print(f"  Tasks:   {NUM_TASKS} parallel HTTP GET requests")
print(f"  Date:    {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print()

# ──────────────────────────────────────────────────
# 2. BASELINE: Single request latency
# ──────────────────────────────────────────────────

print("─" * 70)
print("PHASE 1: Single HTTP request latency")
print("─" * 70)

for _ in range(20):
    urlopen(f"{BASE_URL}/warmup").read()

single_times = []
for i in range(100):
    t0 = time.perf_counter()
    urlopen(f"{BASE_URL}/s/{i}").read()
    t1 = time.perf_counter()
    single_times.append((t1 - t0) * 1000)

avg_single = statistics.mean(single_times)
p50 = statistics.median(single_times)
p99 = sorted(single_times)[98]
print(f"  avg={avg_single:.2f}ms  p50={p50:.2f}ms  p99={p99:.2f}ms  (100 samples)")

# ──────────────────────────────────────────────────
# 3. BASELINE: Parallel (10 workers, matching Stabilize)
# ──────────────────────────────────────────────────

print()
print("─" * 70)
print("PHASE 2: Raw baseline — 32 parallel reqs (10 worker threads)")
print("─" * 70)

pool = ThreadPoolExecutor(max_workers=10)
# Warm pool threads
futs = [pool.submit(lambda: urlopen(f"{BASE_URL}/w").read()) for _ in range(10)]
for f in futs:
    f.result()


def raw_call(i):
    t0 = time.perf_counter()
    urlopen(f"{BASE_URL}/raw/{i}").read()
    t1 = time.perf_counter()
    return i, t0, t1, (t1 - t0) * 1000


raw_wall_times = []
for run in range(10):
    t0 = time.perf_counter()
    futures = [pool.submit(raw_call, i) for i in range(NUM_TASKS)]
    results = [f.result() for f in futures]
    t1 = time.perf_counter()
    raw_wall_times.append((t1 - t0) * 1000)

    if run == 9:
        raw_per_task = [d for _, _, _, d in results]
        raw_starts = [s for _, s, _, _ in results]
        raw_ends = [e for _, _, e, _ in results]

pool.shutdown(wait=False)

raw_avg = statistics.mean(raw_wall_times)
raw_best = min(raw_wall_times)
raw_median = statistics.median(raw_wall_times)
raw_start_spread = (max(raw_starts) - min(raw_starts)) * 1000

print(f"  Wall (10 runs): min={raw_best:.0f}ms  avg={raw_avg:.0f}ms  median={raw_median:.0f}ms")
print(f"  Per-task: avg={statistics.mean(raw_per_task):.1f}ms  max={max(raw_per_task):.1f}ms")
print(f"  Start spread: {raw_start_spread:.1f}ms")
ideal_parallel = avg_single * (NUM_TASKS / 10)  # 32 tasks / 10 workers = 3.2 batches
print(f"  Ideal (3.2 batches × {avg_single:.1f}ms): {ideal_parallel:.0f}ms")

# ──────────────────────────────────────────────────
# 4. STABILIZE WORKFLOW
# ──────────────────────────────────────────────────

print()
print("─" * 70)
print("PHASE 3: Stabilize — 32 parallel HTTP tasks via workflow engine")
print("─" * 70)

from stabilize import (
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.events import (
    EventReplayer,
    SqliteEventStore,
    WorkflowTimelineProjection,
    configure_event_sourcing,
    get_event_bus,
    reset_event_bus,
    reset_event_recorder,
)
from stabilize.models.status import WorkflowStatus
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.tasks.http import HTTPTask

db_path = "/tmp/stabilize_bench.db"
events_db_path = "/tmp/stabilize_bench_events.db"
for p in [db_path, events_db_path]:
    if os.path.exists(p):
        os.unlink(p)

store = SqliteWorkflowStore(f"sqlite:///{db_path}", create_tables=True)
queue = SqliteQueue(f"sqlite:///{db_path}")
queue._create_table()

registry = TaskRegistry()
registry.register("http", HTTPTask)


class NoOpTask(Task):
    def execute(self, stage):
        return TaskResult.success(outputs={"trigger": True})


registry.register("noop", NoOpTask)

# Event sourcing
reset_event_bus()
reset_event_recorder()
event_store = SqliteEventStore(f"sqlite:///{events_db_path}", create_tables=True)
recorder = configure_event_sourcing(event_store)

config = QueueProcessorConfig(poll_frequency_ms=5, max_workers=10)
processor = QueueProcessor(queue, config=config, store=store, task_registry=registry)
orchestrator = Orchestrator(queue)

# Build workflow: trigger → 32 parallel HTTP stages
stages = [
    StageExecution(
        ref_id="trigger",
        type="noop",
        name="Trigger",
        tasks=[TaskExecution.create(name="Trigger", implementing_class="noop", stage_start=True, stage_end=True)],
        context={},
    ),
]
for i in range(NUM_TASKS):
    stages.append(
        StageExecution(
            ref_id=f"http_{i}",
            type="http",
            name=f"HTTP-{i}",
            tasks=[TaskExecution.create(name=f"HTTP-{i}", implementing_class="http", stage_start=True, stage_end=True)],
            requisite_stage_ref_ids={"trigger"},
            context={"url": f"{BASE_URL}/stabilize/{i}", "parse_json": True, "timeout": 10},
        ),
    )

workflow = Workflow.create(application="benchmark", name="32 Parallel HTTP", stages=stages)

# Timeline projection
timeline = WorkflowTimelineProjection(workflow.id)
bus = get_event_bus()
bus.subscribe("bench_timeline", timeline.apply)

# Execute
store.store(workflow)
t0 = time.perf_counter()
orchestrator.start(workflow)
processor.start()

deadline = time.time() + 60.0
while time.time() < deadline:
    wf = store.retrieve(workflow.id)
    if wf.status.is_complete:
        break
    time.sleep(0.01)

t1 = time.perf_counter()
processor.stop()

stab_wall = (t1 - t0) * 1000
wf = store.retrieve(workflow.id)
assert wf.status == WorkflowStatus.SUCCEEDED, f"Workflow failed: {wf.status}"
print(f"  Status: {wf.status.name}")
print(f"  Wall clock: {stab_wall:.0f}ms")

# ──────────────────────────────────────────────────
# 5. DB TIMESTAMP ANALYSIS
# ──────────────────────────────────────────────────

print()
print("─" * 70)
print("PHASE 4: Parallelism verification (DB timestamps)")
print("─" * 70)

http_stages = sorted(
    [s for s in wf.stages if s.ref_id.startswith("http_")],
    key=lambda s: s.start_time or 0,
)
assert len(http_stages) == NUM_TASKS

starts = [s.start_time for s in http_stages if s.start_time]
ends = [s.end_time for s in http_stages if s.end_time]
stage_durs = [s.end_time - s.start_time for s in http_stages if s.start_time and s.end_time]

start_spread = max(starts) - min(starts)
window = max(ends) - min(starts)

# Count max concurrency
ev = []
for s in http_stages:
    if s.start_time and s.end_time:
        ev.append((s.start_time, +1))
        ev.append((s.end_time, -1))
ev.sort()
max_conc = cur = 0
for _, d in ev:
    cur += d
    max_conc = max(max_conc, cur)

# Task-level
task_durs = []
for s in http_stages:
    for t in s.tasks:
        if t.start_time and t.end_time:
            task_durs.append(t.end_time - t.start_time)

print(f"  Stage start spread:    {start_spread}ms")
print(f"  Stage exec window:     {window}ms")
print(
    f"  Per-stage duration:    avg={statistics.mean(stage_durs):.0f}ms  min={min(stage_durs)}ms  max={max(stage_durs)}ms"
)
print(
    f"  Per-task duration:     avg={statistics.mean(task_durs):.0f}ms  min={min(task_durs)}ms  max={max(task_durs)}ms"
)
print(f"  Max concurrency:       {max_conc}")

# Show first 5 stages timing
print()
print("  Stage timing (first 5 and last 5):")
for s in http_stages[:5]:
    d = s.end_time - s.start_time if s.start_time and s.end_time else 0
    offset = s.start_time - min(starts) if s.start_time else 0
    print(f"    {s.ref_id:>8}: start=+{offset}ms  dur={d}ms")
print("    ...")
for s in http_stages[-5:]:
    d = s.end_time - s.start_time if s.start_time and s.end_time else 0
    offset = s.start_time - min(starts) if s.start_time else 0
    print(f"    {s.ref_id:>8}: start=+{offset}ms  dur={d}ms")

# ──────────────────────────────────────────────────
# 6. EVENT SOURCING VERIFICATION
# ──────────────────────────────────────────────────

print()
print("─" * 70)
print("PHASE 5: Event sourcing verification")
print("─" * 70)

replayer = EventReplayer(event_store)
state = replayer.rebuild_workflow_state(workflow.id)
print(f"  Replayed status:   {state.get('status', '?')}")
print(f"  Replayed stages:   {len(state.get('stages', {}))}")

# Get all events for this workflow

events = list(event_store.get_events_for_workflow(workflow.id))
print(f"  Total events:      {len(events)}")

type_counts = Counter(e.event_type.value for e in events)
print(f"  Event breakdown:   {dict(type_counts)}")

# Timeline
tl = timeline.get_state()
print(f"  Timeline entries:  {len(tl.entries)}")
print(f"  Timeline duration: {tl.total_duration_ms}ms")

# Verify all HTTP stages in replay
# Replay keys are stage ULIDs, not ref_ids — build a ref_id lookup
replayed_stages = state.get("stages", {})
replayed_by_ref = {v.get("ref_id"): v for v in replayed_stages.values()}
for s in http_stages:
    assert s.ref_id in replayed_by_ref, f"Missing {s.ref_id} in replay"
    assert replayed_by_ref[s.ref_id].get("status") == "SUCCEEDED", (
        f"Replay {s.ref_id}: {replayed_by_ref[s.ref_id].get('status')}"
    )
print(f"  All {NUM_TASKS} HTTP stages verified via event replay ✓")

# ──────────────────────────────────────────────────
# 7. OVERHEAD SUMMARY
# ──────────────────────────────────────────────────

wf_db_dur = wf.end_time - wf.start_time if wf.start_time and wf.end_time else 0
trigger = next(s for s in wf.stages if s.ref_id == "trigger")
trigger_dur = trigger.end_time - trigger.start_time if trigger.start_time and trigger.end_time else 0

overhead_ms = stab_wall - raw_avg
overhead_pct = (overhead_ms / raw_avg) * 100 if raw_avg > 0 else 0

# Per-message estimate
# 1 StartWorkflow + 33*(StartStage+StartTask+RunTask+CompleteTask+CompleteStage) + 1 CompleteWorkflow
est_messages = 1 + 33 * 5 + 1  # = 167
per_msg = wf_db_dur / est_messages if est_messages > 0 else 0

# Net orchestration overhead = wall - actual HTTP time
# The actual HTTP time is what the tasks spent doing HTTP calls
net_http_time = statistics.mean(task_durs)  # avg task time is the HTTP time per task
# With 10 workers, 32 tasks → ~3.2 batches of HTTP
approx_http_wall = net_http_time * (NUM_TASKS / 10)
orchestration_overhead = wf_db_dur - approx_http_wall if wf_db_dur else 0

print()
print("=" * 70)
print("RESULTS")
print("=" * 70)
print(f"""
  ┌─────────────────────────────────────────────────────────────────┐
  │                        BASELINE                                │
  │  Single request latency:       {avg_single:>6.2f}ms                      │
  │  32 reqs / 10 threads:         {raw_avg:>6.0f}ms (avg of 10 runs)      │
  │  Ideal (3.2 × {avg_single:.1f}ms):          {ideal_parallel:>6.0f}ms                      │
  │                                                                 │
  │                        STABILIZE                                │
  │  Wall clock:                   {stab_wall:>6.0f}ms                      │
  │  DB recorded duration:         {wf_db_dur:>6}ms                      │
  │  Trigger stage:                {trigger_dur:>6}ms                      │
  │  HTTP stage window:            {window:>6}ms                      │
  │  Max concurrency:              {max_conc:>6}/{NUM_TASKS}                     │
  │                                                                 │
  │                        OVERHEAD                                 │
  │  Wall overhead vs raw:         {overhead_ms:>+6.0f}ms ({overhead_pct:>+.0f}%)             │
  │  Orchestration overhead:       ~{orchestration_overhead:>5.0f}ms                      │
  │  Per-msg processing:           ~{per_msg:>5.1f}ms ({est_messages} msgs)        │
  │                                                                 │
  │                        EVENT SOURCING                           │
  │  Events recorded:              {len(events):>6}                      │
  │  Replay verified:              {NUM_TASKS:>6} stages ✓                │
  │  Timeline entries:             {len(tl.entries):>6}                      │
  └─────────────────────────────────────────────────────────────────┘
""")

# Cleanup
server.shutdown()
for p in [db_path, events_db_path]:
    if os.path.exists(p):
        os.unlink(p)
