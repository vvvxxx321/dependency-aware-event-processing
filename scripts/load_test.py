"""LIVE bounded load test: N unique source events (half CreatePaper, half dependent MakeComment), shuffled.

Each event fans out to 2 consumers -> 2N consumer tasks. MakeComment@Search and MakeComment@Reporting both
depend on CreatePaper@Search (same PaperId). Measures counts, per-task latency from DynamoDB timestamps,
side-effect correctness from projections, and CloudWatch metrics for the run window.
N > 1000 requires --allow-large (explicit approval). Sends via send_message_batch (<=10 per call).

Bounded load generation (all optional; defaults reproduce the original unbounded 10/100-event runs):
  --rate R          cap sends at R source events/s
  --wave W          send in waves of W events; each wave is a checkpoint (sent.jsonl, progress.jsonl)
  --max-inflight M  before each wave, wait until non-terminal tasks <= M (closed-loop backpressure)
Checkpoints: workload.json (full bodies) is written before any send and sent.jsonl is appended per chunk,
so `--verify DIR` can re-run the wait/verification phase for an interrupted run without resending anything.
"""
from __future__ import annotations

import json
import random
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_common import Live, dump, parser, results_dir

CONSUMERS = ("SearchService", "ReportingService")


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


def summarize(values):
    return {"count": len(values), "p50": pct(values, 50), "p95": pct(values, 95), "p99": pct(values, 99),
            "max": max(values) if values else None, "mean": round(statistics.mean(values), 1) if values else None}


def build_workload(n: int, seed: int):
    rng = random.Random(seed)  # seed controls arrival order only
    run = uuid.uuid4().hex[:8]
    # PaperIds must be unique per run: a seed-derived base made the 10- and 100-event runs share 5 PaperIds
    # (2026-10-21), mixing two runs' events in the same projections. Derive the base from the run id instead.
    base = 10**12 + int(run, 16) * 10**5
    events, papers = [], []
    for i in range(n // 2):
        paper = base + i
        papers.append(paper)
        ts = int(time.time() * 1000)
        create = {"EventId": str(uuid.uuid4()), "ProducerId": "ContentService", "Action": "CreatePaper",
                  "Timestamp": ts, "Parameters": {"PaperId": paper, "LoadRun": run}}
        comment = {"EventId": str(uuid.uuid4()), "ProducerId": "CommentService", "Action": "MakeComment",
                   "Timestamp": ts, "Parameters": {"PaperId": paper, "CommentId": i, "LoadRun": run}}
        events.append((paper, create, comment))
    flat = [e for _, c, m in events for e in (c, m)]
    rng.shuffle(flat)  # comments frequently arrive before their paper
    return run, events, flat


def batch_get(live: Live, keys):
    out = []
    table = live.out["ReplicationLogTableName"]
    for i in range(0, len(keys), 100):
        req = {table: {"Keys": keys[i:i + 100], "ConsistentRead": True}}
        while req:
            resp = live.ddb.batch_get_item(RequestItems=req)
            out.extend(resp["Responses"].get(table, []))
            req = resp.get("UnprocessedKeys") or None
            if req:
                time.sleep(0.2)
    return out


def cloudwatch(live: Live, start: datetime, end: datetime):
    cw = live.session.client("cloudwatch")
    queries = []

    def add(ns, metric, dims, stat, label):
        queries.append({"Id": f"m{len(queries)}", "Label": label, "ReturnData": True, "MetricStat": {
            "Metric": {"Namespace": ns, "MetricName": metric, "Dimensions": [{"Name": k, "Value": v} for k, v in dims.items()]},
            "Period": 60, "Stat": stat}})
    for key in ("IngestFunctionName", "ResolverFunctionName", "DispatchFunctionName", "CompletionFunctionName",
                "SearchConsumerFunctionName", "ReportingConsumerFunctionName"):
        fn = live.out[key]
        short = key.replace("FunctionName", "")
        for metric, stat in (("Invocations", "Sum"), ("Errors", "Sum"), ("Throttles", "Sum"), ("Duration", "Average"),
                             ("Duration", "Maximum"), ("Duration", "p95"), ("ConcurrentExecutions", "Maximum")):
            add("AWS/Lambda", metric, {"FunctionName": fn}, stat, f"{short}.{metric}.{stat}")
        if short in ("Resolver", "Dispatch"):
            add("AWS/Lambda", "IteratorAge", {"FunctionName": fn}, "Maximum", f"{short}.IteratorAge.Maximum")
    q = live.out["IngressQueueUrl"].rsplit("/", 1)[1]
    for metric, stat in (("NumberOfMessagesSent", "Sum"), ("NumberOfMessagesDeleted", "Sum"),
                         ("ApproximateAgeOfOldestMessage", "Maximum"), ("ApproximateNumberOfMessagesVisible", "Maximum")):
        add("AWS/SQS", metric, {"QueueName": q}, stat, f"IngressQueue.{metric}.{stat}")
    for key in ("ReplicationLogTableName", "DependencyWaitersTableName", "ConsumerProjectionTableName"):
        add("AWS/DynamoDB", "WriteThrottleEvents", {"TableName": live.out[key]}, "Sum", f"{key}.WriteThrottleEvents")
        add("AWS/DynamoDB", "ConsumedWriteCapacityUnits", {"TableName": live.out[key]}, "Sum", f"{key}.ConsumedWCU")
    ns = f"EventPlatform/{live.out.get('Environment', 'dev')}"
    for name, stat in (("EndToEndLatencyMs", "p50"), ("EndToEndLatencyMs", "p95"), ("TasksCompleted", "Sum"),
                       ("EventsIngested", "Sum"), ("DuplicateEvents", "Sum"), ("SideEffectsApplied", "Sum"),
                       ("DuplicateDeliveries", "Sum")):
        comp = {"EndToEndLatencyMs": "completion", "TasksCompleted": "completion", "EventsIngested": "ingest",
                "DuplicateEvents": "ingest"}.get(name, "consumer")
        add(ns, name, {"Component": comp}, stat, f"custom.{name}.{stat}")
    results = {}
    for i in range(0, len(queries), 400):
        resp = cw.get_metric_data(MetricDataQueries=queries[i:i + 400], StartTime=start, EndTime=end)
        for r in resp["MetricDataResults"]:
            results[r["Label"]] = {"timestamps": [t.isoformat() for t in r["Timestamps"]], "values": r["Values"]}
    return results


class Tracker:
    """Polls only tasks that are not yet terminal; terminal items are kept for the final analysis."""

    def __init__(self, live: Live):
        self.live, self.keys, self.done, self.latest = live, {}, {}, {}

    def add(self, events):
        for e in events:
            for c in CONSUMERS:
                self.keys[(e["EventId"], c)] = self.live.task_key(e, c)

    def poll(self):
        open_keys = [k for ident, k in self.keys.items() if ident not in self.done]
        for t in batch_get(self.live, open_keys):
            ident = (t["EventId"], t["ConsumerId"])
            self.latest[ident] = t
            if t["Status"] in ("COMPLETED", "FAILED"):
                self.done[ident] = t
        counts = {}
        for t in self.latest.values():
            counts[t["Status"]] = counts.get(t["Status"], 0) + 1
        return counts

    def inflight(self):
        return len(self.keys) - len(self.done)


QUEUES = ("IngressQueueUrl", "IngressDLQUrl", "ConsumerDLQUrl", "StreamFailureQueueUrl")


def snapshot(live: Live, tracker: Tracker, t0: float, progress: Path, note: str = ""):
    counts = tracker.poll()
    queues = {k.replace("Url", ""): live.queue_depth(k) for k in QUEUES}
    row = {"t": round(time.time() - t0, 1), "tasks_expected": len(tracker.keys), "tasks_found": len(tracker.latest),
           "counts": counts, "queues": {k: v["ApproximateNumberOfMessages"] + v["ApproximateNumberOfMessagesNotVisible"]
                                        for k, v in queues.items()}, "note": note}
    with progress.open("a") as f:
        f.write(json.dumps(row) + "\n")
    print(f"  t+{row['t']:.0f}s tasks {row['tasks_found']}/{row['tasks_expected']} {counts} queues {row['queues']}"
          f"{' ' + note if note else ''}", flush=True)
    return row


def send_all(live: Live, flat, args, tracker: Tracker, out: Path, t0: float):
    sent_at, failures = {}, []
    wave = args.wave or len(flat)
    sent_log, progress = out / "sent.jsonl", out / "progress.jsonl"

    def send(chunk):
        ts = int(time.time() * 1000)
        failed = live.send_batch(chunk)
        return ts, chunk, failed
    for w in range(0, len(flat), wave):
        events = flat[w:w + wave]
        if args.max_inflight:
            while tracker.inflight() > args.max_inflight:
                snapshot(live, tracker, t0, progress, f"backpressure: inflight {tracker.inflight()} > {args.max_inflight}")
                time.sleep(args.poll)
        chunks = [events[i:i + 10] for i in range(0, len(events), 10)]
        wave_t0 = time.time()
        with ThreadPoolExecutor(args.threads) as pool:
            if args.rate:
                # Submit chunks on a fixed schedule so the offered load never exceeds --rate events/s.
                futures = []
                for i, chunk in enumerate(chunks):
                    delay = wave_t0 + i * 10 / args.rate - time.time()
                    if delay > 0:
                        time.sleep(delay)
                    futures.append(pool.submit(send, chunk))
                results = [f.result() for f in futures]
            else:
                results = list(pool.map(send, chunks))
        with sent_log.open("a") as f:
            for ts, chunk, failed in results:
                failures.extend(failed)
                for e in chunk:
                    sent_at[e["EventId"]] = ts
                    f.write(json.dumps({"EventId": e["EventId"], "sent_at": ts, "failed": e["EventId"] in failed}) + "\n")
        tracker.add(e for e in events if e["EventId"] not in failures)
        if args.wave:
            snapshot(live, tracker, t0, progress, f"wave {w // wave + 1} sent ({len(sent_at)}/{len(flat)} events)")
    return sent_at, failures


def wait_done(live: Live, tracker: Tracker, args, out: Path, t0: float):
    deadline, best, best_at = time.time() + args.timeout, -1, time.time()
    while True:
        row = snapshot(live, tracker, t0, out / "progress.jsonl")
        if row["tasks_found"] == row["tasks_expected"] and tracker.inflight() == 0:
            return "done"
        if len(tracker.done) > best:
            best, best_at = len(tracker.done), time.time()
        if args.stall_timeout and time.time() - best_at > args.stall_timeout:
            print(f"STALL: no task reached a terminal state for {args.stall_timeout}s")
            return "stalled"
        if time.time() > deadline:
            print("TIMEOUT waiting for completion")
            return "timeout"
        time.sleep(args.poll)


def main() -> None:
    p = parser(__doc__)
    p.add_argument("--events", type=int)
    p.add_argument("--allow-large", action="store_true")
    p.add_argument("--seed", type=int, default=1724)
    p.add_argument("--timeout", type=int, default=900)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--rate", type=float, default=0, help="max source events/s (0 = unbounded)")
    p.add_argument("--wave", type=int, default=0, help="events per wave/checkpoint (0 = one wave)")
    p.add_argument("--max-inflight", type=int, default=0, help="non-terminal task cap before next wave (0 = off)")
    p.add_argument("--poll", type=int, default=10)
    p.add_argument("--stall-timeout", type=int, default=0, help="stop waiting after S s without progress (0 = off)")
    p.add_argument("--verify", help="existing results dir: resume waiting/verification from its checkpoints")
    args = p.parse_args()
    live = Live(args.profile, args.region, args.stack)
    tracker = Tracker(live)

    if args.verify:
        out = Path(args.verify)
        saved = json.loads((out / "workload.json").read_text())
        run, flat, cfg = saved["run"], saved["events"], saved["config"]
        pairs = [tuple(x) for x in saved["pairs"]]
        sent = [json.loads(line) for line in (out / "sent.jsonl").read_text().splitlines()]
        sent_at = {r["EventId"]: r["sent_at"] for r in sent}
        send_failures = [r["EventId"] for r in sent if r["failed"]]
        tracker.add(e for e in flat if e["EventId"] in sent_at and e["EventId"] not in send_failures)
        start = datetime.fromtimestamp(min(sent_at.values()) / 1000, timezone.utc)
        t0, send_seconds = time.time(), cfg.get("send_seconds")
        print(f"resuming run {run}: {len(sent_at)} events sent previously; {len(tracker.keys)} tasks to verify")
    else:
        if not args.events:
            raise SystemExit("--events required (or --verify DIR)")
        if args.events > 1000 and not args.allow_large:
            raise SystemExit("N > 1000 requires --allow-large (explicit human approval)")
        if args.events % 2:
            raise SystemExit("--events must be even")
        out = results_dir(f"load-{args.events}")
        run, pairs, flat = build_workload(args.events, args.seed)
        cfg = {k: getattr(args, k) for k in ("events", "seed", "threads", "rate", "wave", "max_inflight", "poll",
                                              "timeout", "stall_timeout")}
        dump(out, "workload.json", {"run": run, "config": cfg, "pairs": pairs, "events": flat})
        print(f"run {run}: {len(flat)} unique events, {2 * len(flat)} consumer tasks expected; account "
              f"{live.account_hint()}; config {cfg}", flush=True)
        start = datetime.now(timezone.utc)
        t0 = time.time()
        sent_at, send_failures = send_all(live, flat, args, tracker, out, t0)
        send_seconds = time.time() - t0
        cfg["send_seconds"] = round(send_seconds, 1)
        dump(out, "workload.json", {"run": run, "config": cfg, "pairs": pairs, "events": flat})
        print(f"sent in {send_seconds:.1f}s ({len(flat) / send_seconds:.0f} msg/s), send failures {len(send_failures)}",
              flush=True)

    outcome = wait_done(live, tracker, args, out, t0)
    end = datetime.now(timezone.utc)
    tasks = list(tracker.latest.values())
    counts = {}
    for t in tasks:
        counts[t["Status"]] = counts.get(t["Status"], 0) + 1
    keys = tracker.keys

    by_key = {(t["EventId"], t["ConsumerId"]): t for t in tasks}
    completed = [t for t in tasks if t["Status"] == "COMPLETED"]
    platform_lat = [int(t["CompletedAt"]) - int(t["IngestedAt"]) for t in completed]
    e2e_lat = [int(t["CompletedAt"]) - sent_at[t["EventId"]] for t in completed]
    dispatch_lat = [int(t["CompletedAt"]) - int(t["ReadyAt"]) for t in completed]
    first_send = min(sent_at.values())
    last_done = max((int(t["CompletedAt"]) for t in completed), default=first_send)

    # Dependency-order + duplicate check from consumer projections
    proj_keys, expected = [], {}
    from event_platform.consumer import projection_key
    for paper, create, comment in pairs:
        for c in CONSUMERS:
            k = projection_key(c, {"PaperId": paper}, "")
            proj_keys.append(k)
            expected[(k["PK"])] = [create["EventId"], comment["EventId"]]
    projections = {}
    table = live.out["ConsumerProjectionTableName"]
    for i in range(0, len(proj_keys), 100):
        req = {table: {"Keys": proj_keys[i:i + 100], "ConsistentRead": True}}
        while req:
            resp = live.ddb.batch_get_item(RequestItems=req)
            for item in resp["Responses"].get(table, []):
                projections[item["PK"]] = item
            req = resp.get("UnprocessedKeys") or None
    missing = [k for k in expected if k not in projections]
    wrong_order = [k for k, v in expected.items() if k in projections and projections[k]["AppliedOrder"] != v
                   and k.startswith("PROJ#SearchService")]
    applied_counts = [int(p["AppliedCount"]) for p in projections.values()]
    duplicated = sum(1 for c in applied_counts if c > 2)
    # Reporting MakeComment depends on Search CreatePaper (cross-consumer); its own CreatePaper is independent,
    # so Reporting order is only constrained comment-after-Search-paper; check via task timestamps.
    cross_violations = sum(1 for paper, create, comment in pairs
                           if (comment["EventId"], "ReportingService") in by_key
                           and by_key[(comment["EventId"], "ReportingService")].get("ReadyAt") is not None
                           and (create["EventId"], "SearchService") in by_key
                           and by_key[(create["EventId"], "SearchService")].get("CompletedAt") is not None
                           and int(by_key[(comment["EventId"], "ReportingService")]["ReadyAt"])
                           < int(by_key[(create["EventId"], "SearchService")]["CompletedAt"]))
    attempts = {}
    for t in tasks:
        a = int(t.get("DispatchAttempts", 0))
        attempts[a] = attempts.get(a, 0) + 1
    time.sleep(90)  # let CloudWatch ingest the final minute
    metrics = cloudwatch(live, start - timedelta(minutes=2), datetime.now(timezone.utc) + timedelta(minutes=1))

    summary = {
        "run": run, "seed": cfg["seed"], "config": cfg, "outcome": outcome,
        "paper_id_range": [pairs[0][0], pairs[-1][0]], "account": live.account_hint(), "region": args.region, "stack": args.stack,
        "unique_source_events_sent": len(sent_at), "send_failures": len(send_failures),
        "send_seconds": send_seconds if send_seconds is None else round(send_seconds, 1),
        "consumer_tasks_expected": len(keys), "consumer_tasks_found": len(tasks),
        "status_counts": counts, "completed": len(completed), "dispatch_attempts_distribution": attempts,
        "wall_clock_first_send_to_last_ack_s": round((last_done - first_send) / 1000, 1),
        "completed_tasks_per_second": round(len(completed) / max((last_done - first_send) / 1000, 0.001), 1),
        "latency_ms": {"ingested_to_ack": summarize(platform_lat), "client_send_to_ack": summarize(e2e_lat),
                       "ready_to_ack": summarize(dispatch_lat)},
        "side_effects": {"projections_expected": len(expected), "projections_missing": len(missing),
                         "search_dependency_order_violations": len(wrong_order),
                         "reporting_cross_consumer_violations": cross_violations,
                         "projections_with_duplicate_applies": duplicated},
        "queues_after": {k: live.queue_depth(k) for k in QUEUES},
        "window": [start.isoformat(), end.isoformat()],
    }
    dump(out, "summary.json", summary)
    dump(out, "cloudwatch.json", metrics)
    dump(out, "workload_event_ids.json", [e["EventId"] for e in flat])
    if outcome != "done":
        dump(out, "non_terminal_tasks.json", [t for t in tasks if t["Status"] not in ("COMPLETED", "FAILED")])
    print(json.dumps(summary, indent=2, default=str))
    print(f"evidence: {out}")


if __name__ == "__main__":
    main()
