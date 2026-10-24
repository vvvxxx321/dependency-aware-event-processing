"""Read-only latency decomposition for a load-test results dir (re-reads its tasks from DynamoDB).

The raw send->ACK latency mixes platform delay with *intended* dependency waits: a MakeComment that arrives
before its CreatePaper must stay PENDING. This splits tasks into:
  independent  CreatePaper@{Search,Reporting}: IngestedAt -> CompletedAt
  dependent    MakeComment@{Search,Reporting}, prerequisite CreatePaper@Search (same PaperId):
                 eligible_at  = max(own IngestedAt, prerequisite CompletedAt)
                 wakeup       = ReadyAt - eligible_at          (resolver reaction to the prerequisite/ingest)
                 eligible_to_ack = CompletedAt - eligible_at  (platform delay excluding the intended wait)
                 dependency_wait = eligible_at - IngestedAt    (time blocked on a not-yet-completed prerequisite)
Also checks per-task invariants: ReadyAt >= prerequisite CompletedAt; COMPLETED tasks have a consumer marker.
Writes analysis.json and tasks.json.gz (final task records) into the dir.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

from live_common import Live, parser
from load_test import CONSUMERS, batch_get, summarize


def main() -> None:
    p = parser(__doc__)
    p.add_argument("dir", type=Path)
    p.add_argument("--check-markers", action="store_true", help="also batch-read every consumer side-effect marker")
    args = p.parse_args()
    live = Live(args.profile, args.region, args.stack)
    d = args.dir
    if (d / "workload.json").exists():
        w = json.loads((d / "workload.json").read_text())
        pairs = [tuple(x) for x in w["pairs"]]
    else:  # runs before checkpoints existed: rebuild pairs from the saved EventIds via the identity records
        raise SystemExit(f"{d} has no workload.json (pre-checkpoint run); cannot decompose")
    events = {e["EventId"]: e for _, c, m in pairs for e in (c, m)}
    keys = [live.task_key(e, c) for e in events.values() for c in CONSUMERS]
    tasks = {(t["EventId"], t["ConsumerId"]): t for t in batch_get(live, keys)}
    with gzip.open(d / "tasks.json.gz", "wt") as f:
        json.dump(list(tasks.values()), f, default=str)

    indep, wake, elig_ack, dep_wait, blocked = [], [], [], [], 0
    violations, missing_markers, no_dispatch_bookkeeping = [], [], []
    for paper, create, comment in pairs:
        pre = tasks.get((create["EventId"], "SearchService"))
        for c in CONSUMERS:
            t = tasks.get((create["EventId"], c))
            if t and t["Status"] == "COMPLETED":
                indep.append(int(t["CompletedAt"]) - int(t["IngestedAt"]))
            m = tasks.get((comment["EventId"], c))
            if not (m and pre and m["Status"] == "COMPLETED" and pre.get("CompletedAt")):
                continue
            eligible = max(int(m["IngestedAt"]), int(pre["CompletedAt"]))
            if int(m["ReadyAt"]) < int(pre["CompletedAt"]):
                violations.append(m["TaskId"])
            wake.append(int(m["ReadyAt"]) - eligible)
            elig_ack.append(int(m["CompletedAt"]) - eligible)
            dep_wait.append(eligible - int(m["IngestedAt"]))
            blocked += int(pre["CompletedAt"]) > int(m["IngestedAt"])
    for t in tasks.values():
        if t["Status"] == "COMPLETED" and not t.get("DispatchedAt"):
            no_dispatch_bookkeeping.append({k: t.get(k) for k in ("TaskId", "EventId", "ConsumerId", "ReadyAt",
                                                                  "CompletedAt", "DispatchAttempts")})
    if args.check_markers:
        from event_platform.consumer import marker_key
        table = live.out["ConsumerProjectionTableName"]
        mkeys = [marker_key(t["ConsumerId"], t["TaskId"]) for t in tasks.values() if t["Status"] == "COMPLETED"]
        found = set()
        for i in range(0, len(mkeys), 100):
            req = {table: {"Keys": mkeys[i:i + 100], "ConsistentRead": True}}
            while req:
                resp = live.ddb.batch_get_item(RequestItems=req)
                found.update((it["PK"], it["SK"]) for it in resp["Responses"].get(table, []))
                req = resp.get("UnprocessedKeys") or None
        missing_markers = [k for k in mkeys if (k["PK"], k["SK"]) not in found]

    out = {"tasks_read": len(tasks), "tasks_expected": len(keys),
           "independent_ingested_to_ack_ms": summarize(indep),
           "dependent_wakeup_ms": summarize(wake), "dependent_eligible_to_ack_ms": summarize(elig_ack),
           "dependent_intended_wait_ms": summarize(dep_wait),
           "dependent_tasks_ingested_before_prerequisite_completed": blocked,
           "ready_before_prerequisite_completed": len(violations),
           "completed_without_marker": len(missing_markers) if args.check_markers else "not checked",
           "completed_without_dispatch_bookkeeping": no_dispatch_bookkeeping}
    (d / "analysis.json").write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
