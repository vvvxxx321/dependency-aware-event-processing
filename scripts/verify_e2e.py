"""LIVE AWS end-to-end verification. Each scenario uses fresh UUIDs/PaperIds; writes evidence to tests/results/.

Requires a deployed stack + seed_config.py --commit. Every check is recorded PASS/FAIL with observed values.
Slow scenarios (SQS visibility-timeout retries, DLQ after 5 receives) only run with --slow.
"""
from __future__ import annotations

import json
import random
import uuid
import time
import traceback
from typing import Any

from live_common import Live, dump, new_event, parser, results_dir, wait_for

SEARCH, REPORT = "SearchService", "ReportingService"


class Checks:
    def __init__(self, scenario: str):
        self.scenario = scenario
        self.items: list[dict[str, Any]] = []

    def check(self, name: str, ok: bool, observed: Any = None) -> bool:
        self.items.append({"check": name, "result": "PASS" if ok else "FAIL", "observed": observed})
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} :: {observed}")
        return ok


def pid() -> int:
    return random.randint(10**8, 10**9)


def await_status(live: Live, event, consumer, status, timeout=90):
    return wait_for(lambda: live.status(event, consumer) == status, timeout) and status


def ints(task, *names):
    return {n: int(task[n]) for n in names if task and n in task}


# ---------------- scenarios ----------------
def p0_out_of_order(live: Live, c: Checks) -> dict:
    paper = pid()
    e1 = new_event("ContentService", "CreatePaper", {"PaperId": paper})
    e2 = new_event("CommentService", "MakeComment", {"PaperId": paper, "CommentId": pid()})
    live.send(e2)
    c.check("E2@Search task created PENDING", wait_for(lambda: live.status(e2, SEARCH) == "PENDING", 60),
            live.status(e2, SEARCH))
    time.sleep(10)  # give any (incorrect) dispatch time to happen
    c.check("E2@Search still PENDING after 10s with no E1", live.status(e2, SEARCH) == "PENDING", live.status(e2, SEARCH))
    c.check("no Search projection before E1", live.projection(SEARCH, paper) is None)
    live.send(e1)
    c.check("E1@Search COMPLETED", await_status(live, e1, SEARCH, "COMPLETED") == "COMPLETED", live.status(e1, SEARCH))
    c.check("E2@Search COMPLETED", await_status(live, e2, SEARCH, "COMPLETED") == "COMPLETED", live.status(e2, SEARCH))
    t1, t2 = live.task(e1, SEARCH), live.task(e2, SEARCH)
    times = {"E1": ints(t1, "IngestedAt", "ReadyAt", "DispatchedAt", "CompletedAt"),
             "E2": ints(t2, "IngestedAt", "ReadyAt", "DispatchedAt", "CompletedAt")}
    c.check("E1 READY < E1 COMPLETED (ACK after READY)", int(t1["ReadyAt"]) < int(t1["CompletedAt"]), times["E1"])
    c.check("E1 COMPLETED <= E2 READY (dependency)", int(t1["CompletedAt"]) <= int(t2["ReadyAt"]), times)
    c.check("E1 ACK came from Search consumer role", "SearchConsumerFunctionRole" in t1.get("AckedBy", ""), t1.get("AckedBy"))
    proj = live.projection(SEARCH, paper)
    c.check("Search projection applied E1 then E2", proj and proj["AppliedOrder"] == [e1["EventId"], e2["EventId"]],
            proj and proj["AppliedOrder"])
    m1, m2 = live.marker(SEARCH, t1["TaskId"]), live.marker(SEARCH, t2["TaskId"])
    c.check("side-effect markers ProcessedAt E1 < E2", m1 and m2 and m1["ProcessedAt"] < m2["ProcessedAt"],
            {"E1": m1 and int(m1["ProcessedAt"]), "E2": m2 and int(m2["ProcessedAt"])})
    c.check("E1 marker ProcessedAt <= E1 CompletedAt", m1 and int(m1["ProcessedAt"]) <= int(t1["CompletedAt"]))
    # Cross-consumer rule: E2@Reporting requires E1@Search
    c.check("E2@Reporting COMPLETED", await_status(live, e2, REPORT, "COMPLETED") == "COMPLETED", live.status(e2, REPORT))
    return {"E1": e1, "E2": e2, "times": times}


def cross_consumer(live: Live, c: Checks) -> dict:
    paper = pid()
    e1 = new_event("ContentService", "CreatePaper", {"PaperId": paper})
    e2 = new_event("CommentService", "MakeComment", {"PaperId": paper, "CommentId": pid()})
    live.inject_consumer_fault(e1["EventId"], SEARCH, "TRANSIENT", remaining=1)  # first Search attempt fails
    live.send(e1); live.send(e2)
    c.check("E1@Reporting COMPLETED", await_status(live, e1, REPORT, "COMPLETED") == "COMPLETED", live.status(e1, REPORT))
    s1, r2 = live.status(e1, SEARCH), live.status(e2, REPORT)
    c.check("while E1@Search not COMPLETED, E2@Reporting not READY/COMPLETED",
            s1 == "COMPLETED" or r2 == "PENDING", {"E1@Search": s1, "E2@Reporting": r2})
    c.check("E1@Search COMPLETED after Lambda async retry",
            await_status(live, e1, SEARCH, "COMPLETED", timeout=420) == "COMPLETED", live.status(e1, SEARCH))
    c.check("E2@Reporting COMPLETED after E1@Search", await_status(live, e2, REPORT, "COMPLETED") == "COMPLETED")
    t1s, t2r = live.task(e1, SEARCH), live.task(e2, REPORT)
    c.check("E2@Reporting ReadyAt >= E1@Search CompletedAt", int(t2r["ReadyAt"]) >= int(t1s["CompletedAt"]),
            {"E1@Search.CompletedAt": int(t1s["CompletedAt"]), "E2@Reporting.ReadyAt": int(t2r["ReadyAt"])})
    proj = live.projection(SEARCH, paper)
    c.check("Search projection: E1 applied once despite failed attempt, then E2",
            proj and proj["AppliedOrder"] == [e1["EventId"], e2["EventId"]], proj and proj["AppliedOrder"])
    live.clear_consumer_fault(e1["EventId"], SEARCH)
    return {"E1": e1, "E2": e2}


def multi_and(live: Live, c: Checks) -> dict:
    paper, review = pid(), pid()
    review_ev = new_event("ReviewService", "SubmitReview", {"PaperId": paper, "ReviewId": review})
    e1 = new_event("ContentService", "CreatePaper", {"PaperId": paper})
    approve = new_event("ModerationService", "ApproveReview", {"ReviewId": review})
    live.send(review_ev); live.send(e1)
    c.check("E1@Search COMPLETED", await_status(live, e1, SEARCH, "COMPLETED") == "COMPLETED")
    time.sleep(8)
    c.check("SubmitReview PENDING with only 1 of 2 prerequisites", live.status(review_ev, SEARCH) == "PENDING",
            live.status(review_ev, SEARCH))
    live.send(approve)
    c.check("SubmitReview COMPLETED after both", await_status(live, review_ev, SEARCH, "COMPLETED") == "COMPLETED")
    t = live.task(review_ev, SEARCH)
    c.check("ReadyAt after both predecessors completed",
            int(t["ReadyAt"]) >= max(int(live.task(e1, SEARCH)["CompletedAt"]), int(live.task(approve, SEARCH)["CompletedAt"])))
    return {"review": review_ev}


def wrong_param(live: Live, c: Checks) -> dict:
    e1 = new_event("ContentService", "CreatePaper", {"PaperId": pid()})
    e2 = new_event("CommentService", "MakeComment", {"PaperId": pid(), "CommentId": 1})
    live.send(e1)
    await_status(live, e1, SEARCH, "COMPLETED")
    live.send(e2)
    time.sleep(15)
    c.check("MakeComment with different PaperId stays PENDING", live.status(e2, SEARCH) == "PENDING", live.status(e2, SEARCH))
    return {"E2_left_pending": e2}


def time_window(live: Live, c: Checks) -> dict:
    paper = pid()
    old = new_event("ContentService", "CreatePaper", {"PaperId": paper})
    live.send(old)
    c.check("first CreatePaper@Reporting COMPLETED", await_status(live, old, REPORT, "COMPLETED") == "COMPLETED")
    time.sleep(35)  # rule gap = 30s since predecessor StatusUpdatedOn
    archive = new_event("AuditService", "ArchivePaper", {"PaperId": paper})
    live.send(archive)
    time.sleep(12)
    c.check("ArchivePaper PENDING: only predecessor older than 30s", live.status(archive, REPORT) == "PENDING",
            live.status(archive, REPORT))
    fresh = new_event("ContentService", "CreatePaper", {"PaperId": paper})
    live.send(fresh)
    c.check("ArchivePaper COMPLETED after fresh predecessor", await_status(live, archive, REPORT, "COMPLETED") == "COMPLETED")
    return {"archive": archive}


def concurrency(live: Live, c: Checks, n: int = 20) -> dict:
    events = [new_event("ContentService", "CreatePaper", {"PaperId": pid()}) for _ in range(n)]
    blocked = new_event("CommentService", "MakeComment", {"PaperId": pid(), "CommentId": 1})
    live.send(blocked)
    failed = live.send_batch(events)
    c.check("batch send accepted", not failed, failed)
    done = wait_for(lambda: all(live.status(e, SEARCH) == "COMPLETED" for e in events), 120, 2)
    c.check(f"all {n} independent CreatePaper@Search COMPLETED while unrelated task blocked", bool(done))
    c.check("unrelated blocked MakeComment still PENDING", live.status(blocked, SEARCH) == "PENDING")
    spans = sorted((int(t["ReadyAt"]), int(t["CompletedAt"])) for t in (live.task(e, SEARCH) for e in events))
    max_overlap = max(sum(1 for s, f in spans if s <= start < f) for start, _ in spans)
    c.check("independent tasks processed concurrently (overlapping READY..COMPLETED)", max_overlap > 1,
            {"max_concurrent_in_flight": max_overlap})
    return {"max_overlap": max_overlap}


def duplicates(live: Live, c: Checks) -> dict:
    paper = pid()
    e1 = new_event("ContentService", "CreatePaper", {"PaperId": paper})
    for _ in range(3):
        live.send(e1)
    live.send_batch([e1, e1])
    c.check("E1@Search COMPLETED", await_status(live, e1, SEARCH, "COMPLETED") == "COMPLETED")
    time.sleep(10)
    tasks = live.tasks_for_event(e1)
    c.check("5 sends -> exactly 2 consumer tasks", len(tasks) == 2, len(tasks))
    proj = live.projection(SEARCH, paper)
    c.check("Search side effect applied once", int(proj["AppliedCount"]) == 1, int(proj["AppliedCount"]))
    t = live.task(e1, SEARCH)
    from event_platform.dispatch import task_message
    live.sns.publish(TopicArn=live.out["DispatchTopicArn"], Message=json.dumps(task_message(t)),
                     MessageAttributes={"ConsumerId": {"DataType": "String", "StringValue": SEARCH}})
    time.sleep(10)
    proj = live.projection(SEARCH, paper)
    c.check("duplicate SNS delivery -> no second side effect", int(proj["AppliedCount"]) == 1, int(proj["AppliedCount"]))
    c.check("task still COMPLETED after duplicate ACK", live.status(e1, SEARCH) == "COMPLETED")
    conflict = {**e1, "Parameters": {"PaperId": paper + 1}}
    live.send(conflict)
    return {"E1": e1, "conflict_sent_expect_dlq_after_5_receives": conflict["EventId"]}


def forged_ack(live: Live, c: Checks) -> dict:
    from event_platform.consumer import signed_ack_client
    e1 = new_event("ContentService", "CreatePaper", {"PaperId": pid()})
    live.send(e1)
    await_status(live, e1, SEARCH, "COMPLETED")
    t = live.task(e1, SEARCH)
    payload = {"TaskId": t["TaskId"], "ProducerId": t["ProducerId"], "TaskSK": t["SK"], "EventId": t["EventId"],
               "ConsumerId": SEARCH, "Outcome": "FAILED"}
    from live_common import ssl_context
    code, body = signed_ack_client(live.out["CompletionUrl"], live.region, live.session, ssl_context())(payload)
    c.check("operator IAM identity cannot ACK consumer task (403)", code == 403, {"status": code, "body": body})
    import urllib.request, urllib.error
    try:
        urllib.request.urlopen(urllib.request.Request(live.out["CompletionUrl"], data=json.dumps(payload).encode(),
                                                      method="POST"), timeout=5, context=ssl_context())
        code = 200
    except urllib.error.HTTPError as err:
        code = err.code
    c.check("unsigned request rejected by API Gateway IAM auth (403)", code == 403, code)
    c.check("task unchanged", live.status(e1, SEARCH) == "COMPLETED")
    return {}


def terminal_failure_replay(live: Live, c: Checks) -> dict:
    from event_platform.aws_store import PlatformStore
    from event_platform.ops import replay_failed
    paper = pid()
    e1 = new_event("ContentService", "CreatePaper", {"PaperId": paper})
    e2 = new_event("CommentService", "MakeComment", {"PaperId": paper, "CommentId": 1})
    live.inject_consumer_fault(e1["EventId"], SEARCH, "TERMINAL")
    live.send(e2); live.send(e1)
    c.check("E1@Search FAILED via consumer terminal ACK", await_status(live, e1, SEARCH, "FAILED") == "FAILED")
    c.check("E2@Search FAILED by propagation", await_status(live, e2, SEARCH, "FAILED") == "FAILED")
    c.check("E2@Reporting FAILED by propagation (cross-consumer)", await_status(live, e2, REPORT, "FAILED") == "FAILED")
    d = live.task(e2, SEARCH)
    c.check("RootFailureEventId + FailureChain preserved", d.get("RootFailureEventId") == e1["EventId"]
            and len(d.get("FailureChain", [])) == 2, {k: d.get(k) for k in ("RootFailureEventId", "FailureChain", "FailureReason")})
    c.check("E1@Reporting unaffected (COMPLETED)", live.status(e1, REPORT) == "COMPLETED")
    live.clear_consumer_fault(e1["EventId"], SEARCH)
    store = PlatformStore(config_table=live.out["ConfigurationTableName"], log_table=live.out["ReplicationLogTableName"],
                          route_index="BySourceAction", waiters_table=live.out["DependencyWaitersTableName"],
                          resource=live.ddb, client=live.ddb_client)
    replayed = replay_failed(store, root_event_id=e1["EventId"], now_ms=int(time.time() * 1000), operator="verify_e2e")
    c.check("replay reset root + 2 dependents", len(replayed) == 3, len(replayed))
    for ev, cons in ((e1, SEARCH), (e2, SEARCH), (e2, REPORT)):
        c.check(f"after replay {ev['Action']}@{cons} COMPLETED", await_status(live, ev, cons, "COMPLETED", 120) == "COMPLETED",
                live.status(ev, cons))
    proj = live.projection(SEARCH, paper)
    c.check("projection order E1 then E2 after replay", proj and proj["AppliedOrder"] == [e1["EventId"], e2["EventId"]])
    return {"E1": e1, "E2": e2}


def transient_ingest(live: Live, c: Checks) -> dict:
    e1 = new_event("ContentService", "CreatePaper", {"PaperId": pid()})
    start = time.time()
    live.send(e1, {"FaultInjectIngestFailures": "2"})
    ok = wait_for(lambda: live.status(e1, SEARCH) == "COMPLETED", 400, 5)
    c.check("ingest failed twice then succeeded on receive 3 -> COMPLETED", bool(ok),
            {"seconds": round(time.time() - start, 1), "status": live.status(e1, SEARCH)})
    c.check("elapsed consistent with >=2 visibility timeouts (~120s)", time.time() - start >= 100, round(time.time() - start, 1))
    return {}


def dlq_bodies(live: Live) -> list[str]:
    """Peek DLQ bodies without deleting (short visibility so other peeks still see them)."""
    bodies = []
    for _ in range(5):
        resp = live.sqs.receive_message(QueueUrl=live.out["IngressDLQUrl"], MaxNumberOfMessages=10,
                                        VisibilityTimeout=2, WaitTimeSeconds=1)
        bodies.extend(m["Body"] for m in resp.get("Messages", []))
    return bodies


def ingress_dlq(live: Live, c: Checks) -> dict:
    # Match the specific poison body: DLQ depth alone was a false positive on 2026-10-21 (a conflicting-payload
    # message from the duplicates scenario arrived first).
    poison = f"not-json-poison-{uuid.uuid4()}"
    sent = time.time()
    live.sqs.send_message(QueueUrl=live.out["IngressQueueUrl"], MessageBody=poison)
    ok = wait_for(lambda: poison in dlq_bodies(live), 900, 20)
    c.check("this specific poison message reaches Ingress DLQ (maxReceiveCount=5)", bool(ok),
            {"seconds": round(time.time() - sent, 1), "body": poison})
    return {"poison_body": poison}


SCENARIOS = {"p0": p0_out_of_order, "cross_consumer": cross_consumer, "multi_and": multi_and,
             "wrong_param": wrong_param, "time_window": time_window, "concurrency": concurrency,
             "duplicates": duplicates, "forged_ack": forged_ack, "terminal_replay": terminal_failure_replay}
SLOW = {"transient_ingest": transient_ingest, "ingress_dlq": ingress_dlq}


def main() -> None:
    p = parser(__doc__)
    p.add_argument("--only", nargs="*")
    p.add_argument("--slow", action="store_true")
    args = p.parse_args()
    live = Live(args.profile, args.region, args.stack)
    out = results_dir("live-e2e")
    selected = {**SCENARIOS, **(SLOW if args.slow else {})}
    if args.only:
        selected = {k: v for k, v in {**SCENARIOS, **SLOW}.items() if k in args.only}
    summary = {"account": live.account_hint(), "region": args.region, "stack": args.stack, "scenarios": {}}
    for name, fn in selected.items():
        print(f"== {name}")
        c = Checks(name)
        started = time.time()
        try:
            data = fn(live, c)
        except Exception as exc:
            c.check("scenario raised no exception", False, traceback.format_exc(limit=3))
            data = {"error": repr(exc)}
        summary["scenarios"][name] = {"checks": c.items, "data": data, "seconds": round(time.time() - started, 1),
                                      "result": "PASS" if all(i["result"] == "PASS" for i in c.items) else "FAIL"}
    summary["queues"] = {k: live.queue_depth(k) for k in ("IngressDLQUrl", "ConsumerDLQUrl", "StreamFailureQueueUrl")}
    dump(out, "summary.json", summary)
    print(f"\nResults written to {out}")
    for name, s in summary["scenarios"].items():
        print(f"{s['result']:4}  {name}  ({s['seconds']}s)")
    print("queues:", summary["queues"])


if __name__ == "__main__":
    main()
