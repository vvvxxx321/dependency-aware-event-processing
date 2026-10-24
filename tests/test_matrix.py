"""P1 test matrix on the local pipeline (moto DynamoDB + streams). LOCAL ONLY - not AWS evidence."""
import json

import pytest

from event_platform.ops import reconcile_pending, redispatch_stale_ready, replay_failed
from local_pipeline import make_event

PAPER = ("ContentService", "CreatePaper")
COMMENT = ("CommentService", "MakeComment")


def paper(pid, **kw):
    return make_event(*PAPER, {"PaperId": pid}, **kw)


def comment(pid, cid=1, **kw):
    return make_event(*COMMENT, {"PaperId": pid, "CommentId": cid}, **kw)


# ---------------- out-of-order + in-order baseline ----------------
def test_in_order_delivery_also_completes(demo):
    e1, e2 = paper(1), comment(1)
    demo.send(e1); demo.pump()
    demo.send(e2); demo.pump()
    assert demo.status(e2["EventId"], "SearchService") == "COMPLETED"
    assert demo.projection("SearchService", 1)["AppliedOrder"] == [e1["EventId"], e2["EventId"]]


def test_never_ready_before_predecessor_completed(demo):
    demo.consumers["SearchService"].ack = lambda payload: (503, {})  # E1 side effect happens but no ACK
    e2, e1 = comment(5), paper(5)
    demo.send(e2); demo.send(e1); demo.pump()
    assert demo.status(e1["EventId"], "SearchService") == "READY"
    assert demo.status(e2["EventId"], "SearchService") == "PENDING"


# ---------------- cross-consumer ----------------
def test_cross_consumer_dependency_requires_named_service_consumer(demo):
    # ReportingService's MakeComment requires SearchService (not ReportingService) to have completed CreatePaper.
    demo.add_rule("ReportingService", "MakeComment", *PAPER, "SearchService", "PaperId")
    e1, e2 = paper(9), comment(9)
    demo.inject_consumer_fault(e1["EventId"], "SearchService", "TRANSIENT", remaining=10)
    demo.send(e1); demo.send(e2); demo.pump()
    assert demo.status(e1["EventId"], "ReportingService") == "COMPLETED"
    assert demo.status(e1["EventId"], "SearchService") == "READY"       # retries exhausted, no ACK
    assert demo.status(e2["EventId"], "ReportingService") == "PENDING"  # (E1,Reporting) done is NOT enough
    assert len(demo.consumer_dlq) == 1
    # Operator clears the fault and re-dispatches the stale READY task.
    demo.clear_consumer_fault(e1["EventId"], "SearchService")
    sent = redispatch_stale_ready(demo.store, demo._publish, now_ms=demo.clock() + 10**9, older_than_ms=60_000)
    assert sent == [demo.task(e1["EventId"], "SearchService")["TaskId"]]
    demo.pump()
    assert demo.status(e2["EventId"], "ReportingService") == "COMPLETED"
    assert demo.status(e2["EventId"], "SearchService") == "COMPLETED"


# ---------------- multiple AND prerequisites ----------------
@pytest.mark.parametrize("order", ["paper-first", "approval-first"])
def test_multiple_rules_are_and(demo, order):
    demo.subscribe("ModerationService", "ApproveComment", "SearchService")
    demo.add_rule("SearchService", "MakeComment", "ModerationService", "ApproveComment", "SearchService", "CommentId")
    e2 = comment(3, cid=77)
    e1 = paper(3)
    e3 = make_event("ModerationService", "ApproveComment", {"CommentId": 77})
    demo.send(e2); demo.pump()
    first, second = (e1, e3) if order == "paper-first" else (e3, e1)
    demo.send(first); demo.pump()
    assert demo.status(first["EventId"], "SearchService") == "COMPLETED"
    assert demo.status(e2["EventId"], "SearchService") == "PENDING"  # one of two rules is not enough
    demo.send(second); demo.pump()
    t2 = demo.task(e2["EventId"], "SearchService")
    assert t2["Status"] == "COMPLETED"
    assert t2["ReadyAt"] > demo.task(e1["EventId"], "SearchService")["CompletedAt"]
    assert t2["ReadyAt"] > demo.task(e3["EventId"], "SearchService")["CompletedAt"]


# ---------------- business parameter matching ----------------
def test_wrong_parameter_value_does_not_satisfy(demo):
    demo.send(paper(999)); demo.pump()
    e2 = comment(123)
    demo.send(e2); demo.pump()
    assert demo.status(e2["EventId"], "SearchService") == "PENDING"


def test_equal_is_type_strict_string_vs_number(demo):
    demo.send(paper("123")); demo.pump()
    e2 = comment(123)
    demo.send(e2); demo.pump()
    assert demo.status(e2["EventId"], "SearchService") == "PENDING"


def test_dependent_missing_parameter_fails_closed(demo):
    e2 = make_event(*COMMENT, {"CommentId": 1})  # no PaperId
    demo.send(paper(1)); demo.send(e2); demo.pump()
    assert demo.status(e2["EventId"], "SearchService") == "PENDING"


def test_wrong_producer_or_action_does_not_satisfy(demo):
    demo.subscribe("OtherService", "CreatePaper", "SearchService")
    demo.subscribe("ContentService", "UpdatePaper", "SearchService")
    demo.send(make_event("OtherService", "CreatePaper", {"PaperId": 4}))
    demo.send(make_event("ContentService", "UpdatePaper", {"PaperId": 4}))
    e2 = comment(4)
    demo.send(e2); demo.pump()
    assert demo.status(e2["EventId"], "SearchService") == "PENDING"


# ---------------- time window (StatusUpdatedOn, not event Timestamp) ----------------
def test_last_event_gap_uses_completion_time(pipeline):
    p = pipeline
    p.subscribe(*PAPER, "SearchService"); p.subscribe(*COMMENT, "SearchService")
    p.add_rule("SearchService", "MakeComment", *PAPER, "SearchService", "PaperId", gap=1000)
    old = paper(8, timestamp=1)  # ancient event Timestamp is irrelevant
    p.send(old); p.pump()
    p.clock.advance(7 * 24 * 3600)  # predecessor completion now 7 days old
    e2 = comment(8)
    p.send(e2); p.pump()
    assert p.status(e2["EventId"], "SearchService") == "PENDING"
    fresh = paper(8)
    p.send(fresh); p.pump()
    assert p.status(e2["EventId"], "SearchService") == "COMPLETED"


def test_recent_predecessor_within_gap_satisfies(pipeline):
    p = pipeline
    p.subscribe(*PAPER, "SearchService"); p.subscribe(*COMMENT, "SearchService")
    p.add_rule("SearchService", "MakeComment", *PAPER, "SearchService", "PaperId", gap=1000)
    p.send(paper(8, timestamp=1)); p.pump()
    p.clock.advance(999)
    e2 = comment(8)
    p.send(e2); p.pump()
    assert p.status(e2["EventId"], "SearchService") == "COMPLETED"


# ---------------- independence / no global FIFO ----------------
def test_blocked_task_does_not_block_unrelated_work(demo):
    blocked = comment(1000)
    demo.send(blocked)
    others = [paper(2000 + i) for i in range(20)] + [comment(2000 + i) for i in range(20)]
    result = demo.send_batch(others[:10]); assert result["batchItemFailures"] == []
    result = demo.send_batch(others[10:20]); assert result["batchItemFailures"] == []
    demo.pump()
    for e in others[20:]:
        demo.send(e)
    demo.pump()
    assert demo.status(blocked["EventId"], "SearchService") == "PENDING"
    assert all(demo.status(e["EventId"], c) == "COMPLETED" for e in others for c in ("SearchService", "ReportingService"))


def test_one_event_fans_out_to_independent_consumer_tasks(demo):
    e1 = paper(55)
    demo.consumers["ReportingService"].ack = lambda payload: (503, {})
    demo.send(e1); demo.pump()
    assert demo.status(e1["EventId"], "SearchService") == "COMPLETED"
    assert demo.status(e1["EventId"], "ReportingService") == "READY"


# ---------------- duplicates ----------------
def test_duplicate_ingestion_is_one_identity_and_one_task_per_consumer(demo):
    e1 = paper(11)
    for _ in range(3):
        demo.send(e1)
    demo.send_batch([e1, e1])
    demo.pump()
    assert len([t for t in demo.tasks() if t["EventId"] == e1["EventId"]]) == 2
    assert demo.projection("SearchService", 11)["AppliedCount"] == 1
    assert len(demo.sns_log) == 2


def test_conflicting_payload_same_event_id_goes_to_dlq(demo):
    e1 = paper(12)
    demo.send(e1)
    assert demo.send({**e1, "Parameters": {"PaperId": 13}}) == "dlq"
    demo.pump()
    assert demo.task(e1["EventId"], "SearchService")["Parameters"] == {"PaperId": 12}


def test_duplicate_sns_and_stream_deliveries_do_not_duplicate_side_effects(demo):
    demo.duplicate_sns_deliveries = True
    demo.duplicate_stream_deliveries = True
    e2, e1 = comment(21), paper(21)
    demo.send(e2); demo.send(e1); demo.pump()
    proj = demo.projection("SearchService", 21)
    assert proj["AppliedOrder"] == [e1["EventId"], e2["EventId"]]
    assert proj["AppliedCount"] == 2 and proj["CommentCount"] == 1
    statuses = [code for _, payload, code in demo.ack_log if payload["EventId"] == e1["EventId"]]
    assert 200 in statuses and all(code == 200 for code in statuses)  # duplicate ACKs are idempotent


def test_crash_between_publish_and_bookkeeping_republishes_safely(demo):
    original = demo.store.mark_dispatched
    calls = {"n": 0}

    def crash_once(task, now, mid):
        if task["ConsumerId"] == "SearchService":
            calls["n"] += 1
        if calls["n"] == 1 and task["ConsumerId"] == "SearchService":
            raise RuntimeError("simulated Lambda crash after SNS publish")
        return original(task, now, mid)
    demo.store.mark_dispatched = crash_once
    e1 = paper(31)
    demo.send(e1); demo.pump()
    search_msgs = [m for m in demo.sns_log if m["Attributes"]["ConsumerId"] == "SearchService"]
    assert len(search_msgs) == 2  # stream retry re-published
    assert demo.projection("SearchService", 31)["AppliedCount"] == 1
    assert demo.status(e1["EventId"], "SearchService") == "COMPLETED"


# ---------------- ACK security ----------------
def _ack_payload(task, outcome="COMPLETED"):
    return {"TaskId": task["TaskId"], "ProducerId": task["ProducerId"], "TaskSK": task["SK"],
            "EventId": task["EventId"], "ConsumerId": task["ConsumerId"], "Outcome": outcome}


def test_forged_and_wrong_consumer_acks_rejected(demo):
    demo.consumers["SearchService"].ack = lambda payload: (503, {})
    e1 = paper(41)
    demo.send(e1); demo.pump()
    t = demo.task(e1["EventId"], "SearchService")
    assert t["Status"] == "READY"
    assert demo.call_completion(_ack_payload(t), role=None)[0] == 403                       # IAM user, not consumer
    assert demo.call_completion(_ack_payload(t), role="reporting-consumer-role")[0] == 403  # other consumer's role
    forged = {**_ack_payload(t), "ConsumerId": "ReportingService"}
    assert demo.call_completion(forged, role="reporting-consumer-role")[0] == 403           # claims wrong task owner
    assert demo.call_completion({**_ack_payload(t), "TaskId": "0" * 64}, role="search-consumer-role")[0] == 403
    assert demo.call_completion({**_ack_payload(t), "TaskSK": "nope"}, role="search-consumer-role")[0] == 404
    assert demo.status(e1["EventId"], "SearchService") == "READY"
    assert demo.call_completion(_ack_payload(t), role="search-consumer-role")[0] == 200
    assert demo.call_completion(_ack_payload(t), role="search-consumer-role")[1]["result"] == "duplicate"


def test_ack_on_pending_task_is_conflict(demo):
    e2 = comment(42)
    demo.send(e2); demo.pump()
    t = demo.task(e2["EventId"], "SearchService")
    code, _ = demo.call_completion(_ack_payload(t), role="search-consumer-role")
    assert code == 409 and demo.status(e2["EventId"], "SearchService") == "PENDING"


# ---------------- transient failures, DLQ, terminal failure, replay ----------------
def test_transient_ingest_failure_retried_then_succeeds(demo):
    e1 = paper(51)
    assert demo.send(e1, fault_ingest_failures=2) == "ingested@3"
    demo.pump()
    assert demo.status(e1["EventId"], "SearchService") == "COMPLETED"


def test_persistent_ingest_failure_reaches_dlq_without_tasks(demo):
    e1 = paper(52)
    assert demo.send(e1, fault_ingest_failures=99) == "dlq"
    assert not [t for t in demo.tasks() if t["EventId"] == e1["EventId"]]
    assert demo.ingress_dlq[0]["receives"] == 5
    # Operator replay from DLQ keeps original EventId/Timestamp.
    replayed = json.loads(demo.ingress_dlq[0]["body"])
    assert demo.send(replayed) == "ingested@1"
    demo.pump()
    assert demo.status(e1["EventId"], "SearchService") == "COMPLETED"


def test_malformed_message_reaches_dlq(demo):
    assert demo.send("not json") == "dlq"
    assert demo.send(json.dumps({"EventId": "x"})) == "dlq"


def test_unsupported_operator_fails_closed(demo):
    demo.add_rule("ReportingService", "MakeComment", *PAPER, "ReportingService", "PaperId", operation="GREATER")
    assert demo.send(comment(53)) == "dlq"


def test_transient_consumer_failure_retried_side_effect_once(demo):
    e1 = paper(54)
    demo.inject_consumer_fault(e1["EventId"], "SearchService", "TRANSIENT", remaining=2)
    demo.send(e1); demo.pump()
    assert demo.status(e1["EventId"], "SearchService") == "COMPLETED"
    assert demo.projection("SearchService", 54)["AppliedCount"] == 1
    assert not demo.consumer_dlq


def test_terminal_failure_propagates_then_replay_recovers(demo):
    demo.add_rule("ReportingService", "MakeComment", *PAPER, "SearchService", "PaperId")
    e1, e2 = paper(61), comment(61)
    unrelated = comment(62)
    demo.inject_consumer_fault(e1["EventId"], "SearchService", "TERMINAL")
    demo.send(e2); demo.send(unrelated); demo.send(e1); demo.pump()
    root = demo.task(e1["EventId"], "SearchService")
    assert root["Status"] == "FAILED" and root["FailureReason"] == "CONSUMER_TERMINAL_FAILURE"
    assert demo.status(e1["EventId"], "ReportingService") == "COMPLETED"  # completed work never rolled back
    for consumer in ("SearchService", "ReportingService"):
        dep = demo.task(e2["EventId"], consumer)
        assert dep["Status"] == "FAILED" and dep["FailureReason"] == "DEPENDENCY_FAILED"
        assert dep["RootFailureEventId"] == e1["EventId"]
        assert dep["FailureChain"] == [root["TaskId"], dep["TaskId"]]
    assert demo.status(unrelated["EventId"], "SearchService") == "PENDING"  # different PaperId, unaffected
    assert demo.projection("SearchService", 61) is None

    demo.clear_consumer_fault(e1["EventId"], "SearchService")
    replayed = replay_failed(demo.store, root_event_id=e1["EventId"], now_ms=demo.clock(), operator="test-operator")
    assert len(replayed) == 3
    demo.pump()
    for consumer in ("SearchService", "ReportingService"):
        assert demo.status(e2["EventId"], consumer) == "COMPLETED"
        assert int(demo.task(e2["EventId"], consumer)["ReplayGeneration"]) == 1
    assert demo.projection("SearchService", 61)["AppliedOrder"] == [e1["EventId"], e2["EventId"]]


def test_failure_does_not_fail_dependent_when_alternative_predecessor_completed(demo):
    good, bad = paper(71), paper(71)
    demo.send(good); demo.pump()
    demo.inject_consumer_fault(bad["EventId"], "SearchService", "TERMINAL")
    e2 = comment(71)
    # Register E2 as waiter BEFORE bad fails, by sending e2 after good completed (it goes READY immediately).
    demo.send(e2); demo.send(bad); demo.pump()
    assert demo.status(bad["EventId"], "SearchService") == "FAILED"
    assert demo.status(e2["EventId"], "SearchService") == "COMPLETED"


# ---------------- race regressions (explicit interleavings) ----------------
def _complete_now(p, event_id, consumer):
    t = p.task(event_id, consumer)
    assert p.store.complete(t, p.clock(), acked_by="x")


@pytest.mark.parametrize("when", ["before-waiter", "between-waiter-and-check", "after-check"])
def test_no_lost_wakeup_for_any_completion_interleaving(demo, when):
    demo.consumers["SearchService"].ack = lambda payload: (503, {})  # we complete E1 manually
    e1, e2 = paper(81), comment(81)
    demo.send(e1); demo.pump()
    assert demo.status(e1["EventId"], "SearchService") == "READY"
    demo.consumers["SearchService"].ack = demo._acker("SearchService")

    store = demo.store
    orig_put, orig_has = store.put_waiter, store.has_done
    fired = []

    def complete_once(point):
        if when == point and not fired:  # inject the parent completion exactly once, at this point
            fired.append(point)
            _complete_now(demo, e1["EventId"], "SearchService")
            # A concurrent Resolver invocation consumes the completion stream record right now.
            parent = demo.task(e1["EventId"], "SearchService")
            demo.resolver.on_completed(parent)
            demo.resolver_consumed.add((parent["TaskId"], "COMPLETED"))

    def put_waiter(*a, **k):
        complete_once("before-waiter")
        orig_put(*a, **k)
        complete_once("between-waiter-and-check")

    def has_done(*a, **k):
        result = orig_has(*a, **k)
        complete_once("after-check")
        return result
    store.put_waiter, store.has_done = put_waiter, has_done
    demo.send(e2)
    demo.pump()  # first evaluation runs with the injected interleaving
    store.put_waiter, store.has_done = orig_put, orig_has
    demo.pump()
    assert fired == [when]
    assert demo.status(e2["EventId"], "SearchService") == "COMPLETED"
    search_e2 = [m for m in demo.sns_log if json.loads(m["Message"])["EventId"] == e2["EventId"]
                 and m["Attributes"]["ConsumerId"] == "SearchService"]
    assert len(search_e2) == 1  # exactly one READY transition -> one dispatch


def test_simultaneous_completion_of_two_and_parents_single_ready(demo):
    demo.subscribe("ModerationService", "ApproveComment", "SearchService")
    demo.add_rule("SearchService", "MakeComment", "ModerationService", "ApproveComment", "SearchService", "CommentId")
    demo.consumers["SearchService"].ack = lambda payload: (503, {})
    e2, e1, e3 = comment(91, cid=5), paper(91), make_event("ModerationService", "ApproveComment", {"CommentId": 5})
    demo.send(e2); demo.send(e1); demo.send(e3); demo.pump()
    demo.consumers["SearchService"].ack = demo._acker("SearchService")
    _complete_now(demo, e1["EventId"], "SearchService")
    _complete_now(demo, e3["EventId"], "SearchService")  # both commit before either stream record is processed
    demo.pump()
    msgs = [m for m in demo.sns_log if json.loads(m["Message"])["EventId"] == e2["EventId"]
            and m["Attributes"]["ConsumerId"] == "SearchService"]
    assert len(msgs) == 1
    assert demo.status(e2["EventId"], "SearchService") == "COMPLETED"


def test_reconcile_pending_recovers_missed_stream_wakeup(demo):
    e2, e1 = comment(95), paper(95)
    demo.send(e2); demo.pump()
    waiters = demo.store.waiters_for
    demo.store.waiters_for = lambda key: []  # simulate lost/expired completion stream record
    demo.send(e1); demo.pump()
    demo.store.waiters_for = waiters
    assert demo.status(e2["EventId"], "SearchService") == "PENDING"
    results = reconcile_pending(demo.store, demo.resolver)
    assert results[demo.task(e2["EventId"], "SearchService")["TaskId"]] == "ready"
    demo.pump()
    assert demo.status(e2["EventId"], "SearchService") == "COMPLETED"


def test_throttled_ack_is_retried_in_process_without_second_side_effect(demo):
    """Account Lambda concurrency is 10: Completion API may return 429/5xx under load."""
    real_ack = demo.consumers["SearchService"].ack
    responses = iter([(429, {}), (502, {})])
    demo.consumers["SearchService"].ack = lambda payload: next(responses, None) or real_ack(payload)
    e1 = paper(101)
    demo.send(e1); demo.pump()
    assert demo.status(e1["EventId"], "SearchService") == "COMPLETED"
    assert demo.projection("SearchService", 101)["AppliedCount"] == 1
    assert not demo.consumer_dlq


def test_non_retryable_ack_error_is_not_retried(demo):
    calls = []
    demo.consumers["SearchService"].ack = lambda payload: calls.append(1) or (403, {})
    demo.send(paper(102)); demo.pump()
    assert len(calls) == 3  # one per Lambda attempt (1 + 2 async retries), no in-process retry on 403
    assert len(demo.consumer_dlq) == 1
