"""P0 canonical scenario on the local pipeline (moto). LOCAL ONLY - not AWS evidence."""
from local_pipeline import make_event


def test_e2_before_e1_out_of_order_full_lifecycle(demo):
    p = demo
    e1 = make_event("ContentService", "CreatePaper", {"PaperId": 123})
    e2 = make_event("CommentService", "MakeComment", {"PaperId": 123, "CommentId": 456})

    assert p.send(e2) == "ingested@1"
    p.pump()
    assert p.status(e2["EventId"], "SearchService") == "PENDING"
    # ReportingService has no rule for MakeComment: independent task proceeds (no global FIFO).
    assert p.status(e2["EventId"], "ReportingService") == "COMPLETED"
    assert p.projection("SearchService", 123) is None  # no side effect for blocked task

    assert p.send(e1) == "ingested@1"
    p.pump()
    t1 = p.task(e1["EventId"], "SearchService")
    t2 = p.task(e2["EventId"], "SearchService")
    assert t1["Status"] == "COMPLETED" and t2["Status"] == "COMPLETED"
    # E1 READY -> COMPLETED only after ACK; E2 READY only after E1 COMPLETED.
    assert t1["ReadyAt"] < t1["CompletedAt"] <= t2["ReadyAt"] < t2["CompletedAt"]
    assert t1["AckedBy"] == "search-consumer-role"
    # Real downstream side effect, applied in dependency order.
    proj = p.projection("SearchService", 123)
    assert proj["AppliedOrder"] == [e1["EventId"], e2["EventId"]]
    assert proj["PaperIndexed"] is True and proj["CommentCount"] == 1
    markers = {m["EventId"]: m["ProcessedAt"] for m in p.markers("SearchService")}
    assert markers[e1["EventId"]] < markers[e2["EventId"]]
    assert not p.ingress_dlq and not p.consumer_dlq and not p.stream_failures


def test_ready_is_not_completed_without_ack(demo):
    p = demo
    p.consumers["SearchService"].ack = lambda payload: (503, {"error": "api down"})  # ACK never lands
    e1 = make_event("ContentService", "CreatePaper", {"PaperId": 7})
    p.send(e1)
    p.pump()
    t = p.task(e1["EventId"], "SearchService")
    assert t["Status"] == "READY" and int(t["DispatchAttempts"]) == 1
    assert p.projection("SearchService", 7)["AppliedCount"] == 1  # side effect happened once despite retries
    assert len(p.consumer_dlq) == 1  # retries exhausted -> on-failure destination
