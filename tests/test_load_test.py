"""Load-generator control logic (waves, rate cap, backpressure, checkpoints) against a fake Live; no AWS."""
import json
import sys
import time
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_test  # noqa: E402


class FakeLive:
    """Tasks become COMPLETED on the second read after they are sent."""

    def __init__(self):
        self.sent, self.reads, self.batches = [], {}, []
        self.out = {"ReplicationLogTableName": "log"}

    def send_batch(self, chunk):
        self.batches.append((time.time(), len(chunk)))
        self.sent.extend(chunk)
        return []

    def task_key(self, event, consumer):
        return {"EventId": event["EventId"], "ConsumerId": consumer}

    def queue_depth(self, key):
        return {"ApproximateNumberOfMessages": 0, "ApproximateNumberOfMessagesNotVisible": 0}


def fake_batch_get(live, keys):
    items = []
    for k in keys:
        ident = (k["EventId"], k["ConsumerId"])
        live.reads[ident] = live.reads.get(ident, 0) + 1
        items.append({**k, "Status": "COMPLETED" if live.reads[ident] >= 2 else "READY"})
    return items


def args(**kw):
    base = dict(wave=0, rate=0, max_inflight=0, threads=4, poll=0, timeout=30, stall_timeout=0)
    return Namespace(**{**base, **kw})


def test_waves_checkpoint_and_backpressure(tmp_path, monkeypatch):
    monkeypatch.setattr(load_test, "batch_get", fake_batch_get)
    live = FakeLive()
    _, pairs, flat = load_test.build_workload(40, seed=1)
    tracker = load_test.Tracker(live)
    sent_at, failures = load_test.send_all(live, flat, args(wave=10, max_inflight=5), tracker, tmp_path, time.time())
    assert failures == [] and len(sent_at) == 40 and [e["EventId"] for e in live.sent] == [e["EventId"] for e in flat]
    rows = [json.loads(line) for line in (tmp_path / "progress.jsonl").read_text().splitlines()]
    # every wave after the first waited for the previous wave's 20 tasks to drain below the cap
    assert sum("backpressure" in r["note"] for r in rows) >= 3
    assert len((tmp_path / "sent.jsonl").read_text().splitlines()) == 40
    assert load_test.wait_done(live, tracker, args(), tmp_path, time.time()) == "done"
    assert len(tracker.done) == 80 and tracker.inflight() == 0


def test_rate_cap_spaces_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(load_test, "batch_get", fake_batch_get)
    live = FakeLive()
    _, _, flat = load_test.build_workload(40, seed=2)
    t0 = time.time()
    load_test.send_all(live, flat, args(rate=100), load_test.Tracker(live), tmp_path, t0)
    # 4 batches of 10 at 100 events/s -> scheduled at 0, 0.1, 0.2, 0.3 s
    assert time.time() - t0 >= 0.29


def test_stall_detection_stops_waiting(tmp_path, monkeypatch):
    monkeypatch.setattr(load_test, "batch_get", lambda live, keys: [{**k, "Status": "READY"} for k in keys])
    live = FakeLive()
    _, _, flat = load_test.build_workload(4, seed=3)
    tracker = load_test.Tracker(live)
    tracker.add(flat)
    assert load_test.wait_done(live, tracker, args(stall_timeout=0.05, poll=0.02), tmp_path, time.time()) == "stalled"
