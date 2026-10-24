"""Unit tests for handler plumbing, filters and template wiring (no AWS)."""
import json

import pytest

from event_platform.completion import caller_role_name
from event_platform.config import (DependencyRule, InvalidRule, canonical_value, completion_keys, dependency_key,
                                   rule_sort_key)
from event_platform.dispatch import is_ready_transition
from event_platform.handlers import handle_stream_batch
from local_pipeline import load_template, matches


def _img(**kw):
    return {k: {"S": v} for k, v in kw.items()}


def test_stream_batch_reports_first_failing_sequence_and_stops():
    seen = []

    class H:
        def handle_change(self, name, old, new):
            seen.append(new["TaskId"])
            if new["TaskId"] == "t2":
                raise RuntimeError("boom")
            return "ok"
    records = [{"eventName": "INSERT", "dynamodb": {"SequenceNumber": str(i), "NewImage": _img(TaskId=f"t{i}")}}
               for i in (1, 2, 3)]
    assert handle_stream_batch({"Records": records}, H()) == {"batchItemFailures": [{"itemIdentifier": "2"}]}
    assert seen == ["t1", "t2"]  # t3 is retried by Lambda from the checkpoint, not skipped


def test_dispatch_filter_and_code_ignore_ready_bookkeeping_updates():
    pattern = json.loads(load_template()["Resources"]["DispatchFunction"]["Properties"]["Events"]["LogStream"]
                         ["Properties"]["FilterCriteria"]["Filters"][0]["Pattern"])
    ready = {"eventName": "MODIFY", "dynamodb": {"OldImage": _img(Status="PENDING"),
                                                 "NewImage": _img(Status="READY", RecordType="TASK")}}
    bookkeeping = {"eventName": "MODIFY", "dynamodb": {"OldImage": _img(Status="READY"),
                                                       "NewImage": _img(Status="READY", RecordType="TASK")}}
    identity = {"eventName": "INSERT", "dynamodb": {"NewImage": _img(RecordType="EVENT")}}
    assert matches(pattern, ready) and not matches(pattern, bookkeeping) and not matches(pattern, identity)
    assert is_ready_transition("MODIFY", {"Status": "PENDING"}, {"Status": "READY", "RecordType": "TASK"})
    assert not is_ready_transition("MODIFY", {"Status": "READY"}, {"Status": "READY", "RecordType": "TASK"})


def test_resolver_filters_cover_insert_completion_failure_and_replay_only():
    filters = [json.loads(f["Pattern"]) for f in load_template()["Resources"]["ResolverFunction"]["Properties"]
               ["Events"]["LogStream"]["Properties"]["FilterCriteria"]["Filters"]]

    def rec(name, old, new):
        d = {"NewImage": _img(RecordType="TASK", Status=new)}
        if old:
            d["OldImage"] = _img(Status=old)
        return {"eventName": name, "dynamodb": d}
    hit = lambda r: any(matches(f, r) for f in filters)  # noqa: E731
    assert hit(rec("INSERT", None, "PENDING"))
    assert hit(rec("MODIFY", "READY", "COMPLETED"))
    assert hit(rec("MODIFY", "PENDING", "FAILED")) and hit(rec("MODIFY", "READY", "FAILED"))
    assert hit(rec("MODIFY", "FAILED", "PENDING"))
    assert not hit(rec("MODIFY", "PENDING", "READY")) and not hit(rec("MODIFY", "READY", "READY"))


def test_rule_validation_rejects_unsupported_operator_and_bad_gap():
    base = {"ClientId": "S", "SK": rule_sort_key("MakeComment", "ContentService", "CreatePaper"),
            "ClientAction": "MakeComment", "ProducerId": "ContentService", "Action": "CreatePaper",
            "ServiceConsumer": "S", "Parameter": "PaperId", "Operation": "EQUAL", "lastEventGap": -1}
    assert DependencyRule.from_item(base).last_event_gap == -1
    for bad in ({"Operation": "GT"}, {"lastEventGap": -2}, {"lastEventGap": 1.5}, {"SK": "Other#x#y"}):
        with pytest.raises(InvalidRule):
            DependencyRule.from_item({**base, **bad})


def test_dependency_key_is_type_strict_and_decimal_stable():
    from decimal import Decimal
    assert canonical_value(123) == canonical_value(Decimal("123")) != canonical_value("123")
    assert dependency_key("P", "A", "C", "PaperId", 1) != dependency_key("P", "A", "D", "PaperId", 1)
    assert completion_keys("P", "A", "C", {"x": {"nested": 1}, "y": 2}) == [dependency_key("P", "A", "C", "y", 2)]


def test_caller_role_parsing():
    ctx = {"identity": {"userArn": "arn:aws:sts::111122223333:assumed-role/stack-SearchConsumerFunctionRole-AB/fn"}}
    assert caller_role_name(ctx) == "stack-SearchConsumerFunctionRole-AB"
    assert caller_role_name({"identity": {"userArn": "arn:aws:iam::111122223333:user/alice"}}) is None


def test_signed_ack_client_adds_sigv4_headers(monkeypatch):
    import boto3
    from event_platform import consumer as consumer_mod
    captured = {}

    class Resp:
        status = 200
        def read(self): return b'{"result":"updated"}'
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(req, timeout, context=None):
        captured["headers"] = {k.lower(): v for k, v in req.header_items()}
        return Resp()
    monkeypatch.setattr(consumer_mod.urllib.request, "urlopen", fake_urlopen)
    session = boto3.Session(aws_access_key_id="AKIDEXAMPLE", aws_secret_access_key="secret", region_name="ca-central-1")
    post = consumer_mod.signed_ack_client("https://abc.execute-api.ca-central-1.amazonaws.com/dev/tasks/complete",
                                          "ca-central-1", session)
    assert post({"TaskId": "t"}) == (200, {"result": "updated"})
    assert captured["headers"]["authorization"].startswith("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
    assert "/ca-central-1/execute-api/aws4_request" in captured["headers"]["authorization"]


def test_template_grants_item_actions_used_inside_transactions():
    """TransactWriteItems is authorized per contained action (PutItem/UpdateItem), not a separate IAM action."""
    res = load_template()["Resources"]

    def actions(fn):
        out = set()
        for pol in res[fn]["Properties"]["Policies"]:
            for st in (pol.get("Statement") or []) if isinstance(pol, dict) else []:
                out.update(st["Action"])
        return out
    assert {"dynamodb:PutItem", "dynamodb:GetItem"} <= actions("IngestFunction")
    assert {"dynamodb:UpdateItem", "dynamodb:PutItem"} <= actions("CompletionFunction")
    assert {"dynamodb:PutItem", "dynamodb:UpdateItem"} <= actions("SearchConsumerFunction")
    assert res["CompletionApi"]["Properties"]["Auth"]["DefaultAuthorizer"] == "AWS_IAM"


def test_completion_api_does_not_invoke_lambda_with_caller_credentials():
    """Regression (live 2026-10-21): SAM AWS_IAM default InvokeRole=CALLER_CREDENTIALS caused API GW 500s."""
    assert load_template()["Resources"]["CompletionApi"]["Properties"]["Auth"]["InvokeRole"] == "NONE"
