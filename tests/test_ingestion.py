import json
from copy import deepcopy
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from event_platform.aws_store import DynamoIngestStore, to_dynamo
from event_platform.handlers import handle_sqs_batch
from event_platform.ingestion import (EventConflict, InvalidEvent, SourceEvent, escape_segment,
                                      ingest, records_for, route_key, task_id, task_sort_key)


def make_message(event_id="9cddcab2-435b-4da0-a410-22794d727779", **changes):
    payload = {"EventId": event_id, "ProducerId": "ContentService", "Action": "CreatePaper",
               "Timestamp": 1792280000000, "Parameters": {"PaperId": "123"}}
    payload.update(changes)
    return json.dumps(payload)


class InMemoryStore:
    def __init__(self, consumers=("SearchService", "ReportingService")):
        self.registered = tuple(consumers)
        self.identity = {}
        self.rows = []

    def consumers_for(self, producer_id, action):
        return list(self.registered) if (producer_id, action) == ("ContentService", "CreatePaper") else []

    def rules_for(self, consumer_id, client_action):
        return []

    def atomically_ingest(self, event, rows):
        key = event.producer_id, event.event_id
        if key in self.identity:
            if self.identity[key] != event.fingerprint():
                raise EventConflict("conflicting original payload")
            return "duplicate"
        self.identity[key] = event.fingerprint()
        self.rows.extend(deepcopy(rows))
        return "created"


def test_phase1_fanout_and_stable_uuid_semantics():
    store = InMemoryStore()
    body = make_message("72acb384-fcbe-4eb2-aae2-37ad6c33e982")
    assert ingest(body, store) == "created"
    assert len(store.rows) == 3
    assert {r["ConsumerId"] for r in store.rows if r["RecordType"] == "TASK"} == {
        "SearchService", "ReportingService"}
    assert all(r["Status"] == "PENDING" for r in store.rows if r["RecordType"] == "TASK")
    assert ingest(body, store) == "duplicate"
    assert len(store.rows) == 3


def test_same_event_id_new_payload_is_conflict():
    store = InMemoryStore()
    assert ingest(make_message(), store) == "created"
    with pytest.raises(EventConflict):
        ingest(make_message(Parameters={"PaperId": "DIFFERENT"}), store)
    assert len(store.rows) == 3


def test_identity_has_no_subscribers_not_fake_complete():
    store = InMemoryStore(consumers=())
    assert ingest(make_message(), store) == "created"
    assert len(store.rows) == 1
    assert store.rows[0]["RoutingStatus"] == "NO_SUBSCRIBERS"


def test_duplicate_event_does_not_fanout_to_late_subscriber():
    store = InMemoryStore(consumers=("SearchService",))
    body = make_message()
    assert ingest(body, store) == "created"
    store.registered = ("SearchService", "LateConsumer")
    assert ingest(body, store) == "duplicate"
    assert len(store.rows) == 2


def test_stable_key_prefix_and_collision_safe_components():
    e = SourceEvent.from_json(make_message(Action="Act#ion"))
    assert task_sort_key(e, "Consumer#A").startswith("Act%23ion#Consumer%23A#")
    assert escape_segment("one#two") == "one%23two"
    assert route_key("ContentService", "CreatePaper") == "ContentService#CreatePaper"
    assert task_id(e, "Consumer#A") != task_id(e, "Consumer#B")


@pytest.mark.parametrize("body", [
    "[]", "{", '{"EventId":"x","EventId":"y"}',
    make_message(Timestamp=True), make_message(Timestamp=-1),
    make_message(Parameters=[]), make_message(Action=""),
    make_message(UnapprovedField="bad"), make_message(event_id="event-1"),
    '{"EventId":"x","ProducerId":"p","Action":"a","Timestamp":1,"Parameters":{"x":NaN}}',
])
def test_rejects_invalid_events(body):
    with pytest.raises(InvalidEvent):
        SourceEvent.from_json(body)


def test_max_transaction_fanout_is_explicit():
    e = SourceEvent.from_json(make_message())
    assert len(records_for(e, [f"C{i}" for i in range(99)])) == 100
    with pytest.raises(InvalidEvent):
        records_for(e, [f"C{i}" for i in range(100)])


def test_sqs_partial_batch_failure_only_invalid_message():
    store = InMemoryStore()
    response = handle_sqs_batch({"Records": [
        {"messageId": "ok-1", "body": make_message()},
        {"messageId": "bad-2", "body": "not JSON"},
        {"messageId": "ok-3", "body": make_message()},
    ]}, store)
    assert response == {"batchItemFailures": [{"itemIdentifier": "bad-2"}]}
    assert len(store.rows) == 3


def test_aws_store_uses_transactional_conditional_put():
    resource = MagicMock()
    client = MagicMock()
    adapter = DynamoIngestStore(config_table="Config", log_table="Log",
                                route_index="BySourceAction", resource=resource, client=client)
    e = SourceEvent.from_json(make_message())
    result = adapter.atomically_ingest(e, records_for(e, ["SearchService"]))
    assert result == "created"
    req = client.transact_write_items.call_args.kwargs
    assert len(req["TransactItems"]) == 2
    assert all("attribute_not_exists" in x["Put"]["ConditionExpression"] for x in req["TransactItems"])
    assert req["TransactItems"][0]["Put"]["Item"]["SK"]["S"] == "EVENT#9cddcab2-435b-4da0-a410-22794d727779"


def test_aws_store_existing_identity_conflict_and_duplicate():
    resource, client = MagicMock(), MagicMock()
    adapter = DynamoIngestStore(config_table="Config", log_table="Log",
                                route_index="BySourceAction", resource=resource, client=client)
    e = SourceEvent.from_json(make_message())
    client.transact_write_items.side_effect = ClientError(
        {"Error": {"Code": "TransactionCanceledException", "Message": "condition failed"}},
        "TransactWriteItems")
    resource.Table.return_value.get_item.return_value = {"Item": {"PayloadFingerprint": e.fingerprint()}}
    assert adapter.atomically_ingest(e, records_for(e, ["SearchService"])) == "duplicate"
    resource.Table.return_value.get_item.return_value = {"Item": {"PayloadFingerprint": "different"}}
    with pytest.raises(EventConflict):
        adapter.atomically_ingest(e, records_for(e, ["SearchService"]))


def test_dynamodb_float_parameters_get_lossless_decimal_type():
    assert str(to_dynamo({"score": 1.25})["score"]) == "1.25"


def test_ingestion_timestamp_and_same_time_distinct_events():
    e1 = SourceEvent.from_json(make_message())
    e2 = SourceEvent.from_json(make_message(event_id="72acb384-fcbe-4eb2-aae2-37ad6c33e982"))
    first = records_for(e1, ["SearchService"], now_ms=1792280000999)
    second = records_for(e2, ["SearchService"], now_ms=1792280000999)
    assert first[1]["StatusUpdatedOn"] == 1792280000999
    assert first[1]["SK"] != second[1]["SK"]
    assert first[0]["IngestedAt"] == 1792280000999
