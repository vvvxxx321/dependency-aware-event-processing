"""Local end-to-end pipeline on moto DynamoDB (NOT AWS). Exercises the same handler code as Lambda.

Fidelity: real conditional writes/transactions and DynamoDB Streams records from moto; table key schemas,
stream FilterCriteria and SNS FilterPolicies are read from template.yaml. SQS redrive (maxReceiveCount),
Lambda async retries and stream retries are simulated. Concurrency is NOT simulated except where tests
interleave calls explicitly.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any

import boto3
import yaml

from event_platform.aws_store import PlatformStore
from event_platform.completion import CompletionService
from event_platform.config import DependencyRule, rule_sort_key, subscription_item
from event_platform.consumer import DemoConsumer, fault_key
from event_platform.dispatch import Dispatcher
from event_platform.handlers import handle_sqs_batch, handle_stream_batch
from event_platform.resolver import Resolver

TEMPLATE = Path(__file__).resolve().parents[1] / "template.yaml"
ROLES = {"SearchService": "search-consumer-role", "ReportingService": "reporting-consumer-role"}


class _CfnLoader(yaml.SafeLoader):
    pass


_CfnLoader.add_multi_constructor("!", lambda loader, suffix, node: None)


def load_template() -> dict[str, Any]:
    return yaml.load(TEMPLATE.read_text(), Loader=_CfnLoader)


def _match_values(allowed: list, value: Any, present: bool) -> bool:
    for cond in allowed:
        if isinstance(cond, dict) and "anything-but" in cond:
            if present and value not in cond["anything-but"]:
                return True
        elif present and value == cond:
            return True
    return False


def matches(pattern: dict, record: dict) -> bool:
    """Subset of Lambda/EventBridge filter syntax used by template.yaml (exact values, anything-but)."""
    for key, cond in pattern.items():
        present = isinstance(record, dict) and key in record
        if isinstance(cond, dict):
            if not present or not matches(cond, record[key]):
                return False
        elif not _match_values(cond, record.get(key) if present else None, present):
            return False
    return True


class Clock:
    """Monotonic ms clock tracking wall time (ingestion's IngestedAt uses wall time) plus a test offset."""

    def __init__(self):
        self.offset = 0
        self.t = 0

    def __call__(self) -> int:
        self.t = max(self.t + 1, int(time.time() * 1000) + self.offset)
        return self.t

    def advance(self, seconds: float) -> None:
        self.offset += int(seconds * 1000)


class LocalPipeline:
    """Call inside moto.mock_aws()."""

    def __init__(self, *, clock: Clock | None = None):
        os.environ.setdefault("AWS_DEFAULT_REGION", "ca-central-1")
        self.template = load_template()
        res = self.template["Resources"]
        self.client = boto3.client("dynamodb")
        self.tables = {}
        for logical in ("ConfigurationTable", "ReplicationLogTable", "DependencyWaitersTable", "ConsumerProjectionTable"):
            props = dict(res[logical]["Properties"])
            name = f"{logical}-{uuid.uuid4().hex[:6]}"
            args = {"TableName": name, "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": props["AttributeDefinitions"], "KeySchema": props["KeySchema"]}
            if "GlobalSecondaryIndexes" in props:
                args["GlobalSecondaryIndexes"] = props["GlobalSecondaryIndexes"]
            if "StreamSpecification" in props:
                args["StreamSpecification"] = {"StreamEnabled": True, **props["StreamSpecification"]}
            desc = self.client.create_table(**args)["TableDescription"]
            self.tables[logical] = name
            if "LatestStreamArn" in desc:
                self.stream_arn = desc["LatestStreamArn"]
        self.clock = clock or Clock()
        self.store = PlatformStore(config_table=self.tables["ConfigurationTable"],
                                   log_table=self.tables["ReplicationLogTable"], route_index="BySourceAction",
                                   waiters_table=self.tables["DependencyWaitersTable"])
        self.resolver = Resolver(self.store, self.clock)
        self.dispatcher = Dispatcher(self.store, self._publish, self.clock)
        self.completion = CompletionService(self.store, ROLES, self.clock)
        self.resolver_filters = self._filters("ResolverFunction")
        self.dispatch_filters = self._filters("DispatchFunction")
        self.consumer_filters = {
            res[f]["Properties"]["Environment"]["Variables"]["CONSUMER_ID"]:
                res[f]["Properties"]["Events"]["Dispatch"]["Properties"]["FilterPolicy"]
            for f in ("SearchConsumerFunction", "ReportingConsumerFunction")}
        self.max_ingest_receives = res["IngressQueue"]["Properties"]["RedrivePolicy"]["maxReceiveCount"]
        self.consumer_retries = res["SearchConsumerFunction"]["Properties"]["EventInvokeConfig"]["MaximumRetryAttempts"]
        self.stream_retries = res["ResolverFunction"]["Properties"]["Events"]["LogStream"]["Properties"]["MaximumRetryAttempts"]
        self.consumers = {c: DemoConsumer(c, self.client, self.tables["ConsumerProjectionTable"], self._acker(c),
                                          fault_injection=True, clock=self.clock, sleep=lambda s: None)
                          for c in ROLES}
        self.streams = boto3.client("dynamodbstreams")
        self.shard_id = self.streams.describe_stream(StreamArn=self.stream_arn)["StreamDescription"]["Shards"][0]["ShardId"]
        self.last_sequence: str | None = None
        self.sns: deque = deque()
        self.sns_log: list[dict] = []
        self.ingress_dlq: list[dict] = []
        self.consumer_dlq: list[dict] = []
        self.stream_failures: list[dict] = []
        self.ack_log: list[tuple[str, dict, int]] = []
        self.duplicate_sns_deliveries = False
        self.duplicate_stream_deliveries = False
        # (TaskId, Status) stream records already consumed by a simulated concurrent Resolver invocation.
        self.resolver_consumed: set[tuple[str, str]] = set()

    def _filters(self, function: str) -> list[dict]:
        props = self.template["Resources"][function]["Properties"]["Events"]["LogStream"]["Properties"]
        return [json.loads(f["Pattern"]) for f in props["FilterCriteria"]["Filters"]]

    # ---------- configuration ----------
    def subscribe(self, producer: str, action: str, consumer: str) -> None:
        self.client.put_item(TableName=self.tables["ConfigurationTable"],
                             Item={k: {"S": v} for k, v in subscription_item(producer, action, consumer).items()})

    def add_rule(self, client_id: str, client_action: str, producer: str, action: str, service_consumer: str,
                 parameter: str, gap: int = -1, operation: str = "EQUAL", suffix: str | None = None) -> str:
        sk = rule_sort_key(client_action, producer, action, suffix)
        item = {"ClientId": client_id, "SK": sk, "RecordType": "RULE", "ClientAction": client_action,
                "ProducerId": producer, "Action": action, "ServiceConsumer": service_consumer,
                "Parameter": parameter, "Operation": operation, "lastEventGap": gap}
        if operation == "EQUAL":
            DependencyRule.from_item(item)  # onboarding validation
        boto3.resource("dynamodb").Table(self.tables["ConfigurationTable"]).put_item(Item=item)
        return sk

    def inject_consumer_fault(self, event_id: str, consumer: str, mode: str, remaining: int = 0) -> None:
        item = {**fault_key(event_id, consumer), "Mode": mode, "Remaining": remaining}
        boto3.resource("dynamodb").Table(self.tables["ConsumerProjectionTable"]).put_item(Item=item)

    def clear_consumer_fault(self, event_id: str, consumer: str) -> None:
        boto3.resource("dynamodb").Table(self.tables["ConsumerProjectionTable"]).delete_item(Key=fault_key(event_id, consumer))

    # ---------- SQS ingress simulation ----------
    def send(self, event: dict[str, Any] | str, *, fault_ingest_failures: int = 0) -> str:
        body = event if isinstance(event, str) else json.dumps(event)
        msg_id = str(uuid.uuid4())
        attrs = ({"FaultInjectIngestFailures": {"stringValue": str(fault_ingest_failures), "dataType": "Number"}}
                 if fault_ingest_failures else {})
        old = os.environ.get("ENABLE_FAULT_INJECTION")
        os.environ["ENABLE_FAULT_INJECTION"] = "true"
        try:
            for receive in range(1, self.max_ingest_receives + 1):
                record = {"messageId": msg_id, "body": body, "messageAttributes": attrs,
                          "attributes": {"ApproximateReceiveCount": str(receive)}}
                result = handle_sqs_batch({"Records": [record]}, self.store)
                if not result["batchItemFailures"]:
                    return f"ingested@{receive}"
            self.ingress_dlq.append({"messageId": msg_id, "body": body, "receives": self.max_ingest_receives})
            return "dlq"
        finally:
            if old is None:
                os.environ.pop("ENABLE_FAULT_INJECTION", None)
            else:
                os.environ["ENABLE_FAULT_INJECTION"] = old

    def send_batch(self, events: list[dict[str, Any]]) -> dict:
        records = [{"messageId": str(uuid.uuid4()), "body": json.dumps(e), "attributes": {"ApproximateReceiveCount": "1"}}
                   for e in events]
        return handle_sqs_batch({"Records": records}, self.store)

    # ---------- SNS + consumer simulation ----------
    def _publish(self, message: dict[str, Any], attributes: dict[str, str]) -> str:
        message_id = str(uuid.uuid4())
        entry = {"MessageId": message_id, "Message": json.dumps(message), "Attributes": dict(attributes)}
        self.sns.append(entry)
        self.sns_log.append(entry)
        if self.duplicate_sns_deliveries:
            self.sns.append(dict(entry))
        return message_id

    def _acker(self, consumer: str):
        def ack(payload: dict[str, Any]) -> tuple[int, dict]:
            return self.call_completion(payload, role=ROLES[consumer])
        return ack

    def call_completion(self, payload: dict[str, Any], *, role: str | None) -> tuple[int, dict]:
        arn = f"arn:aws:sts::000000000000:assumed-role/{role}/session" if role else "arn:aws:iam::000000000000:user/x"
        resp = self.completion.handle({"body": json.dumps(payload), "requestContext": {"identity": {"userArn": arn}}})
        self.ack_log.append((payload.get("TaskId"), payload, resp["statusCode"]))
        return resp["statusCode"], json.loads(resp["body"])

    def _deliver(self, entry: dict) -> None:
        for consumer_id, policy in self.consumer_filters.items():
            if not all(entry["Attributes"].get(k) in v for k, v in policy.items()):
                continue
            message = json.loads(entry["Message"])
            for attempt in range(self.consumer_retries + 1):
                try:
                    self.consumers[consumer_id].handle_message(message)
                    break
                except Exception as exc:  # Lambda async retry, then on-failure destination
                    if attempt == self.consumer_retries:
                        self.consumer_dlq.append({"message": message, "error": repr(exc)})

    # ---------- streams ----------
    def _read_stream(self) -> list[dict]:
        # Checkpoint by sequence number (like the Lambda poller). A cached iterator goes stale in moto
        # because a cancelled transaction replaces its in-memory stream shard object.
        args = ({"ShardIteratorType": "AFTER_SEQUENCE_NUMBER", "SequenceNumber": self.last_sequence}
                if self.last_sequence else {"ShardIteratorType": "TRIM_HORIZON"})
        iterator = self.streams.get_shard_iterator(StreamArn=self.stream_arn, ShardId=self.shard_id, **args)
        records = self.streams.get_records(ShardIterator=iterator["ShardIterator"])["Records"]
        if records:
            self.last_sequence = records[-1]["dynamodb"]["SequenceNumber"]
        return records

    def _invoke_stream(self, handler, record: dict) -> None:
        for _ in range(self.stream_retries + 1):
            if not handle_stream_batch({"Records": [record]}, handler)["batchItemFailures"]:
                return
        self.stream_failures.append(record)

    def pump(self, max_rounds: int = 200) -> int:
        """Drain stream records and SNS deliveries until quiescent. Returns stream records seen."""
        seen = 0
        for _ in range(max_rounds):
            records = self._read_stream()
            progressed = bool(records) or bool(self.sns)
            for record in records:
                seen += 1
                lambda_view = {"eventName": record["eventName"], "dynamodb": record["dynamodb"]}
                image = record["dynamodb"].get("NewImage", {})
                marker = (image.get("TaskId", {}).get("S"), image.get("Status", {}).get("S"))
                for handler, filters in ((self.resolver, self.resolver_filters), (self.dispatcher, self.dispatch_filters)):
                    if handler is self.resolver and marker in self.resolver_consumed:
                        self.resolver_consumed.discard(marker)
                        continue
                    if any(matches(p, lambda_view) for p in filters):
                        self._invoke_stream(handler, record)
                        if self.duplicate_stream_deliveries:
                            self._invoke_stream(handler, record)
            while self.sns:
                self._deliver(self.sns.popleft())
            if not progressed:
                return seen
        raise RuntimeError("pipeline did not quiesce")

    # ---------- inspection ----------
    def tasks(self) -> list[dict]:
        return self.store.scan_tasks({"PENDING", "READY", "COMPLETED", "FAILED"})

    def task(self, event_id: str, consumer: str) -> dict:
        found = [t for t in self.tasks() if t["EventId"] == event_id and t["ConsumerId"] == consumer]
        assert len(found) == 1, f"expected one task for {event_id}/{consumer}, found {len(found)}"
        return found[0]

    def status(self, event_id: str, consumer: str) -> str:
        return self.task(event_id, consumer)["Status"]

    def projection(self, consumer: str, paper_id: Any) -> dict | None:
        from event_platform.consumer import projection_key
        return boto3.resource("dynamodb").Table(self.tables["ConsumerProjectionTable"]).get_item(
            Key=projection_key(consumer, {"PaperId": paper_id}, ""), ConsistentRead=True).get("Item")

    def markers(self, consumer: str) -> list[dict]:
        from boto3.dynamodb.conditions import Key
        from event_platform.ingestion import escape_segment
        table = boto3.resource("dynamodb").Table(self.tables["ConsumerProjectionTable"])
        return table.query(KeyConditionExpression=Key("PK").eq(f"MARKER#{escape_segment(consumer)}"))["Items"]


def make_event(producer: str, action: str, params: dict[str, Any], *, event_id: str | None = None,
               timestamp: int = 1_792_280_000_000) -> dict[str, Any]:
    return {"EventId": event_id or str(uuid.uuid4()), "ProducerId": producer, "Action": action,
            "Timestamp": timestamp, "Parameters": params}
