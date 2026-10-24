"""DynamoDB adapters: GSI route lookup, atomic identity/task fanout, rules, waiter/DONE index, transitions.

All status changes are conditional; COMPLETED + DONE markers commit in one transaction.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from .config import (MAX_DONE_MARKERS, DependencyRule, completion_keys, done_lower_bound,
                     done_sort_key, waiter_sort_key)
from .ingestion import EventConflict, SourceEvent, escape_segment, event_identity_key, route_key


def to_dynamo(value: Any) -> Any:
    """boto3's DynamoDB TypeSerializer forbids float: convert JSON decimals safely."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: to_dynamo(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_dynamo(v) for v in value]
    return value


def from_dynamo(value: Any) -> Any:
    """Decimal -> int/float for JSON output (SNS message bodies, API responses)."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {k: from_dynamo(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [from_dynamo(v) for v in value]
    if isinstance(value, set):
        return sorted(from_dynamo(v) for v in value)
    return value


def cancellation_codes(exc: ClientError) -> list[str]:
    return [r.get("Code", "None") for r in exc.response.get("CancellationReasons", [])]


class DynamoIngestStore:
    def __init__(self, *, config_table: str, log_table: str, route_index: str,
                 resource: Any | None = None, client: Any | None = None):
        self.resource = resource if resource is not None else boto3.resource("dynamodb")
        self.client = client if client is not None else boto3.client("dynamodb")
        self.config = self.resource.Table(config_table)
        self.log = self.resource.Table(log_table)
        self.log_table = log_table
        self.route_index = route_index
        self.serializer = TypeSerializer()

    @classmethod
    def from_environment(cls) -> "DynamoIngestStore":
        return cls(config_table=os.environ["CONFIG_TABLE"],
                   log_table=os.environ["LOG_TABLE"],
                   route_index=os.environ["ROUTE_INDEX"])

    def _ser(self, item: dict[str, Any]) -> dict[str, Any]:
        return {k: self.serializer.serialize(to_dynamo(v)) for k, v in item.items()}

    def consumers_for(self, producer_id: str, action: str) -> list[str]:
        args = {
            "IndexName": self.route_index,
            "KeyConditionExpression": Key("RoutePK").eq(route_key(producer_id, action)),
            "ProjectionExpression": "ClientId",
        }
        consumers = set()
        while True:
            page = self.config.query(**args)
            consumers.update(row["ClientId"] for row in page.get("Items", []))
            if "LastEvaluatedKey" not in page:
                break
            args["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        return sorted(consumers)

    def rules_for(self, consumer_id: str, client_action: str) -> list[dict[str, Any]]:
        """Strongly consistent base-table read of rules gating (consumer, action); invalid rule fails closed."""
        args = {
            "KeyConditionExpression": Key("ClientId").eq(consumer_id)
            & Key("SK").begins_with(escape_segment(client_action) + "#"),
            "FilterExpression": Attr("RecordType").eq("RULE"),
            "ConsistentRead": True,
        }
        rules = []
        while True:
            page = self.config.query(**args)
            rules.extend(DependencyRule.from_item(item).snapshot() for item in page.get("Items", []))
            if "LastEvaluatedKey" not in page:
                break
            args["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        return sorted(rules, key=lambda r: r["RuleId"])

    def atomically_ingest(self, event: SourceEvent, records: list[dict[str, Any]]) -> str:
        puts = []
        for record in records:
            puts.append({"Put": {
                "TableName": self.log_table,
                "Item": self._ser(record),
                "ConditionExpression": "attribute_not_exists(ProducerId) AND attribute_not_exists(SK)",
            }})
        try:
            self.client.transact_write_items(TransactItems=puts)
            return "created"
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "TransactionCanceledException":
                raise
            # DDB transactions either apply every write or none. On existing identity,
            # matching fingerprint means event was previously ingested: no new fanout.
            existing = self.log.get_item(Key=event_identity_key(event), ConsistentRead=True).get("Item")
            if existing is None:
                raise  # capacity/condition errors not explained by duplicate identity
            if existing.get("PayloadFingerprint") != event.fingerprint():
                raise EventConflict("same EventId reused with a different source message") from exc
            return "duplicate"


class PlatformStore(DynamoIngestStore):
    """Resolver / completion / dispatch operations on ReplicationLog + DependencyWaiters."""

    def __init__(self, *, waiters_table: str, **kwargs):
        super().__init__(**kwargs)
        self.waiters_table = waiters_table
        self.waiters = self.resource.Table(waiters_table)

    @classmethod
    def from_environment(cls) -> "PlatformStore":
        return cls(config_table=os.environ["CONFIG_TABLE"], log_table=os.environ["LOG_TABLE"],
                   route_index=os.environ["ROUTE_INDEX"], waiters_table=os.environ["WAITERS_TABLE"])

    # ---- ReplicationLog task reads/transitions ----
    def get_task(self, producer_id: str, sk: str) -> dict[str, Any] | None:
        return self.log.get_item(Key={"ProducerId": producer_id, "SK": sk}, ConsistentRead=True).get("Item")

    def transition(self, task: dict[str, Any], from_status: str, to_status: str, now_ms: int,
                   extra: dict[str, Any] | None = None) -> bool:
        """Conditional single-task status change; False if another writer already moved it."""
        names = {"#s": "Status"}
        values = {":from": from_status, ":to": to_status, ":now": now_ms, ":tid": task["TaskId"]}
        sets = ["#s = :to", "StatusUpdatedOn = :now", f"{to_status.capitalize()}At = :now"]
        for i, (k, v) in enumerate((extra or {}).items()):
            names[f"#x{i}"] = k
            values[f":x{i}"] = to_dynamo(v)
            sets.append(f"#x{i} = :x{i}")
        try:
            self.log.update_item(
                Key={"ProducerId": task["ProducerId"], "SK": task["SK"]},
                UpdateExpression="SET " + ", ".join(sets),
                ConditionExpression="#s = :from AND TaskId = :tid",
                ExpressionAttributeNames=names, ExpressionAttributeValues=values)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def complete(self, task: dict[str, Any], now_ms: int, acked_by: str) -> bool:
        """READY -> COMPLETED and DONE markers in ONE transaction. False if condition failed."""
        keys = completion_keys(task["ProducerId"], task["Action"], task["ConsumerId"], task["Parameters"])
        if len(keys) > MAX_DONE_MARKERS:
            raise ValueError("too many indexable parameters for one completion transaction")
        items: list[dict[str, Any]] = [{"Update": {
            "TableName": self.log_table,
            "Key": self._ser({"ProducerId": task["ProducerId"], "SK": task["SK"]}),
            "UpdateExpression": "SET #s = :c, StatusUpdatedOn = :now, CompletedAt = :now, AckedBy = :by",
            "ConditionExpression": "#s = :r AND TaskId = :tid AND ConsumerId = :cid",
            "ExpressionAttributeNames": {"#s": "Status"},
            "ExpressionAttributeValues": self._ser({":c": "COMPLETED", ":r": "READY", ":now": now_ms,
                                                    ":by": acked_by, ":tid": task["TaskId"],
                                                    ":cid": task["ConsumerId"]}),
        }}]
        for key in keys:
            items.append({"Put": {"TableName": self.waiters_table, "Item": self._ser({
                "DependencyKey": key, "SK": done_sort_key(now_ms, task["EventId"]),
                "RecordType": "DONE", "EventId": task["EventId"], "TaskId": task["TaskId"],
                "StatusUpdatedOn": now_ms})}})
        try:
            self.client.transact_write_items(TransactItems=items)
            return True
        except ClientError as exc:
            if (exc.response["Error"]["Code"] == "TransactionCanceledException"
                    and cancellation_codes(exc)[0] == "ConditionalCheckFailed"):
                return False
            raise

    def mark_dispatched(self, task: dict[str, Any], now_ms: int, message_id: str) -> None:
        try:
            self.log.update_item(
                Key={"ProducerId": task["ProducerId"], "SK": task["SK"]},
                UpdateExpression="SET DispatchedAt = :now, LastSnsMessageId = :m ADD DispatchAttempts :one",
                ConditionExpression="#s = :r",
                ExpressionAttributeNames={"#s": "Status"},
                ExpressionAttributeValues={":now": now_ms, ":m": message_id, ":one": 1, ":r": "READY"})
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise  # task already ACKed before bookkeeping: harmless

    # ---- DependencyWaiters: WAIT# and DONE# families ----
    def put_waiter(self, dependency_key: str, task: dict[str, Any], rule_id: str, now_ms: int) -> None:
        self.waiters.put_item(Item={
            "DependencyKey": dependency_key, "SK": waiter_sort_key(task["TaskId"], rule_id),
            "RecordType": "WAIT", "TaskId": task["TaskId"], "TaskProducerId": task["ProducerId"],
            "TaskSK": task["SK"], "RuleId": rule_id, "RegisteredAt": now_ms})

    def delete_waiter(self, dependency_key: str, task_id: str, rule_id: str) -> None:
        self.waiters.delete_item(Key={"DependencyKey": dependency_key, "SK": waiter_sort_key(task_id, rule_id)})

    def has_done(self, dependency_key: str, since_ms: int | None) -> bool:
        cond = Key("DependencyKey").eq(dependency_key)
        if since_ms is None:
            cond &= Key("SK").begins_with("DONE#")
        else:
            cond &= Key("SK").between(done_lower_bound(since_ms), "DONE#~")
        page = self.waiters.query(KeyConditionExpression=cond, ConsistentRead=True, Limit=1)
        return bool(page.get("Items"))

    def waiters_for(self, dependency_key: str) -> list[dict[str, Any]]:
        args = {"KeyConditionExpression": Key("DependencyKey").eq(dependency_key) & Key("SK").begins_with("WAIT#"),
                "ConsistentRead": True}
        out = []
        while True:
            page = self.waiters.query(**args)
            out.extend(page.get("Items", []))
            if "LastEvaluatedKey" not in page:
                return out
            args["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    def scan_tasks(self, statuses: set[str]) -> list[dict[str, Any]]:
        """Operator reconciliation only (full scan; not used on the hot path)."""
        args: dict[str, Any] = {"FilterExpression": Attr("RecordType").eq("TASK") & Attr("Status").is_in(sorted(statuses)),
                                "ConsistentRead": True}
        out = []
        while True:
            page = self.log.scan(**args)
            out.extend(page.get("Items", []))
            if "LastEvaluatedKey" not in page:
                return out
            args["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def deserialize_image(image: dict[str, Any]) -> dict[str, Any]:
    d = TypeDeserializer()
    return {k: d.deserialize(v) for k, v in image.items()}
