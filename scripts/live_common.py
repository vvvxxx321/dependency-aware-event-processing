"""Shared helpers for LIVE AWS scripts. Every script requires explicit --profile/--region/--stack."""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import boto3  # noqa: E402

from event_platform.config import DependencyRule, rule_sort_key, subscription_item  # noqa: E402
from event_platform.ingestion import SourceEvent, task_sort_key  # noqa: E402

# Live demo configuration (seeded before traffic).
SUBSCRIPTIONS = [
    ("ContentService", "CreatePaper", "SearchService"),
    ("ContentService", "CreatePaper", "ReportingService"),
    ("CommentService", "MakeComment", "SearchService"),
    ("CommentService", "MakeComment", "ReportingService"),
    ("ReviewService", "SubmitReview", "SearchService"),
    ("ModerationService", "ApproveReview", "SearchService"),
    ("AuditService", "ArchivePaper", "ReportingService"),
]
# (ClientId, ClientAction, ProducerId, Action, ServiceConsumer, Parameter, lastEventGap)
RULES = [
    ("SearchService", "MakeComment", "ContentService", "CreatePaper", "SearchService", "PaperId", -1),       # canonical
    ("ReportingService", "MakeComment", "ContentService", "CreatePaper", "SearchService", "PaperId", -1),    # cross-consumer
    ("SearchService", "SubmitReview", "ContentService", "CreatePaper", "SearchService", "PaperId", -1),      # AND (1/2)
    ("SearchService", "SubmitReview", "ModerationService", "ApproveReview", "SearchService", "ReviewId", -1),  # AND (2/2)
    ("ReportingService", "ArchivePaper", "ContentService", "CreatePaper", "ReportingService", "PaperId", 30),  # window
]


def rule_items() -> list[dict[str, Any]]:
    items = []
    for client, client_action, producer, action, service_consumer, param, gap in RULES:
        item = {"ClientId": client, "SK": rule_sort_key(client_action, producer, action), "RecordType": "RULE",
                "ClientAction": client_action, "ProducerId": producer, "Action": action,
                "ServiceConsumer": service_consumer, "Parameter": param, "Operation": "EQUAL", "lastEventGap": gap}
        DependencyRule.from_item(item)
        items.append(item)
    return items


def subscription_items() -> list[dict[str, Any]]:
    return [subscription_item(*row) for row in SUBSCRIPTIONS]


def parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--profile", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--stack", required=True)
    return p


class Live:
    def __init__(self, profile: str, region: str, stack: str):
        self.session = boto3.Session(profile_name=profile, region_name=region)
        self.region = region
        self.stack = stack
        outputs = self.session.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
        self.out = {o["OutputKey"]: o["OutputValue"] for o in outputs}
        self.ddb = self.session.resource("dynamodb")
        self.ddb_client = self.session.client("dynamodb")
        self.sqs = self.session.client("sqs")
        self.sns = self.session.client("sns")
        self.log = self.ddb.Table(self.out["ReplicationLogTableName"])
        self.config = self.ddb.Table(self.out["ConfigurationTableName"])
        self.proj = self.ddb.Table(self.out["ConsumerProjectionTableName"])
        self.waiters = self.ddb.Table(self.out["DependencyWaitersTableName"])

    def account_hint(self) -> str:
        acct = self.session.client("sts").get_caller_identity()["Account"]
        return f"***{acct[-4:]}"

    # ---- sending ----
    def send(self, event: dict[str, Any], attributes: dict[str, str] | None = None) -> str:
        args = {"QueueUrl": self.out["IngressQueueUrl"], "MessageBody": json.dumps(event)}
        if attributes:
            args["MessageAttributes"] = {k: {"DataType": "Number" if v.isdigit() else "String", "StringValue": v}
                                         for k, v in attributes.items()}
        return self.sqs.send_message(**args)["MessageId"]

    def send_batch(self, events: list[dict[str, Any]]) -> list[str]:
        failed = []
        for i in range(0, len(events), 10):
            chunk = events[i:i + 10]
            resp = self.sqs.send_message_batch(QueueUrl=self.out["IngressQueueUrl"], Entries=[
                {"Id": str(j), "MessageBody": json.dumps(e)} for j, e in enumerate(chunk)])
            failed.extend(chunk[int(f["Id"])]["EventId"] for f in resp.get("Failed", []))
        return failed

    # ---- reads ----
    def task_key(self, event: dict[str, Any], consumer: str) -> dict[str, str]:
        e = SourceEvent.from_json(json.dumps(event))
        return {"ProducerId": e.producer_id, "SK": task_sort_key(e, consumer)}

    def task(self, event: dict[str, Any], consumer: str) -> dict[str, Any] | None:
        return self.log.get_item(Key=self.task_key(event, consumer), ConsistentRead=True).get("Item")

    def status(self, event: dict[str, Any], consumer: str) -> str | None:
        t = self.task(event, consumer)
        return t["Status"] if t else None

    def tasks_for_event(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        from boto3.dynamodb.conditions import Attr, Key
        items, args = [], {"KeyConditionExpression": Key("ProducerId").eq(event["ProducerId"])
                           & Key("SK").begins_with(f"{event['Action']}#"),
                           "FilterExpression": Attr("EventId").eq(event["EventId"]), "ConsistentRead": True}
        while True:
            page = self.log.query(**args)
            items.extend(page["Items"])
            if "LastEvaluatedKey" not in page:
                return items
            args["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    def projection(self, consumer: str, paper_id: Any) -> dict[str, Any] | None:
        from event_platform.consumer import projection_key
        return self.proj.get_item(Key=projection_key(consumer, {"PaperId": paper_id}, ""), ConsistentRead=True).get("Item")

    def marker(self, consumer: str, task_id: str) -> dict[str, Any] | None:
        from event_platform.consumer import marker_key
        return self.proj.get_item(Key=marker_key(consumer, task_id), ConsistentRead=True).get("Item")

    def queue_depth(self, output_key: str) -> dict[str, int]:
        attrs = self.sqs.get_queue_attributes(QueueUrl=self.out[output_key], AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        return {k: int(v) for k, v in attrs.items()}

    # ---- fault directives (dev only; consumer reads FAULT#<EventId>/<ConsumerId>) ----
    def inject_consumer_fault(self, event_id: str, consumer: str, mode: str, remaining: int = 0) -> None:
        from event_platform.consumer import fault_key
        self.proj.put_item(Item={**fault_key(event_id, consumer), "Mode": mode, "Remaining": remaining})

    def clear_consumer_fault(self, event_id: str, consumer: str) -> None:
        from event_platform.consumer import fault_key
        self.proj.delete_item(Key=fault_key(event_id, consumer))


def ssl_context():
    """Local Python may lack a CA store; use botocore's bundled CA file for script-side HTTPS calls."""
    import os
    import ssl
    import botocore
    return ssl.create_default_context(cafile=os.path.join(os.path.dirname(botocore.__file__), "cacert.pem"))


def new_event(producer: str, action: str, params: dict[str, Any]) -> dict[str, Any]:
    return {"EventId": str(uuid.uuid4()), "ProducerId": producer, "Action": action,
            "Timestamp": int(time.time() * 1000), "Parameters": params}


def wait_for(predicate: Callable[[], Any], timeout: float, interval: float = 1.0) -> Any:
    deadline = time.time() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.time() > deadline:
            return value
        time.sleep(interval)


def results_dir(name: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    path = ROOT / "tests" / "results" / f"{stamp}-{name}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def dump(path: Path, name: str, data: Any) -> None:
    (path / name).write_text(json.dumps(data, indent=2, default=str, sort_keys=True))
