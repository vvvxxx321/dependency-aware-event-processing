"""Dispatch: ReplicationLog stream READY transitions -> one SNS publication per consumer-specific task.

READY means eligible for dispatch; publishing does NOT complete the task. A crash between publish and
bookkeeping causes a stream retry and a duplicate SNS message, which the consumer absorbs idempotently.
"""
from __future__ import annotations

import json
from typing import Any, Callable

from .aws_store import from_dynamo
from .observability import log, metric, now_ms as wall_clock_ms

COMPONENT = "dispatch"


def is_ready_transition(event_name: str, old: dict[str, Any] | None, new: dict[str, Any] | None) -> bool:
    return (event_name in ("INSERT", "MODIFY") and bool(new) and new.get("RecordType") == "TASK"
            and new.get("Status") == "READY" and (old or {}).get("Status") != "READY")


def task_message(task: dict[str, Any]) -> dict[str, Any]:
    return from_dynamo({
        "TaskId": task["TaskId"], "EventId": task["EventId"], "ProducerId": task["ProducerId"],
        "TaskSK": task["SK"], "Action": task["Action"], "ConsumerId": task["ConsumerId"],
        "Parameters": task["Parameters"], "Timestamp": task["Timestamp"], "IngestedAt": task["IngestedAt"],
        "ReadyAt": task.get("ReadyAt"), "ReplayGeneration": task.get("ReplayGeneration", 0),
    })


class Dispatcher:
    def __init__(self, store, publish: Callable[[dict[str, Any], dict[str, str]], str],
                 clock: Callable[[], int] = wall_clock_ms):
        self.store = store
        self.publish = publish  # (message, attributes) -> MessageId
        self.clock = clock

    def handle_change(self, event_name: str, old, new) -> str:
        if not is_ready_transition(event_name, old, new):
            return "ignored"
        message = task_message(new)
        message_id = self.publish(message, {"ConsumerId": new["ConsumerId"]})
        self.store.mark_dispatched(new, self.clock(), message_id)
        log(COMPONENT, "dispatched", TaskId=new["TaskId"], EventId=new["EventId"], ConsumerId=new["ConsumerId"],
            Action=new["Action"], SnsMessageId=message_id)
        metric(COMPONENT, "TasksDispatched", EventId=new["EventId"], ConsumerId=new["ConsumerId"])
        return "dispatched"


def sns_publisher(sns_client, topic_arn: str) -> Callable[[dict[str, Any], dict[str, str]], str]:
    def publish(message: dict[str, Any], attributes: dict[str, str]) -> str:
        response = sns_client.publish(
            TopicArn=topic_arn, Message=json.dumps(message, sort_keys=True),
            MessageAttributes={k: {"DataType": "String", "StringValue": v} for k, v in attributes.items()})
        return response["MessageId"]
    return publish
