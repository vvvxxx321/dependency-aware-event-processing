"""Completion API (API Gateway REST, AWS_IAM auth): consumer ACK -> READY->COMPLETED or READY->FAILED.

Only the IAM role registered for a ConsumerId may ACK that consumer's task; the task must exist, match the
submitted TaskId/ConsumerId, and be READY. Repeated identical ACKs are idempotent (200, duplicate).
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable

from .observability import log, metric, now_ms as wall_clock_ms

COMPONENT = "completion"
_ROLE_RE = re.compile(r":assumed-role/([^/]+)/")


def caller_role_name(request_context: dict[str, Any]) -> str | None:
    arn = (request_context.get("identity") or {}).get("userArn") or ""
    match = _ROLE_RE.search(arn)
    return match.group(1) if match else None


def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {"statusCode": status, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body, sort_keys=True, default=str)}


class CompletionService:
    def __init__(self, store, consumer_roles: dict[str, str], clock: Callable[[], int] = wall_clock_ms):
        self.store = store
        self.consumer_roles = consumer_roles  # ConsumerId -> IAM role name allowed to ACK it
        self.clock = clock

    def handle(self, event: dict[str, Any]) -> dict[str, Any]:
        try:
            body = json.loads(event.get("body") or "")
        except ValueError:
            return _response(400, {"error": "body must be JSON"})
        required = ("TaskId", "ProducerId", "TaskSK", "EventId", "ConsumerId", "Outcome")
        if not isinstance(body, dict) or any(not isinstance(body.get(k), str) or not body[k] for k in required):
            return _response(400, {"error": f"required string fields: {list(required)}"})
        if body["Outcome"] not in ("COMPLETED", "FAILED"):
            return _response(400, {"error": "Outcome must be COMPLETED or FAILED"})

        role = caller_role_name(event.get("requestContext") or {})
        expected = self.consumer_roles.get(body["ConsumerId"])
        if not role or not expected or role != expected:
            log(COMPONENT, "ack-rejected", reason="caller-not-consumer", ConsumerId=body["ConsumerId"],
                TaskId=body["TaskId"], callerRole=role)
            metric(COMPONENT, "AckRejected")
            return _response(403, {"error": "caller is not authorized for this ConsumerId"})

        task = self.store.get_task(body["ProducerId"], body["TaskSK"])
        if task is None or task.get("RecordType") != "TASK":
            return _response(404, {"error": "task not found"})
        if (task["TaskId"] != body["TaskId"] or task["ConsumerId"] != body["ConsumerId"]
                or task["EventId"] != body["EventId"]):
            log(COMPONENT, "ack-rejected", reason="task-mismatch", TaskId=body["TaskId"], ConsumerId=body["ConsumerId"])
            metric(COMPONENT, "AckRejected")
            return _response(403, {"error": "task does not belong to this consumer"})

        target = body["Outcome"]
        if task["Status"] == target:
            metric(COMPONENT, "DuplicateAcks")
            return _response(200, {"result": "duplicate", "Status": target})
        if task["Status"] != "READY":
            return _response(409, {"error": f"task is {task['Status']}, only READY may be acknowledged"})

        now = self.clock()
        if target == "COMPLETED":
            changed = self.store.complete(task, now, acked_by=role)
        else:
            changed = self.store.transition(task, "READY", "FAILED", now, {
                "FailureReason": "CONSUMER_TERMINAL_FAILURE", "ConsumerReason": str(body.get("Reason", ""))[:1000],
                "RootFailureEventId": task["EventId"], "FailureChain": [task["TaskId"]], "AckedBy": role})
        if not changed:
            current = self.store.get_task(body["ProducerId"], body["TaskSK"]) or {}
            if current.get("Status") == target:
                metric(COMPONENT, "DuplicateAcks")
                return _response(200, {"result": "duplicate", "Status": target})
            return _response(409, {"error": f"task is {current.get('Status')}"})

        latency = now - int(task["IngestedAt"])
        log(COMPONENT, "transition", TaskId=task["TaskId"], EventId=task["EventId"], ProducerId=task["ProducerId"],
            ConsumerId=task["ConsumerId"], Action=task["Action"], prevStatus="READY", newStatus=target,
            endToEndLatencyMs=latency)
        if target == "COMPLETED":
            metric(COMPONENT, "TasksCompleted", EventId=task["EventId"], ConsumerId=task["ConsumerId"])
            metric(COMPONENT, "EndToEndLatencyMs", latency, "Milliseconds", EventId=task["EventId"])
        else:
            metric(COMPONENT, "TasksFailedByConsumer", EventId=task["EventId"], ConsumerId=task["ConsumerId"])
        return _response(200, {"result": "updated", "Status": target})
