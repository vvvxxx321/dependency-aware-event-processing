"""Operator recovery: reconciliation of PENDING, re-dispatch of stale READY, authorized FAILED replay.

Streams retain records ~24h and stream processing can exhaust retries; these commands close the gap
explicitly rather than claiming indefinite automatic recovery.
"""
from __future__ import annotations

from typing import Any, Callable

from .dispatch import task_message
from .observability import log, metric


def reconcile_pending(store, resolver) -> dict[str, str]:
    """Re-run the full rule evaluation for every PENDING task (idempotent)."""
    return {t["TaskId"]: resolver.evaluate(t["ProducerId"], t["SK"], cause="reconcile")
            for t in store.scan_tasks({"PENDING"})}


def redispatch_stale_ready(store, publish: Callable[[dict[str, Any], dict[str, str]], str], *,
                           now_ms: int, older_than_ms: int) -> list[str]:
    """Re-publish READY tasks with no ACK past the deadline. Consumers are idempotent by TaskId."""
    sent = []
    for task in store.scan_tasks({"READY"}):
        last = int(task.get("DispatchedAt") or task.get("ReadyAt") or task["StatusUpdatedOn"])
        if now_ms - last < older_than_ms:
            continue
        message_id = publish(task_message(task), {"ConsumerId": task["ConsumerId"]})
        store.mark_dispatched(task, now_ms, message_id)
        log("ops", "redispatched", TaskId=task["TaskId"], EventId=task["EventId"], ConsumerId=task["ConsumerId"])
        metric("ops", "TasksRedispatched")
        sent.append(task["TaskId"])
    return sent


def replay_failed(store, *, root_event_id: str, now_ms: int, operator: str) -> list[str]:
    """FAILED -> PENDING for the root failure and its propagated chain; Resolver re-checks ALL rules.

    The original EventId/Timestamp/Parameters are untouched; ReplayGeneration and history are audited.
    """
    replayed = []
    for task in store.scan_tasks({"FAILED"}):
        if task.get("RootFailureEventId") != root_event_id:
            continue
        history = list(task.get("FailureHistory") or []) + [{
            "FailedAt": task.get("FailedAt"), "FailureReason": task.get("FailureReason"),
            "RootFailureEventId": root_event_id, "ReplayedAt": now_ms, "ReplayedBy": operator}]
        if store.transition(task, "FAILED", "PENDING", now_ms, {
                "ReplayGeneration": int(task.get("ReplayGeneration", 0)) + 1, "FailureHistory": history,
                "ReplayedBy": operator}):
            log("ops", "replayed", TaskId=task["TaskId"], EventId=task["EventId"], RootFailureEventId=root_event_id)
            metric("ops", "TasksReplayed")
            replayed.append(task["TaskId"])
    return replayed
