"""Dependency Resolver: driven by ReplicationLog DynamoDB Streams (TASK insert, COMPLETED, FAILED, replay).

Race-freedom: a waiter is registered BEFORE a strongly consistent DONE recheck; completion
writes DONE in the same transaction as COMPLETED; the completion stream record then queries WAIT entries.
Every transition is conditional, so duplicate stream deliveries and concurrent wakeups are no-ops.
"""
from __future__ import annotations

from typing import Any, Callable

from .config import DependencyRule, UnindexableValue, completion_keys, dependency_key
from .observability import log, metric, now_ms as wall_clock_ms

COMPONENT = "resolver"


class Resolver:
    def __init__(self, store, clock: Callable[[], int] = wall_clock_ms):
        self.store = store
        self.clock = clock

    # ---------- per-task evaluation ----------
    def _rules(self, task: dict[str, Any]) -> list[DependencyRule]:
        return [DependencyRule.from_snapshot(r, task["ConsumerId"], task["Action"]) for r in task.get("Rules", [])]

    def _rule_key(self, task: dict[str, Any], rule: DependencyRule) -> str:
        if rule.parameter not in task["Parameters"]:
            raise UnindexableValue(f"current event lacks parameter {rule.parameter}")
        return dependency_key(rule.producer_id, rule.action, rule.service_consumer,
                              rule.parameter, task["Parameters"][rule.parameter])

    def _satisfied(self, key: str, rule: DependencyRule, now: int) -> bool:
        since = None if rule.last_event_gap < 0 else now - rule.last_event_gap * 1000
        return self.store.has_done(key, since)

    def evaluate(self, producer_id: str, sk: str, *, cause: str) -> str:
        """Re-check ALL rules (AND) of one task; PENDING -> READY only if every rule holds."""
        task = self.store.get_task(producer_id, sk)
        if task is None or task.get("RecordType") != "TASK":
            return "missing"
        if task["Status"] != "PENDING":
            return "not-pending"
        now = self.clock()
        rules = self._rules(task)
        unsatisfied, keys = [], []
        for rule in rules:
            try:
                key = self._rule_key(task, rule)
            except UnindexableValue as exc:
                # Fail closed: never dispatch; stays PENDING with reason for operator triage.
                log(COMPONENT, "rule-unevaluable", TaskId=task["TaskId"], EventId=task["EventId"],
                    RuleId=rule.rule_id, reason=str(exc))
                return "blocked"
            keys.append((key, rule))
            self.store.put_waiter(key, task, rule.rule_id, now)        # 1) register waiter first
            if not self._satisfied(key, rule, now):                     # 2) then consistent recheck
                unsatisfied.append(rule.rule_id)
        if unsatisfied:
            log(COMPONENT, "task-pending", TaskId=task["TaskId"], EventId=task["EventId"],
                ConsumerId=task["ConsumerId"], Action=task["Action"], waitingOn=unsatisfied, cause=cause)
            return "pending"
        if not self.store.transition(task, "PENDING", "READY", now):
            return "lost-race"
        for key, rule in keys:
            self.store.delete_waiter(key, task["TaskId"], rule.rule_id)
        log(COMPONENT, "transition", TaskId=task["TaskId"], EventId=task["EventId"], ProducerId=task["ProducerId"],
            ConsumerId=task["ConsumerId"], Action=task["Action"], prevStatus="PENDING", newStatus="READY",
            cause=cause, rules=len(rules))
        metric(COMPONENT, "TasksReady", EventId=task["EventId"], ConsumerId=task["ConsumerId"])
        return "ready"

    # ---------- wakeups ----------
    def on_completed(self, task: dict[str, Any]) -> dict[str, str]:
        results = {}
        for key in completion_keys(task["ProducerId"], task["Action"], task["ConsumerId"], task["Parameters"]):
            for waiter in self.store.waiters_for(key):
                results[waiter["TaskId"]] = self.evaluate(waiter["TaskProducerId"], waiter["TaskSK"],
                                                          cause=f"completed:{task['TaskId']}")
        return results

    def on_failed(self, failed: dict[str, Any]) -> dict[str, str]:
        """Propagate terminal failure to blocked PENDING dependents whose rule has no other valid predecessor."""
        root = failed.get("RootFailureEventId") or failed["EventId"]
        chain = list(failed.get("FailureChain") or [failed["TaskId"]])
        results = {}
        for key in completion_keys(failed["ProducerId"], failed["Action"], failed["ConsumerId"], failed["Parameters"]):
            for waiter in self.store.waiters_for(key):
                task = self.store.get_task(waiter["TaskProducerId"], waiter["TaskSK"])
                if task is None or task["Status"] != "PENDING":
                    continue  # never roll back READY/COMPLETED work
                rule = next((r for r in self._rules(task) if r.rule_id == waiter["RuleId"]), None)
                if rule is None:
                    continue
                if self._satisfied(key, rule, self.clock()):
                    results[task["TaskId"]] = "alternative-predecessor"
                    continue
                ok = self.store.transition(task, "PENDING", "FAILED", self.clock(), {
                    "FailureReason": "DEPENDENCY_FAILED", "RootFailureEventId": root,
                    "FailedPredecessorTaskId": failed["TaskId"], "FailedRuleId": rule.rule_id,
                    "FailureChain": chain + [task["TaskId"]]})
                if ok:
                    self.store.delete_waiter(key, task["TaskId"], rule.rule_id)
                    log(COMPONENT, "transition", TaskId=task["TaskId"], EventId=task["EventId"],
                        ConsumerId=task["ConsumerId"], prevStatus="PENDING", newStatus="FAILED",
                        reason="DEPENDENCY_FAILED", RootFailureEventId=root)
                    metric(COMPONENT, "TasksFailedByPropagation", EventId=task["EventId"])
                results[task["TaskId"]] = "failed" if ok else "lost-race"
        return results

    # ---------- stream entrypoint ----------
    def handle_change(self, event_name: str, old: dict[str, Any] | None, new: dict[str, Any] | None) -> str:
        if not new or new.get("RecordType") != "TASK":
            return "ignored"
        old_status = (old or {}).get("Status")
        status = new["Status"]
        if event_name == "INSERT" and status == "PENDING":
            return self.evaluate(new["ProducerId"], new["SK"], cause="ingested")
        if event_name == "MODIFY" and status == "PENDING" and old_status == "FAILED":
            return self.evaluate(new["ProducerId"], new["SK"], cause="replay")
        if event_name == "MODIFY" and status == "COMPLETED" and old_status != "COMPLETED":
            return f"woke:{len(self.on_completed(new))}"
        if event_name == "MODIFY" and status == "FAILED" and old_status != "FAILED":
            return f"propagated:{len(self.on_failed(new))}"
        return "ignored"
