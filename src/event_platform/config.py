"""Configuration rules, dependency keys and waiter/completion index keys. No AWS calls.

Rule semantics: a rule for (ClientId, ClientAction) is satisfied when there EXISTS a
COMPLETED predecessor task with matching ProducerId + Action + ServiceConsumer and an EQUAL value for
`Parameter`; when lastEventGap >= 0, evaluation_time - predecessor.StatusUpdatedOn <= gap seconds.
All rules for one task are combined with AND.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .ingestion import escape_segment, route_key

SUPPORTED_OPERATIONS = frozenset({"EQUAL"})
MAX_DONE_MARKERS = 99  # task update + markers must fit one 100-item DynamoDB transaction


class InvalidRule(ValueError):
    """Configuration rule is malformed or uses an unsupported operator (fail closed)."""


def rule_sort_key(client_action: str, producer_id: str, action: str, suffix: str | None = None) -> str:
    parts = [client_action, producer_id, action] + ([suffix] if suffix else [])
    return "#".join(escape_segment(p) for p in parts)


def subscription_sort_key(producer_id: str, action: str) -> str:
    return f"SUB#{escape_segment(producer_id)}#{escape_segment(action)}"


@dataclass(frozen=True)
class DependencyRule:
    rule_id: str
    client_id: str
    client_action: str
    producer_id: str
    action: str
    service_consumer: str
    parameter: str
    operation: str
    last_event_gap: int

    @classmethod
    def from_item(cls, item: dict[str, Any]) -> "DependencyRule":
        required = ("ClientId", "SK", "ClientAction", "ProducerId", "Action",
                    "ServiceConsumer", "Parameter", "Operation")
        for name in required:
            if not isinstance(item.get(name), str) or not item[name]:
                raise InvalidRule(f"rule field {name} must be a nonblank string: {item.get('SK')}")
        if item["Operation"] not in SUPPORTED_OPERATIONS:
            raise InvalidRule(f"unsupported Operation {item['Operation']!r} in rule {item['SK']}")
        gap = item.get("lastEventGap", -1)
        if isinstance(gap, Decimal) and gap == int(gap):
            gap = int(gap)
        if type(gap) is not int or gap < -1:
            raise InvalidRule(f"lastEventGap must be -1 or nonnegative integer seconds: {item['SK']}")
        expected_prefix = rule_sort_key(item["ClientAction"], item["ProducerId"], item["Action"])
        if item["SK"] != expected_prefix and not item["SK"].startswith(expected_prefix + "#"):
            raise InvalidRule(f"rule SK {item['SK']} does not match ClientAction#ProducerId#Action")
        return cls(item["SK"], item["ClientId"], item["ClientAction"], item["ProducerId"], item["Action"],
                   item["ServiceConsumer"], item["Parameter"], item["Operation"], gap)

    @classmethod
    def from_snapshot(cls, snap: dict[str, Any], client_id: str, client_action: str) -> "DependencyRule":
        gap = snap["lastEventGap"]
        return cls(snap["RuleId"], client_id, client_action, snap["ProducerId"], snap["Action"],
                   snap["ServiceConsumer"], snap["Parameter"], snap["Operation"], int(gap))

    def to_item(self) -> dict[str, Any]:
        return {"ClientId": self.client_id, "SK": self.rule_id, "RecordType": "RULE",
                "ClientAction": self.client_action, "ProducerId": self.producer_id,
                "Action": self.action, "ServiceConsumer": self.service_consumer,
                "Parameter": self.parameter, "Operation": self.operation,
                "lastEventGap": self.last_event_gap}

    def snapshot(self) -> dict[str, Any]:
        """Immutable copy stored on each task at ingestion (rules versioned per event)."""
        return {"RuleId": self.rule_id, "ProducerId": self.producer_id, "Action": self.action,
                "ServiceConsumer": self.service_consumer, "Parameter": self.parameter,
                "Operation": self.operation, "lastEventGap": self.last_event_gap}


def subscription_item(producer_id: str, action: str, consumer_id: str) -> dict[str, Any]:
    return {"ClientId": consumer_id, "SK": subscription_sort_key(producer_id, action),
            "RecordType": "SUBSCRIPTION", "ProducerId": producer_id, "Action": action,
            "RoutePK": route_key(producer_id, action), "RouteSK": consumer_id}


class UnindexableValue(ValueError):
    """Parameter value cannot participate in EQUAL matching (object, list, null)."""


def canonical_value(value: Any) -> str:
    """Type-strict canonical form; JSON 123 and DynamoDB Decimal('123') agree, "123" differs from 123."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return str(int(value))
        return format(value.normalize(), "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return canonical_value(Decimal(str(value)))
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    raise UnindexableValue(f"unsupported parameter value type {type(value).__name__}")


def dependency_key(producer_id: str, action: str, service_consumer: str, parameter: str, value: Any) -> str:
    parts = (producer_id, action, service_consumer, parameter, canonical_value(value))
    return "#".join(escape_segment(p) for p in parts)


def completion_keys(producer_id: str, action: str, consumer_id: str, parameters: dict[str, Any]) -> list[str]:
    """Every DependencyKey a completed task can satisfy (one per indexable parameter)."""
    keys = []
    for name in sorted(parameters):
        try:
            keys.append(dependency_key(producer_id, action, consumer_id, name, parameters[name]))
        except UnindexableValue:
            continue
    return keys


def waiter_sort_key(task_id: str, rule_id: str) -> str:
    return f"WAIT#{task_id}#{escape_segment(rule_id)}"


def done_sort_key(status_updated_on_ms: int, event_id: str) -> str:
    return f"DONE#{status_updated_on_ms:013d}#{escape_segment(event_id)}"


def done_lower_bound(since_ms: int) -> str:
    return f"DONE#{max(since_ms, 0):013d}"
