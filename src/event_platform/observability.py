"""Structured JSON logs + CloudWatch Embedded Metric Format (EMF) counters via stdout.

EMF needs no cloudwatch:PutMetricData permission: Lambda's log stream is parsed by CloudWatch.
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

NAMESPACE = os.environ.get("METRIC_NAMESPACE", "EventPlatform")


def log(component: str, message: str, **fields: Any) -> None:
    record = {"component": component, "message": message, **fields}
    print(json.dumps(record, default=str, sort_keys=True), file=sys.stdout, flush=True)


def metric(component: str, name: str, value: float = 1, unit: str = "Count", **fields: Any) -> None:
    """One EMF document. Only `Component` is a dimension (bounded cardinality); fields are searchable logs."""
    doc = {
        "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": [{
            "Namespace": NAMESPACE, "Dimensions": [["Component"]],
            "Metrics": [{"Name": name, "Unit": unit}]}]},
        "Component": component, name: value, **fields,
    }
    print(json.dumps(doc, default=str, sort_keys=True), file=sys.stdout, flush=True)


def now_ms() -> int:
    return int(time.time() * 1000)
