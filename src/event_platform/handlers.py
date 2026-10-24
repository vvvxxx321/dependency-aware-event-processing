"""Lambda entrypoints. Each AWS client/store is created lazily once per execution environment."""
from __future__ import annotations

import json
import logging
import os
from functools import lru_cache

from .aws_store import PlatformStore, deserialize_image
from .ingestion import ingest
from .observability import log, metric

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class InjectedIngestFault(RuntimeError):
    """Dev-only transient failure requested via SQS message attribute FaultInjectIngestFailures."""


def _fault_injection_enabled() -> bool:
    return os.environ.get("ENABLE_FAULT_INJECTION", "false").lower() == "true"


def _maybe_inject_ingest_fault(record: dict) -> None:
    attr = (record.get("messageAttributes") or {}).get("FaultInjectIngestFailures")
    if not attr or not _fault_injection_enabled():
        return
    receive_count = int((record.get("attributes") or {}).get("ApproximateReceiveCount", "1"))
    if receive_count <= int(attr.get("stringValue", "0")):
        raise InjectedIngestFault(f"injected ingest failure on receive {receive_count}")


# ---------------- SQS ingestion ----------------
def handle_sqs_batch(event: dict, store) -> dict:
    failures = []
    for record in event.get("Records", []):
        msg_id = record.get("messageId")
        try:
            _maybe_inject_ingest_fault(record)
            outcome = ingest(record["body"], store)
            log("ingest", "ingested", messageId=msg_id, outcome=outcome,
                receiveCount=(record.get("attributes") or {}).get("ApproximateReceiveCount"))
            metric("ingest", "EventsIngested" if outcome == "created" else "DuplicateEvents", messageId=msg_id)
        except Exception as exc:
            logger.exception("SQS ingestion failed for messageId=%s (%s)", msg_id, type(exc).__name__)
            metric("ingest", "IngestFailures", messageId=msg_id, errorType=type(exc).__name__)
            if not msg_id:
                raise  # never silently ACK an unidentifiable failed message
            failures.append({"itemIdentifier": msg_id})
    return {"batchItemFailures": failures}


@lru_cache(maxsize=1)
def _platform_store() -> PlatformStore:
    return PlatformStore.from_environment()


def lambda_handler(event, context):
    return handle_sqs_batch(event, _platform_store())


# ---------------- DynamoDB Streams (resolver + dispatch) ----------------
def handle_stream_batch(event: dict, handler) -> dict:
    """Process in order; on error report the failing SequenceNumber so Lambda retries from there."""
    for record in event.get("Records", []):
        ddb = record.get("dynamodb", {})
        old = deserialize_image(ddb["OldImage"]) if "OldImage" in ddb else None
        new = deserialize_image(ddb["NewImage"]) if "NewImage" in ddb else None
        try:
            outcome = handler.handle_change(record.get("eventName"), old, new)
            if outcome != "ignored":
                log(type(handler).__name__.lower(), "stream-record", outcome=outcome,
                    sequenceNumber=ddb.get("SequenceNumber"), TaskId=(new or {}).get("TaskId"))
        except Exception:
            logger.exception("stream record failed seq=%s", ddb.get("SequenceNumber"))
            return {"batchItemFailures": [{"itemIdentifier": ddb.get("SequenceNumber")}]}
    return {"batchItemFailures": []}


def resolver_handler(event, context):
    from .resolver import Resolver
    return handle_stream_batch(event, Resolver(_platform_store()))


@lru_cache(maxsize=1)
def _dispatcher():
    import boto3
    from .dispatch import Dispatcher, sns_publisher
    return Dispatcher(_platform_store(), sns_publisher(boto3.client("sns"), os.environ["DISPATCH_TOPIC_ARN"]))


def dispatch_handler(event, context):
    return handle_stream_batch(event, _dispatcher())


# ---------------- Completion API ----------------
def completion_handler(event, context):
    from .completion import CompletionService
    roles = json.loads(os.environ["CONSUMER_ROLES"])
    return CompletionService(_platform_store(), roles).handle(event)


# ---------------- Demo consumers (SNS) ----------------
@lru_cache(maxsize=1)
def _consumer():
    import boto3
    from .consumer import DemoConsumer, signed_ack_client
    return DemoConsumer(os.environ["CONSUMER_ID"], boto3.client("dynamodb"), os.environ["PROJECTION_TABLE"],
                        signed_ack_client(os.environ["COMPLETION_URL"], os.environ["AWS_REGION"]),
                        fault_injection=_fault_injection_enabled())


def consumer_handler(event, context):
    """Async SNS invoke: raising makes Lambda retry (MaximumRetryAttempts) then the on-failure destination."""
    for record in event.get("Records", []):
        _consumer().handle_message(json.loads(record["Sns"]["Message"]))
    return {"ok": True}

