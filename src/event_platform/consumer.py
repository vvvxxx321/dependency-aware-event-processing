"""Demo downstream consumer (SearchService / ReportingService): real idempotent projection write, then ACK.

Side effect = DynamoDB projection update + ProcessedTask marker in ONE transaction keyed by TaskId, so a
duplicate SNS delivery cannot apply the business change twice. ACK is sent only after the write commits;
a duplicate delivery re-sends the (idempotent) ACK, covering a crash between side effect and ACK.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Callable

from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

from .aws_store import cancellation_codes, to_dynamo
from .config import canonical_value
from .ingestion import escape_segment
from .observability import log, metric, now_ms as wall_clock_ms

COMPONENT = "consumer"


class TransientFault(RuntimeError):
    """Injected transient failure (dev only) to exercise Lambda async retries / on-failure destination."""


class AckFailed(RuntimeError):
    pass


def marker_key(consumer_id: str, task_id: str) -> dict[str, str]:
    return {"PK": f"MARKER#{escape_segment(consumer_id)}", "SK": f"TASK#{task_id}"}


def projection_key(consumer_id: str, parameters: dict[str, Any], event_id: str) -> dict[str, str]:
    entity = canonical_value(parameters["PaperId"]) if "PaperId" in parameters else f"EVENT:{event_id}"
    return {"PK": f"PROJ#{escape_segment(consumer_id)}#PAPER#{escape_segment(entity)}", "SK": "DOC"}


def fault_key(event_id: str, consumer_id: str) -> dict[str, str]:
    return {"PK": f"FAULT#{event_id}", "SK": consumer_id}


class DemoConsumer:
    def __init__(self, consumer_id: str, client, table_name: str,
                 ack: Callable[[dict[str, Any]], tuple[int, dict[str, Any]]],
                 fault_injection: bool = False, clock: Callable[[], int] = wall_clock_ms,
                 sleep: Callable[[float], None] = time.sleep, ack_backoff: tuple[float, ...] = (0.2, 0.6, 1.5)):
        self.sleep = sleep
        self.ack_backoff = ack_backoff  # retries for throttled/5xx ACKs (account concurrency limit is low)
        self.consumer_id = consumer_id
        self.client = client
        self.table_name = table_name
        self.ack = ack
        self.fault_injection = fault_injection
        self.clock = clock
        self.ser = TypeSerializer()

    def _s(self, d: dict[str, Any]) -> dict[str, Any]:
        return {k: self.ser.serialize(to_dynamo(v)) for k, v in d.items()}

    def _fault(self, msg: dict[str, Any]) -> str | None:
        item = self.client.get_item(TableName=self.table_name, Key=self._s(fault_key(msg["EventId"], self.consumer_id)),
                                    ConsistentRead=True).get("Item")
        if not item:
            return None
        mode = item["Mode"]["S"]
        if mode == "TERMINAL":
            return "TERMINAL"
        if mode == "TRANSIENT":
            try:
                self.client.update_item(
                    TableName=self.table_name, Key=self._s(fault_key(msg["EventId"], self.consumer_id)),
                    UpdateExpression="ADD Remaining :neg", ConditionExpression="Remaining > :zero",
                    ExpressionAttributeValues=self._s({":neg": -1, ":zero": 0}))
                return "TRANSIENT"
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
        return None

    def apply(self, msg: dict[str, Any]) -> str:
        """Business side effect + idempotency marker atomically. Returns 'applied' or 'duplicate'."""
        now = self.clock()
        action = msg["Action"]
        sets = ["AppliedOrder = list_append(if_not_exists(AppliedOrder, :empty), :ev)", "LastAppliedAt = :now",
                "ConsumerId = :cid"]
        values: dict[str, Any] = {":empty": [], ":ev": [msg["EventId"]], ":now": now, ":one": 1,
                                  ":cid": self.consumer_id}
        adds = ["AppliedCount :one"]
        if action == "CreatePaper":
            sets.append("PaperIndexed = :t, PaperCreatedEventId = :eid")
            values.update({":t": True, ":eid": msg["EventId"]})
        elif action == "MakeComment":
            adds.append("CommentCount :one")
        else:
            adds.append("OtherEventCount :one")
        try:
            self.client.transact_write_items(TransactItems=[
                {"Put": {"TableName": self.table_name, "ConditionExpression": "attribute_not_exists(PK)",
                         "Item": self._s({**marker_key(self.consumer_id, msg["TaskId"]), "RecordType": "PROCESSED",
                                          "TaskId": msg["TaskId"], "EventId": msg["EventId"], "Action": action,
                                          "ProducerId": msg["ProducerId"], "Parameters": msg["Parameters"],
                                          "ProcessedAt": now})}},
                {"Update": {"TableName": self.table_name,
                            "Key": self._s(projection_key(self.consumer_id, msg["Parameters"], msg["EventId"])),
                            "UpdateExpression": "SET " + ", ".join(sets) + " ADD " + ", ".join(adds),
                            "ExpressionAttributeValues": self._s(values)}},
            ])
            return "applied"
        except ClientError as exc:
            if (exc.response["Error"]["Code"] == "TransactionCanceledException"
                    and cancellation_codes(exc)[0] == "ConditionalCheckFailed"):
                return "duplicate"
            raise

    def _ack(self, msg: dict[str, Any], outcome: str, reason: str = "") -> int:
        payload = {"TaskId": msg["TaskId"], "ProducerId": msg["ProducerId"], "TaskSK": msg["TaskSK"],
                   "EventId": msg["EventId"], "ConsumerId": self.consumer_id, "Outcome": outcome, "Reason": reason}
        status, body = self.ack(payload)
        for delay in self.ack_backoff:
            if status not in (429, 500, 502, 503, 504):
                break
            metric(COMPONENT, "AckRetries", EventId=msg["EventId"], status=status)
            self.sleep(delay)
            status, body = self.ack(payload)
        if status != 200:
            raise AckFailed(f"ACK {outcome} for {msg['TaskId']} returned {status}: {body}")
        return status

    def handle_message(self, msg: dict[str, Any]) -> str:
        if msg.get("ConsumerId") != self.consumer_id:
            log(COMPONENT, "wrong-consumer-ignored", TaskId=msg.get("TaskId"), ConsumerId=msg.get("ConsumerId"),
                me=self.consumer_id)
            return "ignored"
        if self.fault_injection:
            fault = self._fault(msg)
            if fault == "TERMINAL":
                self._ack(msg, "FAILED", "injected terminal failure")
                log(COMPONENT, "terminal-failure-acked", TaskId=msg["TaskId"], EventId=msg["EventId"])
                return "failed"
            if fault == "TRANSIENT":
                metric(COMPONENT, "InjectedTransientFaults", EventId=msg["EventId"])
                raise TransientFault(f"injected transient failure for {msg['TaskId']}")
        outcome = self.apply(msg)
        metric(COMPONENT, "SideEffectsApplied" if outcome == "applied" else "DuplicateDeliveries",
               EventId=msg["EventId"], ConsumerId=self.consumer_id)
        self._ack(msg, "COMPLETED")
        log(COMPONENT, "processed", TaskId=msg["TaskId"], EventId=msg["EventId"], Action=msg["Action"],
            ConsumerId=self.consumer_id, outcome=outcome)
        return outcome


def signed_ack_client(url: str, region: str, session=None,
                      ssl_context=None) -> Callable[[dict[str, Any]], tuple[int, dict[str, Any]]]:
    """POST to the IAM-authorized Completion API with SigV4 using the Lambda role credentials."""
    import boto3
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    session = session or boto3.Session()

    def post(payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        data = json.dumps(payload, sort_keys=True).encode()
        request = AWSRequest(method="POST", url=url, data=data, headers={"Content-Type": "application/json"})
        SigV4Auth(session.get_credentials().get_frozen_credentials(), "execute-api", region).add_auth(request)
        http = urllib.request.Request(url, data=data, method="POST", headers=dict(request.headers.items()))
        try:
            with urllib.request.urlopen(http, timeout=5, context=ssl_context) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as err:
            try:
                return err.code, json.loads(err.read() or b"{}")
            except ValueError:
                return err.code, {}
    return post
