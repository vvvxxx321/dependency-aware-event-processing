"""Operator CLI (LIVE AWS). Dry-run unless --commit.

  reconcile                 re-evaluate every PENDING task (missed stream wakeups)
  redispatch --older-than S re-publish READY tasks without ACK for S seconds
  replay --root EVENT_ID    FAILED -> PENDING for a root failure chain (original EventId/Timestamp kept)
  dlq-inspect --queue K     print messages in IngressDLQUrl / ConsumerDLQUrl / StreamFailureQueueUrl (no delete)
"""
from __future__ import annotations

import json
import time

from live_common import Live, parser


def store_for(live: Live):
    from event_platform.aws_store import PlatformStore
    return PlatformStore(config_table=live.out["ConfigurationTableName"], log_table=live.out["ReplicationLogTableName"],
                         route_index="BySourceAction", waiters_table=live.out["DependencyWaitersTableName"],
                         resource=live.ddb, client=live.ddb_client)


def main() -> None:
    p = parser(__doc__)
    p.add_argument("command", choices=["reconcile", "redispatch", "replay", "dlq-inspect"])
    p.add_argument("--older-than", type=int, default=300)
    p.add_argument("--root")
    p.add_argument("--queue", default="IngressDLQUrl")
    p.add_argument("--operator", default="ops_cli")
    p.add_argument("--commit", action="store_true")
    args = p.parse_args()
    live = Live(args.profile, args.region, args.stack)
    store = store_for(live)
    now = int(time.time() * 1000)

    if args.command == "dlq-inspect":
        # Long-poll several times (one receive samples a subset of SQS servers); messages are never deleted.
        seen = set()
        for _ in range(5):
            resp = live.sqs.receive_message(QueueUrl=live.out[args.queue], MaxNumberOfMessages=10, VisibilityTimeout=30,
                                            WaitTimeSeconds=2, AttributeNames=["All"], MessageAttributeNames=["All"])
            for m in resp.get("Messages", []):
                if m["MessageId"] not in seen:
                    seen.add(m["MessageId"])
                    print(json.dumps({"MessageId": m["MessageId"], "Attributes": m.get("Attributes"), "Body": m["Body"]}))
        return

    if args.command == "reconcile":
        pending = store.scan_tasks({"PENDING"})
        print(f"{len(pending)} PENDING tasks")
        if args.commit:
            from event_platform.ops import reconcile_pending
            from event_platform.resolver import Resolver
            print(json.dumps(reconcile_pending(store, Resolver(store)), indent=1))
        return

    if args.command == "redispatch":
        stale = [t for t in store.scan_tasks({"READY"})
                 if now - int(t.get("DispatchedAt") or t.get("ReadyAt") or t["StatusUpdatedOn"]) >= args.older_than * 1000]
        print(f"{len(stale)} READY tasks older than {args.older_than}s: {[t['TaskId'][:12] for t in stale]}")
        if args.commit:
            from event_platform.dispatch import sns_publisher
            from event_platform.ops import redispatch_stale_ready
            print(redispatch_stale_ready(store, sns_publisher(live.sns, live.out["DispatchTopicArn"]), now_ms=now,
                                         older_than_ms=args.older_than * 1000))
        return

    if args.command == "replay":
        if not args.root:
            raise SystemExit("--root EVENT_ID required")
        chain = [t for t in store.scan_tasks({"FAILED"}) if t.get("RootFailureEventId") == args.root]
        print(f"{len(chain)} FAILED tasks with RootFailureEventId={args.root}")
        if args.commit:
            from event_platform.ops import replay_failed
            print(replay_failed(store, root_event_id=args.root, now_ms=now, operator=args.operator))


if __name__ == "__main__":
    main()
