"""Seed subscriptions + dependency rules into the stack's Configuration table, then verify GSI visibility.

Dry run by default (prints items, no AWS calls). --commit performs LIVE writes (requires approval).
"""
from __future__ import annotations

import json

from live_common import Live, parser, rule_items, subscription_items, wait_for


def main() -> None:
    p = parser(__doc__)
    p.add_argument("--commit", action="store_true")
    args = p.parse_args()
    items = subscription_items() + rule_items()
    print(json.dumps(items, indent=2))
    if not args.commit:
        print(f"DRY RUN: {len(items)} items. No AWS calls made.")
        return
    live = Live(args.profile, args.region, args.stack)
    print(f"account {live.account_hint()} region {args.region} table {live.out['ConfigurationTableName']}")
    with live.config.batch_writer() as batch:  # idempotent upserts of fixed keys
        for item in items:
            batch.put_item(Item=item)
    for producer, action, consumer in {(i["ProducerId"], i["Action"], i["ClientId"]) for i in subscription_items()}:
        from event_platform.aws_store import DynamoIngestStore
        store = DynamoIngestStore(config_table=live.out["ConfigurationTableName"], log_table=live.out["ReplicationLogTableName"],
                                  route_index="BySourceAction", resource=live.ddb, client=live.ddb_client)
        ok = wait_for(lambda: consumer in store.consumers_for(producer, action), timeout=60)
        print(f"route {producer}/{action} -> {consumer}: {'VISIBLE' if ok else 'NOT VISIBLE after 60s'}")
        if not ok:
            raise SystemExit("GSI route not visible; do not send traffic yet")
    print("Seed complete; all routes query-visible on GSI.")


if __name__ == "__main__":
    main()
