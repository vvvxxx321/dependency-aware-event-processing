"""Read-only audit: every TASK ingested in a time window, grouped by business key, with status timestamps.

Independent of the scenario scripts' own assertions (re-derives ordering from DynamoDB records).
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict

from boto3.dynamodb.conditions import Attr

from live_common import Live, parser


def main() -> None:
    p = parser(__doc__)
    p.add_argument("--start-ms", type=int, required=True)
    p.add_argument("--end-ms", type=int, required=True)
    args = p.parse_args()
    live = Live(args.profile, args.region, args.stack)
    items, kw = [], {"FilterExpression": Attr("RecordType").eq("TASK") & Attr("IngestedAt").between(args.start_ms, args.end_ms)}
    while True:
        page = live.log.scan(**kw)
        items.extend(page["Items"])
        if "LastEvaluatedKey" not in page:
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    groups = defaultdict(list)
    for t in items:
        params = t["Parameters"]
        key = f"PaperId={params['PaperId']}" if "PaperId" in params else f"ReviewId={params.get('ReviewId')}"
        row = {"Action": t["Action"], "Consumer": t["ConsumerId"], "Status": t["Status"], "EventId": t["EventId"]}
        for k in ("IngestedAt", "ReadyAt", "DispatchedAt", "CompletedAt", "FailedAt", "PendingAt", "ReplayGeneration",
                  "DispatchAttempts"):
            if k in t:
                row[k] = int(t[k])
        for k in ("FailureReason", "RootFailureEventId"):
            if k in t:
                row[k] = t[k]
        groups[key].append(row)
    out = {k: sorted(v, key=lambda r: r["IngestedAt"]) for k, v in sorted(groups.items())}
    json.dump({"window": [args.start_ms, args.end_ms], "tasks": len(items), "groups": out}, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
