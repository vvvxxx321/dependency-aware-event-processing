"""Read-only live watch (CloudWatch): per-minute stream IteratorAge, Lambda throttles/errors/concurrency.

Prints one JSON line per poll and flags IteratorAge above --warn-seconds (the stream mappings discard records
older than MaximumRecordAgeInSeconds=3600 to StreamFailureQueue, so the age must stay far below that).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

from live_common import Live, parser

FUNCTIONS = ("Ingest", "Resolver", "Dispatch", "Completion", "SearchConsumer", "ReportingConsumer")


def main() -> None:
    p = parser(__doc__)
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--duration", type=int, default=7200)
    p.add_argument("--warn-seconds", type=int, default=600)
    args = p.parse_args()
    live = Live(args.profile, args.region, args.stack)
    cw = live.session.client("cloudwatch")
    end_at = time.time() + args.duration
    while time.time() < end_at:
        now = datetime.now(timezone.utc)
        queries = []
        for fn in FUNCTIONS:
            name = live.out[f"{fn}FunctionName"]
            metrics = [("Throttles", "Sum"), ("Errors", "Sum"), ("ConcurrentExecutions", "Maximum"),
                       ("Invocations", "Sum")]
            if fn in ("Resolver", "Dispatch"):
                metrics.append(("IteratorAge", "Maximum"))
            for metric, stat in metrics:
                queries.append({"Id": f"q{len(queries)}", "Label": f"{fn}.{metric}", "MetricStat": {
                    "Metric": {"Namespace": "AWS/Lambda", "MetricName": metric,
                               "Dimensions": [{"Name": "FunctionName", "Value": name}]},
                    "Period": 60, "Stat": stat}})
        resp = cw.get_metric_data(MetricDataQueries=queries, StartTime=now - timedelta(minutes=4), EndTime=now,
                                  ScanBy="TimestampDescending")
        row = {"at": now.strftime("%H:%M:%S")}
        for r in resp["MetricDataResults"]:
            if r["Values"]:
                row[r["Label"]] = {"minute": r["Timestamps"][0].strftime("%H:%M"), "value": r["Values"][0]}
        ages = [row.get(f"{fn}.IteratorAge", {}).get("value", 0) / 1000 for fn in ("Resolver", "Dispatch")]
        compact = {k: v["value"] for k, v in row.items() if k != "at" and v["value"]}
        flag = " WARN_ITERATOR_AGE" if max(ages) > args.warn_seconds else ""
        print(json.dumps({"at": row["at"], "iterator_age_s": [round(a, 1) for a in ages], **compact}) + flag,
              flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
