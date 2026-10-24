"""Offline comparison of load-test result dirs (summary.json + cloudwatch.json). No AWS calls.

  python scripts/compare_runs.py OLD_DIR NEW_DIR [...] [--md OUT.md]

Lambda Duration "avg" is invocation-weighted over the per-minute averages; "p95*" is the maximum of per-minute
p95 values (an upper bound, not a true run-level p95). CloudWatch windows start 2 minutes before the first send;
when that overlapped a previous run, cloudwatch-clean.json (re-queried from a later start) is used instead.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

FUNCTIONS = ("Ingest", "Resolver", "Dispatch", "Completion", "SearchConsumer", "ReportingConsumer")


def series(cw, label):
    r = cw.get(label) or {"timestamps": [], "values": []}
    return dict(zip(r["timestamps"], r["values"]))


def lambda_stats(cw):
    out = {}
    for fn in FUNCTIONS:
        inv, avg = series(cw, f"{fn}.Invocations.Sum"), series(cw, f"{fn}.Duration.Average")
        weighted = sum(avg[t] * inv.get(t, 0) for t in avg)
        total = sum(inv.values())
        out[fn] = {"invocations": int(total), "errors": int(sum(series(cw, f"{fn}.Errors.Sum").values())),
                   "throttles": int(sum(series(cw, f"{fn}.Throttles.Sum").values())),
                   "duration_avg_ms": round(weighted / total, 1) if total else None,
                   "duration_p95_max_ms": round(max(series(cw, f"{fn}.Duration.p95").values(), default=0), 1),
                   "duration_max_ms": round(max(series(cw, f"{fn}.Duration.Maximum").values(), default=0), 1),
                   "max_concurrency": int(max(series(cw, f"{fn}.ConcurrentExecutions.Maximum").values(), default=0))}
    for fn in ("Resolver", "Dispatch"):
        out[fn]["iterator_age_max_ms"] = int(max(series(cw, f"{fn}.IteratorAge.Maximum").values(), default=0))
    return out


def load(d: Path):
    s = json.loads((d / "summary.json").read_text())
    cw_file = d / "cloudwatch-clean.json" if (d / "cloudwatch-clean.json").exists() else d / "cloudwatch.json"
    cw = json.loads(cw_file.read_text())
    ddb = {k.split(".")[0]: int(sum(series(cw, k).values())) for k in cw if k.endswith(".WriteThrottleEvents")}
    return {"dir": d.name + (" (clean CW window)" if cw_file.name != "cloudwatch.json" else ""), "summary": s, "lambda": lambda_stats(cw), "ddb_write_throttles": ddb,
            "sqs_oldest_age_max_s": max(series(cw, "IngressQueue.ApproximateAgeOfOldestMessage.Maximum").values(),
                                        default=None)}


def markdown(runs):
    head = "| Metric | " + " | ".join(r["dir"] for r in runs) + " |\n|---|" + "---|" * len(runs) + "\n"
    rows = []

    def row(name, f):
        rows.append(f"| {name} | " + " | ".join(str(f(r)) for r in runs) + " |")
    s = lambda r: r["summary"]  # noqa: E731
    row("Unique source events sent", lambda r: s(r)["unique_source_events_sent"])
    row("Consumer tasks expected / found", lambda r: f'{s(r)["consumer_tasks_expected"]} / {s(r)["consumer_tasks_found"]}')
    row("Status counts", lambda r: s(r)["status_counts"])
    row("Completed tasks", lambda r: s(r)["completed"])
    row("Wall clock first send -> last ACK (s)", lambda r: s(r)["wall_clock_first_send_to_last_ack_s"])
    row("Throughput (completed tasks/s)", lambda r: s(r)["completed_tasks_per_second"])
    for k in ("client_send_to_ack", "ingested_to_ack", "ready_to_ack"):
        row(f"Latency {k} p50/p95/p99 (ms)",
            lambda r, k=k: "/".join(str(s(r)["latency_ms"][k][p]) for p in ("p50", "p95", "p99")))
    row("Dispatch attempts distribution", lambda r: s(r).get("dispatch_attempts_distribution", "n/a (not recorded)"))
    for k, v in (("search_dependency_order_violations", "Search order violations"),
                 ("reporting_cross_consumer_violations", "Cross-consumer order violations"),
                 ("projections_with_duplicate_applies", "Projections with duplicate side effects"),
                 ("projections_missing", "Projections missing")):
        row(v, lambda r, k=k: s(r)["side_effects"][k])
    for fn in FUNCTIONS:
        row(f"{fn} inv / err / throttles", lambda r, fn=fn: "{invocations} / {errors} / {throttles}".format(**r["lambda"][fn]))
        row(f"{fn} Duration avg / p95* / max (ms)",
            lambda r, fn=fn: "{duration_avg_ms} / {duration_p95_max_ms} / {duration_max_ms}".format(**r["lambda"][fn]))
        row(f"{fn} max concurrency", lambda r, fn=fn: r["lambda"][fn]["max_concurrency"])
    for fn in ("Resolver", "Dispatch"):
        row(f"{fn} IteratorAge max (ms)", lambda r, fn=fn: r["lambda"][fn]["iterator_age_max_ms"])
    row("DynamoDB write throttles", lambda r: r["ddb_write_throttles"])
    row("Ingress queue oldest message age max (s)", lambda r: r["sqs_oldest_age_max_s"])
    row("Queues after (visible)", lambda r: {k.replace("Url", ""): v["ApproximateNumberOfMessages"]
                                             for k, v in s(r)["queues_after"].items()})
    return head + "\n".join(rows) + "\n"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("dirs", nargs="+", type=Path)
    p.add_argument("--md", type=Path)
    a = p.parse_args()
    runs = [load(d) for d in a.dirs]
    md = markdown(runs)
    print(md)
    if a.md:
        a.md.write_text(md)


if __name__ == "__main__":
    main()
