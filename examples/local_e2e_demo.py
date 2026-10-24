"""Local-only P0 demo on moto (NOT AWS): E2 before E1, then print task timeline and projection order."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests")]
for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
             "AWS_DEFAULT_REGION": "ca-central-1", "AWS_REGION": "ca-central-1"}.items():
    os.environ[k] = v
os.environ.pop("AWS_PROFILE", None)

import io, contextlib  # noqa: E401,E402
from moto import mock_aws  # noqa: E402
from local_pipeline import LocalPipeline, make_event  # noqa: E402

with mock_aws():
    p = LocalPipeline()
    for row in [("ContentService", "CreatePaper", "SearchService"), ("CommentService", "MakeComment", "SearchService")]:
        p.subscribe(*row)
    p.add_rule("SearchService", "MakeComment", "ContentService", "CreatePaper", "SearchService", "PaperId")
    e1 = make_event("ContentService", "CreatePaper", {"PaperId": 123}, event_id="11111111-1111-4111-8111-111111111111")
    e2 = make_event("CommentService", "MakeComment", {"PaperId": 123, "CommentId": 456},
                    event_id="22222222-2222-4222-8222-222222222222")
    with contextlib.redirect_stdout(io.StringIO()):  # hide JSON logs
        p.send(e2); p.pump()
    print("after E2 only:  E2@SearchService =", p.status(e2["EventId"], "SearchService"))
    with contextlib.redirect_stdout(io.StringIO()):
        p.send(e1); p.pump()
    t0 = min(int(p.task(e["EventId"], "SearchService")["IngestedAt"]) for e in (e1, e2))
    for name, e in (("E1 CreatePaper", e1), ("E2 MakeComment", e2)):
        t = p.task(e["EventId"], "SearchService")
        print(f"{name}: status={t['Status']} " + " ".join(
            f"{k}=+{int(t[k]) - t0}ms" for k in ("IngestedAt", "ReadyAt", "DispatchedAt", "CompletedAt")))
    print("SearchService projection AppliedOrder:", [x[:8] for x in p.projection("SearchService", 123)["AppliedOrder"]])
    print("Local moto demo only (NOT an AWS integration test).")
