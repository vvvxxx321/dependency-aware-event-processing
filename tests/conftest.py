import os

import pytest

# Fake credentials so nothing can reach a real AWS account from unit/local tests.
for _k, _v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_SESSION_TOKEN": "testing",
               "AWS_DEFAULT_REGION": "ca-central-1", "AWS_REGION": "ca-central-1"}.items():
    os.environ[_k] = _v
os.environ.pop("AWS_PROFILE", None)


@pytest.fixture
def pipeline():
    from moto import mock_aws
    from local_pipeline import LocalPipeline
    with mock_aws():
        yield LocalPipeline()


@pytest.fixture
def demo(pipeline):
    """Canonical demo configuration (CreatePaper -> MakeComment example)."""
    p = pipeline
    for producer, action, consumer in [
        ("ContentService", "CreatePaper", "SearchService"), ("ContentService", "CreatePaper", "ReportingService"),
        ("CommentService", "MakeComment", "SearchService"), ("CommentService", "MakeComment", "ReportingService"),
    ]:
        p.subscribe(producer, action, consumer)
    p.add_rule("SearchService", "MakeComment", "ContentService", "CreatePaper", "SearchService", "PaperId")
    return p
