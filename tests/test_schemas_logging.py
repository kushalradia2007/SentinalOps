import io
import json
import logging

import pytest
from pydantic import ValidationError

from sentinalops.logging import JsonFormatter
from sentinalops.schemas import LabelEvent, PackageEvent, Prediction, ScoreRequest


def test_package_event_defaults_and_registry_validation():
    e = PackageEvent(registry="pypi", name="requests")
    assert e.version == "0" and e.published_at > 0
    with pytest.raises(ValidationError):
        PackageEvent(registry="rubygems", name="x")


def test_score_request_requires_name():
    with pytest.raises(ValidationError):
        ScoreRequest(files={}, meta={})
    assert ScoreRequest(files={}, meta={"name": "a"}).meta["name"] == "a"


def test_prediction_bounds():
    with pytest.raises(ValidationError):
        Prediction(package="a", score=1.2, blocked=True, model_version="v")
    assert LabelEvent(package="a", is_malicious=True).source == "manual"


def test_json_formatter_emits_extras():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    log = logging.getLogger("t.json")
    log.handlers[:] = [handler]
    log.setLevel(logging.INFO)
    log.info("hello", extra={"package": "evil", "score": 0.5})
    row = json.loads(stream.getvalue())
    assert row["msg"] == "hello" and row["package"] == "evil" and row["level"] == "INFO"
