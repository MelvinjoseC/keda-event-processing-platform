import json

import pytest
from app.main import decode_event


def test_decode_event_requires_object():
    with pytest.raises(ValueError):
        decode_event(json.dumps(["not", "an", "object"]).encode("utf-8"))


def test_decode_event_sets_default_payload():
    event = decode_event(
        json.dumps({"id": "1", "type": "order.created"}).encode("utf-8")
    )

    assert event["payload"] == {}
