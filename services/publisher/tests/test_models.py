import pytest
from pydantic import ValidationError

from app.main import EventIn


def test_event_payload_defaults_to_empty_dict():
    event = EventIn(type="order.created")

    assert event.payload == {}
    assert event.correlation_id is None


def test_event_type_rejects_whitespace():
    with pytest.raises(ValidationError):
        EventIn(type="bad type")
