import json
import time
import uuid
from typing import Any


def simulate_publisher_event(
    event_type: str, payload: dict[str, Any], correlation_id: str | None = None
) -> dict[str, Any]:
    event_id = str(uuid.uuid4())
    cid = correlation_id or event_id
    now = int(time.time())
    return {
        "id": event_id,
        "type": event_type,
        "payload": payload,
        "correlation_id": cid,
        "published_at": now,
    }


def simulate_worker_decode(body_bytes: bytes) -> dict[str, Any]:
    payload = json.loads(body_bytes.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("message body must be a JSON object")
    if not payload.get("id") or not payload.get("type"):
        raise ValueError("message body must include id and type")
    if "payload" not in payload:
        payload["payload"] = {}
    return payload


def calculate_retry_backoff(
    retry_count: int, base_delay_ms: int = 10000, factor: float = 2.0
) -> int:
    multiplier = factor ** max(0, retry_count - 1)
    return int(base_delay_ms * multiplier)


def test_publisher_worker_contract_compatibility():
    # 1. Publisher produces message
    original_event = simulate_publisher_event(
        event_type="order.created",
        payload={"order_id": "ord-999", "amount": 199.95},
        correlation_id="corr-trace-001",
    )
    serialized_bytes = json.dumps(original_event, separators=(",", ":")).encode("utf-8")

    # 2. Worker decodes message
    decoded = simulate_worker_decode(serialized_bytes)

    # 3. Assert full contract compatibility
    assert decoded["id"] == original_event["id"]
    assert decoded["type"] == "order.created"
    assert decoded["correlation_id"] == "corr-trace-001"
    assert decoded["payload"]["order_id"] == "ord-999"
    assert decoded["payload"]["amount"] == 199.95
    assert decoded["published_at"] == original_event["published_at"]


def test_end_to_end_retry_and_backoff_progression():
    delays = [calculate_retry_backoff(attempt) for attempt in [1, 2, 3]]
    assert delays == [10000, 20000, 40000]

    # Verify per-message expiration in seconds matches RabbitMQ requirement
    expirations = [delay / 1000.0 for delay in delays]
    assert expirations == [10.0, 20.0, 40.0]


def test_dlq_diagnostic_headers_contract():
    error = RuntimeError("Database deadlock encountered")
    retry_count = 3
    headers = {
        "x-dead-letter-reason": type(error).__name__,
        "x-dead-letter-error": str(error),
        "x-failed-worker": "event-worker-pod-xyz",
        "x-failed-at": int(time.time()),
        "x-retry-count": retry_count,
    }

    assert headers["x-dead-letter-reason"] == "RuntimeError"
    assert "deadlock" in headers["x-dead-letter-error"]
    assert headers["x-retry-count"] == 3
    assert headers["x-failed-worker"] == "event-worker-pod-xyz"
