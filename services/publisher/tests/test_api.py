import json
from unittest.mock import AsyncMock, patch

import pytest
from app.main import app
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def mock_rabbitmq():
    with patch("aio_pika.connect_robust") as mock_connect:
        mock_conn = AsyncMock()
        mock_conn.is_closed = False
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_connect.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.declare_exchange.return_value = mock_exchange
        mock_channel.declare_queue.return_value = mock_queue

        yield {
            "connect": mock_connect,
            "connection": mock_conn,
            "channel": mock_channel,
            "exchange": mock_exchange,
            "queue": mock_queue,
        }


def test_healthz():
    # Healthz doesn't depend on lifespan or RabbitMQ connection
    with TestClient(app) as client:
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_readyz_healthy(mock_rabbitmq):
    with TestClient(app) as client:
        response = client.get("/readyz")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}


def test_publisher_confirms_initialized(mock_rabbitmq):
    with TestClient(app):
        mock_rabbitmq["connection"].channel.assert_called_with(publisher_confirms=True)


def test_readyz_unhealthy_closed(mock_rabbitmq):
    # Set connection to closed
    mock_rabbitmq["connection"].is_closed = True
    with TestClient(app) as client:
        response = client.get("/readyz")
        assert response.status_code == 503


def test_readyz_unhealthy_none(mock_rabbitmq):
    # Simulate connection not set
    with TestClient(app) as client:
        app.state.connection = None
        response = client.get("/readyz")
        assert response.status_code == 503


def test_publish_event_success(mock_rabbitmq):
    with TestClient(app) as client:
        event_data = {
            "type": "order.created",
            "payload": {"order_id": "12345"},
            "correlation_id": "test-correlation-id",
        }
        response = client.post("/events", json=event_data)
        assert response.status_code == 202
        response_data = response.json()
        assert response_data["status"] == "accepted"
        assert response_data["queue"] == "events"
        assert response_data["correlation_id"] == "test-correlation-id"
        assert "id" in response_data
        assert response.headers["X-Correlation-ID"] == "test-correlation-id"


def test_correlation_id_propagated_from_header(mock_rabbitmq):
    with TestClient(app) as client:
        event_data = {
            "type": "order.created",
            "payload": {"order_id": "12345"},
        }
        response = client.post(
            "/events",
            json=event_data,
            headers={"X-Correlation-ID": "header-corr-id"},
        )
        assert response.status_code == 202
        response_data = response.json()
        assert response_data["correlation_id"] == "header-corr-id"
        assert response.headers["X-Correlation-ID"] == "header-corr-id"


def test_correlation_id_generated_when_omitted(mock_rabbitmq):
    with TestClient(app) as client:
        event_data = {
            "type": "order.created",
            "payload": {"order_id": "12345"},
        }
        response = client.post("/events", json=event_data)
        assert response.status_code == 202
        assert "X-Correlation-ID" in response.headers
        assert response.json()["correlation_id"] == response.headers["X-Correlation-ID"]

        # Verify RabbitMQ publish was called
        mock_exchange = mock_rabbitmq["exchange"]
        mock_exchange.publish.assert_called_once()
        # Retrieve the published message argument
        published_msg = mock_exchange.publish.call_args[0][0]
        body = json.loads(published_msg.body.decode("utf-8"))
        assert body["type"] == "order.created"
        assert body["payload"] == {"order_id": "12345"}
        assert body["correlation_id"] == response.headers["X-Correlation-ID"]


def test_publish_event_failure(mock_rabbitmq):
    # Mock publish throwing exception
    mock_rabbitmq["exchange"].publish.side_effect = Exception("Connection lost")
    with TestClient(app) as client:
        event_data = {
            "type": "order.created",
            "payload": {"order_id": "12345"},
        }
        response = client.post("/events", json=event_data)
        assert response.status_code == 503
        assert response.json()["detail"] == "Failed to publish event"


def test_metrics_endpoint(mock_rabbitmq):
    with TestClient(app) as client:
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "publisher_events_published_total" in response.text


def test_publish_event_payload_too_large(mock_rabbitmq):
    with TestClient(app) as client:
        # Generate payload larger than 256 KiB
        large_payload = {"data": "x" * 300000}
        event_data = {
            "type": "large.event",
            "payload": large_payload,
        }
        response = client.post("/events", json=event_data)
        assert response.status_code == 413
        assert "exceeds limit" in response.json()["detail"]


def test_publish_event_content_length_header_exceeded(mock_rabbitmq):
    with TestClient(app) as client:
        event_data = {
            "type": "order.created",
            "payload": {"key": "val"},
        }
        response = client.post(
            "/events",
            json=event_data,
            headers={"Content-Length": "9999999"},
        )
        assert response.status_code == 413


def test_publish_events_batch_success(mock_rabbitmq):
    with TestClient(app) as client:
        batch_data = {
            "events": [
                {"type": "order.created", "payload": {"order_id": "1"}},
                {
                    "type": "order.created",
                    "payload": {"order_id": "2"},
                    "correlation_id": "custom-batch-2",
                },
            ]
        }
        response = client.post("/events/batch", json=batch_data)
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["total"] == 2
        assert data["accepted"] == 2
        assert data["failed"] == 0
        assert len(data["events"]) == 2
        assert data["events"][1]["correlation_id"] == "custom-batch-2"
        assert mock_rabbitmq["exchange"].publish.call_count == 2


def test_publish_events_batch_empty_rejected(mock_rabbitmq):
    with TestClient(app) as client:
        response = client.post("/events/batch", json={"events": []})
        assert response.status_code == 422


def test_publish_events_batch_all_failed(mock_rabbitmq):
    mock_rabbitmq["exchange"].publish.side_effect = Exception("Broker down")
    with TestClient(app) as client:
        batch_data = {
            "events": [
                {"type": "order.created", "payload": {"order_id": "1"}},
            ]
        }
        response = client.post("/events/batch", json=batch_data)
        assert response.status_code == 503
        assert "Failed to publish" in response.json()["detail"]
