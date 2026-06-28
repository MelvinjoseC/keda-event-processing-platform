import json
from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient

from app.main import app


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

        # Verify RabbitMQ publish was called
        mock_exchange = mock_rabbitmq["exchange"]
        mock_exchange.publish.assert_called_once()
        # Retrieve the published message argument
        published_msg = mock_exchange.publish.call_args[0][0]
        body = json.loads(published_msg.body.decode("utf-8"))
        assert body["type"] == "order.created"
        assert body["payload"] == {"order_id": "12345"}
        assert body["correlation_id"] == "test-correlation-id"


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
