import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.main import app, handle_message
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def mock_rabbitmq():
    with patch("aio_pika.connect_robust") as mock_connect:
        mock_conn = AsyncMock()
        mock_conn.is_closed = False
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_queue = MagicMock()
        mock_queue.bind = AsyncMock()

        mock_queue.consume = AsyncMock(return_value="test-consumer-tag")
        mock_channel.basic_cancel = AsyncMock()

        mock_connect.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.declare_exchange.return_value = mock_exchange
        mock_channel.declare_queue.return_value = mock_queue
        mock_channel.get_exchange.return_value = mock_exchange

        yield {
            "connect": mock_connect,
            "connection": mock_conn,
            "channel": mock_channel,
            "exchange": mock_exchange,
            "queue": mock_queue,
        }


def test_healthz():
    with TestClient(app) as client:
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_readyz_healthy(mock_rabbitmq):
    with TestClient(app) as client:
        # Override the finished consumer task with a mock running task
        mock_task = MagicMock()
        mock_task.done.return_value = False
        app.state.consumer_task = mock_task
        app.state.consumer_ready = True

        response = client.get("/readyz")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}


def test_readyz_unhealthy_closed(mock_rabbitmq):
    mock_rabbitmq["connection"].is_closed = True
    with TestClient(app) as client:
        # Override the finished consumer task with a mock running task
        mock_task = MagicMock()
        mock_task.done.return_value = False
        app.state.consumer_task = mock_task

        response = client.get("/readyz")
        assert response.status_code == 503


def test_readyz_unhealthy_not_ready(mock_rabbitmq):
    with TestClient(app) as client:
        # Override the finished consumer task with a mock running task
        mock_task = MagicMock()
        mock_task.done.return_value = False
        app.state.consumer_task = mock_task
        app.state.consumer_ready = False

        response = client.get("/readyz")
        assert response.status_code == 503


def test_readyz_unhealthy_task_crashed(mock_rabbitmq):
    with TestClient(app) as client:
        # Simulate consumer task crashed with an exception
        mock_task = MagicMock()
        mock_task.done.return_value = True
        mock_task.exception.return_value = Exception("RabbitMQ connection lost")
        app.state.consumer_task = mock_task

        response = client.get("/readyz")
        assert response.status_code == 503
        assert "failed" in response.json()["detail"].lower()


def test_handle_message_success(mock_rabbitmq):
    async def run():
        # Mock incoming message
        message = AsyncMock()
        message.body = json.dumps(
            {
                "id": "event-123",
                "type": "order.created",
                "payload": {"order_id": "1"},
            }
        ).encode("utf-8")
        message.message_id = "event-123"
        message.correlation_id = "corr-123"
        message.headers = {}

        exchange = mock_rabbitmq["exchange"]

        await handle_message(message, exchange)

        # Message must be acknowledged
        message.ack.assert_called_once()
        message.reject.assert_not_called()
        exchange.publish.assert_not_called()

    asyncio.run(run())


def test_handle_message_retry_trigger(mock_rabbitmq):
    async def run():
        # Mock incoming message with payload that forces processing error
        message = AsyncMock()
        message.body = json.dumps(
            {
                "id": "event-123",
                "type": "order.created",
                "payload": {"force_error": True},
            }
        ).encode("utf-8")
        message.message_id = "event-123"
        message.correlation_id = "corr-123"
        message.headers = {"x-retry-count": 0}

        exchange = mock_rabbitmq["exchange"]

        await handle_message(message, exchange)

        # Message must be acknowledged and republished for retry
        message.ack.assert_called_once()
        message.reject.assert_not_called()
        exchange.publish.assert_called_once()

        # Verify published retry headers and routing key
        publish_args = exchange.publish.call_args
        published_msg = publish_args[0][0]
        assert published_msg.headers["x-retry-count"] == 1
        assert publish_args[1].get("routing_key") == "events.retry"

    asyncio.run(run())


def test_handle_message_retry_exhausted(mock_rabbitmq):
    async def run():
        # Mock incoming message that forces error, with max retries already reached
        message = AsyncMock()
        message.body = json.dumps(
            {
                "id": "event-123",
                "type": "order.created",
                "payload": {"force_error": True},
            }
        ).encode("utf-8")
        message.message_id = "event-123"
        message.correlation_id = "corr-123"
        message.headers = {"x-retry-count": 3}  # MAX_RETRIES is 3

        exchange = mock_rabbitmq["exchange"]

        await handle_message(message, exchange)

        # Message must be rejected (sending it to DLQ) without a new retry publication
        message.ack.assert_not_called()
        message.reject.assert_called_once_with(requeue=False)
        exchange.publish.assert_not_called()

    asyncio.run(run())


def test_metrics_endpoint(mock_rabbitmq):
    with TestClient(app) as client:
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "worker_events_processed_total" in response.text
