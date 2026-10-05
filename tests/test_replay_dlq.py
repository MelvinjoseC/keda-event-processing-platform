from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from scripts.replay_dlq import ReplayOptions, inspect_or_replay_messages, parse_args


def test_parse_args_defaults():
    args = parse_args([])
    assert args.dlq_queue == "events.dead"
    assert args.target_exchange == "events"
    assert args.target_routing_key == "events.order"
    assert args.limit == 100
    assert args.filter_reason is None
    assert args.dry_run is False


def test_parse_args_custom():
    args = parse_args(
        [
            "--dlq-queue",
            "custom.dead",
            "--target-exchange",
            "custom.events",
            "--target-routing-key",
            "custom.key",
            "--limit",
            "25",
            "--filter-reason",
            "RuntimeError",
            "--dry-run",
        ]
    )
    assert args.dlq_queue == "custom.dead"
    assert args.target_exchange == "custom.events"
    assert args.target_routing_key == "custom.key"
    assert args.limit == 25
    assert args.filter_reason == "RuntimeError"
    assert args.dry_run is True


def test_inspect_or_replay_messages_empty_queue():
    async def run():
        mock_queue = AsyncMock()
        mock_queue.get.return_value = None
        mock_exchange = AsyncMock()

        options = ReplayOptions(limit=10, dry_run=False)
        stats = await inspect_or_replay_messages(mock_queue, mock_exchange, options)

        assert stats == {"processed": 0, "replayed": 0, "skipped": 0, "inspected": 0}
        mock_exchange.publish.assert_not_called()

    asyncio.run(run())


def test_inspect_or_replay_messages_dry_run():
    async def run():
        mock_msg = MagicMock()
        mock_msg.message_id = "msg-123"
        mock_msg.correlation_id = "corr-456"
        mock_msg.body = b'{"test": "data"}'
        mock_msg.headers = {
            "x-dead-letter-reason": "ValueError",
            "x-dead-letter-error": "Invalid format",
            "x-failed-worker": "worker-1",
            "x-retry-count": 3,
        }
        mock_msg.nack = AsyncMock()
        mock_msg.ack = AsyncMock()

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [mock_msg, None]
        mock_exchange = AsyncMock()

        options = ReplayOptions(limit=10, dry_run=True)
        stats = await inspect_or_replay_messages(mock_queue, mock_exchange, options)

        assert stats["inspected"] == 1
        assert stats["replayed"] == 0
        mock_msg.nack.assert_awaited_once_with(requeue=True)
        mock_msg.ack.assert_not_called()
        mock_exchange.publish.assert_not_called()

    asyncio.run(run())


def test_inspect_or_replay_messages_republish_and_ack():
    async def run():
        mock_msg = MagicMock()
        mock_msg.message_id = "msg-123"
        mock_msg.correlation_id = "corr-456"
        mock_msg.content_type = "application/json"
        mock_msg.body = b'{"test": "data"}'
        mock_msg.headers = {
            "x-dead-letter-reason": "RuntimeError",
            "x-dead-letter-error": "Service unavailable",
            "x-failed-worker": "worker-1",
            "x-retry-count": 3,
        }
        mock_msg.ack = AsyncMock()
        mock_msg.nack = AsyncMock()

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [mock_msg, None]
        mock_exchange = AsyncMock()

        options = ReplayOptions(
            limit=10, dry_run=False, target_routing_key="events.order"
        )
        stats = await inspect_or_replay_messages(mock_queue, mock_exchange, options)

        assert stats["replayed"] == 1
        assert stats["inspected"] == 0
        mock_exchange.publish.assert_awaited_once()
        published_msg = mock_exchange.publish.call_args[0][0]
        assert published_msg.headers["x-replayed-by"] == "replay_dlq"
        assert published_msg.headers["x-retry-count"] == 0
        assert published_msg.headers["x-original-dlq-reason"] == "RuntimeError"
        mock_msg.ack.assert_awaited_once()
        mock_msg.nack.assert_not_called()

    asyncio.run(run())


def test_inspect_or_replay_messages_filter_reason_skip():
    async def run():
        mock_msg = MagicMock()
        mock_msg.message_id = "msg-999"
        mock_msg.headers = {"x-dead-letter-reason": "KeyError"}
        mock_msg.ack = AsyncMock()
        mock_msg.nack = AsyncMock()

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [mock_msg, None]
        mock_exchange = AsyncMock()

        options = ReplayOptions(limit=10, filter_reason="ValueError")
        stats = await inspect_or_replay_messages(mock_queue, mock_exchange, options)

        assert stats["skipped"] == 1
        assert stats["replayed"] == 0
        mock_msg.nack.assert_awaited_once_with(requeue=True)
        mock_exchange.publish.assert_not_called()

    asyncio.run(run())
