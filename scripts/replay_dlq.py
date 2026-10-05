#!/usr/bin/env python3
"""Enterprise Dead Letter Queue (DLQ) replay and inspection utility.

Connects to RabbitMQ, inspects messages stored in the dead-letter queue (events.dead),
displays diagnostic headers (reason, error trace, failed worker, failure timestamp),
and safely replays messages back to the primary exchange once downstream fixes are deployed.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractRobustConnection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("replay_dlq")


@dataclass(frozen=True)
class ReplayOptions:
    """Configuration options for inspecting and replaying DLQ messages."""

    target_routing_key: str = "events.order"
    limit: int = 100
    filter_reason: str | None = None
    dry_run: bool = False


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect and replay dead-lettered messages in RabbitMQ.",
    )
    parser.add_argument(
        "--amqp-url",
        default=os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
        help="RabbitMQ AMQP connection URL (default: $RABBITMQ_URL or localhost)",
    )
    parser.add_argument(
        "--dlq-queue",
        default=os.getenv("DEAD_LETTER_QUEUE", "events.dead"),
        help="Dead letter queue name to inspect/consume (default: events.dead)",
    )
    parser.add_argument(
        "--target-exchange",
        default=os.getenv("TARGET_EXCHANGE", "events"),
        help="Target exchange to republish replayed messages to (default: events)",
    )
    parser.add_argument(
        "--target-routing-key",
        default=os.getenv("TARGET_ROUTING_KEY", "events.order"),
        help="Target routing key for republished messages (default: events.order)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of messages to process (default: 100)",
    )
    parser.add_argument(
        "--filter-reason",
        type=str,
        default=None,
        help="Only replay messages whose x-dead-letter-reason matches this string",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect DLQ messages and display failure diagnostics without republishing or acking",
    )
    return parser.parse_args(args)


async def inspect_or_replay_messages(
    dlq_queue: AbstractQueue,
    target_exchange: AbstractExchange,
    options: ReplayOptions,
) -> dict[str, int]:
    stats = {"processed": 0, "replayed": 0, "skipped": 0, "inspected": 0}

    for _ in range(options.limit):
        # Fetch message without auto-ack
        incoming_message = await dlq_queue.get(no_ack=False, fail=False)
        if incoming_message is None:
            logger.info("No more messages available in DLQ.")
            break

        stats["processed"] += 1
        headers: dict[str, Any] = dict(incoming_message.headers or {})
        dead_letter_reason = str(headers.get("x-dead-letter-reason", "Unknown"))
        dead_letter_error = str(headers.get("x-dead-letter-error", "None"))
        failed_worker = str(headers.get("x-failed-worker", "Unknown"))
        retry_count = headers.get("x-retry-count", 0)

        # Apply filter if provided
        if (
            options.filter_reason
            and options.filter_reason.lower() not in dead_letter_reason.lower()
        ):
            logger.info(
                "Skipping message %s: reason '%s' does not match filter '%s'",
                incoming_message.message_id,
                dead_letter_reason,
                options.filter_reason,
            )
            # Requeue back to DLQ so other messages can be processed
            await incoming_message.nack(requeue=True)
            stats["skipped"] += 1
            continue

        if options.dry_run:
            stats["inspected"] += 1
            logger.info(
                "[DRY-RUN] Message ID: %s | Correlation ID: %s | Reason: %s | Error: %s | "
                "Worker: %s | Retries: %s | Body bytes: %d",
                incoming_message.message_id,
                incoming_message.correlation_id,
                dead_letter_reason,
                dead_letter_error[:100],
                failed_worker,
                retry_count,
                len(incoming_message.body),
            )
            # Put back into DLQ
            await incoming_message.nack(requeue=True)
        else:
            # Prepare replayed message
            replayed_headers = dict(headers)
            replayed_headers["x-replayed-at"] = int(time.time())
            replayed_headers["x-replayed-by"] = "replay_dlq"
            replayed_headers["x-original-dlq-reason"] = dead_letter_reason
            # Reset retry counter so worker gives it a fresh retry window
            replayed_headers["x-retry-count"] = 0

            new_message = aio_pika.Message(
                body=incoming_message.body,
                content_type=incoming_message.content_type,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=incoming_message.message_id,
                correlation_id=incoming_message.correlation_id,
                timestamp=int(time.time()),
                headers=replayed_headers,
            )

            await target_exchange.publish(
                new_message,
                routing_key=options.target_routing_key,
            )
            # Ack on DLQ only after successful publication
            await incoming_message.ack()
            stats["replayed"] += 1
            logger.info(
                "Replayed message %s to exchange '%s' (routing_key='%s')",
                incoming_message.message_id,
                target_exchange.name,
                options.target_routing_key,
            )

    return stats


async def run_replay(args: argparse.Namespace) -> int:
    logger.info("Connecting to RabbitMQ broker at %s", args.amqp_url)
    try:
        connection: AbstractRobustConnection = await aio_pika.connect_robust(
            args.amqp_url
        )
    except Exception as exc:
        logger.error("Failed to connect to RabbitMQ broker: %s", exc)
        return 1

    async with connection:
        channel: AbstractChannel = await connection.channel()
        try:
            dlq_queue = await channel.declare_queue(
                args.dlq_queue, durable=True, passive=True
            )
        except Exception as exc:
            logger.error(
                "Failed to find or declare DLQ queue '%s': %s", args.dlq_queue, exc
            )
            return 1

        try:
            target_exchange = await channel.declare_exchange(
                args.target_exchange,
                aio_pika.ExchangeType.DIRECT,
                durable=True,
                passive=True,
            )
        except Exception as exc:
            logger.error(
                "Failed to find target exchange '%s': %s", args.target_exchange, exc
            )
            return 1

        options = ReplayOptions(
            target_routing_key=args.target_routing_key,
            limit=args.limit,
            filter_reason=args.filter_reason,
            dry_run=args.dry_run,
        )

        logger.info(
            "Starting DLQ processing: DLQ='%s', Target='%s', Key='%s', Limit=%d, DryRun=%s",
            args.dlq_queue,
            args.target_exchange,
            options.target_routing_key,
            options.limit,
            options.dry_run,
        )

        stats = await inspect_or_replay_messages(
            dlq_queue=dlq_queue,
            target_exchange=target_exchange,
            options=options,
        )

        logger.info(
            "DLQ operation complete: %d messages evaluated, %d replayed, %d inspected, %d skipped",
            stats["processed"],
            stats["replayed"],
            stats["inspected"],
            stats["skipped"],
        )

    return 0


def main() -> None:
    args = parse_args()
    exit_code = asyncio.run(run_replay(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
