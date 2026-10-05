import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
)
from fastapi import FastAPI, HTTPException, status
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.responses import Response

SERVICE_NAME = os.getenv("SERVICE_NAME", "event-worker")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "events")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "events")
ROUTING_KEY = os.getenv("ROUTING_KEY", "events.created")
DEAD_LETTER_EXCHANGE = os.getenv("DEAD_LETTER_EXCHANGE", "events.dlx")
DEAD_LETTER_QUEUE = os.getenv("DEAD_LETTER_QUEUE", "events.dead")
DEAD_LETTER_ROUTING_KEY = os.getenv("DEAD_LETTER_ROUTING_KEY", "events.dead")
PROCESS_SECONDS = float(os.getenv("PROCESS_SECONDS", "0.15"))
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

RETRY_EXCHANGE = os.getenv("RETRY_EXCHANGE", "events.retry.dx")
RETRY_QUEUE = os.getenv("RETRY_QUEUE", "events.retry")
RETRY_ROUTING_KEY = os.getenv("RETRY_ROUTING_KEY", "events.retry")
RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "10000"))

PROCESSED_EVENTS = Counter(
    "worker_events_processed_total",
    "Events successfully processed by the worker",
    ["event_type"],
)
FAILED_EVENTS = Counter(
    "worker_events_failed_total",
    "Events that exhausted retries and were rejected to the dead letter queue",
    ["event_type"],
)
RETRIED_EVENTS = Counter(
    "worker_events_retried_total",
    "Events republished for retry",
    ["event_type"],
)
PROCESSING_LATENCY = Histogram(
    "worker_processing_seconds",
    "Time spent processing one event",
    ["event_type"],
)
INFLIGHT_MESSAGES = Gauge(
    "worker_inflight_messages",
    "Messages currently being processed by this worker pod",
)


def decode_event(body: bytes) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("message body must be a JSON object")
    if not payload.get("id") or not payload.get("type"):
        raise ValueError("message body must include id and type")
    if "payload" not in payload:
        payload["payload"] = {}
    return payload


async def declare_topology(channel: AbstractChannel) -> AbstractQueue:
    dlx = await channel.declare_exchange(
        DEAD_LETTER_EXCHANGE,
        ExchangeType.DIRECT,
        durable=True,
    )
    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        ExchangeType.DIRECT,
        durable=True,
    )

    # Declare Retry DLX and Retry Queue with TTL pointing back to the main exchange
    retry_dlx = await channel.declare_exchange(
        RETRY_EXCHANGE,
        ExchangeType.DIRECT,
        durable=True,
    )
    retry_queue = await channel.declare_queue(
        RETRY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": EXCHANGE_NAME,
            "x-dead-letter-routing-key": ROUTING_KEY,
            "x-message-ttl": RETRY_DELAY_MS,
        },
    )
    await retry_queue.bind(retry_dlx, routing_key=RETRY_ROUTING_KEY)

    queue = await channel.declare_queue(
        QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
        },
    )
    dead_letter_queue = await channel.declare_queue(DEAD_LETTER_QUEUE, durable=True)
    await queue.bind(exchange, routing_key=ROUTING_KEY)
    await dead_letter_queue.bind(dlx, routing_key=DEAD_LETTER_ROUTING_KEY)
    return queue


async def process_event(event: dict[str, Any]) -> None:
    payload = event.get("payload", {})
    if payload.get("force_error") is True:
        raise RuntimeError("forced processing error")
    await asyncio.sleep(PROCESS_SECONDS)


async def republish_for_retry(
    retry_exchange: AbstractExchange,
    original: AbstractIncomingMessage,
    retry_count: int,
) -> None:
    logger.info(
        "Republishing message %s to retry exchange (%d/%d)",
        original.message_id,
        retry_count,
        MAX_RETRIES,
    )
    headers = dict(original.headers or {})
    headers["x-retry-count"] = retry_count

    await retry_exchange.publish(
        Message(
            body=original.body,
            content_type=original.content_type,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=original.message_id,
            correlation_id=original.correlation_id,
            timestamp=int(time.time()),
            headers=headers,
        ),
        routing_key=RETRY_ROUTING_KEY,
    )


async def republish_to_dlq(
    dlx_exchange: AbstractExchange,
    original: AbstractIncomingMessage,
    error: Exception,
    retry_count: int,
) -> None:
    logger.info(
        "Republishing message %s to DLQ exchange (%s) with diagnostic metadata",
        original.message_id,
        DEAD_LETTER_ROUTING_KEY,
    )
    headers = dict(original.headers or {})
    headers["x-dead-letter-reason"] = type(error).__name__
    headers["x-dead-letter-error"] = str(error)[:500]
    headers["x-failed-worker"] = os.getenv("HOSTNAME", SERVICE_NAME)
    headers["x-failed-at"] = int(time.time())
    headers["x-retry-count"] = retry_count

    await dlx_exchange.publish(
        Message(
            body=original.body,
            content_type=original.content_type,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=original.message_id,
            correlation_id=original.correlation_id,
            timestamp=int(time.time()),
            headers=headers,
        ),
        routing_key=DEAD_LETTER_ROUTING_KEY,
    )


async def handle_message(
    message: AbstractIncomingMessage,
    retry_exchange: AbstractExchange,
    dlx_exchange: AbstractExchange | None = None,
) -> None:
    raw_retry = (message.headers or {}).get("x-retry-count", 0)
    retry_count = int(str(raw_retry)) if raw_retry is not None else 0
    event_type = "unknown"
    event_id = message.message_id or "unknown"

    INFLIGHT_MESSAGES.inc()
    started = time.perf_counter()
    try:
        event = decode_event(message.body)
        event_type = event["type"]
        event_id = event["id"]
        logger.info(
            "Processing event: id=%s, type=%s, correlation_id=%s, attempt=%d",
            event_id,
            event_type,
            event.get("correlation_id"),
            retry_count + 1,
        )
        await process_event(event)
    except Exception as exc:
        duration = time.perf_counter() - started
        if retry_count < MAX_RETRIES:
            logger.warning(
                "Error processing event %s of type %s on attempt %d (took %.3fs). Retrying... Error: %r",
                event_id,
                event_type,
                retry_count + 1,
                duration,
                exc,
            )
            await republish_for_retry(retry_exchange, message, retry_count + 1)
            await message.ack()
            RETRIED_EVENTS.labels(event_type=event_type).inc()
        else:
            logger.error(
                "Error processing event %s of type %s. Retries exhausted (%d/%d). Rejecting to DLQ. Error: %s",
                event_id,
                event_type,
                retry_count,
                MAX_RETRIES,
                exc,
                exc_info=True,
            )
            if dlx_exchange is not None:
                await republish_to_dlq(dlx_exchange, message, exc, retry_count)
                await message.ack()
            else:
                await message.reject(requeue=False)
            FAILED_EVENTS.labels(event_type=event_type).inc()
    else:
        duration = time.perf_counter() - started
        await message.ack()
        PROCESSED_EVENTS.labels(event_type=event_type).inc()
        logger.info(
            "Successfully processed event %s of type %s (took %.3fs)",
            event_id,
            event_type,
            duration,
        )
    finally:
        PROCESSING_LATENCY.labels(event_type=event_type).observe(
            time.perf_counter() - started
        )
        INFLIGHT_MESSAGES.dec()


async def consume(app: FastAPI) -> None:
    logger.info("Starting background RabbitMQ consumer task...")
    try:
        connection = await aio_pika.connect_robust(
            RABBITMQ_URL,
            client_properties={"connection_name": SERVICE_NAME},
        )
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=PREFETCH_COUNT)
        queue = await declare_topology(channel)
        retry_exchange = await channel.get_exchange(RETRY_EXCHANGE)
        dlx_exchange = await channel.get_exchange(DEAD_LETTER_EXCHANGE)

        app.state.connection = connection
        app.state.channel = channel

        async def on_message(message: AbstractIncomingMessage) -> None:
            task = asyncio.create_task(
                handle_message(message, retry_exchange, dlx_exchange)
            )
            app.state.active_tasks.add(task)
            task.add_done_callback(app.state.active_tasks.discard)

        consumer_tag = await queue.consume(on_message)
        app.state.consumer_tag = consumer_tag
        app.state.consumer_ready = True
        logger.info(
            "RabbitMQ consumer connected, topology declared, consumer tag: %s",
            consumer_tag,
        )

        while not app.state.is_shutting_down:
            await asyncio.sleep(0.5)

    except Exception as exc:
        logger.exception("Fatal error in consumer background task: %s", exc)
        app.state.consumer_ready = False
        raise exc


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting event worker service...")
    app.state.consumer_ready = False
    app.state.is_shutting_down = False
    app.state.active_tasks = set()
    app.state.consumer_tag = None

    task = asyncio.create_task(consume(app))
    app.state.consumer_task = task
    try:
        yield
    finally:
        logger.info("Shutting down event worker service (graceful)...")
        app.state.is_shutting_down = True
        app.state.consumer_ready = False

        channel = getattr(app.state, "channel", None)
        consumer_tag = getattr(app.state, "consumer_tag", None)
        if channel is not None and consumer_tag is not None and not channel.is_closed:
            logger.info(
                "Canceling consumer tag %s to stop receiving new messages", consumer_tag
            )
            try:
                await channel.basic_cancel(consumer_tag)
            except Exception as exc:
                logger.warning("Failed to cancel consumer: %r", exc)

        active_tasks: set[asyncio.Task[Any]] = getattr(app.state, "active_tasks", set())
        if active_tasks:
            logger.info("Waiting for %d active tasks to complete...", len(active_tasks))
            try:
                await asyncio.wait_for(
                    asyncio.gather(*active_tasks, return_exceptions=True),
                    timeout=30.0,
                )
                logger.info("All active tasks completed")
            except TimeoutError:
                logger.warning("Timeout reached. Forcing cancellation of active tasks.")
                for active_task in active_tasks:
                    active_task.cancel()
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(*active_tasks, return_exceptions=True)

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

        connection = getattr(app.state, "connection", None)
        if channel is not None and not channel.is_closed:
            await channel.close()
        if connection is not None and not connection.is_closed:
            await connection.close()
        logger.info("RabbitMQ connections closed, worker shutdown complete")


app = FastAPI(
    title="Event Worker",
    version="0.1.0",
    docs_url="/docs",
    redoc_url=None,
    lifespan=lifespan,
)


@app.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz", include_in_schema=False)
async def readyz() -> dict[str, str]:
    task = getattr(app.state, "consumer_task", None)
    if task is not None and task.done():
        try:
            exc = task.exception()
            if exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Consumer background task failed: {exc}",
                )
        except asyncio.CancelledError:
            pass
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Consumer background task terminated unexpectedly",
        )

    connection = getattr(app.state, "connection", None)
    if not getattr(app.state, "consumer_ready", False):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    if connection is None or connection.is_closed:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    return {"status": "ready"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
