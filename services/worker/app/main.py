import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
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


async def declare_topology(channel: aio_pika.RobustChannel) -> aio_pika.RobustQueue:
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
    exchange: aio_pika.RobustExchange,
    original: aio_pika.IncomingMessage,
    retry_count: int,
) -> None:
    logger.info(
        "Republishing message %s for retry %d/%d",
        original.message_id,
        retry_count,
        MAX_RETRIES,
    )
    headers = dict(original.headers or {})
    headers["x-retry-count"] = retry_count

    await exchange.publish(
        Message(
            body=original.body,
            content_type=original.content_type,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=original.message_id,
            correlation_id=original.correlation_id,
            timestamp=int(time.time()),
            headers=headers,
        ),
        routing_key=ROUTING_KEY,
    )


async def handle_message(
    message: aio_pika.IncomingMessage,
    exchange: aio_pika.RobustExchange,
) -> None:
    retry_count = int((message.headers or {}).get("x-retry-count", 0))
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
            await republish_for_retry(exchange, message, retry_count + 1)
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
        exchange = await channel.get_exchange(EXCHANGE_NAME)

        app.state.connection = connection
        app.state.channel = channel
        app.state.consumer_ready = True
        logger.info("RabbitMQ consumer connected, topology declared, starting message loop")

        async with queue.iterator() as iterator:
            async for message in iterator:
                await handle_message(message, exchange)
    except Exception as exc:
        logger.exception("Fatal error in consumer background task: %s", exc)
        app.state.consumer_ready = False
        raise exc


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting event worker service...")
    app.state.consumer_ready = False
    task = asyncio.create_task(consume(app))
    app.state.consumer_task = task
    try:
        yield
    finally:
        logger.info("Shutting down event worker service...")
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

        channel = getattr(app.state, "channel", None)
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
