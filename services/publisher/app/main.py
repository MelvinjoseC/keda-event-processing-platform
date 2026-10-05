import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import AbstractChannel, AbstractExchange
from fastapi import FastAPI, HTTPException, status
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field
from starlette.responses import Response

SERVICE_NAME = os.getenv("SERVICE_NAME", "event-publisher")

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

PUBLISHED_EVENTS = Counter(
    "publisher_events_published_total",
    "Events accepted and published to RabbitMQ",
    ["event_type"],
)
PUBLISH_ERRORS = Counter(
    "publisher_publish_errors_total",
    "Failed attempts to publish events to RabbitMQ",
)
PUBLISH_LATENCY = Histogram(
    "publisher_publish_seconds",
    "Time spent publishing events to RabbitMQ",
)


class EventIn(BaseModel):
    type: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    payload: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = Field(default=None, max_length=128)


class EventAccepted(BaseModel):
    id: str
    status: str
    queue: str
    correlation_id: str | None


async def declare_topology(channel: AbstractChannel) -> AbstractExchange:
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
    return exchange


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting event publisher service...")
    connection = await aio_pika.connect_robust(
        RABBITMQ_URL,
        client_properties={"connection_name": SERVICE_NAME},
    )
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)
    exchange = await declare_topology(channel)

    app.state.connection = connection
    app.state.channel = channel
    app.state.exchange = exchange
    logger.info("Connected to RabbitMQ and declared event topology")

    try:
        yield
    except Exception as exc:
        logger.exception("Exception occurred during service runtime: %s", exc)
        raise
    finally:
        logger.info("Shutting down event publisher service...")
        await channel.close()
        await connection.close()
        logger.info("RabbitMQ connections closed successfully")


app = FastAPI(
    title="Event Publisher",
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
    connection = getattr(app.state, "connection", None)
    if connection is None or connection.is_closed:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    return {"status": "ready"}


@app.post("/events", response_model=EventAccepted, status_code=status.HTTP_202_ACCEPTED)
async def publish_event(event: EventIn) -> EventAccepted:
    event_id = str(uuid.uuid4())
    now = int(time.time())
    body: dict[str, Any] = {
        "id": event_id,
        "type": event.type,
        "payload": event.payload,
        "correlation_id": event.correlation_id or event_id,
        "published_at": now,
    }

    message = Message(
        body=json.dumps(body, separators=(",", ":")).encode("utf-8"),
        content_type="application/json",
        delivery_mode=DeliveryMode.PERSISTENT,
        message_id=event_id,
        correlation_id=body["correlation_id"],
        timestamp=now,
        headers={"event_type": event.type},
    )

    exchange = getattr(app.state, "exchange", None)
    if exchange is None:
        PUBLISH_ERRORS.inc()
        logger.error(
            "Failed to publish event %s: RabbitMQ exchange is not ready", event_id
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="RabbitMQ exchange is not ready",
        )

    try:
        with PUBLISH_LATENCY.time():
            await exchange.publish(message, routing_key=ROUTING_KEY)
    except Exception as exc:
        PUBLISH_ERRORS.inc()
        logger.exception(
            "Failed to publish event %s of type %s to RabbitMQ", event_id, event.type
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to publish event",
        ) from exc

    PUBLISHED_EVENTS.labels(event_type=event.type).inc()
    logger.info(
        "Published event: id=%s, type=%s, correlation_id=%s",
        event_id,
        event.type,
        body["correlation_id"],
    )
    return EventAccepted(
        id=event_id,
        status="accepted",
        queue=QUEUE_NAME,
        correlation_id=body["correlation_id"],
    )


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
