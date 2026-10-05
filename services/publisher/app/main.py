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
from fastapi import FastAPI, HTTPException, Request, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

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
PUBLISHER_CONFIRMS = os.getenv("PUBLISHER_CONFIRMS", "true").lower() in (
    "true",
    "1",
    "yes",
)
MAX_PAYLOAD_BYTES = int(os.getenv("MAX_PAYLOAD_BYTES", "262144"))  # 256 KiB default

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
    channel = await connection.channel(publisher_confirms=PUBLISHER_CONFIRMS)
    await channel.set_qos(prefetch_count=10)
    exchange = await declare_topology(channel)

    app.state.connection = connection
    app.state.channel = channel
    app.state.exchange = exchange
    logger.info(
        "Connected to RabbitMQ (publisher_confirms=%s) and declared event topology",
        PUBLISHER_CONFIRMS,
    )

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


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        correlation_id = (
            request.headers.get("X-Correlation-ID")
            or request.headers.get("X-Request-ID")
            or str(uuid.uuid4())
        )
        request.state.correlation_id = correlation_id
        response = await call_next(request)
        if "X-Correlation-ID" not in response.headers:
            response.headers["X-Correlation-ID"] = correlation_id
        return response


app = FastAPI(
    title="Event Publisher",
    version="0.1.0",
    docs_url="/docs",
    redoc_url=None,
    lifespan=lifespan,
)
app.add_middleware(CorrelationIdMiddleware)


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
async def publish_event(
    event: EventIn, request: Request, response: Response
) -> EventAccepted:
    event_id = str(uuid.uuid4())
    correlation_id = (
        event.correlation_id
        or getattr(request.state, "correlation_id", None)
        or event_id
    )
    response.headers["X-Correlation-ID"] = correlation_id
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_PAYLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Request size exceeds limit of {MAX_PAYLOAD_BYTES} bytes",
        )

    now = int(time.time())
    body: dict[str, Any] = {
        "id": event_id,
        "type": event.type,
        "payload": event.payload,
        "correlation_id": correlation_id,
        "published_at": now,
    }

    body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
    if len(body_bytes) > MAX_PAYLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Serialized event size exceeds limit of {MAX_PAYLOAD_BYTES} bytes",
        )

    message = Message(
        body=body_bytes,
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
