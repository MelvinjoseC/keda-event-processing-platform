import asyncio
import json
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

from app.logging_config import configure_logging, current_correlation_id

SERVICE_NAME = os.getenv("SERVICE_NAME", "event-publisher")
logger = configure_logging(SERVICE_NAME)
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


class BatchEventsIn(BaseModel):
    events: list[EventIn] = Field(min_length=1, max_length=100)


class BatchEventItem(BaseModel):
    id: str
    type: str
    correlation_id: str


class BatchEventsResponse(BaseModel):
    status: str
    total: int
    accepted: int
    failed: int
    events: list[BatchEventItem]


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
        token = current_correlation_id.set(correlation_id)
        try:
            response = await call_next(request)
        finally:
            current_correlation_id.reset(token)

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


async def _publish_single_event(
    exchange: AbstractExchange,
    event: EventIn,
    fallback_correlation_id: str | None = None,
) -> BatchEventItem:
    event_id = str(uuid.uuid4())
    correlation_id = event.correlation_id or fallback_correlation_id or event_id
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
            detail=f"Event {event_id} size exceeds limit of {MAX_PAYLOAD_BYTES} bytes",
        )

    message = Message(
        body=body_bytes,
        content_type="application/json",
        delivery_mode=DeliveryMode.PERSISTENT,
        message_id=event_id,
        correlation_id=correlation_id,
        timestamp=now,
        headers={"event_type": event.type},
    )

    with PUBLISH_LATENCY.time():
        await exchange.publish(message, routing_key=ROUTING_KEY)

    PUBLISHED_EVENTS.labels(event_type=event.type).inc()
    logger.info(
        "Published event: id=%s, type=%s, correlation_id=%s",
        event_id,
        event.type,
        correlation_id,
    )
    return BatchEventItem(id=event_id, type=event.type, correlation_id=correlation_id)


@app.post("/events", response_model=EventAccepted, status_code=status.HTTP_202_ACCEPTED)
async def publish_event(
    event: EventIn, request: Request, response: Response
) -> EventAccepted:
    exchange = getattr(app.state, "exchange", None)
    if exchange is None:
        PUBLISH_ERRORS.inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="RabbitMQ exchange is not ready",
        )

    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_PAYLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Request size exceeds limit of {MAX_PAYLOAD_BYTES} bytes",
        )

    correlation_id = (
        event.correlation_id
        or getattr(request.state, "correlation_id", None)
        or str(uuid.uuid4())
    )
    response.headers["X-Correlation-ID"] = correlation_id

    try:
        item = await _publish_single_event(
            exchange, event, fallback_correlation_id=correlation_id
        )
    except HTTPException:
        raise
    except Exception as exc:
        PUBLISH_ERRORS.inc()
        logger.exception("Failed to publish event %s to RabbitMQ", event.type)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to publish event",
        ) from exc

    return EventAccepted(
        id=item.id,
        status="accepted",
        queue=QUEUE_NAME,
        correlation_id=item.correlation_id,
    )


@app.post(
    "/events/batch",
    response_model=BatchEventsResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def publish_events_batch(
    batch: BatchEventsIn, request: Request, response: Response
) -> BatchEventsResponse:
    exchange = getattr(app.state, "exchange", None)
    if exchange is None:
        PUBLISH_ERRORS.inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="RabbitMQ exchange is not ready",
        )

    correlation_id = getattr(request.state, "correlation_id", None) or str(uuid.uuid4())
    response.headers["X-Correlation-ID"] = correlation_id

    tasks = [
        _publish_single_event(exchange, ev, fallback_correlation_id=correlation_id)
        for ev in batch.events
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    accepted_items: list[BatchEventItem] = []
    failed_count = 0
    for res in results:
        if isinstance(res, BaseException):
            failed_count += 1
            PUBLISH_ERRORS.inc()
            logger.error("Failed to publish an event in batch: %r", res)
        elif isinstance(res, BatchEventItem):
            accepted_items.append(res)

    if failed_count == len(batch.events):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to publish batch events to RabbitMQ",
        )

    logger.info(
        "Published batch events: total=%d, accepted=%d, failed=%d, correlation_id=%s",
        len(batch.events),
        len(accepted_items),
        failed_count,
        correlation_id,
    )
    return BatchEventsResponse(
        status="accepted" if failed_count == 0 else "partial",
        total=len(batch.events),
        accepted=len(accepted_items),
        failed=failed_count,
        events=accepted_items,
    )


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
