import os
import random
import uuid

from locust import HttpUser, between, task

EVENT_TYPES = [
    "order.created",
    "order.paid",
    "shipment.requested",
    "inventory.adjusted",
    "notification.dispatched",
]

# Configurable error injection rate (default ~2%)
ERROR_INJECTION_RATE = float(os.getenv("ERROR_INJECTION_RATE", "0.02"))


class EventPublisher(HttpUser):
    wait_time = between(0.01, 0.2)

    @task(4)
    def publish_single_event(self) -> None:
        """Publish a single event with optional failure injection."""
        event_id = str(uuid.uuid4())
        should_fail = random.random() < ERROR_INJECTION_RATE

        payload = {
            "type": random.choice(EVENT_TYPES),
            "correlation_id": event_id,
            "payload": {
                "id": event_id,
                "amount": random.randint(10, 5000),
                "source": "locust",
                "force_error": should_fail,
            },
        }
        headers = {
            "Content-Type": "application/json",
            "X-Correlation-ID": event_id,
        }
        self.client.post("/events", json=payload, headers=headers, name="/events")

    @task(1)
    def publish_batch_events(self) -> None:
        """Publish a batch of events (5-10 events) to test high-throughput ingestion."""
        batch_size = random.randint(5, 10)
        batch_id = str(uuid.uuid4())
        events = []

        for _ in range(batch_size):
            event_id = str(uuid.uuid4())
            should_fail = random.random() < ERROR_INJECTION_RATE
            events.append(
                {
                    "type": random.choice(EVENT_TYPES),
                    "correlation_id": event_id,
                    "payload": {
                        "id": event_id,
                        "batch_id": batch_id,
                        "amount": random.randint(10, 5000),
                        "source": "locust-batch",
                        "force_error": should_fail,
                    },
                }
            )

        headers = {
            "Content-Type": "application/json",
            "X-Correlation-ID": batch_id,
        }
        self.client.post(
            "/events/batch",
            json={"events": events},
            headers=headers,
            name="/events/batch",
        )
