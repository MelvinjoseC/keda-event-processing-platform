import random
import uuid

from locust import HttpUser, between, task


EVENT_TYPES = [
    "order.created",
    "order.paid",
    "shipment.requested",
    "inventory.adjusted",
]


class EventPublisher(HttpUser):
    wait_time = between(0.01, 0.2)

    @task
    def publish_event(self):
        event_id = str(uuid.uuid4())
        payload = {
            "type": random.choice(EVENT_TYPES),
            "correlation_id": event_id,
            "payload": {
                "id": event_id,
                "amount": random.randint(10, 5000),
                "source": "locust",
            },
        }
        self.client.post("/events", json=payload, name="/events")
