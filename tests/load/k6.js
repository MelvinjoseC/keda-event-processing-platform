import http from "k6/http";
import { check, sleep } from "k6";
import { randomItem, uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

export const options = {
  scenarios: {
    burst_then_idle: {
      executor: "ramping-vus",
      stages: [
        { duration: "2m", target: 50 },
        { duration: "5m", target: 200 },
        { duration: "2m", target: 0 }
      ],
      gracefulRampDown: "30s"
    }
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"]
  }
};

const eventTypes = [
  "order.created",
  "order.paid",
  "shipment.requested",
  "inventory.adjusted",
  "notification.dispatched"
];

const errorInjectionRate = parseFloat(__ENV.ERROR_INJECTION_RATE || "0.02");

export default function () {
  const isBatch = Math.random() < 0.25; // 25% batch requests, 75% single requests
  const baseUrl = __ENV.BASE_URL || "http://localhost:8000";

  if (isBatch) {
    const batchSize = Math.floor(Math.random() * 6) + 5; // 5 to 10 items
    const batchId = uuidv4();
    const events = [];

    for (let i = 0; i < batchSize; i++) {
      const id = uuidv4();
      const shouldFail = Math.random() < errorInjectionRate;
      events.push({
        type: randomItem(eventTypes),
        correlation_id: id,
        payload: {
          id: id,
          batch_id: batchId,
          source: "k6-batch",
          amount: Math.floor(Math.random() * 5000),
          force_error: shouldFail
        }
      });
    }

    const batchRes = http.post(
      `${baseUrl}/events/batch`,
      JSON.stringify({ events: events }),
      {
        headers: {
          "content-type": "application/json",
          "x-correlation-id": batchId
        }
      }
    );

    check(batchRes, {
      "batch accepted (202)": (r) => r.status === 202,
      "batch count matches": (r) => {
        try {
          const body = JSON.parse(r.body);
          return body.published === batchSize;
        } catch (_) {
          return false;
        }
      }
    });
  } else {
    const id = uuidv4();
    const shouldFail = Math.random() < errorInjectionRate;

    const res = http.post(
      `${baseUrl}/events`,
      JSON.stringify({
        type: randomItem(eventTypes),
        correlation_id: id,
        payload: {
          id: id,
          source: "k6-single",
          amount: Math.floor(Math.random() * 5000),
          force_error: shouldFail
        }
      }),
      {
        headers: {
          "content-type": "application/json",
          "x-correlation-id": id
        }
      }
    );

    check(res, {
      "single accepted (202)": (r) => r.status === 202
    });
  }

  sleep(0.1);
}
