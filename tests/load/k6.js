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
  "inventory.adjusted"
];

export default function () {
  const id = uuidv4();
  const response = http.post(
    `${__ENV.BASE_URL || "http://localhost:8000"}/events`,
    JSON.stringify({
      type: randomItem(eventTypes),
      correlation_id: id,
      payload: {
        id,
        source: "k6",
        amount: Math.floor(Math.random() * 5000)
      }
    }),
    {
      headers: {
        "content-type": "application/json"
      }
    }
  );

  check(response, {
    "accepted": (r) => r.status === 202
  });
  sleep(0.1);
}
