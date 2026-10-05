# Load Testing Suite

Comprehensive load and resilience testing suite utilizing Locust and k6 to benchmark event ingestion, autoscaling dynamics, retry queues, and dead-letter routing.

## Overview

The load testing suite verifies:
- Single event ingestion throughput (`POST /events`)
- High-throughput batch event ingestion (`POST /events/batch`)
- Distributed request tracing via `X-Correlation-ID` propagation
- Error handling and DLQ routing via configurable failure injection (`ERROR_INJECTION_RATE`)
- KEDA horizontal pod autoscaling under burst workloads (scaling worker pods from `0` to max replicas)

---

## Running with Locust

### Interactive Web UI
Run Locust against the local Docker Compose or Kubernetes publisher service:
```bash
locust -f tests/load/locustfile.py --host http://localhost:8000
```
Then navigate to `http://localhost:8089` to configure user count and spawn rate.

### Headless Execution (Automated / CI)
Run a 3-minute headless test with 100 users and 2% failure injection:
```bash
ERROR_INJECTION_RATE=0.02 locust -f tests/load/locustfile.py \
  --headless \
  --users 100 \
  --spawn-rate 10 \
  --run-time 3m \
  --host http://localhost:8000
```

---

## Running with k6

Execute the staged ramping load test:
```bash
k6 run -e BASE_URL=http://localhost:8000 -e ERROR_INJECTION_RATE=0.03 tests/load/k6.js
```

### Stages Profile:
- **Ramp-up (2 min)**: 0 -> 50 VUs
- **Peak Burst (5 min)**: 50 -> 200 VUs
- **Ramp-down (2 min)**: 200 -> 0 VUs
- **Graceful cooldown**: 30s

---

## Monitoring Autoscaling & DLQ Behavior

While running load tests, observe KEDA autoscaling and RabbitMQ queues in real time:

1. **Watch Kubernetes Pod Scaling**:
   ```bash
   kubectl -n event-platform get scaledobject,hpa,deploy -w
   ```
2. **Inspect RabbitMQ Queue Backlogs**:
   - `events`: Active event queue being consumed by workers.
   - `events.retry`: Exponential backoff queue for retrying events.
   - `events.dead`: Dead-letter queue containing unrecoverable events with diagnostic headers (`x-dead-letter-reason`, `x-dead-letter-error`).
3. **Grafana Dashboards**:
   - Navigate to Grafana at `http://localhost:3000` (or the cluster ingress) and open the **KEDA Event Processing Platform** dashboard to observe DLQ depth, retry rates, and consumer inflight messages.
