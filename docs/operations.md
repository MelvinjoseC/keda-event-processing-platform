# Operations

## Common Checks

```bash
kubectl -n event-platform get pods,svc,deploy,scaledobject,hpa
kubectl -n event-platform describe scaledobject event-platform-worker
kubectl -n event-platform logs deploy/event-platform-publisher
kubectl -n event-platform logs deploy/event-platform-worker
```

## Publish a Synthetic Event

```bash
kubectl -n event-platform port-forward svc/event-platform-publisher 8000:80
curl -X POST http://localhost:8000/events \
  -H "content-type: application/json" \
  -d '{"type":"smoke.test","payload":{"source":"runbook"}}'
```

### High-Throughput Batch Publishing

```bash
curl -X POST http://localhost:8000/events/batch \
  -H "content-type: application/json" \
  -H "x-correlation-id: runbook-batch-001" \
  -d '{
    "events": [
      {"type": "order.created", "payload": {"amount": 100}},
      {"type": "order.paid", "payload": {"amount": 100}}
    ]
  }'
```

## Watch Scale-to-Zero

```bash
kubectl -n event-platform get deploy event-platform-worker -w
kubectl -n event-platform get hpa -w
```

Expected behavior:

- Idle queue: worker deployment reaches `0/0`.
- Queue backlog appears: worker deployment scales above zero.
- Queue drains and cooldown elapses: worker deployment returns to zero.

## Dead Letter Queue (DLQ) Operations

When worker tasks fail continuously beyond `MAX_RETRIES` (3), messages are automatically routed to the dead-letter exchange `events.dead` with failure metadata (`x-dead-letter-reason`, `x-dead-letter-error`, `x-failed-worker`, `x-failed-at`, `x-retry-count`).

### Inspect DLQ Backlog (Dry-Run Mode)

Port-forward RabbitMQ or run directly within the cluster:
```bash
kubectl -n event-platform port-forward svc/event-platform-rabbitmq 5672:5672
```

Inspect failed messages and diagnostic failure traces without modifying queue state:
```bash
python scripts/replay_dlq.py \
  --amqp-url "amqp://guest:guest@localhost:5672/" \
  --dlq-queue events.dead \
  --dry-run \
  --limit 50
```

### Replay Filtered DLQ Messages

Once downstream fixes or service migrations are deployed, republish dead-lettered messages back to the primary `events` exchange:

```bash
# Replay only messages that failed due to a specific transient error (e.g., TimeoutError)
python scripts/replay_dlq.py \
  --amqp-url "amqp://guest:guest@localhost:5672/" \
  --dlq-queue events.dead \
  --target-exchange events \
  --target-routing-key events.order \
  --filter-reason TimeoutError \
  --limit 100
```

Replayed messages automatically have `x-retry-count` reset to 0, with `x-replayed-at` and `x-replayed-by` metadata stamped.

## Grafana Dashboards & Metrics

```bash
kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
```

Open `http://localhost:3000` and search for the `KEDA Event Processing Platform` dashboard:
- **Queue Depths**: Tracks `events`, `events.retry`, and `events.dead`.
- **Worker Concurrency**: Inflight tasks per pod and active consumer channels.
- **Latency Histograms**: P95/P99 publish and worker execution durations.
- **Alert Status**: DLQ backlog and high retry rates.

## Argo CD GitOps Verification

```bash
kubectl -n argocd port-forward svc/argo-cd-argocd-server 8080:443
```

Check application synchronization:

```bash
kubectl -n argocd get applications
kubectl -n argocd describe application event-platform
```
