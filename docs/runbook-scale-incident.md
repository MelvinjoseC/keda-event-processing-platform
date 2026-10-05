# Runbook: Queue Backlog, DLQ Spike, or Scaling Incident

## Symptoms

- `EventPlatformQueueBacklog` alert fires (> 500 messages in `events` queue for 5m).
- `EventPlatformDeadLetterQueueBacklog` alert fires (> 0 messages in `events.dead` queue for 2m).
- `EventPlatformHighRetryRate` alert fires (> 5 retry republishes/sec).
- RabbitMQ queue depth keeps increasing while worker pods fail to scale or crash.

---

## Triage Phase

### 1. Check KEDA ScaledObject and HPA
```bash
kubectl -n event-platform describe scaledobject event-platform-worker
kubectl -n event-platform get hpa
kubectl -n event-platform get events --sort-by=.lastTimestamp
kubectl -n event-platform logs deploy/event-platform-worker --tail=200
```

### 2. Inspect RabbitMQ Queues and Management UI
```bash
kubectl -n event-platform port-forward svc/event-platform-rabbitmq 15672:15672
```
Open `http://localhost:15672` (default user/pass from secret) and inspect:
- `events`: Active unprocessed backlog.
- `events.retry`: Messages waiting out exponential backoff TTL before re-injection.
- `events.dead`: Unrecoverable messages exhausted after `MAX_RETRIES`.

---

## Root Cause Analysis

| Symptom | Probable Cause | Action |
|---|---|---|
| Pods stuck at `0/0` despite backlog | KEDA operator unable to authenticate with RabbitMQ | Verify `TriggerAuthentication` secret and RabbitMQ credentials. |
| Pods stuck in `Pending` | EKS node pool capacity exhausted or Karpenter/CAS lag | Check node capacity: `kubectl describe nodes`, verify AWS ASG/Karpenter events. |
| Pods crash loop (`CrashLoopBackOff`) | RabbitMQ connection failure or read-only filesystem crash | Verify RabbitMQ service DNS, and ensure `emptyDir` tmpfs is mounted at `/tmp`. |
| Rapidly growing `events.dead` queue | Poison pill messages, downstream API failure, schema incompatibility | Run DLQ dry-run inspection script: `python scripts/replay_dlq.py --dry-run`. |

---

## Emergency Mitigation Procedures

### A. Emergency Scaling (Bypassing KEDA)
If KEDA metrics server is degraded or slow to respond:
```bash
# Temporarily scale worker deployment directly
kubectl -n event-platform scale deploy event-platform-worker --replicas 20
```

### B. Increase Maximum Pod Concurrency
If backlog exceeds default `maxReplicaCount`:
```bash
kubectl -n event-platform patch scaledobject event-platform-worker \
  --type merge \
  -p '{"spec":{"maxReplicaCount":150}}'
```

### C. Dead Letter Queue Disaster Recovery
When bad payloads or downstream outages cause an accumulation of dead letters:

1. **Diagnose failure cause without affecting queue**:
   ```bash
   python scripts/replay_dlq.py --dry-run --limit 50
   ```
   Inspect headers: `x-dead-letter-reason`, `x-dead-letter-error`, `x-failed-worker`.

2. **Deploy hotfix or wait for downstream service recovery**:
   Fix application logic or database availability before replaying to prevent cascading failures.

3. **Replay messages with throttling**:
   ```bash
   # Replay in controlled batches of 100 messages
   python scripts/replay_dlq.py \
     --amqp-url "$RABBITMQ_URL" \
     --limit 100 \
     --filter-reason "<TargetErrorName>"
   ```

4. **Verify recovery**:
   Monitor the Grafana `DLQ Depth` and `Retry Rate` panels to ensure zero re-failures.

---

## Post-Incident Cleanup

Revert manual overrides using Argo CD:
```bash
# Refresh Argo CD app to re-apply GitOps-declared state
argocd app sync event-platform --prune
```
