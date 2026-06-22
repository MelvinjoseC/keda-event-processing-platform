# Runbook: Queue Backlog or Scaling Incident

## Symptoms

- `EventPlatformQueueBacklog` alert fires.
- RabbitMQ queue depth keeps increasing.
- Worker deployment does not scale from zero or does not scale high enough.

## Triage

```bash
kubectl -n event-platform describe scaledobject event-platform-worker
kubectl -n event-platform get hpa
kubectl -n event-platform get events --sort-by=.lastTimestamp
kubectl -n event-platform logs deploy/event-platform-worker --tail=200
```

Check RabbitMQ:

```bash
kubectl -n event-platform port-forward svc/event-platform-rabbitmq 15672:15672
```

Open `http://localhost:15672` and inspect the `events` and `events.dead` queues.

## Likely Causes

- RabbitMQ credentials secret does not match the broker credentials.
- KEDA is not installed or its operator cannot read the `TriggerAuthentication`.
- Worker image is failing readiness due to RabbitMQ connection issues.
- `maxReplicaCount` is too low for the current backlog.
- Worker processing is slow or throwing errors, causing retries and dead letters.
- Node capacity is not being provisioned fast enough after KEDA creates pending pods.

## Mitigation

Temporarily raise max replicas:

```bash
kubectl -n event-platform patch scaledobject event-platform-worker \
  --type merge \
  -p '{"spec":{"maxReplicaCount":150}}'
```

Scale a fixed worker pool while debugging KEDA:

```bash
kubectl -n event-platform scale deploy event-platform-worker --replicas 5
```

After the queue drains, revert through GitOps by updating the Helm values and letting Argo CD sync.
