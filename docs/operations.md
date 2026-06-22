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

## Watch Scale-to-Zero

```bash
kubectl -n event-platform get deploy event-platform-worker -w
kubectl -n event-platform get hpa -w
```

Expected behavior:

- Idle queue: worker deployment reaches `0/0`.
- Queue backlog appears: worker deployment scales above zero.
- Queue drains and cooldown elapses: worker deployment returns to zero.

## Grafana

```bash
kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
```

Open `http://localhost:3000` and search for the `Event Platform` dashboard.

## Argo CD

```bash
kubectl -n argocd port-forward svc/argo-cd-argocd-server 8080:443
```

Check application health:

```bash
kubectl -n argocd get applications
kubectl -n argocd describe application event-platform
```
