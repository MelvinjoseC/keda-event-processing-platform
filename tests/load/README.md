# Load Tests

Run against a local `docker compose up` stack:

```bash
locust -f tests/load/locustfile.py --host http://localhost:8000
```

Or with k6:

```bash
k6 run -e BASE_URL=http://localhost:8000 tests/load/k6.js
```

Run against EKS by port-forwarding or targeting the ingress/load balancer for the publisher service. During the burst phase, watch KEDA move the worker deployment from zero replicas to active workers:

```bash
kubectl -n event-platform get scaledobject,hpa,deploy -w
```
