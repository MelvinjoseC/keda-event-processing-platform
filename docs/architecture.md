# Architecture

## Runtime Flow

1. Clients submit events to the publisher over HTTP.
2. The publisher validates the event and writes a persistent RabbitMQ message.
3. Worker pods consume the queue, process messages, retry transient failures, and reject exhausted messages to the dead letter queue.
4. KEDA watches RabbitMQ queue depth and controls the worker deployment replica count.
5. Prometheus scrapes service metrics, RabbitMQ metrics, KEDA metrics, and Kubernetes state metrics.
6. Grafana visualizes throughput, queue depth, worker replicas, latency, retries, and failures.

## Scale-to-Zero Boundary

The publisher and RabbitMQ stay online. The worker deployment is allowed to run from `0` to `maxReplicaCount`.

KEDA creates the Kubernetes HPA from the `ScaledObject`. It scales from zero when the RabbitMQ queue has at least `activationValue` messages, and scales down after `cooldownPeriod` once the queue is drained.

## Infrastructure Layers

`infra/terraform/aws` creates AWS resources:

- VPC
- EKS
- ECR
- VPC endpoints
- Optional GitHub OIDC role

`infra/terraform/bootstrap` installs cluster add-ons:

- KEDA
- Argo CD
- kube-prometheus-stack

Argo CD owns the application chart and monitoring overlays after bootstrap.
