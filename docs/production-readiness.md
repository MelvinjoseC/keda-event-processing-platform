# Production Readiness

## Included

- Private-subnet EKS workers
- EKS control plane audit logging
- ECR scan-on-push and immutable image tags
- Durable RabbitMQ queue and dead letter queue
- Health and readiness probes
- CPU and memory requests/limits
- PodDisruptionBudget for the publisher
- KEDA fallback replicas if scaler metrics fail
- Prometheus alerts and Grafana dashboard
- GitHub OIDC path for keyless AWS authentication
- GitOps promotion workflow
- Load tests and cost model

## Required Before Real Production

- Replace all placeholder GitHub repo URLs and ECR repository URLs.
- Restrict `endpoint_public_access_cidrs` in `infra/terraform/aws/terraform.tfvars`.
- Replace demo RabbitMQ credentials with managed secrets.
- Decide whether RabbitMQ should remain in-cluster or move to Amazon MQ for RabbitMQ.
- Configure ingress, TLS certificates, and DNS for the publisher API.
- Add backup and restore procedures for RabbitMQ persistent volumes.
- Add policy controls such as Kyverno, Gatekeeper, or Pod Security Admission labels.
- Review region-specific pricing before using the cost report for financial commitments.
- Define SLOs for queue latency, processing latency, and dead letter rate.
