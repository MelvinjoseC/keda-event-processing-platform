# Production-Ready Event Processing Platform on EKS

This repository is a complete KEDA-backed event processing platform:

- FastAPI publisher and worker microservices
- RabbitMQ queueing with durable queues, retries, and a dead letter queue
- KEDA scale-to-zero for event workers from RabbitMQ queue depth
- Terraform for EKS, VPC, ECR, GitHub OIDC, and platform bootstrap add-ons
- Argo CD GitOps applications
- Prometheus, Grafana, ServiceMonitors, alerts, and dashboard
- Locust and k6 load tests
- Cost comparison report for always-on workers vs KEDA scale-to-zero

## Architecture

```text
clients/load tests
      |
      v
FastAPI publisher ---> RabbitMQ durable queue ---> FastAPI worker deployment
                            ^                         |
                            |                         v
                         KEDA ScaledObject <--- Prometheus metrics
                            |
                            v
                  worker replicas: 0..N
```

KEDA scales only the worker deployment. EKS Auto Mode is enabled by default in Terraform so empty worker capacity can be reclaimed after the deployment scales to zero.

## Local Run

```bash
cp .env.example .env
docker compose up --build
```

Publish a test event:

```bash
curl -X POST http://localhost:8000/events \
  -H "content-type: application/json" \
  -d '{"type":"order.created","payload":{"order_id":"demo-1"}}'
```

RabbitMQ management UI is available at `http://localhost:15672` with `platform/platform` from `.env.example`.

## AWS Deployment

Provision AWS infrastructure:

```bash
cd infra/terraform/aws
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -out tfplan
terraform apply tfplan
```

Install cluster add-ons:

```bash
cd ../bootstrap
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -out tfplan
terraform apply tfplan
```

Create production secrets before syncing the app:

```bash
kubectl create namespace event-platform
kubectl -n event-platform create secret generic event-platform-rabbitmq-auth \
  --from-literal=rabbitmq-password='replace-me' \
  --from-literal=rabbitmq-erlang-cookie='replace-me'
kubectl -n event-platform create secret generic event-platform-rabbitmq-connection \
  --from-literal=RABBITMQ_URL='amqp://platform:replace-me@event-platform-rabbitmq:5672/'
```

Apply Argo CD applications:

```bash
kubectl apply -f deploy/argocd/project.yaml
kubectl apply -f deploy/argocd/event-platform-app.yaml
kubectl apply -f deploy/argocd/monitoring-app.yaml
```

Replace the placeholder repo URL and ECR image URLs in `deploy/argocd/*.yaml` and `deploy/environments/prod/values.yaml` for your GitHub org, AWS account, and region.

## CI/CD

The workflows under `.github/workflows` cover:

- Python tests, Docker builds, Helm linting, and Terraform validation
- ECR multi-arch image publishing
- Terraform plan on infrastructure pull requests
- Manual GitOps promotion by updating `deploy/environments/prod/values.yaml`

Required repository secrets:

- `AWS_GITHUB_ACTIONS_ROLE_ARN` for image publishing
- `AWS_TERRAFORM_PLAN_ROLE_ARN` for Terraform plans

## Load Testing

Locust:

```bash
locust -f tests/load/locustfile.py --host http://localhost:8000
```

k6:

```bash
k6 run -e BASE_URL=http://localhost:8000 tests/load/k6.js
```

Watch scaling in EKS:

```bash
kubectl -n event-platform get scaledobject,hpa,deploy -w
```

## Cost Report

Regenerate the cost comparison:

```bash
python scripts/cost_compare.py --output reports/cost-comparison.md
```

The report isolates the worker compute savings. Control plane, NAT, RabbitMQ, monitoring, storage, and network charges remain baseline platform costs.

## References

- KEDA RabbitMQ scaler: https://keda.sh/docs/2.20/scalers/rabbitmq-queue/
- KEDA Helm install: https://keda.sh/docs/2.20/deploy/
- terraform-aws-eks module: https://registry.terraform.io/modules/terraform-aws-modules/eks/aws/latest
- Argo CD declarative setup: https://argo-cd.readthedocs.io/en/stable/operator-manual/declarative-setup/
- EKS pricing: https://aws.amazon.com/eks/pricing/
- EC2 On-Demand pricing: https://aws.amazon.com/ec2/pricing/on-demand/
