# Bootstrap Add-ons

This layer installs in-cluster platform add-ons after the EKS cluster exists:

- KEDA
- Argo CD
- kube-prometheus-stack with Prometheus and Grafana

## Deploy

```bash
cd infra/terraform/bootstrap
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -out tfplan
terraform apply tfplan
```

After Argo CD is running, apply the GitOps applications:

```bash
kubectl apply -f deploy/argocd/project.yaml
kubectl apply -f deploy/argocd/event-platform-app.yaml
kubectl apply -f deploy/argocd/monitoring-app.yaml
```
