# AWS Infrastructure

This layer provisions the AWS foundation:

- VPC across three availability zones
- Private EKS cluster with EKS Auto Mode enabled by default
- Core EKS add-ons
- ECR repositories for `publisher` and `worker`
- Optional GitHub Actions OIDC role for image publishing
- VPC endpoints for ECR, CloudWatch Logs, STS, EC2, and S3

## Deploy

```bash
cd infra/terraform/aws
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -out tfplan
terraform apply tfplan
```

Then configure `kubectl` from the output command:

```bash
aws eks update-kubeconfig --region us-east-1 --name event-platform-prod
```

The `bootstrap` Terraform layer installs KEDA, Argo CD, and monitoring after this cluster exists.
