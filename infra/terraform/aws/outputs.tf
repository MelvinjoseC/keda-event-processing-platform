output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API endpoint."
  value       = module.eks.cluster_endpoint
}

output "aws_region" {
  description = "AWS region."
  value       = var.aws_region
}

output "vpc_id" {
  description = "VPC ID."
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs."
  value       = module.vpc.private_subnets
}

output "ecr_repository_urls" {
  description = "ECR repository URLs keyed by service name."
  value       = { for service, repo in aws_ecr_repository.services : service => repo.repository_url }
}

output "github_actions_role_arn" {
  description = "IAM role ARN for GitHub Actions, when enabled."
  value       = var.github_actions_role_enabled ? aws_iam_role.github_actions[0].arn : null
}

output "kubectl_update_command" {
  description = "Command to configure kubectl for this cluster."
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${module.eks.cluster_name}"
}
