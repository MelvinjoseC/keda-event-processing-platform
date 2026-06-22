variable "project" {
  description = "Short project name used for AWS resource names."
  type        = string
  default     = "event-platform"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "prod"
}

variable "aws_region" {
  description = "AWS region for the platform."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the EKS VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones to use."
  type        = number
  default     = 3
}

variable "kubernetes_version" {
  description = "EKS Kubernetes minor version."
  type        = string
  default     = "1.35"
}

variable "endpoint_public_access" {
  description = "Whether to enable the public EKS API endpoint."
  type        = bool
  default     = true
}

variable "endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint when enabled."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "single_nat_gateway" {
  description = "Use one NAT gateway instead of one per AZ. Lower cost, lower availability."
  type        = bool
  default     = false
}

variable "enable_eks_auto_mode" {
  description = "Use EKS Auto Mode node pools so empty worker capacity can be reclaimed after KEDA scales pods to zero."
  type        = bool
  default     = true
}

variable "system_node_instance_types" {
  description = "Instance types for the fallback managed node group when EKS Auto Mode is disabled."
  type        = list(string)
  default     = ["m7g.large"]
}

variable "github_actions_role_enabled" {
  description = "Create an IAM role for GitHub Actions ECR publishing."
  type        = bool
  default     = false
}

variable "github_org" {
  description = "GitHub organization or user that owns this repository."
  type        = string
  default     = "your-org"
}

variable "github_repo" {
  description = "GitHub repository name."
  type        = string
  default     = "keda-event-platform"
}

variable "github_oidc_thumbprints" {
  description = "Thumbprints for token.actions.githubusercontent.com."
  type        = list(string)
  default     = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

variable "tags" {
  description = "Additional tags applied to AWS resources."
  type        = map(string)
  default     = {}
}
