variable "aws_region" {
  description = "AWS region where the EKS cluster exists."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Existing EKS cluster name from the aws Terraform layer."
  type        = string
}

variable "keda_chart_version" {
  description = "KEDA Helm chart version."
  type        = string
  default     = "2.20.1"
}

variable "argocd_chart_version" {
  description = "Argo CD Helm chart version."
  type        = string
  default     = "9.5.22"
}

variable "kube_prometheus_stack_chart_version" {
  description = "kube-prometheus-stack Helm chart version."
  type        = string
  default     = "86.3.2"
}

variable "grafana_admin_password" {
  description = "Initial Grafana admin password."
  type        = string
  sensitive   = true
  default     = "change-me"
}
