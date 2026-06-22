output "keda_namespace" {
  value = helm_release.keda.namespace
}

output "argocd_namespace" {
  value = helm_release.argocd.namespace
}

output "monitoring_namespace" {
  value = helm_release.monitoring.namespace
}

output "argocd_port_forward" {
  value = "kubectl -n argocd port-forward svc/argo-cd-argocd-server 8080:443"
}

output "grafana_port_forward" {
  value = "kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80"
}
