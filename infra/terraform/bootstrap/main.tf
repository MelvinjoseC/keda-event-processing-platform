resource "helm_release" "keda" {
  name             = "keda"
  namespace        = "keda"
  create_namespace = true
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = var.keda_chart_version
  wait             = true
  timeout          = 600
}

resource "helm_release" "argocd" {
  name             = "argo-cd"
  namespace        = "argocd"
  create_namespace = true
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = var.argocd_chart_version
  wait             = true
  timeout          = 900

  values = [
    yamlencode({
      controller = {
        replicas = 2
      }
      repoServer = {
        autoscaling = {
          enabled     = true
          minReplicas = 2
          maxReplicas = 5
        }
      }
      applicationSet = {
        replicas = 2
      }
      server = {
        autoscaling = {
          enabled     = true
          minReplicas = 2
          maxReplicas = 5
        }
        service = {
          type = "ClusterIP"
        }
      }
    })
  ]
}

resource "helm_release" "monitoring" {
  name             = "kube-prometheus-stack"
  namespace        = "monitoring"
  create_namespace = true
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = var.kube_prometheus_stack_chart_version
  wait             = true
  timeout          = 900

  values = [
    yamlencode({
      grafana = {
        persistence = {
          enabled = true
          size    = "10Gi"
        }
        defaultDashboardsTimezone = "utc"
      }
      prometheus = {
        prometheusSpec = {
          retention                               = "15d"
          serviceMonitorSelectorNilUsesHelmValues = false
          podMonitorSelectorNilUsesHelmValues     = false
          ruleSelectorNilUsesHelmValues           = false
          storageSpec = {
            volumeClaimTemplate = {
              spec = {
                accessModes = ["ReadWriteOnce"]
                resources = {
                  requests = {
                    storage = "50Gi"
                  }
                }
              }
            }
          }
        }
      }
    })
  ]

  set_sensitive {
    name  = "grafana.adminPassword"
    value = var.grafana_admin_password
  }
}

resource "helm_release" "external_secrets" {
  count            = var.enable_external_secrets ? 1 : 0
  name             = "external-secrets"
  namespace        = "external-secrets"
  create_namespace = true
  repository       = "https://charts.external-secrets.io"
  chart            = "external-secrets"
  version          = var.external_secrets_chart_version
  wait             = true
  timeout          = 600

  values = [
    yamlencode({
      installCRDs = true
      webhook = {
        port = 9443
      }
    })
  ]
}
