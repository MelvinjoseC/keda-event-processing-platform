{{- define "event-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "event-platform.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "event-platform.componentName" -}}
{{- printf "%s-%s" (include "event-platform.fullname" .root) .component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "event-platform.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "event-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "event-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ include "event-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "event-platform.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "event-platform.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "event-platform.secretName" -}}
{{- if .Values.secrets.existingName -}}
{{- .Values.secrets.existingName -}}
{{- else -}}
{{- include "event-platform.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "event-platform.rabbitmqHost" -}}
{{- printf "%s-rabbitmq" .Release.Name -}}
{{- end -}}

{{- define "event-platform.rabbitmqUrl" -}}
{{- if .Values.secrets.rabbitmqUrl -}}
{{- .Values.secrets.rabbitmqUrl -}}
{{- else -}}
{{- printf "amqp://%s:%s@%s:5672/" .Values.rabbitmq.auth.username .Values.rabbitmq.auth.password (include "event-platform.rabbitmqHost" .) -}}
{{- end -}}
{{- end -}}
