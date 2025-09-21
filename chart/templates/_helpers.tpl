{{/*
Expand the name of the chart.
*/}}
{{- define "lamina.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "lamina.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "lamina.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "lamina.labels" -}}
helm.sh/chart: {{ include "lamina.chart" . }}
{{ include "lamina.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "lamina.selectorLabels" -}}
app.kubernetes.io/name: {{ include "lamina.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "lamina.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "lamina.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Detect if we're running on OpenShift
*/}}
{{- define "lamina.isOpenShift" -}}
{{- if eq .Values.platform.type "openshift" }}
{{- true }}
{{- else if eq .Values.platform.type "kubernetes" }}
{{- false }}
{{- else }}
{{- if .Capabilities.APIVersions.Has "route.openshift.io/v1" }}
{{- true }}
{{- else }}
{{- false }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Determine if Ingress should be enabled
*/}}
{{- define "lamina.ingressEnabled" -}}
{{- if eq (toString .Values.ingress.enabled) "auto" }}
{{- if eq (include "lamina.isOpenShift" .) "true" }}
{{- false }}
{{- else }}
{{- true }}
{{- end }}
{{- else }}
{{- .Values.ingress.enabled }}
{{- end }}
{{- end }}

{{/*
Determine if Route should be enabled
*/}}
{{- define "lamina.routeEnabled" -}}
{{- if eq (toString .Values.route.enabled) "auto" }}
{{- if eq (include "lamina.isOpenShift" .) "true" }}
{{- true }}
{{- else }}
{{- false }}
{{- end }}
{{- else }}
{{- .Values.route.enabled }}
{{- end }}
{{- end }}

{{/*
Determine if ImageStream should be enabled
*/}}
{{- define "lamina.imageStreamEnabled" -}}
{{- if eq (toString .Values.imageStream.enabled) "auto" }}
{{- if eq (include "lamina.isOpenShift" .) "true" }}
{{- true }}
{{- else }}
{{- false }}
{{- end }}
{{- else }}
{{- .Values.imageStream.enabled }}
{{- end }}
{{- end }}

{{/*
Get the image reference
*/}}
{{- define "lamina.image" -}}
{{- if eq (toString (include "lamina.imageStreamEnabled" .)) "true" }}
{{- $namespace := .Values.imageStream.namespace | default .Release.Namespace }}
{{- $imageName := .Values.imageStream.name | default (include "lamina.fullname" .) }}
{{- printf "image-registry.openshift-image-registry.svc:5000/%s/%s:latest" $namespace $imageName }}
{{- else }}
{{- $tag := .Values.image.tag | default .Chart.AppVersion }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}
{{- end }}