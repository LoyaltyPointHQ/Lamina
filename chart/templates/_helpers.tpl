{{/*
Expand the name of the chart.
*/}}
{{- define "s3test.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "s3test.fullname" -}}
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
{{- define "s3test.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "s3test.labels" -}}
helm.sh/chart: {{ include "s3test.chart" . }}
{{ include "s3test.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "s3test.selectorLabels" -}}
app.kubernetes.io/name: {{ include "s3test.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "s3test.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "s3test.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Detect if we're running on OpenShift
*/}}
{{- define "s3test.isOpenShift" -}}
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
{{- define "s3test.ingressEnabled" -}}
{{- if eq .Values.ingress.enabled "auto" }}
{{- if eq (include "s3test.isOpenShift" .) "true" }}
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
{{- define "s3test.routeEnabled" -}}
{{- if eq .Values.route.enabled "auto" }}
{{- if eq (include "s3test.isOpenShift" .) "true" }}
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
{{- define "s3test.imageStreamEnabled" -}}
{{- if eq .Values.imageStream.enabled "auto" }}
{{- if eq (include "s3test.isOpenShift" .) "true" }}
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
{{- define "s3test.image" -}}
{{- if eq (include "s3test.imageStreamEnabled" .) "true" }}
{{- $namespace := .Values.imageStream.namespace | default .Release.Namespace }}
{{- printf "%s:latest" (include "s3test.fullname" .) }}
{{- else }}
{{- $tag := .Values.image.tag | default .Chart.AppVersion }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}
{{- end }}