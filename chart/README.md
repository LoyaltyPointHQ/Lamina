# Lamina Helm Chart

This Helm chart deploys the Lamina application on Kubernetes or OpenShift clusters with automatic platform detection.

## Features

- **Automatic Platform Detection**: Automatically detects whether it's being deployed to Kubernetes or OpenShift
- **OpenShift Support**:
  - Creates Route instead of Ingress when on OpenShift
  - Supports ImageStream for OpenShift image management
  - Configures appropriate security contexts for OpenShift
- **Kubernetes Support**:
  - Standard Ingress resource for external access
  - Support for multiple Ingress controllers
- **Storage Options**:
  - In-memory storage (default)
  - Filesystem storage with optional persistent volumes
- **Authentication**: Optional AWS Signature V4 authentication
- **Auto-scaling**: HorizontalPodAutoscaler support
- **Monitoring**: Built-in health checks and readiness probes

## Prerequisites

- Kubernetes 1.19+ or OpenShift 4.x+
- Helm 3.x
- kubectl or oc CLI configured to access your cluster

## Installation

### Basic Installation

```bash
# Install with default values
helm install lamina ./chart

# Install with custom release name
helm install my-s3-api ./chart

# Install in specific namespace
helm install lamina ./chart -n lamina --create-namespace
```

### Installation with Custom Values

```bash
# Install with custom values file
helm install lamina ./chart -f my-values.yaml

# Install with specific parameters
helm install lamina ./chart \
  --set config.StorageType=Filesystem \
  --set persistentVolume.enabled=true \
  --set config.Authentication.Enabled=true
```

## Configuration

Key configuration options:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `platform.type` | Platform type (`auto`, `kubernetes`, `openshift`) | `auto` |
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Container image repository | `ghcr.io/loyaltypointhq/lamina` |
| `image.tag` | Container image tag | `Chart.AppVersion` |
| `imageStream.enabled` | Enable ImageStream (OpenShift) | `auto` |
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.port` | Service port | `80` |
| `ingress.enabled` | Enable Ingress (Kubernetes) | `false` |
| `route.enabled` | Enable Route (OpenShift) | `false` |
| `persistentVolume.enabled` | Enable persistent volume for data | `false` |
| `metadataPersistentVolume.enabled` | Enable persistent volume for metadata | `false` |
| `config` | Application configuration (JSON object) | `{}` |

### Storage Configuration

#### In-Memory Storage (Default)

```yaml
config:
  StorageType: InMemory
```

#### Filesystem Storage with Persistent Volume

```yaml
config:
  StorageType: Filesystem
  FilesystemStorage:
    DataDirectory: /data
    MetadataDirectory: /metadata
    MetadataMode: SeparateDirectory

persistentVolume:
  enabled: true
  storageClass: standard
  size: 10Gi

metadataPersistentVolume:
  enabled: true
  storageClass: standard
  size: 5Gi
```

#### Filesystem Storage with Single Volume (Inline Metadata)

```yaml
config:
  StorageType: Filesystem
  FilesystemStorage:
    DataDirectory: /data
    MetadataMode: Inline

persistentVolume:
  enabled: true
  storageClass: standard
  size: 10Gi

sameVolumeForDataAndMeta: true
```

### Authentication Configuration

```yaml
config:
  Authentication:
    Enabled: true
    Users:
      - AccessKeyId: admin
        SecretAccessKey: supersecret
        Name: admin-user
        BucketPermissions:
          - BucketName: "*"
            Permissions: ["*"]
      - AccessKeyId: readonly
        SecretAccessKey: readonlysecret
        Name: readonly-user
        BucketPermissions:
          - BucketName: public-*
            Permissions: ["read", "list"]
```

### Platform-Specific Configuration

#### Force Kubernetes Mode

```yaml
platform:
  type: kubernetes
ingress:
  enabled: true
  className: nginx
  hosts:
    - host: s3api.example.com
      paths:
        - path: /
          pathType: Prefix
```

#### Force OpenShift Mode

```yaml
platform:
  type: openshift
route:
  enabled: true
  host: s3api.apps.openshift.example.com
  tls:
    enabled: true
    termination: edge
imageStream:
  enabled: true
```

## Usage Examples

### Deploy to OpenShift with Persistent Storage

```bash
helm install lamina ./chart \
  --set config.StorageType=Filesystem \
  --set persistentVolume.enabled=true \
  --set persistentVolume.size=20Gi \
  --set metadataPersistentVolume.enabled=true \
  --set metadataPersistentVolume.size=5Gi
```

### Deploy to Kubernetes with Ingress

```bash
helm install lamina ./chart \
  --set ingress.enabled=true \
  --set ingress.hosts[0].host=s3.mydomain.com \
  --set ingress.className=nginx
```

### Deploy with Authentication and Custom Limits

```bash
helm install lamina ./chart \
  --set config.Authentication.Enabled=true \
  --set 'config.Authentication.Users[0].AccessKeyId=admin' \
  --set 'config.Authentication.Users[0].SecretAccessKey=secret123' \
  --set 'config.Authentication.Users[0].Name=admin'
```

## Testing the Deployment

After installation, test the S3 API:

```bash
# Get the service URL (varies by configuration)
# For NodePort/LoadBalancer:
kubectl get svc lamina

# For Ingress:
kubectl get ingress lamina

# For OpenShift Route:
oc get route lamina

# Test with curl
export S3_ENDPOINT=<your-endpoint-url>
curl -X PUT $S3_ENDPOINT/test-bucket

# Test with AWS CLI
aws s3 --endpoint-url $S3_ENDPOINT ls
aws s3 --endpoint-url $S3_ENDPOINT mb s3://test-bucket
```

## Monitoring

```bash
# Check pod status
kubectl get pods -l app.kubernetes.io/name=lamina

# View logs
kubectl logs -l app.kubernetes.io/name=lamina --tail=100

# Check health endpoint
curl $S3_ENDPOINT/health
```

## Uninstallation

```bash
# Uninstall the release
helm uninstall lamina

# Uninstall and delete namespace
helm uninstall lamina -n lamina
kubectl delete namespace lamina
```

## Troubleshooting

### ImagePullBackOff on OpenShift

If using ImageStream and getting image pull errors:

1. Check the ImageStream:
   ```bash
   oc get imagestream lamina -o yaml
   ```

2. Manually import the image:
   ```bash
   oc import-image lamina:latest --from=docker.io/yourreg/lamina:latest --confirm
   ```

### Permission Denied on OpenShift

OpenShift has stricter security policies. Ensure the security context is appropriate:

```yaml
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 1001
```

### PVC Not Binding

Check available storage classes:

```bash
kubectl get storageclass
```

Then update the chart values:

```yaml
config:
  filesystemStorage:
    persistentVolume:
      storageClass: your-storage-class
```

## Development

### Testing Locally with Kind/Minikube

```bash
# Start local cluster
kind create cluster
# or
minikube start

# Build and load local image
docker build -t lamina:dev .
kind load docker-image lamina:dev
# or
minikube image load lamina:dev

# Install with local image
helm install lamina ./chart \
  --set image.repository=lamina \
  --set image.tag=dev \
  --set image.pullPolicy=Never
```

### Dry Run

```bash
# Generate manifests without installing
helm install lamina ./chart --dry-run --debug

# Template specific values
helm template lamina ./chart -f values-prod.yaml
```

## License

See LICENSE file in the repository root.