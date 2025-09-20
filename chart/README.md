# S3Test Helm Chart

This Helm chart deploys the S3Test application on Kubernetes or OpenShift clusters with automatic platform detection.

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
helm install s3test ./chart

# Install with custom release name
helm install my-s3-api ./chart

# Install in specific namespace
helm install s3test ./chart -n s3test --create-namespace
```

### Installation with Custom Values

```bash
# Install with custom values file
helm install s3test ./chart -f my-values.yaml

# Install with specific parameters
helm install s3test ./chart \
  --set config.storageType=Filesystem \
  --set config.filesystemStorage.enabled=true \
  --set config.authentication.enabled=true
```

## Configuration

Key configuration options:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `platform.type` | Platform type (`auto`, `kubernetes`, `openshift`) | `auto` |
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Container image repository | `s3test` |
| `image.tag` | Container image tag | `Chart.AppVersion` |
| `imageStream.enabled` | Enable ImageStream (OpenShift) | `auto` |
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.port` | Service port | `80` |
| `ingress.enabled` | Enable Ingress (Kubernetes) | `auto` |
| `route.enabled` | Enable Route (OpenShift) | `auto` |
| `config.storageType` | Storage type (`InMemory`, `Filesystem`) | `InMemory` |
| `config.authentication.enabled` | Enable authentication | `false` |

### Storage Configuration

#### In-Memory Storage (Default)

```yaml
config:
  storageType: InMemory
```

#### Filesystem Storage with Persistent Volume

```yaml
config:
  storageType: Filesystem
  filesystemStorage:
    enabled: true
    dataDirectory: /data/s3test/data
    metadataDirectory: /data/s3test/metadata
    persistentVolume:
      enabled: true
      storageClass: standard
      size: 10Gi
```

### Authentication Configuration

```yaml
config:
  authentication:
    enabled: true
    users:
      - accessKeyId: admin
        secretAccessKey: supersecret
        name: admin-user
        bucketPermissions:
          - bucketName: "*"
            permissions: ["*"]
      - accessKeyId: readonly
        secretAccessKey: readonlysecret
        name: readonly-user
        bucketPermissions:
          - bucketName: public-*
            permissions: ["read", "list"]
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
helm install s3test ./chart \
  --set config.storageType=Filesystem \
  --set config.filesystemStorage.enabled=true \
  --set config.filesystemStorage.persistentVolume.enabled=true \
  --set config.filesystemStorage.persistentVolume.size=20Gi
```

### Deploy to Kubernetes with Ingress

```bash
helm install s3test ./chart \
  --set ingress.enabled=true \
  --set ingress.hosts[0].host=s3.mydomain.com \
  --set ingress.className=nginx
```

### Deploy with Authentication and Custom Limits

```bash
helm install s3test ./chart \
  --set config.authentication.enabled=true \
  --set 'config.authentication.users[0].accessKeyId=admin' \
  --set 'config.authentication.users[0].secretAccessKey=secret123' \
  --set 'config.authentication.users[0].name=admin' \
  --set config.limits.maxBuckets=50 \
  --set config.limits.maxObjectSize=1073741824
```

## Testing the Deployment

After installation, test the S3 API:

```bash
# Get the service URL (varies by configuration)
# For NodePort/LoadBalancer:
kubectl get svc s3test

# For Ingress:
kubectl get ingress s3test

# For OpenShift Route:
oc get route s3test

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
kubectl get pods -l app.kubernetes.io/name=s3test

# View logs
kubectl logs -l app.kubernetes.io/name=s3test --tail=100

# Check health endpoint
curl $S3_ENDPOINT/health
```

## Uninstallation

```bash
# Uninstall the release
helm uninstall s3test

# Uninstall and delete namespace
helm uninstall s3test -n s3test
kubectl delete namespace s3test
```

## Troubleshooting

### ImagePullBackOff on OpenShift

If using ImageStream and getting image pull errors:

1. Check the ImageStream:
   ```bash
   oc get imagestream s3test -o yaml
   ```

2. Manually import the image:
   ```bash
   oc import-image s3test:latest --from=docker.io/yourreg/s3test:latest --confirm
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
docker build -t s3test:dev .
kind load docker-image s3test:dev
# or
minikube image load s3test:dev

# Install with local image
helm install s3test ./chart \
  --set image.repository=s3test \
  --set image.tag=dev \
  --set image.pullPolicy=Never
```

### Dry Run

```bash
# Generate manifests without installing
helm install s3test ./chart --dry-run --debug

# Template specific values
helm template s3test ./chart -f values-prod.yaml
```

## License

See LICENSE file in the repository root.