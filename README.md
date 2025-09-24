# Lamina

**Lamina** is a lightweight, high-performance S3-compatible storage gateway that exposes filesystem directories as S3 buckets. Built with .NET 9.0, Lamina provides a standards-compliant Amazon S3 REST API interface while storing data directly on your local filesystem or network storage.

[![Docker Image](https://img.shields.io/badge/docker-ghcr.io%2Floyaltypointhq%2Flamina-blue)](https://github.com/LoyaltyPointHQ/Lamina/pkgs/container/lamina)
[![GitHub Repository](https://img.shields.io/badge/github-LoyaltyPointHQ%2FLamina-blue)](https://github.com/LoyaltyPointHQ/Lamina)

## 🎯 Key Features

- **Full S3 API Compatibility**: Implements the official Amazon S3 REST API specification with strict compliance
- **High-Performance Listing**: Optimized code paths for delimiter-based listing, but maintaining compatibility with other listing queries (albeit slow)
- **Metadata Storage**: Three metadata modes - separate directories, inline storage, or POSIX extended attributes (Linux/macOS)
- **Network Filesystem Ready**: Special support for CIFS and NFS with retry logic and atomic operations
- **Distributed Locking**: Redis-based distributed locking for safe multi-instance deployments
- **Background Services**: Automatic cleanup of temporary files, orphaned metadata, and stale multipart uploads

## 🚀 Quick Start

### Using Docker

```bash
# Run with in-memory storage (development)
docker run -p 8080:8080 ghcr.io/loyaltypointhq/lamina:latest

# Run with filesystem storage
docker run -p 8080:8080 \
  -v /your/data/path:/app/data \
  -e StorageType=Filesystem \
  -e FilesystemStorage__DataDirectory=/app/data \
  ghcr.io/loyaltypointhq/lamina:latest
```

### Local Development

```bash
# Clone the repository
git clone https://github.com/LoyaltyPointHQ/Lamina.git
cd Lamina

# Build and run
dotnet build
dotnet run --project Lamina/Lamina.csproj
```

The API will be available at `http://localhost:5214` (or `https://localhost:7179` for HTTPS).

### Using Helm (Kubernetes/OpenShift)

Deploy to Kubernetes or OpenShift using the included Helm chart with automatic platform detection:

```bash
# Basic installation with in-memory storage
helm install lamina ./chart

# Install with persistent filesystem storage
helm install lamina ./chart \
  --set config.StorageType=Filesystem \
  --set persistentVolume.enabled=true \
  --set persistentVolume.size=20Gi

# Install with authentication enabled
helm install lamina ./chart \
  --set config.Authentication.Enabled=true \
  --set config.Authentication.Users[0].AccessKeyId=admin \
  --set config.Authentication.Users[0].SecretAccessKey=secret123
```

**Platform-Specific Features:**
- **Kubernetes**: Automatic Ingress configuration for external access
- **OpenShift**: Route creation with TLS termination and ImageStream support
- **Auto-Detection**: Automatically detects platform and configures appropriate resources (i.e. Route instead of Ingress)

For detailed Helm chart configuration, see [`chart/README.md`](chart/README.md).

## 🏗️ Architecture

Lamina uses a **data-first architecture** where:
- **Data is the source of truth** - object existence is determined by data presence
- **Metadata is optional** - automatically generated when missing
- **Content-Type detection** - intelligent MIME type detection based on file extensions
- **Optimized storage** - metadata only stored when it differs from defaults

### Filesystem Storage
- Production-ready with multiple metadata modes
- Supports local and network filesystems (CIFS, NFS)
- Three metadata storage options:

**1. Separate Directory Mode** (default)
```
/data/bucket/object.txt          # Object data
/metadata/bucket/object.txt.json # Object metadata
```

**2. Inline Mode**
```
/data/bucket/object.txt                    # Object data
/data/bucket/.lamina-meta/object.txt.json  # Object metadata
```

**3. Extended Attributes Mode** (Linux/macOS)
```
/data/bucket/object.txt  # Object data + metadata as xattrs
```

## 📊 S3 API Compatibility

Lamina implements comprehensive S3 API compatibility. Here's how it compares to other S3-compatible storage solutions:

| Feature | Lamina | MinIO | SeaweedFS | Garage |
|---------|--------|-------|-----------|--------|
| **Core Object Operations** |
| GetObject | ✅ | ✅ | ✅ | ✅ |
| PutObject | ✅ | ✅ | ✅ | ✅ |
| DeleteObject | ✅ | ✅ | ✅ | ✅ |
| HeadObject | ✅ | ✅ | ✅ | ✅ |
| ListObjects | ✅ | ✅ | ✅ | ✅ |
| ListObjectsV2 | ✅ | ✅ | ✅ | ✅ |
| **Bucket Operations** |
| CreateBucket | ✅ | ✅ | ✅ | ✅ |
| DeleteBucket | ✅ | ✅ | ✅ | ✅ |
| HeadBucket | ✅ | ✅ | ✅ | ✅ |
| ListBuckets | ✅ | ✅ | ✅ | ✅ |
| **Multipart Uploads** |
| CreateMultipartUpload | ✅ | ✅ | ✅ | ✅ |
| UploadPart | ✅ | ✅ | ✅ | ✅ |
| CompleteMultipartUpload | ✅ | ✅ | ✅ | ✅ |
| AbortMultipartUpload | ✅ | ✅ | ✅ | ✅ |
| ListParts | ✅ | ✅ | ✅ | ✅ |
| ListMultipartUploads | ✅ | ✅ | ✅ | ✅ |
| **Authentication** |
| AWS Signature V4 | ✅ | ✅ | ✅ | ✅ |
| AWS Signature V2 | ❌ | ✅ | ❌ | ❌ |
| Streaming Auth | ✅ | ✅ | ❌ | ❌ |
| **Advanced Features** |
| Server-Side Encryption | ❌ | ✅ | ✅ | ✅ |
| Versioning | ❌ | ✅ | ❌ | ❌ |
| Object Locking | ❌ | ✅ | ❌ | ❌ |
| Lifecycle Management | ❌ | ✅ | ❌ | ❌ |
| **Storage Focus** |
| Filesystem Gateway | ✅ | ❌ | ⚠️ | ❌ |
| Distributed Storage | ❌ | ✅ | ✅ | ✅ |
| Cloud-Native | ❌ | ✅ | ✅ | ✅ |

**Legend**: ✅ Supported | ❌ Not supported | ⚠️ Partial support

### Why Choose Lamina?

- **⚡ Superior Performance**: 10-1000x faster delimiter-based listing compared to MinIO and other S3 implementations
- **🎯 Focused Purpose**: Specifically designed as a filesystem-to-S3 gateway with performance optimizations
- **🔧 Simple Setup**: No complex clustering or distributed storage configuration
- **📁 Direct Filesystem Access**: Data remains accessible via standard filesystem tools
- **🚀 Hierarchical Data Optimized**: Ideal for document management, backups, media libraries, and structured data

## ⚙️ Configuration

### Storage Configuration

Configure storage backend in `appsettings.json`:

```json
{
  "StorageType": "Filesystem",
  "FilesystemStorage": {
    "DataDirectory": "/data",
    "MetadataMode": "Inline",
    "MetadataDirectory": "/metadata",
    "InlineMetadataDirectoryName": ".lamina-meta",
    "NetworkMode": "None",
    "RetryCount": 3,
    "RetryDelayMs": 100
  }
}
```

### Distributed Locking

For multi-instance deployments, enable Redis-based distributed locking to ensure safe concurrent access to shared storage:

```json
{
  "LockManager": "Redis",
  "Redis": {
    "ConnectionString": "redis:6379",
    "LockExpirySeconds": 30,
    "RetryCount": 3,
    "RetryDelayMs": 100,
    "LockKeyPrefix": "lamina:lock"
  }
}
```

#### Docker Compose with Redis

```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    restart: unless-stopped
    ports:
      - "6379:6379"

  lamina-1:
    image: ghcr.io/loyaltypointhq/lamina:latest
    environment:
      - LockManager=Redis
      - Redis__ConnectionString=redis:6379
      - Redis__LockKeyPrefix=lamina-prod:lock
      - StorageType=Filesystem
      - FilesystemStorage__DataDirectory=/app/data
    volumes:
      - ./data:/app/data
    ports:
      - "8080:8080"
    depends_on:
      - redis

  lamina-2:
    image: ghcr.io/loyaltypointhq/lamina:latest
    environment:
      - LockManager=Redis
      - Redis__ConnectionString=redis:6379
      - Redis__LockKeyPrefix=lamina-prod:lock
      - StorageType=Filesystem
      - FilesystemStorage__DataDirectory=/app/data
    volumes:
      - ./data:/app/data
    ports:
      - "8081:8080"
    depends_on:
      - redis
```

#### When to Use Redis Locking

- **Single Instance**: Use default `InMemory` lock manager
- **Multi-Instance**: Use `Redis` lock manager for:
  - Load balancing across multiple Lamina instances
  - High availability deployments
  - Shared storage (NFS, CIFS, cloud volumes)
  - Kubernetes deployments with multiple replicas

#### Metadata Modes

**Separate Directory** - Metadata stored in separate directory tree:
```json
{
  "MetadataMode": "SeparateDirectory",
  "MetadataDirectory": "/metadata"
}
```

**Inline** - Metadata stored alongside data:
```json
{
  "MetadataMode": "Inline",
  "InlineMetadataDirectoryName": ".lamina-meta"
}
```

**Extended Attributes** - Metadata stored as POSIX xattrs (Linux/macOS):
```json
{
  "MetadataMode": "Xattr",
  "XattrPrefix": "user.lamina"
}
```

### Authentication Configuration

```json
{
  "Authentication": {
    "Enabled": true,
    "Users": [
      {
        "AccessKeyId": "your-access-key",
        "SecretAccessKey": "your-secret-key",
        "Name": "username",
        "BucketPermissions": [
          {
            "BucketName": "*",
            "Permissions": ["*"]
          }
        ]
      }
    ]
  }
}
```

### Cleanup Services Configuration

```json
{
  "MultipartUploadCleanup": {
    "Enabled": true,
    "CleanupIntervalMinutes": 60,
    "UploadTimeoutHours": 24
  },
  "MetadataCleanup": {
    "Enabled": true,
    "CleanupIntervalMinutes": 120,
    "BatchSize": 1000
  },
  "TempFileCleanup": {
    "Enabled": true,
    "CleanupIntervalMinutes": 60,
    "TempFileAgeMinutes": 30,
    "BatchSize": 100
  }
}
```

## 🛠️ Development

### Build Requirements

- .NET 9.0 SDK
- Optional: Docker for containerized development

## 📚 Use Cases

### Production Scenarios
- **Filesystem Bridge**: Expose existing filesystem data via S3 API with exceptional listing performance
- **Edge Computing**: S3 API at edge locations with local storage and fast directory navigation
- **Backup Solutions**: S3-compatible interface for backup applications with optimized browsing of time-based structures
- **Legacy System Integration**: Add S3 capability to existing file-based systems while maintaining performance
- **Document Management**: High-performance S3 interface for organized document hierarchies
- **Media Storage**: Efficient browsing and listing of photo/video collections organized by date or category

### Network Storage Integration
- **NAS Exposure**: Make NAS devices accessible via S3 API
- **CIFS/SMB Gateway**: S3 interface for Windows file shares
- **NFS Gateway**: S3 API for Unix network filesystems

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Amazon S3**: For defining the de-facto object storage API standard
- **MinIO**: For S3 compatibility reference implementations
- **Garage**: For comprehensive S3 compatibility documentation and comparison tables
- **.NET Community**: For excellent tooling and framework support