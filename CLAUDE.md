# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

.NET 9.0 ASP.NET Core Web API implementing S3-compatible storage API with strict compliance to Amazon S3 REST API specification.

**Repository**: https://github.com/LoyaltyPointHQ/Lamina
**Docker**: `ghcr.io/loyaltypointhq/lamina:latest`

### Storage Backends
- **In-Memory**: Default, uses ConcurrentDictionary
- **Filesystem**: Disk storage with configurable metadata modes
- **SQL**: Entity Framework Core-based storage supporting PostgreSQL and SQLite

### Key Features
- Complete S3 API operations (buckets, objects, multipart uploads)
- AWS Signature V4 authentication with streaming support
- Data-first architecture (data is source of truth)
- Thread-safe operations with distributed locking

## Commands

### Build & Test
```bash
# Build and run
dotnet build
dotnet run --project Lamina/Lamina.csproj

# Test
dotnet test
```

### Database Migrations

#### Prerequisites
```bash
# Install Entity Framework CLI tool (if not already installed)
dotnet tool install --global dotnet-ef
```

#### Creating New Migrations
```bash
# For SQLite (default)
dotnet ef migrations add <MigrationName> \
  --context LaminaDbContext \
  --output-dir Migrations/Sqlite \
  --project Lamina/Lamina.csproj \
  -- --SqlStorage:Provider=SQLite

# For PostgreSQL  
dotnet ef migrations add <MigrationName> \
  --context LaminaDbContext \
  --output-dir Migrations/PostgreSql \
  --project Lamina/Lamina.csproj \
  -- --SqlStorage:Provider=PostgreSQL

# Note: After creating PostgreSQL migrations, update the timestamp by 1 second
# to avoid conflicts with SQLite migrations (e.g., 20250925003327 -> 20250925003328)
```

#### Applying Migrations
Migrations are automatically applied on startup when `SqlStorage.MigrateOnStartup` is set to `true` (default).

To manually apply migrations:
```bash
# For SQLite
dotnet ef database update \
  --context LaminaDbContext \
  --project Lamina/Lamina.csproj \
  -- --SqlStorage:Provider=SQLite \
     --SqlStorage:ConnectionString="Data Source=/path/to/lamina.db"

# For PostgreSQL
dotnet ef database update \
  --context LaminaDbContext \
  --project Lamina/Lamina.csproj \
  -- --SqlStorage:Provider=PostgreSQL \
     --SqlStorage:ConnectionString="Host=localhost;Database=lamina;Username=user;Password=pass"
```

#### Migration Structure
- `Migrations/Sqlite/` - SQLite-specific migrations
- `Migrations/PostgreSql/` - PostgreSQL-specific migrations
- Both sets of migrations are maintained in parallel
- The application automatically uses the correct migrations based on the configured provider

#### Environment Variables for Migrations
The `LaminaDbContextFactory` supports environment variables for easier CI/CD:
```bash
# Set database provider
export LAMINA_DB_PROVIDER=PostgreSQL  # or SQLite

# Set connection string
export LAMINA_DB_CONNECTION_STRING="Host=localhost;Database=lamina;Username=user;Password=pass"

# Then run migrations without command line arguments
dotnet ef migrations add <MigrationName> --context LaminaDbContext --output-dir Migrations/PostgreSql
```

### Docker
```bash
docker build -f Lamina/Dockerfile -t lamina .
docker run -p 8080:8080 lamina
```

### Helm Deployment
```bash
# Basic deployment
helm install lamina ./chart

# Production with persistent storage
helm install lamina ./chart \
  --set config.StorageType=Filesystem \
  --set persistentVolume.enabled=true \
  --set persistentVolume.size=50Gi
```

## Project Structure

### Core Components
- **Controllers**: `S3BucketsController`, `S3ObjectsController`
- **Storage Layer**: Facade pattern with separate Data/Metadata components
  - **Abstract**: Interfaces and facades
  - **InMemory**: ConcurrentDictionary implementations
  - **Filesystem**: Disk-based storage with 3 metadata modes
- **Streaming**: AWS chunked encoding and signature validation
- **Services**: Authentication, cleanup services (multipart, metadata, temp files)

### Storage Implementations
- **InMemory**: Thread-safe dictionaries
- **Filesystem**:
  - **SeparateDirectory**: `DataDir/` + `MetadataDir/` (default)
  - **Inline**: Metadata in `.lamina-meta/` subdirectories
  - **Xattr**: POSIX extended attributes (Linux/macOS only)
- **SQL**: Entity Framework Core storage
  - **SQLite**: Lightweight file-based database
  - **PostgreSQL**: Full-featured relational database
  - Metadata stored in database, data still on filesystem
  - Supports migrations for schema updates

## Configuration

### Storage
```json
{
  "StorageType": "InMemory",  // or "Filesystem" or "Sql"
  "MetadataStorageType": "InMemory",  // or "Filesystem" or "Sql" (defaults to StorageType)
  "FilesystemStorage": {
    "DataDirectory": "/tmp/laminas/data",
    "MetadataDirectory": "/tmp/laminas/metadata",
    "MetadataMode": "SeparateDirectory",  // or "Inline" or "Xattr"
    "NetworkMode": "None"  // or "CIFS" or "NFS"
  },
  "SqlStorage": {
    "Provider": "SQLite",  // or "PostgreSQL"
    "ConnectionString": "Data Source=/tmp/lamina/metadata.db",  // SQLite example
    // "ConnectionString": "Host=localhost;Database=lamina;Username=user;Password=pass",  // PostgreSQL example
    "MigrateOnStartup": true,
    "CommandTimeout": 30,
    "EnableSensitiveDataLogging": false,
    "EnableDetailedErrors": false
  }
}
```

### Authentication
```json
{
  "Authentication": {
    "Enabled": false,
    "Users": [
      {
        "AccessKeyId": "key",
        "SecretAccessKey": "secret",
        "BucketPermissions": [{"BucketName": "*", "Permissions": ["*"]}]
      }
    ]
  }
}
```

### Redis Distributed Locking
```json
{
  "LockManager": "Redis",  // "InMemory" (default) or "Redis"
  "Redis": {
    "ConnectionString": "localhost:6379",
    "LockExpirySeconds": 30
  }
}
```

## S3 Implementation Notes

### Routing
- Buckets: `/{bucketName}`
- Objects: `/{bucketName}/{*key}`
- Query parameters determine operations (`?uploads`, `?partNumber=N`)

### Multipart Upload Flow
1. Initiate: `POST /{bucket}/{key}?uploads`
2. Upload Part: `PUT /{bucket}/{key}?partNumber=N&uploadId=ID`
3. Complete: `POST /{bucket}/{key}?uploadId=ID`

### Data-First Architecture
- **Object existence** determined by data presence, not metadata
- **Metadata is optional** - generated on-the-fly when missing
- **Content type detection** based on file extensions
- **Optimized storage** - metadata only stored when differs from defaults

### Performance Optimizations
- **Delimiter-based listing**: Single directory scans for `delimiter="/"`
- **Streaming multipart assembly**: No memory overhead for large uploads
- **Optimized metadata**: Only store non-default values

## Key Implementation Details

### ETag Handling
- MD5 hash for S3 compatibility (centralized in `ETagHelper`)
- Stored without quotes, returned with quotes

### XML Responses
- S3-compliant XML with namespace support
- Both namespaced and non-namespaced XML accepted

### Known S3 Incompatibilities
- **Single-region system**: Region constraints accepted but ignored
- **No cross-region replication**
- **No region-specific endpoints**

## Development Tasks

### Adding S3 Operations
1. Consult official S3 API documentation
2. Add XML models to `S3XmlResponses.cs`
3. Update controllers with exact S3 routing
4. Implement in both storage backends
5. Add comprehensive tests

### Testing Filesystem Storage
```bash
# Create test data
curl -X PUT http://localhost:5214/test-bucket
echo "content" | curl -X PUT --data-binary @- http://localhost:5214/test-bucket/test.txt

# Check storage structure
ls -la /tmp/laminas/data/test-bucket/
ls -la /tmp/laminas/metadata/test-bucket/  # SeparateDirectory mode
```

### S3 Client Testing
- AWS CLI: `aws s3 --endpoint-url http://localhost:5214`
- MinIO Client: `mc alias set local http://localhost:5214`

## Helm Chart Features

- **Platform detection**: Kubernetes vs OpenShift
- **Storage options**: In-memory or filesystem with PVs
- **Redis integration**: Optional distributed locking
- **Security**: Automatic secret management
- **Auto-scaling**: HPA support

## Cleanup Services

Three background services handle maintenance:
- **Multipart Upload Cleanup**: Removes stale uploads
- **Metadata Cleanup**: Removes orphaned metadata
- **Temp File Cleanup**: Removes interrupted upload temp files

## Auto-Update Instructions

Update CLAUDE.md after significant changes:
- New S3 operations or features
- Storage implementation changes
- Configuration modifications
- Architectural changes
- New dependencies/services

Do not update for minor bug fixes or refactoring.