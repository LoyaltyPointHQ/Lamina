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

# Test all projects
dotnet test

# Test specific projects
dotnet test Lamina.Storage.Core.Tests
dotnet test Lamina.Storage.Filesystem.Tests
dotnet test Lamina.Storage.Sql.Tests
dotnet test Lamina.WebApi.Tests
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
docker build -f Lamina.WebApi/Dockerfile -t lamina .
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

### Multi-Project Architecture

The solution is organized into multiple projects for better separation of concerns:

#### Main Projects
- **Lamina**: Simple startup project and main entry point
- **Lamina.Core**: Core models, interfaces, and shared types
- **Lamina.Storage.Core**: Storage abstractions, facades, and helpers
- **Lamina.Storage.Filesystem**: Filesystem-based storage implementation
- **Lamina.Storage.InMemory**: In-memory storage implementation
- **Lamina.Storage.Sql**: SQL database storage implementation (SQLite/PostgreSQL)
- **Lamina.WebApi**: ASP.NET Core Web API implementation and controllers

#### Test Projects
- **Lamina.Storage.Core.Tests**: Tests for storage abstractions and facades
- **Lamina.Storage.Filesystem.Tests**: Tests for filesystem storage implementation
- **Lamina.Storage.Sql.Tests**: Tests for SQL storage implementation
- **Lamina.WebApi.Tests**: Tests for controllers, services, authentication, and integration

### Core Components

- **Controllers**: `S3BucketsController`, `S3ObjectsController`, `S3MultipartController` (all inherit from `S3ControllerBase`)
- **Authentication**: ASP.NET Core authentication with S3-specific handlers
- **Authorization**: Policy-based authorization with S3AuthorizeAttribute
- **Storage Layer**: Facade pattern with separate Data/Metadata components
    - **Abstract**: Interfaces and facades
    - **InMemory**: ConcurrentDictionary implementations
    - **Filesystem**: Disk-based storage with 3 metadata modes
- **Streaming**: AWS chunked encoding and signature validation
- **Services**: Cleanup services (multipart, metadata, temp files)

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

### Controller Attributes

- **RequireQueryParameterAttribute**: Routes actions based on presence of specific query parameters
- **RequireNoQueryParametersAttribute**: Routes actions when no query parameters are present
- These attributes enable S3 operation differentiation on the same route patterns

## Authentication & Authorization

### ASP.NET Core Integration

Lamina uses standard ASP.NET Core authentication and authorization framework:

- **S3AuthenticationHandler**: Validates AWS Signature V4 authentication
- **S3AuthorizationHandler**: Enforces bucket and object permissions
- **S3AuthorizeAttribute**: Declarative authorization for controllers

### S3 Operations

The following operations are supported for authorization:

- `S3Operations.Read`: GET, HEAD operations
- `S3Operations.Write`: PUT, POST operations
- `S3Operations.Delete`: DELETE operations
- `S3Operations.List`: Bucket and object listing operations
- `S3Operations.All`: Wildcard for all operations

### Usage Example

```csharp
[S3Authorize(S3Operations.Write, S3ResourceType.Object)]
public async Task<IActionResult> PutObject(string bucketName, string key)
{
    // Implementation
}
```

## Configuration

### Storage

```json
{
  "StorageType": "InMemory",
  // or "Filesystem" or "Sql"
  "MetadataStorageType": "InMemory",
  // or "Filesystem" or "Sql" (defaults to StorageType)
  "FilesystemStorage": {
    "DataDirectory": "/tmp/laminas/data",
    "MetadataDirectory": "/tmp/laminas/metadata",
    "MetadataMode": "SeparateDirectory",
    // or "Inline" or "Xattr"
    "NetworkMode": "None"
    // or "CIFS" or "NFS"
  },
  "SqlStorage": {
    "Provider": "SQLite",
    // or "PostgreSQL"
    "ConnectionString": "Data Source=/tmp/lamina/metadata.db",
    // SQLite example
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
        "BucketPermissions": [
          {
            "BucketName": "*",
            "Permissions": [
              "*"
            ]
          }
        ]
      }
    ]
  }
}
```

Note: Authentication now uses the standard ASP.NET Core authentication framework with S3-specific handlers.

### Redis Distributed Locking

```json
{
  "LockManager": "Redis",
  // "InMemory" (default) or "Redis"
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

## Release Process

When user says "release vX.X.X", perform these steps:

1. Check commits since last release with `git log --oneline $(git describe --tags --abbrev=0)..HEAD` to identify [Feature], [Enhancement], [Fix], and [Breaking] commits for release notes
2. Create annotated git tag: `git tag -a vX.X.X -m "Release vX.X.X\n\n[organize commits by type with Features/Enhancements/Bug Fixes/Breaking Changes sections]"`
3. Push tag: `git push origin vX.X.X`
4. Create GitHub release: `gh release create vX.X.X --title "Release vX.X.X" --notes "[organize by sections: ## Features, ## Enhancements, ## Bug Fixes, ## Breaking Changes as applicable]"`

### Release Notes Convention

When making commits that introduce features or changes that should appear in release notes, include them in commit messages with the format:

```
[Feature] Brief description of the feature
[Enhancement] Brief description of the improvement
[Fix] Brief description of the bug fix (only for user-facing fixes)
[Breaking] Brief description of breaking change
```

This helps identify release-worthy changes when creating GitHub releases.

## Auto-Update Instructions

Update CLAUDE.md after significant changes:

- New S3 operations or features
- Storage implementation changes
- Configuration modifications
- Architectural changes
- New dependencies/services

Do not update for minor bug fixes or refactoring.