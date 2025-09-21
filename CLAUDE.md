# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a .NET 9.0 ASP.NET Core Web API project called "Lamina" that implements an S3-compatible storage API.

**GitHub Repository**: https://github.com/LoyaltyPointHQ/Lamina
**Docker Image**: `ghcr.io/loyaltypointhq/lamina:latest`

The application provides two storage backends (configurable via appsettings.json):

1. **In-Memory Storage**: Default option, stores all data in memory using ConcurrentDictionary
2. **Filesystem Storage**: Stores objects on disk with separate data and metadata directories

Features supported:
- Bucket operations (create, list, delete, force delete non-empty buckets)
- Object operations (put, get, delete, head, list with prefix/delimiter support)
- Multipart uploads (initiate, upload parts, complete, abort, list parts)
- S3-compliant XML responses
- AWS Signature V4 authentication (optional, configurable)
- Thread-safe file operations with FileSystemLockManager

## Development Commands

### Build and Run
```bash
# Build the solution
dotnet build

# Run the application (development)
dotnet run --project Lamina/Lamina.csproj

# Run with specific profile
dotnet run --project Lamina/Lamina.csproj --launch-profile http   # Port 5214
dotnet run --project Lamina/Lamina.csproj --launch-profile https  # Ports 7179/5214
```

### Testing
```bash
# Run all tests
dotnet test

# Run specific test
dotnet test --filter "FullyQualifiedName~TestName"

# Run with detailed output
dotnet test --logger "console;verbosity=detailed"
```

### Docker
```bash
# Build Docker image
docker build -f Lamina/Dockerfile -t lamina .

# Run containerized application
docker run -p 8080:8080 lamina
```

## Project Structure

### Core Components
- **Lamina/Program.cs**: Main application entry point with service registration
- **Lamina/Controllers/**:
  - `S3BucketsController.cs`: Handles bucket operations (PUT, GET, DELETE, HEAD)
  - `S3ObjectsController.cs`: Handles object operations including multipart uploads

### Models
- **Lamina/Models/**:
  - `Bucket.cs`: Bucket entity model
  - `S3Object.cs`: Object entity and related models (PutObjectRequest, GetObjectResponse, etc.)
  - `MultipartUpload.cs`: Multipart upload related models
  - `S3XmlResponses.cs`: XML response DTOs for S3 API compliance

### Storage Layer

**Architecture**: The storage layer uses a Facade pattern with separate Data and Metadata storage components:

- **Lamina/Storage/Abstract/** (Interfaces and Facade implementations):
  - **Facade Interfaces & Implementations** (Main entry points):
    - `IBucketStorageFacade` / `BucketStorageFacade`: Orchestrates bucket operations
    - `IObjectStorageFacade` / `ObjectStorageFacade`: Orchestrates object operations
    - `IMultipartUploadStorageFacade` / `MultipartUploadStorageFacade`: Orchestrates multipart uploads

  - **Storage Interfaces** (Define storage contracts):
    - `IBucketDataStorage`: Interface for bucket data operations
    - `IBucketMetadataStorage`: Interface for bucket metadata
    - `IObjectDataStorage`: Interface for object data operations
    - `IObjectMetadataStorage`: Interface for object metadata
    - `IMultipartUploadDataStorage`: Interface for multipart data operations
    - `IMultipartUploadMetadataStorage`: Interface for multipart metadata

- **Lamina/Storage/InMemory/** (In-memory implementations):
  - `InMemoryBucketDataStorage`: In-memory bucket data storage
  - `InMemoryBucketMetadataStorage`: In-memory bucket metadata storage
  - `InMemoryObjectDataStorage`: In-memory object data storage
  - `InMemoryObjectMetadataStorage`: In-memory object metadata storage
  - `InMemoryMultipartUploadDataStorage`: In-memory multipart data storage
  - `InMemoryMultipartUploadMetadataStorage`: In-memory multipart metadata storage

- **Lamina/Storage/Filesystem/** (Filesystem implementations):
  - `FilesystemBucketDataStorage`: Filesystem bucket data storage
  - `FilesystemBucketMetadataStorage`: Filesystem bucket metadata storage
  - `FilesystemObjectDataStorage`: Filesystem object data storage
  - `FilesystemObjectMetadataStorage`: Filesystem object metadata storage
  - `FilesystemMultipartUploadDataStorage`: Filesystem multipart data storage
  - `FilesystemMultipartUploadMetadataStorage`: Filesystem multipart metadata storage
  - `FileSystemLockManager`: Thread-safe file operations manager

### Services

- **Lamina/Services/**:
  - `IAuthenticationService` / `AuthenticationService`: AWS Signature V4 authentication
  - `MultipartUploadCleanupService`: Background service for automatic cleanup of stale multipart uploads

### Helpers
- **Lamina/Helpers/**:
  - `ETagHelper.cs`: Centralized ETag computation using SHA1 (supports byte arrays, files, and streams)

### Tests
- **Lamina.Tests/**:
  - `Controllers/BucketsControllerIntegrationTests.cs`: Bucket API integration tests
  - `Controllers/ObjectsControllerIntegrationTests.cs`: Object API integration tests
  - `Services/BucketServiceTests.cs`: Bucket storage unit tests
  - `Services/ObjectServiceTests.cs`: Object storage unit tests
  - `Services/MultipartUploadServiceTests.cs`: Multipart upload tests
  - `Services/MultipartUploadCleanupServiceTests.cs`: Cleanup service tests

## S3 API Implementation Details

### XML Response Format
- All responses use S3-compliant XML format
- Controllers are configured with `[Produces("application/xml")]`
- XML models use appropriate `XmlRoot` and `XmlElement` attributes
- Supports both namespaced and non-namespaced XML for compatibility

### Routing Structure
- Bucket operations: `/{bucketName}`
- Object operations: `/{bucketName}/{*key}` (catch-all for nested paths)
- Query parameters determine operation type (e.g., `?uploads` for multipart operations)

### Multipart Upload Flow
1. **Initiate**: `POST /{bucket}/{key}?uploads`
2. **Upload Part**: `PUT /{bucket}/{key}?partNumber=N&uploadId=ID`
3. **Complete**: `POST /{bucket}/{key}?uploadId=ID` (with XML body)
4. **Abort**: `DELETE /{bucket}/{key}?uploadId=ID`
5. **List Parts**: `GET /{bucket}/{key}?uploadId=ID`
6. **List Uploads**: `GET /{bucket}?uploads`

### ETag Handling
- ETags are computed using SHA1 hash of content (changed from MD5)
- Stored internally without quotes
- Returned in responses with quotes (e.g., `"etag-value"`)
- Comparison normalizes by trimming quotes
- ETag computation is centralized in `ETagHelper` class

### Important Implementation Notes

1. **XML Deserialization**: CompleteMultipartUpload supports both:
   - Non-namespaced XML (common from clients)
   - Namespaced XML (S3 spec: `http://s3.amazonaws.com/doc/2006-03-01/`)

2. **Route Disambiguation**: Query parameters determine operation type:
   - Use `Request.Query.ContainsKey()` to check for parameter presence
   - Parameter value can be empty (e.g., `?uploads` vs `?uploads=`)

3. **Error Responses**: Return S3-compliant error XML with appropriate HTTP status codes

4. **Data-First Architecture**:
   - **Data is the source of truth** - object existence is determined by data presence, not metadata
   - **Metadata is optional** - if metadata doesn't exist but data does, metadata is generated on-the-fly
   - **Content Type Detection** - automatically detects MIME types based on file extensions
   - **No forced metadata storage** - missing metadata is generated for read operations but not persisted

5. **Storage Backends**:

   **In-Memory Storage**:
   - Uses `ConcurrentDictionary` for thread-safety
   - Multipart uploads store parts temporarily until completion
   - Combined data is created on CompleteMultipartUpload

   **Filesystem Storage**:
   - Data stored in: `{DataDirectory}/{bucketName}/{key}`
   - Metadata stored in: `{MetadataDirectory}/{bucketName}/{key}.json`
   - Multipart uploads: `{MetadataDirectory}/_multipart_uploads/{uploadId}/`
   - File size is ALWAYS read from filesystem (not stored in metadata)
   - Metadata format (JSON):
     ```json
     {
       "Key": "object-key",
       "BucketName": "bucket-name",
       "LastModified": "2025-09-18T12:23:54.7926647Z",
       "ETag": "sha1-hash",  // Note: SHA1 hash, not MD5
       "ContentType": "text/plain",
       "UserMetadata": {}
     }
     ```

## Testing Strategy

- **Integration Tests**: Use `WebApplicationFactory` to test full HTTP pipeline
- **Unit Tests**: Test storage logic in isolation
- **XML Validation**: Tests verify both request and response XML formats
- **Multipart Flow**: Comprehensive tests for complete upload lifecycle

## Configuration

### Storage Backend Selection
Configure in `appsettings.json` or `appsettings.Development.json`:

```json
{
  "StorageType": "InMemory",  // or "Filesystem"
  "FilesystemStorage": {
    "DataDirectory": "/tmp/laminas/data",
    "MetadataDirectory": "/tmp/laminas/metadata",  // Required for SeparateDirectory mode
    "MetadataMode": "SeparateDirectory",  // or "Inline" (default: SeparateDirectory)
    "InlineMetadataDirectoryName": ".lamina-meta"  // Name of metadata directory in inline mode (default: .lamina-meta)
  }
}
```

#### Metadata Storage Modes

**SeparateDirectory Mode** (default):
- Metadata is stored in a separate directory tree specified by `MetadataDirectory`
- Data: `/tmp/laminas/data/bucket/key`
- Metadata: `/tmp/laminas/metadata/bucket/key.json`
- Multipart uploads: `/tmp/laminas/metadata/_multipart_uploads/`

**Inline Mode**:
- Metadata is stored alongside data in special directories
- Data: `/tmp/laminas/data/bucket/path/to/object.zip`
- Metadata: `/tmp/laminas/data/bucket/path/to/.lamina-meta/object.zip.json`
- Multipart uploads: `/tmp/laminas/data/.lamina-meta/_multipart_uploads/`
- The metadata directory name is configurable via `InlineMetadataDirectoryName`
- Objects with keys containing the metadata directory name are forbidden
- Metadata directories are automatically excluded from object listings

Example inline mode configuration:
```json
{
  "StorageType": "Filesystem",
  "FilesystemStorage": {
    "DataDirectory": "/tmp/laminas/data",
    "MetadataMode": "Inline",
    "InlineMetadataDirectoryName": ".lamina-meta"
  }
}
```

### Authentication Configuration
```json
{
  "Authentication": {
    "Enabled": false,  // Set to true to enable AWS Signature V4 authentication
    "Users": [  // Note: Changed from "AccessKeys" to "Users"
      {
        "AccessKeyId": "your-access-key",
        "SecretAccessKey": "your-secret-key",
        "Name": "username",
        "BucketPermissions": [
          {
            "BucketName": "*",  // Wildcard for all buckets
            "Permissions": ["*"]  // Or specific: ["read", "write", "list", "delete"]
          }
        ]
      }
    ]
  }
}
```

### Multipart Upload Cleanup Configuration
```json
{
  "MultipartUploadCleanup": {
    "Enabled": true,  // Enable/disable automatic cleanup
    "CleanupIntervalMinutes": 60,  // How often to run cleanup
    "UploadTimeoutHours": 24  // Uploads older than this are considered stale
  }
}
```

## Recent Updates

### Data-First Object Storage
- **Data is now the source of truth** for object existence
- Metadata is optional and generated on-the-fly when missing
- Content type is intelligently detected based on file extensions
- Generated metadata is not persisted to storage
- `ObjectStorageFacade.ObjectExistsAsync` checks data existence, not metadata
- List operations include orphaned data files without metadata

### SHA1 Migration (from MD5)
- All ETag computations now use SHA1 instead of MD5
- Centralized in `ETagHelper` class for consistency
- Supports byte arrays, files, and streams
- Files are processed without loading into memory

### Multipart Upload Cleanup Service
- Background service that automatically cleans up stale/abandoned multipart uploads
- Runs periodically (configurable interval)
- Removes uploads older than configured timeout
- Helps prevent storage leaks from incomplete uploads

### Streaming Multipart Assembly
- Multipart uploads are now assembled using streaming to eliminate memory overhead

### Inline Metadata Mode
- New metadata storage mode where metadata is stored alongside data files
- Metadata stored in configurable directories (default: `.lamina-meta`) within the data tree
- Automatic filtering excludes metadata directories from object listings
- Validation prevents operations on paths containing metadata directory names
- Supports both inline and separate directory modes for different deployment scenarios
- Parts are read directly from storage and written to the final object
- Significantly reduces memory usage for large multipart uploads

## Common Development Tasks

### Adding New S3 Operations
1. Check S3 API documentation for request/response format
2. Add XML models to `S3XmlResponses.cs` if needed
3. Update controller with new endpoint/query parameter handling
4. Add corresponding storage interface methods in `Storage/Abstract/`
5. Implement in both `Storage/InMemory/` and `Storage/Filesystem/`
6. Add integration and unit tests

### Debugging XML Issues
1. Enable console logging in controller to see raw XML
2. Check for namespace mismatches
3. Verify XmlElement names match exactly (case-sensitive)
4. Test with both namespaced and non-namespaced XML

### Running S3 Client Tests
The API is compatible with standard S3 clients. Test with:
- AWS CLI: `aws s3 --endpoint-url http://localhost:5214`
- MinIO Client: `mc alias set local http://localhost:5214`
- AWS SDKs with custom endpoint configuration

### Working with Filesystem Storage

**Important Implementation Details:**
- File size is **never** stored in metadata; it's always read from the filesystem
- This ensures size is always accurate even if files are modified directly on disk
- Object keys with '/' are stored as nested directories on filesystem
- Metadata files have `.json` extension appended to the object key
- Both data and metadata directories are created automatically on startup
- Thread-safe file operations are ensured through FileSystemLockManager
- Directory cleanup: Empty subdirectories are automatically removed after object deletion, but bucket directories are always preserved even when empty
- Multipart uploads use temporary directories until completion

**Testing Filesystem Storage:**

*SeparateDirectory Mode:*
```bash
# Edit appsettings.json: "MetadataMode": "SeparateDirectory"
# Run the application
dotnet run --project Lamina/Lamina.csproj

# Create bucket and upload object
curl -X PUT http://localhost:5214/test-bucket
echo "test content" | curl -X PUT -H "Content-Type: text/plain" --data-binary @- http://localhost:5214/test-bucket/test.txt

# Check filesystem
ls -la /tmp/laminas/data/test-bucket/
ls -la /tmp/laminas/metadata/test-bucket/

# Verify metadata doesn't contain size
cat /tmp/laminas/metadata/test-bucket/test.txt.json | jq .
```

*Inline Mode:*
```bash
# Use inline metadata configuration
dotnet run --project Lamina/Lamina.csproj --launch-profile http -- --config appsettings.InlineMetadata.json

# Create bucket and upload object
curl -X PUT http://localhost:5214/test-bucket
echo "test content" | curl -X PUT -H "Content-Type: text/plain" --data-binary @- http://localhost:5214/test-bucket/path/to/test.txt

# Check filesystem - metadata is in .lamina-meta directories
ls -la /tmp/laminas/data/test-bucket/path/to/
ls -la /tmp/laminas/data/test-bucket/path/to/.lamina-meta/

# Verify metadata file exists
cat /tmp/laminas/data/test-bucket/path/to/.lamina-meta/test.txt.json | jq .

# Try to create object with forbidden key (will fail)
curl -X PUT http://localhost:5214/test-bucket/.lamina-meta/forbidden.txt --data "test"
```

## Auto-Update Instructions

After making significant code changes that affect the project structure, configuration, or implementation details documented in this file, update CLAUDE.md to reflect those changes. This includes:
- New features or S3 operations added
- Changes to storage implementations
- Configuration option modifications
- Significant architectural changes
- New dependencies or services

Do not update for minor changes like bug fixes, refactoring without architectural impact, or test additions unless they introduce new concepts that should be documented.