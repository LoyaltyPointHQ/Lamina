# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a .NET 9.0 ASP.NET Core Web API project called "Lamina" that implements an S3-compatible storage API.

**IMPORTANT**: This project implements the official Amazon S3 REST API specification and must maintain strict compliance with it. Any deviations from the S3 specification are considered bugs and should be fixed. All API endpoints, request/response formats, headers, status codes, and behaviors must exactly match the official S3 API documentation.

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
- AWS Streaming with Signature V4 (STREAMING-AWS4-HMAC-SHA256-PAYLOAD)
- AWS Streaming with Trailers (STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER)
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
  - `StreamingTrailer.cs`: Models for AWS streaming trailer support

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

### Streaming Support

- **Lamina/Streaming/** (AWS streaming authentication and chunk processing):
  - **Core Services**:
    - `IStreamingAuthenticationService` / `StreamingAuthenticationService`: Creates chunk validators for streaming requests

  - **Chunked Data Processing** (`Lamina/Streaming/Chunked/`):
    - `ChunkConstants.cs`: Constants for AWS chunked encoding
    - `ChunkHeader.cs`: Models chunk header information
    - `ChunkBuffer.cs`: Manages chunk data buffering
    - `IChunkedDataParser` / `ChunkedDataParser`: Parses AWS chunked encoding format (registered as DI service)

  - **Signature Validation** (`Lamina/Streaming/Validation/`):
    - `IChunkSignatureValidator` / `ChunkSignatureValidator`: Validates chunk signatures
    - `SignatureCalculator.cs`: AWS Signature V4 calculations for streaming

  - **Trailer Support** (`Lamina/Streaming/Trailers/`):
    - `TrailerParser.cs`: Parses HTTP trailers in streaming requests

### Services

- **Lamina/Services/**:
  - `IAuthenticationService` / `AuthenticationService`: AWS Signature V4 authentication
  - `MultipartUploadCleanupService`: Background service for automatic cleanup of stale multipart uploads
  - `MetadataCleanupService`: Background service for automatic cleanup of orphaned metadata (metadata without corresponding data)
  - `TempFileCleanupService`: Background service for automatic cleanup of stale temporary files in filesystem storage

### Helpers
- **Lamina/Helpers/**:
  - `ETagHelper.cs`: Centralized ETag computation using MD5 (supports byte arrays, files, and streams)

### Tests
- **Lamina.Tests/**:
  - `Controllers/BucketsControllerIntegrationTests.cs`: Bucket API integration tests
  - `Controllers/ObjectsControllerIntegrationTests.cs`: Object API integration tests
  - `Controllers/StreamingAuthenticationIntegrationTests.cs`: Streaming authentication integration tests
  - `Controllers/StreamingMultipartUploadIntegrationTests.cs`: Streaming multipart upload tests
  - `Controllers/StreamingTrailerIntegrationTests.cs`: Streaming with trailers integration tests
  - `Services/BucketServiceTests.cs`: Bucket storage unit tests
  - `Services/ObjectServiceTests.cs`: Object storage unit tests
  - `Services/MultipartUploadServiceTests.cs`: Multipart upload tests
  - `Services/MultipartUploadCleanupServiceTests.cs`: Cleanup service tests
  - `Services/MetadataCleanupServiceTests.cs`: Metadata cleanup service tests
  - `Services/TempFileCleanupServiceTests.cs`: Temporary file cleanup service tests
  - `Services/StreamingAuthenticationServiceTests.cs`: Streaming authentication service tests
  - `Services/StreamingTrailerSupportTests.cs`: Streaming trailer support tests
  - `Helpers/AwsChunkedEncodingStreamTests.cs`: Chunked encoding stream processing tests
  - `Helpers/AwsChunkedEncodingTrailerTests.cs`: Chunked encoding with trailers tests
  - `Models/StreamingTrailerModelTests.cs`: Streaming trailer model tests

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
- ETags are computed using MD5 hash of content (S3 standard)
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
   - **Optimized metadata storage** - metadata is only stored when it differs from auto-generated defaults (custom content types or user metadata)
   - **Automatic metadata generation** - missing metadata is generated for read operations but not persisted, making this the normal case

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
       "ETag": "md5-hash",  // Note: MD5 hash for S3 compatibility
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
    "MetadataMode": "Inline",  // or "SeparateDirectory" (default: Inline)
    "InlineMetadataDirectoryName": ".lamina-meta",  // Name of metadata directory in inline mode (default: .lamina-meta)
    "TempFilePrefix": ".lamina-tmp-",  // Prefix for temporary files (default: .lamina-tmp-)
    "NetworkMode": "None",  // or "CIFS" or "NFS" for network filesystem support
    "RetryCount": 3,  // Number of retries for file operations (used with CIFS/NFS)
    "RetryDelayMs": 100  // Initial delay between retries in milliseconds
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

#### Network Filesystem Support (CIFS/NFS)

Lamina includes special support for network filesystems to handle their unique characteristics:

**CIFS Configuration Example** (`appsettings.CIFS.json`):
```json
{
  "StorageType": "Filesystem",
  "FilesystemStorage": {
    "DataDirectory": "/mnt/cifs/lamina/data",
    "MetadataDirectory": "/mnt/cifs/lamina/metadata",
    "MetadataMode": "SeparateDirectory",
    "NetworkMode": "CIFS",
    "RetryCount": 5,  // More retries for CIFS
    "RetryDelayMs": 200  // Longer initial delay
  }
}
```

**NFS Configuration Example** (`appsettings.NFS.json`):
```json
{
  "StorageType": "Filesystem",
  "FilesystemStorage": {
    "DataDirectory": "/mnt/nfs/lamina/data",
    "MetadataDirectory": "/mnt/nfs/lamina/metadata",
    "MetadataMode": "SeparateDirectory",
    "NetworkMode": "NFS",
    "RetryCount": 3,
    "RetryDelayMs": 100
  }
}
```

**Network Filesystem Features**:
- **Retry Logic**: Automatic retry with exponential backoff for transient network errors
- **CIFS-Safe Atomic Moves**: Special handling for file overwrites on CIFS
- **ESTALE Error Handling**: Automatic detection and retry of stale NFS file handles
- **Network-Aware Error Detection**: Recognizes network-specific error patterns

**Important Limitations**:
- The in-memory lock manager only protects within a single Lamina instance
- For multi-instance deployments on network filesystems, implement distributed locking
- Recommended: Use single instance or implement Redis/database-based locking

**Recommended Mount Options**:
- **NFS**: `mount -t nfs4 -o vers=4.2,hard,timeo=600,retrans=2,rsize=1048576,wsize=1048576`
- **CIFS**: `mount -t cifs -o vers=3.0,cache=none,actimeo=0`

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

### Metadata Cleanup Configuration
```json
{
  "MetadataCleanup": {
    "Enabled": true,  // Enable/disable automatic metadata cleanup
    "CleanupIntervalMinutes": 120,  // How often to run cleanup (default: every 2 hours)
    "BatchSize": 1000  // Number of metadata entries to process per batch
  }
}
```

### Temporary File Cleanup Configuration
```json
{
  "TempFileCleanup": {
    "Enabled": true,  // Enable/disable automatic temp file cleanup
    "CleanupIntervalMinutes": 60,  // How often to run cleanup (default: every hour)
    "TempFileAgeMinutes": 30,  // Files older than this are considered stale (default: 30 minutes)
    "BatchSize": 100  // Number of files to process per batch (default: 100)
  }
}
```

## Recent Updates

### Temporary File Cleanup Service
- **Automated temporary file cleanup**: Background service that periodically scans and removes stale temporary files left by interrupted upload operations
- **Filesystem storage specific**: Only operates when filesystem storage is configured, ensuring no unnecessary overhead for in-memory storage
- **Age-based cleanup**: Removes temporary files older than a configurable threshold (default: 30 minutes) to avoid deleting files currently being written
- **Recursive directory scanning**: Scans all subdirectories in the data directory to find temporary files with the configured prefix (`.lamina-tmp-*`)
- **Batch processing**: Processes files in configurable batches for memory efficiency and performance monitoring
- **Comprehensive error handling**: Gracefully handles file access errors, locked files, and permission issues without stopping the cleanup process
- **Configurable intervals**: Customizable cleanup frequency and age thresholds for different deployment scenarios

### Metadata Cleanup Service
- **Automated orphaned metadata cleanup**: Background service that periodically scans for and removes metadata entries that no longer have corresponding data files
- **Provider-agnostic design**: Works with both InMemory and Filesystem storage implementations through storage interfaces
- **Configurable intervals and batch processing**: Customizable cleanup frequency and batch sizes for optimal performance
- **Comprehensive error handling**: Resilient to errors during cleanup operations, continues processing remaining entries
- **Data-first approach**: Respects the data-first architecture where data existence is the source of truth
- **Memory-efficient scanning**: Uses `IAsyncEnumerable` for streaming metadata enumeration without loading everything into memory
- **Support for both metadata modes**: Handles both SeparateDirectory and Inline metadata storage modes in filesystem storage

### Optimized Metadata Storage
- **Storage optimization**: Metadata is only stored when it differs from auto-generated defaults
- **Smart comparison**: Compares provided content type with auto-detected type based on file extension
- **User metadata detection**: Only stores metadata when custom user metadata is present
- **Reduced storage overhead**: Eliminates redundant metadata files for typical object uploads
- **Backward compatibility**: Existing metadata files are still read; missing metadata is generated on-the-fly
- **Code refactoring**: Combined duplicate `PutObjectAsync` overloads using delegation pattern to reduce maintenance overhead

### Data-First Object Storage
- **Data is now the source of truth** for object existence
- Metadata is optional and generated on-the-fly when missing
- Content type is intelligently detected based on file extensions
- Generated metadata is not persisted to storage
- `ObjectStorageFacade.ObjectExistsAsync` checks data existence, not metadata
- List operations include orphaned data files without metadata

### MD5 ETags (S3 Standard)
- All ETag computations use MD5 hash for S3 compatibility
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

### AWS Streaming Authentication
- **AWS Signature V4 Streaming Support**: Full implementation of STREAMING-AWS4-HMAC-SHA256-PAYLOAD authentication
- **AWS Streaming with Trailers**: Support for STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER with HTTP trailers
- **Refactored Architecture**: Streaming code moved from `Services/` to dedicated `Streaming/` namespace for better organization
- **Modular Components**: Separated chunked data parsing, signature validation, and trailer processing into focused components
- **Enhanced S3 Compatibility**: Improved compatibility with AWS SDKs and S3 clients using streaming uploads
- **Middleware Integration**: Streaming authentication integrated into S3AuthenticationMiddleware for seamless handling

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
1. **Always consult the official S3 API documentation** for exact request/response format
2. Ensure strict compliance with S3 specification - no custom extensions or deviations
3. Add XML models to `S3XmlResponses.cs` matching S3's exact XML schema
4. Update controller with new endpoint/query parameter handling per S3 spec
5. Add corresponding storage interface methods in `Storage/Abstract/`
6. Implement in both `Storage/InMemory/` and `Storage/Filesystem/`
7. Add integration and unit tests that verify S3 specification compliance

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