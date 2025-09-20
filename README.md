# Lamina - S3-Compatible Storage API

A lightweight, S3-compatible storage API implementation built with .NET 9.0 and ASP.NET Core. This project provides both in-memory and filesystem storage backends with full support for essential S3 operations, making it ideal for development, testing, and production use cases.

## Features

- **Full S3 API Compatibility**: Implements core S3 operations with XML request/response format matching AWS S3 specifications
- **Dual Storage Backends**:
  - **In-Memory Storage**: Fast, thread-safe storage using ConcurrentDictionary
  - **Filesystem Storage**: Persistent storage with separate data and metadata directories
- **Bucket Management**: Create, list, delete (including force delete), and check bucket existence
- **Object Operations**: Upload, download, delete, and list objects with metadata support
- **Multipart Uploads**: Complete support for large file uploads using S3's multipart upload protocol with streaming assembly
- **AWS Signature V4 Authentication**: Optional authentication with per-bucket permissions
- **Automatic Cleanup**: Background service for cleaning up stale multipart uploads
- **Thread-Safe Operations**: FileSystemLockManager ensures concurrent access safety
- **SHA1 ETags**: Secure ETag generation using SHA1 hash algorithm
- **Comprehensive Testing**: 65+ tests covering all operations with both unit and integration tests
- **Docker Support**: Ready-to-deploy containerized application

## Quick Start

### Prerequisites

- .NET 9.0 SDK
- Docker (optional, for containerized deployment)

### Running the Application

```bash
# Clone the repository
git clone [repository-url]
cd Lamina

# Build the project
dotnet build

# Run the application
dotnet run --project Lamina/Lamina.csproj

# The API will be available at:
# http://localhost:5214 (HTTP)
# https://localhost:7179 (HTTPS)
```

### Running with Docker

```bash
# Build the Docker image
docker build -f Lamina/Dockerfile -t lamina .

# Run the container
docker run -p 8080:8080 lamina
```

### Running Tests

```bash
# Run all tests
dotnet test

# Run with detailed output
dotnet test --logger "console;verbosity=detailed"

# Run only unit tests
dotnet test --filter "FullyQualifiedName~Lamina.Tests.Services"

# Run only integration tests
dotnet test --filter "FullyQualifiedName~Lamina.Tests.Controllers"
```

## S3-Compatible API Endpoints

This implementation follows the S3 REST API specification with XML responses.

### Bucket Operations

| Operation | Method | Endpoint | Description |
|-----------|--------|----------|-------------|
| Create Bucket | PUT | `/{bucketName}` | Creates a new bucket |
| List Buckets | GET | `/` | Lists all buckets |
| Delete Bucket | DELETE | `/{bucketName}` | Deletes an empty bucket |
| Delete Bucket (Force) | DELETE | `/{bucketName}?force=true` | Deletes bucket and all contents |
| Head Bucket | HEAD | `/{bucketName}` | Checks if bucket exists |
| List Objects | GET | `/{bucketName}?prefix=...&max-keys=...&delimiter=...` | Lists objects in bucket |

### Object Operations

| Operation | Method | Endpoint | Description |
|-----------|--------|----------|-------------|
| Put Object | PUT | `/{bucketName}/{key}` | Uploads an object |
| Get Object | GET | `/{bucketName}/{key}` | Downloads an object |
| Delete Object | DELETE | `/{bucketName}/{key}` | Deletes an object |
| Head Object | HEAD | `/{bucketName}/{key}` | Gets object metadata |

### Multipart Upload Operations

| Operation | Method | Endpoint | Description |
|-----------|--------|----------|-------------|
| Initiate Upload | POST | `/{bucket}/{key}?uploads` | Starts multipart upload |
| Upload Part | PUT | `/{bucket}/{key}?partNumber=N&uploadId=ID` | Uploads a part |
| Complete Upload | POST | `/{bucket}/{key}?uploadId=ID` | Completes upload |
| Abort Upload | DELETE | `/{bucket}/{key}?uploadId=ID` | Cancels upload |
| List Parts | GET | `/{bucket}/{key}?uploadId=ID` | Lists uploaded parts |
| List Uploads | GET | `/{bucket}?uploads` | Lists active uploads |

## Usage Examples

### Using AWS CLI

```bash
# Configure AWS CLI to use local endpoint
aws configure set default.s3.signature_version s3v4
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Create a bucket
aws s3 mb s3://my-bucket --endpoint-url http://localhost:5214

# Upload a file
aws s3 cp file.txt s3://my-bucket/file.txt --endpoint-url http://localhost:5214

# List objects
aws s3 ls s3://my-bucket --endpoint-url http://localhost:5214

# Download a file
aws s3 cp s3://my-bucket/file.txt downloaded.txt --endpoint-url http://localhost:5214
```

### Using curl

```bash
# Create a bucket
curl -X PUT http://localhost:5214/my-bucket

# Upload an object
curl -X PUT http://localhost:5214/my-bucket/test.txt \
  -H "Content-Type: text/plain" \
  -d "Hello, World!"

# Get an object
curl http://localhost:5214/my-bucket/test.txt

# List objects in bucket (returns XML)
curl http://localhost:5214/my-bucket
```

### Multipart Upload Example

```bash
# 1. Initiate multipart upload (returns XML with UploadId)
curl -X POST http://localhost:5214/my-bucket/large-file.bin?uploads

# 2. Upload parts (save ETags from response headers)
curl -X PUT "http://localhost:5214/my-bucket/large-file.bin?partNumber=1&uploadId=$UPLOAD_ID" \
  -d "Part 1 data" -i

curl -X PUT "http://localhost:5214/my-bucket/large-file.bin?partNumber=2&uploadId=$UPLOAD_ID" \
  -d "Part 2 data" -i

# 3. Complete the upload (use ETags from part uploads)
curl -X POST "http://localhost:5214/my-bucket/large-file.bin?uploadId=$UPLOAD_ID" \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>etag-from-part1</ETag>
    </Part>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>etag-from-part2</ETag>
    </Part>
</CompleteMultipartUpload>'
```

## Technical Details

### Architecture

The application follows a layered architecture with clear separation of concerns:

- **Controllers**: Handle HTTP requests and S3 API compatibility
- **Storage Layer**: Organized into three main components:
  - **Abstract**: Interfaces and facade implementations that orchestrate operations
  - **InMemory**: In-memory storage implementations
  - **Filesystem**: Filesystem-based storage implementations
- **Services**: Supporting services like authentication and cleanup
- **Models**: Data models and S3 XML response DTOs

### Storage Layer Organization

The storage layer uses a facade pattern to orchestrate operations between data and metadata storage components. It is organized into three main directories:

- **Abstract**: Contains interfaces and facade implementations that define the storage contracts
- **InMemory**: In-memory storage implementations using ConcurrentDictionary for thread-safety
- **Filesystem**: Filesystem-based storage implementations with FileSystemLockManager for concurrent access

### S3 Compatibility
- **XML Responses**: All responses use S3-compliant XML format
- **HTTP Headers**: Supports standard S3 headers (ETag, Content-Type, x-amz-meta-*, etc.)
- **Error Responses**: Returns S3-compatible error XML with appropriate HTTP status codes
- **Namespace Support**: Handles both namespaced and non-namespaced XML for client compatibility
- **AWS Signature V4**: Optional authentication compatible with AWS SDKs and CLI

### Storage Implementations

#### In-Memory Storage
- **Thread-Safe**: Uses `ConcurrentDictionary` for concurrent access
- **Fast Performance**: All data is stored in memory
- **Non-Persistent**: Data is lost on restart
- **Best For**: Development, testing, temporary storage
- **Implementation**: Located in `Storage/InMemory/` directory

#### Filesystem Storage
- **Persistent**: Data survives application restarts
- **Thread-Safe**: FileSystemLockManager ensures safe concurrent access
- **Scalable**: Limited only by disk space
- **Directory Structure**:
  - Data: `/configured/path/data/{bucket}/{key}`
  - Metadata: `/configured/path/metadata/{bucket}/{key}.json`
- **Best For**: Production use, persistent storage needs
- **Implementation**: Located in `Storage/Filesystem/` directory

### Common Features
- **ETag Generation**: SHA1 hash of object content (more secure than MD5)
- **Metadata Support**: Custom metadata with `x-amz-meta-*` headers
- **Binary Data**: Full support for binary file uploads/downloads
- **Streaming**: Efficient streaming for multipart assembly and object transfers
- **Automatic Cleanup**: Configurable background service for stale upload cleanup

### Configuration Options

#### Storage Backend
```json
{
  "StorageType": "InMemory",  // or "Filesystem"
  "FilesystemStorage": {
    "DataDirectory": "/tmp/laminas/data",
    "MetadataDirectory": "/tmp/laminas/metadata"
  }
}
```

#### Authentication
```json
{
  "Authentication": {
    "Enabled": false,  // Set to true to enable AWS Signature V4
    "Users": [
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

#### Multipart Upload Cleanup
```json
{
  "MultipartUploadCleanup": {
    "Enabled": true,  // Enable/disable automatic cleanup
    "CleanupIntervalMinutes": 60,  // How often to run cleanup (default: 60)
    "UploadTimeoutHours": 24  // Uploads older than this are cleaned up (default: 24)
  }
}
```

### Current Limitations
- No versioning support
- No bucket policies or ACLs
- No server-side encryption
- No support for pre-signed URLs
- No support for object tagging
- No support for bucket lifecycle rules
- No cross-region replication
- No S3 Select or analytics features

## Development

### Building from Source

```bash
# Clone the repository
git clone [repository-url]
cd Lamina

# Restore dependencies
dotnet restore

# Build the solution
dotnet build

# Run tests
dotnet test
```

### Adding New Features

1. Check the [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
2. Add XML models to `S3XmlResponses.cs` if needed
3. Update controllers with new endpoints
4. Implement service methods in the in-memory services
5. Add comprehensive unit and integration tests

### Debugging Tips

- Enable console logging in controllers to see raw XML requests/responses
- Use `dotnet test --logger "console;verbosity=detailed"` for detailed test output
- Check XML namespace compatibility when dealing with different S3 clients
- Monitor ETag handling - stored without quotes internally, returned with quotes
- ETags use SHA1 hashing (not MD5) for improved security
- Check cleanup service logs to monitor stale multipart upload removal

## Testing Coverage

The project includes 65+ comprehensive tests:

- **Unit Tests (39 tests)**
  - BucketServiceTests: 10 tests
  - ObjectServiceTests: 16 tests
  - MultipartUploadServiceTests: 10 tests
  - MultipartUploadCleanupServiceTests: 3 tests

- **Integration Tests (26 tests)**
  - BucketsControllerIntegrationTests: 10 tests
  - ObjectsControllerIntegrationTests: 16 tests

All tests verify both positive and negative scenarios, including error handling and edge cases.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

### Guidelines
- Follow existing code patterns and conventions
- Add tests for any new functionality
- Ensure all tests pass before submitting PR
- Update documentation as needed

## License

[Add your license information here]

## Acknowledgments

This implementation is inspired by the AWS S3 API specification and designed for educational, development, and testing purposes. It provides a lightweight alternative to full S3 or MinIO for scenarios where a simple S3-compatible API is needed.