# S3Test - S3-Compatible Storage API

A lightweight, S3-compatible storage API implementation built with .NET 9.0 and ASP.NET Core. This project provides an in-memory storage backend with full support for essential S3 operations, making it ideal for development, testing, and learning purposes.

## Features

- **Full S3 API Compatibility**: Implements core S3 operations with XML request/response format matching AWS S3 specifications
- **Bucket Management**: Create, list, delete, and check bucket existence
- **Object Operations**: Upload, download, delete, and list objects with metadata support
- **Multipart Uploads**: Complete support for large file uploads using S3's multipart upload protocol
- **In-Memory Storage**: Fast, thread-safe storage implementation using ConcurrentDictionary
- **Comprehensive Testing**: 62+ tests covering all operations with both unit and integration tests
- **Docker Support**: Ready-to-deploy containerized application

## Quick Start

### Prerequisites

- .NET 9.0 SDK
- Docker (optional, for containerized deployment)

### Running the Application

```bash
# Clone the repository
git clone [repository-url]
cd S3Test

# Build the project
dotnet build

# Run the application
dotnet run --project S3Test/S3Test.csproj

# The API will be available at:
# http://localhost:5214 (HTTP)
# https://localhost:7179 (HTTPS)
```

### Running with Docker

```bash
# Build the Docker image
docker build -f S3Test/Dockerfile -t s3test .

# Run the container
docker run -p 8080:8080 s3test
```

### Running Tests

```bash
# Run all tests
dotnet test

# Run with detailed output
dotnet test --logger "console;verbosity=detailed"

# Run only unit tests
dotnet test --filter "FullyQualifiedName~S3Test.Tests.Services"

# Run only integration tests
dotnet test --filter "FullyQualifiedName~S3Test.Tests.Controllers"
```

## S3-Compatible API Endpoints

This implementation follows the S3 REST API specification with XML responses.

### Bucket Operations

| Operation | Method | Endpoint | Description |
|-----------|--------|----------|-------------|
| Create Bucket | PUT | `/{bucketName}` | Creates a new bucket |
| List Buckets | GET | `/` | Lists all buckets |
| Delete Bucket | DELETE | `/{bucketName}` | Deletes an empty bucket |
| Head Bucket | HEAD | `/{bucketName}` | Checks if bucket exists |
| List Objects | GET | `/{bucketName}?prefix=...&max-keys=...` | Lists objects in bucket |

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

## Project Structure

```
S3Test/
├── Controllers/
│   ├── S3BucketsController.cs    # S3-compatible bucket operations
│   └── S3ObjectsController.cs     # S3-compatible object operations
├── Models/
│   ├── Bucket.cs                  # Bucket entity
│   ├── S3Object.cs                # Object entities and DTOs
│   ├── MultipartUpload.cs         # Multipart upload models
│   └── S3XmlResponses.cs          # S3 XML response DTOs
├── Services/
│   ├── IBucketService.cs          # Bucket service interface
│   ├── InMemoryBucketService.cs   # Bucket implementation
│   ├── IObjectService.cs          # Object service interface
│   └── InMemoryObjectService.cs   # Object implementation
└── Program.cs                     # Application entry point

S3Test.Tests/
├── Controllers/
│   ├── BucketsControllerIntegrationTests.cs  # 10 tests
│   └── ObjectsControllerIntegrationTests.cs  # 16 tests
└── Services/
    ├── BucketServiceTests.cs                 # 10 tests
    ├── ObjectServiceTests.cs                 # 16 tests
    └── MultipartUploadServiceTests.cs        # 10 tests
```

## Technical Details

### S3 Compatibility
- **XML Responses**: All responses use S3-compliant XML format
- **HTTP Headers**: Supports standard S3 headers (ETag, Content-Type, x-amz-meta-*, etc.)
- **Error Responses**: Returns S3-compatible error XML with appropriate HTTP status codes
- **Namespace Support**: Handles both namespaced and non-namespaced XML for client compatibility

### Storage Implementation
- **Thread-Safe**: Uses `ConcurrentDictionary` for concurrent access
- **In-Memory**: All data is stored in memory (not persistent across restarts)
- **ETag Generation**: MD5 hash of object content
- **Metadata Support**: Custom metadata with `x-amz-meta-*` headers

### Limitations
- No authentication/authorization (accepts any credentials)
- No versioning support
- No bucket policies or ACLs
- No server-side encryption
- Storage is not persistent (in-memory only)
- No support for pre-signed URLs
- No support for object tagging
- No support for bucket lifecycle rules

## Development

### Building from Source

```bash
# Clone the repository
git clone [repository-url]
cd S3Test

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

## Testing Coverage

The project includes 62 comprehensive tests:

- **Unit Tests (36 tests)**
  - BucketServiceTests: 10 tests
  - ObjectServiceTests: 16 tests
  - MultipartUploadServiceTests: 10 tests

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