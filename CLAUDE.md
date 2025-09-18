# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a .NET 9.0 ASP.NET Core Web API project called "S3Test" that implements an S3-compatible storage API. The application provides in-memory storage with full support for:
- Bucket operations (create, list, delete)
- Object operations (put, get, delete, head, list)
- Multipart uploads (initiate, upload parts, complete, abort, list parts)
- S3-compliant XML responses

## Development Commands

### Build and Run
```bash
# Build the solution
dotnet build

# Run the application (development)
dotnet run --project S3Test/S3Test.csproj

# Run with specific profile
dotnet run --project S3Test/S3Test.csproj --launch-profile http   # Port 5214
dotnet run --project S3Test/S3Test.csproj --launch-profile https  # Ports 7179/5214
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
docker build -f S3Test/Dockerfile -t s3test .

# Run containerized application
docker run -p 8080:8080 s3test
```

## Project Structure

### Core Components
- **S3Test/Program.cs**: Main application entry point with service registration
- **S3Test/Controllers/**:
  - `S3BucketsController.cs`: Handles bucket operations (PUT, GET, DELETE, HEAD)
  - `S3ObjectsController.cs`: Handles object operations including multipart uploads

### Models
- **S3Test/Models/**:
  - `Bucket.cs`: Bucket entity model
  - `S3Object.cs`: Object entity and related models (PutObjectRequest, GetObjectResponse, etc.)
  - `MultipartUpload.cs`: Multipart upload related models
  - `S3XmlResponses.cs`: XML response DTOs for S3 API compliance

### Services
- **S3Test/Services/**:
  - `IBucketService.cs` / `InMemoryBucketService.cs`: Bucket storage operations
  - `IObjectService.cs` / `InMemoryObjectService.cs`: Object storage operations including multipart

### Tests
- **S3Test.Tests/**:
  - `Controllers/BucketsControllerIntegrationTests.cs`: Bucket API integration tests
  - `Controllers/ObjectsControllerIntegrationTests.cs`: Object API integration tests
  - `Services/BucketServiceTests.cs`: Bucket service unit tests
  - `Services/ObjectServiceTests.cs`: Object service unit tests
  - `Services/MultipartUploadServiceTests.cs`: Multipart upload tests

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
- ETags are computed using MD5 hash of content
- Stored internally without quotes
- Returned in responses with quotes (e.g., `"etag-value"`)
- Comparison normalizes by trimming quotes

### Important Implementation Notes

1. **XML Deserialization**: CompleteMultipartUpload supports both:
   - Non-namespaced XML (common from clients)
   - Namespaced XML (S3 spec: `http://s3.amazonaws.com/doc/2006-03-01/`)

2. **Route Disambiguation**: Query parameters determine operation type:
   - Use `Request.Query.ContainsKey()` to check for parameter presence
   - Parameter value can be empty (e.g., `?uploads` vs `?uploads=`)

3. **Error Responses**: Return S3-compliant error XML with appropriate HTTP status codes

4. **In-Memory Storage**:
   - Uses `ConcurrentDictionary` for thread-safety
   - Multipart uploads store parts temporarily until completion
   - Combined data is created on CompleteMultipartUpload

## Testing Strategy

- **Integration Tests**: Use `WebApplicationFactory` to test full HTTP pipeline
- **Unit Tests**: Test service logic in isolation
- **XML Validation**: Tests verify both request and response XML formats
- **Multipart Flow**: Comprehensive tests for complete upload lifecycle

## Common Development Tasks

### Adding New S3 Operations
1. Check S3 API documentation for request/response format
2. Add XML models to `S3XmlResponses.cs` if needed
3. Update controller with new endpoint/query parameter handling
4. Add corresponding service interface methods
5. Implement in-memory service logic
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