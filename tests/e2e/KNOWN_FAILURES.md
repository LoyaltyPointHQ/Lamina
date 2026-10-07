# Observed compatibility failures

These are **ordinary failing assertions**, not xfails or an allowlist. This file is
an observation of the tested revision, not a promise that the failures will stay
unchanged. The original E2E contribution changed no production code; subsequent
fixes and their focused verification are recorded separately below.

## Findings

| Scenario | Observed behavior | Required behavior / scope |
| --- | --- | --- |
| `test_delete_bucket_after_external_nested_file_removed` | Empty bucket cannot be deleted when only empty filesystem directories remain | Delete the bucket when it contains no S3 objects; Filesystem data profiles |
| `test_explicit_checksums` | Xattr GET response lacks CRC32/SHA1/SHA256 that were supplied on PUT | Return stored requested checksums; `fs-xattr` |
| `test_aws_cli_api_pagination_checksum_metadata` | Xattr HEAD response lacks the uploaded SHA256 | Return stored requested checksum; `fs-xattr` |

Tests for ordinary synchronization use nonempty files; empty transfers have
dedicated interoperability and signed-streaming cases. Tests that
create directories outside S3 clean up their own directories; the deletion bug
has a dedicated assertion instead of being hidden in fixture teardown.

The signed-streaming tests add no proxy or packet inspection: HTTP, SigV4, no
`--checksum` and the pinned mc/minio-go code determine the upload path. The
empty-input case is deliberately separate from nonempty signed chunk boundaries.
Signed trailers and deliberately corrupted chunk signatures remain outside this
E2E group; separate .NET regressions cover invalid no-trailer signed streams.

## Diagnosis boundaries

Read-only investigation confirmed:

- `FilesystemBucketDataStorage` checks for remaining directory entries, so empty
  directories left by external filesystem edits trigger `BucketNotEmpty`.
- The Xattr preflight successfully writes and reads a `user.*` attribute. Its
  checksum response failure is not lack of filesystem xattr support.

No broad failure expectation is coded into the tests. Future product fixes should
turn the corresponding assertions green without changing this suite.

## Contract references

- [S3 error codes: NoSuchUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html)
- [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [DeleteBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html)
- [GetObject checksums](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
- [HeadObject checksums](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)

## Resolved: ListParts for a missing upload — 2026-10-07 UTC

The shared multipart facade now returns `NoSuchUpload` only when both upload
metadata and stored parts are absent. The HTTP endpoint maps this to 404.
An initiated upload with no parts still succeeds, as do stored parts without
metadata (the existing data-first contract). HEAD retains bodyless errors;
UploadPartCopy handles the facade's error result before copying.

TDD evidence: before the fix, the new HTTP tests for ListParts after abort,
after completion, and with an unknown upload ID failed with 200 instead of 404.
The active empty-upload test and existing listing tests already passed.
After the fix:

- `Lamina.Storage.Core.Tests`: **195 passed**; includes all four combinations
  of metadata/parts presence and metadata hints/checksum merging.
- `Lamina.WebApi.Tests`: **563 passed**; includes the new regressions, pagination,
  UploadPartCopy, HEAD and completion heartbeat tests.
- `uv run --project tests/e2e --locked pytest tests/e2e/test_s3_api.py -k multipart --storage-matrix all`:
  **44 passed, 319 deselected in 34.59 seconds**. Abort, completion/pagination,
  copy range and invalid part ETag each passed on all 11 profiles.

Thus `test_multipart_abort_and_listing` and
`test_multipart_complete_and_part_pagination` are no longer known failures.
The full suite was not rerun at that stage. The baseline below remains historical;
the later full run after the empty-mc fix is recorded separately.
Concurrent upload/abort races and filesystem bucket/key identity checks were
not changed by this fix. E2E runtime directories and owned containers were cleaned;
no new container volumes remained.

## Resolved: form Content-Type consuming upload bytes — 2026-10-07 UTC

Default MVC form value providers parsed request bodies before PutObject and
UploadPart could read their raw streams. URL-encoded bodies were consumed;
multipart/form-data with non-form object bytes was rejected. This was not a
generic presigned-authentication or storage defect.

An action-scoped resource filter now removes FormValueProviderFactory,
FormFileValueProviderFactory and JQueryFormValueProviderFactory before model
binding, only on PutObject and UploadPart. Route/query/header binding, checksum
validation and streaming stay intact; no body buffering or rewinding was added.
This follows the [ASP.NET Core streaming upload pattern](https://learn.microsoft.com/en-us/aspnet/core/mvc/models/file-uploads?view=aspnetcore-10.0#upload-large-files-with-streaming).

TDD evidence before the fix:

- New HTTP regressions: **6 failed, 2 passed**. Form content types failed on both
  actions; octet-stream controls passed. Requests included valid Content-MD5.
- Expanded real-process E2E on fs-inline: **9 failed, 3 passed** across boto3
  PutObject, presigned PutObject and presigned UploadPart. All three form types
  failed, while octet-stream controls passed.

Verification after the fix:

- `Lamina.WebApi.Tests`: **573 passed**, including 8 new HTTP cases and 2 filter
  cases, plus existing checksum, authentication, streaming and copy tests.
- `uv run --project tests/e2e --locked pytest tests/e2e/test_s3_api.py -k 'form_content_type or multipart or copy or presigned' --storage-matrix all`:
  **220 passed, 264 deselected in 80.28 seconds**. This includes **132 content-type
  cases** (88 PutObject, 44 UploadPart), and 88 multipart/copy/presigned regressions.
- All 11 storage profiles passed. Payloads include form-like syntax and binary
  bytes. Tests verify full bytes, length, ETag and Content-Type; UploadPart also
  verifies ListParts, Complete and preservation of metadata set at initiation.
- Ruff lint/format, locked dependencies and `git diff --check` passed. E2E storage
  and owned processes/containers were cleaned; no extra container volumes remained.

The full suite was not rerun at that stage. Other endpoints' form binding is
unchanged; the later full run after the empty-mc fix is recorded separately.

## Resolved: empty mc upload without HTTP Content-Length — 2026-10-07 UTC

Pinned mc/minio-go emits a signed terminal AWS chunk even for an empty file. Go
sends that body using HTTP Transfer-Encoding: chunked without Content-Length;
x-amz-decoded-content-length remains zero. The shared upload header guard had
rejected this valid transport before the decoder could verify the terminal signature.

The guard now allows missing Content-Length only for the no-trailer signed
payload marker, HTTP chunked transport, an existing no-trailer validator and one
nonnegative decoded length matching that validator. Ordinary missing-length
requests and trailer variants retain their previous behavior. No empty-object
shortcut or artificial Content-Length is used.

The validated no-trailer parser also now requires the full final chunk/CRLF and
EOF, rejects extra bytes and decoded-length mismatch, stops oversized chunks
before writing them, and propagates cancellation. Both data backends publish
only successful results. Legacy unvalidated parsing and trailer parsing are not
expanded by this fix.

TDD and focused verification:

- Original empty-mc and signed-streaming-empty E2E: **2 failed** on fs-inline
  before the fix, both MissingContentLength; **22 passed** across all 11 profiles
  after the fix (34.28 seconds).
- Header guard RED: **2 failed, 16 passed**. Parser RED: **14 failed, 3 passed**.
  Initial HTTP RED: **24 failed, 2 passed**; another 4 invalid-empty-signature
  cases were subsequently added.
- Final focused .NET: **78 passed**; full WebApi **640 passed**, Storage.Core
  **195 passed**, Filesystem **216 passed**.
- HTTP regressions exercise real signatures and verify absent Content-Length
  and chunked transport on the server. Invalid uploads do not publish or replace
  objects/parts in InMemory; filesystem temp-file publication was also reviewed,
  and its existing storage tests pass.

See the latest full-run results below for all-client E2E coverage. The negative
signature cases above are .NET tests, not deliberate mutations by CLI clients.

## Latest full run, after all three fixes — 2026-10-07 UTC

**1056 cases: 1019 passed, 10 failed, 27 skipped, 0 errors; 533.01 seconds.**
One complete `pytest tests/e2e --storage-matrix all` run, using the pinned boto3,
AWS CLI, rclone and mc clients on all 11 profiles. The 96 variants per profile
include the added whole-object part-copy and expanded form-content-type cases.

All **231 interoperability + signed-streaming cases passed**, including both
empty-mc cases on each profile. The form-content-type and ListParts fixes remain
passing in this full run. Remaining failures are only:

- **6** DeleteBucket failures when empty filesystem directories remain;
- **4** Xattr checksum-response failures (CRC32/SHA1/SHA256 GET and SHA256 HEAD).

The 27 skips are expected profile limitations: 25 direct-filesystem cases with
InMemory data, and 2 restart cases with volatile InMemory metadata.

| Profile | Passed | Failed | Skipped |
| --- | ---: | ---: | ---: |
| `fs-inline` | 95 | 1 | 0 |
| `fs-separate` | 95 | 1 | 0 |
| `fs-xattr` | 91 | 5 | 0 |
| `fs-memory` | 93 | 1 | 2 |
| `fs-sqlite` | 95 | 1 | 0 |
| `memory-memory` | 91 | 0 | 5 |
| `memory-inline` | 91 | 0 | 5 |
| `memory-separate` | 91 | 0 | 5 |
| `memory-sqlite` | 91 | 0 | 5 |
| `fs-postgres` | 95 | 1 | 0 |
| `memory-postgres` | 91 | 0 | 5 |

The ignored root `e2e-report.html` was regenerated from this full run and contains
all case statuses, timings and failure details, grouped by category with tests
as rows and profiles as columns. This replaces the old local HTML, not the
historical baseline recorded below. Runtime directories and owned processes/
containers were cleaned; the pre-existing Podman volume was left untouched.

## Historical full run, before the ListParts, form Content-Type and empty-mc fixes — 2026-10-07 UTC

**935 cases: 832 passed, 76 failed, 27 skipped, 0 errors.**
Executed as three independent parallel batches: 514.38 seconds summed suite time,
185.59 seconds from the first suite start to the last suite end.
All four clients ran across all 11 profiles. The 76 failed cases repeated the
findings above, including the now-resolved ListParts, form Content-Type and empty-mc failures, across profiles;
they were not 76 independent root causes.
The new signed-streaming group contributes **99 cases: 88 passed, 11 failed**;
only the empty-input case fails, once per profile.

| Profile | Passed | Failed | Skipped |
| --- | ---: | ---: | ---: |
| `fs-inline` | 78 | 7 | 0 |
| `fs-separate` | 78 | 7 | 0 |
| `fs-xattr` | 74 | 11 | 0 |
| `fs-memory` | 76 | 7 | 2 |
| `fs-sqlite` | 78 | 7 | 0 |
| `memory-memory` | 74 | 6 | 5 |
| `memory-inline` | 74 | 6 | 5 |
| `memory-separate` | 74 | 6 | 5 |
| `memory-sqlite` | 74 | 6 | 5 |
| `fs-postgres` | 78 | 7 | 0 |
| `memory-postgres` | 74 | 6 | 5 |

Verification environment: .NET SDK 10.0.401 / ASP.NET Core runtime 10.0.12,
Python 3.14.7, boto3 1.42.68 / botocore 1.42.97, AWS CLI 2.34.7,
rclone 1.73.1, mc source revision 7394ce0dd2a8 built with Go 1.25.7,
rootless Podman and PostgreSQL 17.6. An earlier baseline run also verified
environment isolation with a nonexistent ambient AWS profile and broken HTTP(S)
proxy URLs; those injected values were not used in these three batches.

The selected Xattr profile's write/read preflight passed. After the full run:
no Lamina child processes, E2E containers or runtime data directories remained;
the corrected PostgreSQL setup created no additional volumes. The 20 anonymous
volumes from an earlier harness iteration were removed individually with explicit
user approval; the pre-existing volume was left untouched.

Static checks: Ruff lint/format, locked dependency validation, shell syntax and
`git diff --check` pass. Lamina Release build succeeds (0 warnings/errors).
Existing .NET unit/integration tests were not rerun; no .NET source was modified.
