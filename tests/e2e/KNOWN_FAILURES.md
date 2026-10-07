# Observed compatibility failures

These are **ordinary failing assertions**, not xfails or an allowlist. This file is
an observation of the tested revision, not a promise that the failures will stay
unchanged. The E2E contribution intentionally changes no production code.

## Findings

| Scenario | Observed behavior | Required behavior / scope |
| --- | --- | --- |
| `test_multipart_complete_and_part_pagination` | `ListParts` succeeds after completion | `NoSuchUpload`, HTTP 404; all profiles |
| `test_multipart_abort_and_listing` | `ListParts` succeeds after abort | `NoSuchUpload`, HTTP 404; all profiles |
| `test_form_content_type_preserves_object_bytes[boto3]` | `application/x-www-form-urlencoded` PUT fails with a checksum calculated over empty bytes | Store the supplied bytes; all profiles |
| `test_form_content_type_preserves_object_bytes[presigned]` | Form-content-type presigned PUT returns success, but subsequent GET returns empty bytes | Store the supplied bytes; all profiles. Ordinary octet-stream presigned PUT/GET passes |
| `test_all_client_interoperability[empty-mc]` | MinIO mc cannot upload a zero-byte file: server returns a missing Content-Length error | Zero-byte upload must work; all profiles. Other clients' empty uploads and mc's nonempty transfers are tested independently |
| `test_mc_signed_streaming_single_put[empty]` | The explicit HTTP/SigV4 single-PUT path also rejects a zero-byte file with MissingContentLength | Empty-file compatibility edge case; all profiles. This alone does not attribute the defect to Lamina's chunk decoder |
| `test_delete_bucket_after_external_nested_file_removed` | Empty bucket cannot be deleted when only empty filesystem directories remain | Delete the bucket when it contains no S3 objects; Filesystem data profiles |
| `test_explicit_checksums` | Xattr GET response lacks CRC32/SHA1/SHA256 that were supplied on PUT | Return stored requested checksums; `fs-xattr` |
| `test_aws_cli_api_pagination_checksum_metadata` | Xattr HEAD response lacks the uploaded SHA256 | Return stored requested checksum; `fs-xattr` |

Tests for ordinary synchronization use nonempty files so the separate mc empty-PUT
failure does not prevent checking the rest of sync/delete behavior. Tests that
create directories outside S3 clean up their own directories; the deletion bug
has a dedicated assertion instead of being hidden in fixture teardown.

The signed-streaming tests add no proxy or packet inspection: HTTP, SigV4, no
`--checksum` and the pinned mc/minio-go code determine the upload path. The
empty-input case is deliberately separate from nonempty signed chunk boundaries;
minio-go's signer calculates encoded stream length zero for empty input. Its
MissingContentLength result is recorded as a compatibility failure, not proof
of a particular server-side decoder defect. Signed trailers and deliberately
corrupted chunk signatures remain outside this E2E group.

## Diagnosis boundaries

Read-only investigation confirmed:

- `S3MultipartController.ListParts` / `MultipartUploadStorageFacade.ListPartsAsync`
  can return a successful empty list for a missing upload.
- Form-urlencoded payload consumption is content-type-specific, not a generic
  presigned-signature failure. Explicit octet-stream passes the same transfer.
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

## Verified full run — 2026-10-07 UTC

**935 cases: 832 passed, 76 failed, 27 skipped, 0 errors.**
Executed as three independent parallel batches: 514.38 seconds summed suite time,
185.59 seconds from the first suite start to the last suite end.
All four clients ran across all 11 profiles. The 76 failed cases repeat the
findings above across profiles; they are not 76 independent root causes.
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
