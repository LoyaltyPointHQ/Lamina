# Real-client S3 E2E

Black-box tests of the **real Lamina executable over HTTP**, with authentication
and the actual storage registrations/migrations. No `WebApplicationFactory`,
mocked storage or replacement services. Clients: **boto3, AWS CLI v2, rclone and
MinIO Client (`mc`, not Midnight Commander)**.

These are compatibility tests, **not an assertion that every supported operation
currently works**. Product failures remain failures; no `xfail`, disabled checksum
validation or automatic compatibility fallback.

## Run

From the repository root. Requirements: .NET 10 SDK, Python >=3.12 managed by
[uv](https://docs.astral.sh/uv/), and the three client executables on `PATH`.
The provided installer additionally needs Bash, curl, unzip, sha256sum and Go
(verified with Go 1.25.7). It supports Linux x86_64, requires no sudo, pins the
clients and verifies archive SHA256 hashes. Other platforms can install the same
client versions independently.

```bash
# Once: install clients in a dedicated directory; does not configure real accounts.
tests/e2e/install-clients.sh "$HOME/.local/share/lamina-e2e-clients"
export PATH="$HOME/.local/share/lamina-e2e-clients/bin:$PATH"
uv sync --project tests/e2e --locked

# Default: Filesystem data + Filesystem Inline metadata. Builds Release once.
uv run --project tests/e2e --locked pytest tests/e2e

# All 9 combinations that need no containers.
uv run --project tests/e2e --locked pytest tests/e2e --storage-matrix local

# All 11 combinations, including ephemeral PostgreSQL (rootless Podman by default).
uv run --project tests/e2e --locked pytest tests/e2e --storage-matrix all

# Individual/repeated combinations.
uv run --project tests/e2e --locked pytest tests/e2e --storage fs-inline --storage fs-xattr

# API/storage tests without installed CLI binaries; explicitly selects a smaller suite.
uv run --project tests/e2e --locked pytest tests/e2e -m 'not cli'

# Reuse an existing build, produce a retained JUnit report.
uv run --project tests/e2e --locked pytest tests/e2e \
  --lamina-dll Lamina/bin/Release/net10.0/Lamina.dll --junitxml=e2e-results.xml

# Generate an offline HTML report, including failures and the tested scope.
# Run this even when pytest exits with failures (do not join the commands with &&).
uv run --project tests/e2e --locked python tests/e2e/render_report.py \
  e2e-results.xml --output e2e-report.html
```

Selected profiles require their prerequisites: missing CLI binaries, unsupported
xattrs or an unavailable container engine are **errors**, not silent skips.
`--container-engine docker` is available where the environment requires Docker.
Do not use `pytest-xdist`: a profile's server is intentionally shared and restart
checks are sequential. The collection is grouped by profile to avoid repeated
container startup/migrations.

## Storage matrix

| Profile | Data | Metadata |
| --- | --- | --- |
| `fs-inline` (default) | Filesystem | Filesystem / Inline |
| `fs-separate` | Filesystem | Filesystem / SeparateDirectory |
| `fs-xattr` | Filesystem | Filesystem / Xattr |
| `fs-memory` | Filesystem | InMemory |
| `fs-sqlite` | Filesystem | SQL / SQLite |
| `memory-memory` | InMemory | InMemory |
| `memory-inline` | InMemory | Filesystem / Inline |
| `memory-separate` | InMemory | Filesystem / SeparateDirectory |
| `memory-sqlite` | InMemory | SQL / SQLite |
| `fs-postgres` | Filesystem | SQL / PostgreSQL |
| `memory-postgres` | InMemory | SQL / PostgreSQL |

`StorageType=Sql` is not a valid data backend: SQL stores metadata. InMemory data
+ Xattr is intentionally not offered because xattrs need physical object files.
Xattr is checked with an actual `user.*` write/read in `/tmp` before starting the
profile. Modern Linux tmpfs supports `user.*` xattrs; `/tmp` need not be tmpfs.

## Coverage

- **All four clients:** upload, download and cross-client byte equality; binary,
  empty and Unicode/reserved-character names. Each uploader is checked against
  boto3 and all three CLI downloaders.
- **CLI workflows:** bucket create/list/remove; server-side copy, move, delete;
  upload/download sync with deletion, updates, nested Unicode paths and protection
  of an unrelated prefix; AWS API-level pagination, metadata and SHA256.
- **Multipart:** actual multipart transfers from all three CLI clients, verified
  by multipart ETags and content; boto3 initiate/upload/list-parts pagination,
  completion, abort, listing, invalid part ETag, whole-object and byte-range part
  copy. Whole-object part copy omits `CopySourceRange` and verifies listed part
  size/ETag, completed object bytes/size/multipart ETag, unchanged source bytes
  and upload cleanup. Small
  bounded payloads: ~12 MiB for AWS/rclone and ~70 MiB for mc.
- **Signed aws-chunked:** explicit mc single PUT (`--disable-multipart`) and
  multipart on HTTP + SigV4 without `--checksum`. Single PUT covers zero/one byte,
  the 64-KiB chunk boundary, exact/partial multiple chunks and a ~70-MiB object
  above the normal multipart threshold. Checks include decoded length, exact ETag,
  complete downloaded bytes and absence of unfinished multipart uploads.
- **S3 contract:** bucket lifecycle and errors, CRUD/overwrite/idempotent deletion,
  MD5 ETags, ranges, conditional GET/HEAD, V1/V2 pagination and delimiters,
  UTF-8 ordering, start-after, batch delete, same/cross-bucket copy, metadata,
  tagging/directives, lifecycle CRUD and validation, explicit CRC32/SHA1/SHA256
  checked on GET/HEAD and after CopyObject,
  rejected Content-MD5, presigned PUT/GET, concurrent independent objects.
- **Opaque upload bodies:** boto3/presigned PutObject and presigned UploadPart
  preserve raw bytes for URL-encoded Content-Type (with/without charset),
  multipart/form-data (payload deliberately not a valid form), and octet-stream.
  Checks include binary/form-like bytes, length, ETag and object Content-Type;
  UploadPart also covers ListParts, Complete and metadata set at initiation.
- **Authentication:** real SigV4, rejection of anonymous/bad signatures, read-only
  permissions (read/list succeed; write/delete/tag/multipart initiation denied),
  anonymous health endpoint.
- **Directory buckets:** Lamina's signed creation-header extension, HEAD headers,
  deterministic V2 pagination, combined prefix/object page limits, invalid
  arguments and the V1 `d1:` marker extension. Not AWS S3 Express CreateSession.
- **Storage:** direct filesystem add/change/delete with metadata refresh,
  repeated GET/HEAD after external changes verifying refreshed SHA256/ETag and
  preserved user metadata/tags/content type,
  exclusion of internal files/directories, empty-directory deletion semantics;
  process restart preserving object bytes, checksums, metadata, tags, lifecycle and an
  incomplete multipart upload that is completed after restart.

All selected profiles execute the same client/API cases. Only direct filesystem
mutation tests on InMemory data, and persistence tests on volatile data or
metadata, are skipped with explicit reasons. InMemory configurations still run
all ordinary client/API cases.

### Signed streaming contract

```bash
uv run --project tests/e2e --locked pytest tests/e2e -m signed_streaming --storage-matrix all
```

This separate group depends on the pinned mc/minio-go versions, not on generic
SigV4 support. The harness creates a SigV4 alias pointing at its own **HTTP**
endpoint. Tests deliberately do not pass `--checksum`: in mc revision
`7394ce0dd2a8` that option disables SHA256 streaming payload signing. minio-go
7.0.90 selects `StreamingSignV4` on HTTP when payload SHA256 is enabled;
`--disable-multipart` selects single PUT, not unsigned payloads. Uploads that
exceed the default 16-MiB minio-go part size otherwise use multipart, with signed
streaming for each part. There is an explicit HTTP guard against accidentally
switching these tests to the client's HTTPS/unsigned path.

The choice of encoding is established by those client/configuration invariants;
**no traffic capture or signature logging** is used. The tests do not claim
coverage of signed trailers or rejection of deliberately damaged chunk signatures.
Review this contract when upgrading mc/minio-go; other client versions are not
automatically equivalent. Ordinary default-checksum tests remain unchanged.

For empty files this mc version emits a signed terminal AWS chunk (86 encoded
bytes) using HTTP `Transfer-Encoding: chunked`, without `Content-Length`, and
with `x-amz-decoded-content-length: 0`. Lamina accepts this no-trailer signed
streaming path on PutObject and UploadPart only with a streaming validator and a
valid matching decoded length. It still validates the terminal signature, full
framing and decoded byte count; zero is not a shortcut around validation.
Ordinary uploads without Content-Length remain rejected. This exception does
not add support for unknown-length trailer streaming or HTTP/2 uploads.

The separate .NET parser/HTTP regressions cover truncated streams, damaged final
signatures (including empty payloads), length mismatch, cancellation, and rejected
overwrites preserving existing data. These negative cases are not generated by
the unmodified CLI clients in this E2E group.

Pinned upstream references:
[mc upload options](https://github.com/minio/mc/blob/7394ce0dd2a80935aded936b09fa12cbb3cb8096/cmd/client-s3.go#L1089-L1108),
[minio-go signing selection](https://github.com/minio/minio-go/blob/68fb5ee339f2e3a798c14d12ca0e04c51f304d58/api.go#L914-L942),
[streaming signer](https://github.com/minio/minio-go/blob/68fb5ee339f2e3a798c14d12ca0e04c51f304d58/pkg/signer/request-signature-streaming.go).

Protocol reference: [AWS SigV4 streaming and transfer length](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html).

## Isolation and cleanup

- Only a newly launched, loopback-bound Lamina is used. No option accepts a remote
  endpoint, existing bucket, real credentials, existing database or data path.
- All runtime files (data, metadata, SQLite DB, uploads/downloads, server logs,
  CLI HOME/config and generated test-only credentials) live in an owned
  `/tmp/lamina-e2e-*` directory, including test case directories. The harness
  overrides pytest's usual retained `tmp_path` behavior.
- Child environments are allowlisted. boto3's AWS profiles/config and proxy
  environment are separately isolated inside pytest, without reading user
  credential files. API authentication stays enabled; generated keys are redacted
  from reported CLI/server errors. Do not enable `--showlocals` or client debug
  logging, which may expose temporary test credentials.
- Each test owns UUID-named buckets. Bucket teardown aborts uploads and removes
  objects; the profile teardown stops **its own process** and removes the entire
  temporary directory even when API cleanup or an assertion fails.
- PostgreSQL uses an exact UUID-named disposable container, a loopback-only
  dynamic port and trust auth **only for this ephemeral test DB**. Its `PGDATA`
  lives in container tmpfs at `/tmp/lamina-postgres/data`; the image's declared
  volume path is also covered by tmpfs, so no named/anonymous data volume is
  created. Teardown removes only this container, never prunes volumes.
- Cleanup runs on normal completion, failures and Python/pytest interruption
  (Ctrl-C). SIGKILL, a host crash or forced CI termination cannot guarantee Python
  finalizers. Build outputs, dependency caches, installed clients and an explicitly
  requested JUnit report are not runtime storage and are retained.

## Versions and CI

Pins: boto3 1.42.68 (full transitive versions/hashes in `uv.lock`), AWS CLI 2.34.7,
rclone 1.73.1, MinIO mc release `RELEASE.2025-08-13T08-35-41Z` / source commit
`7394ce0dd2a8`, PostgreSQL image `postgres:17.6`. The mc binary archive currently
returns HTTP 410; the installer builds that pinned Go module, verified by Go's
checksum database. Its `--version` reports `DEVELOPMENT.GOGET`; `go version -m`
records the exact source revision (also included in CI artifacts).

`.github/workflows/e2e.yml` provides a **manual `workflow_dispatch`** workflow with
11 independent jobs, fail-fast disabled and per-profile HTML/JUnit/client-version
artifacts. It does not hide failures or replace the existing .NET test workflow.
Manual triggering is intentional while documented compatibility failures remain.

```bash
uv run --project tests/e2e --locked ruff check tests/e2e
uv run --project tests/e2e --locked ruff format --check tests/e2e
bash -n tests/e2e/install-clients.sh
```

## Boundaries

This suite does not claim exhaustive deployment coverage. Redis/distributed
locking, multi-replica behavior, NFS/CIFS mounts, TLS/HTTP2, Helm, network-fault
injection and sustained large-scale load are outside this matrix. Cleanup and
lifecycle-expiration background jobs are disabled for deterministic client tests;
lifecycle **configuration** is covered, timed expiration is not. Metadata caching
is enabled; the zero-copy multipart option uses its normal default.

## Contract references

- [AWS API reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
- [AWS integrity defaults](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html):
  no `WHEN_REQUIRED` workaround; AWS CLI and boto3 keep normal checksum behavior.
- [AWS CLI S3 configuration](https://docs.aws.amazon.com/cli/latest/topic/s3-config.html)
- [boto3 configuration](https://docs.aws.amazon.com/boto3/latest/guide/configuration.html)
- [rclone S3](https://rclone.org/s3/) and [mc source](https://github.com/minio/mc/tree/RELEASE.2025-08-13T08-35-41Z)
- [Linux tmpfs xattrs](https://docs.kernel.org/filesystems/tmpfs.html)
