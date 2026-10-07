"""Signed aws-chunked uploads using the pinned MinIO mc, without a wire proxy.

The harness configures S3v4. HTTP plus no --checksum selects StreamingSignV4;
--disable-multipart changes single PUT vs UploadPart, not chunk signing.
This contract is specific to mc 7394ce0dd2a8 and minio-go v7.0.90:
https://github.com/minio/mc/blob/7394ce0dd2a8/cmd/client-s3.go#L1089-L1108
https://github.com/minio/minio-go/blob/v7.0.90/api.go#L918-L927
https://github.com/minio/minio-go/blob/v7.0.90/api-put-object.go#L337-L365

These are positive signed-payload tests, not corrupted-signature or trailer tests.
"""

import hashlib
import random
from urllib.parse import urlsplit

import pytest

pytestmark = [pytest.mark.cli, pytest.mark.signed_streaming]

CHUNK_SIZE = 64 * 1024
MIB = 1024 * 1024


@pytest.mark.parametrize(
    "size",
    [
        0,
        1,
        CHUNK_SIZE - 1,
        CHUNK_SIZE,
        CHUNK_SIZE + 1,
        3 * CHUNK_SIZE,
        3 * CHUNK_SIZE + 17,
        70 * MIB + 37,
    ],
    ids=[
        "empty",
        "one-byte",
        "chunk-minus-one",
        "exact-chunk",
        "chunk-plus-one",
        "three-chunks",
        "three-chunks-plus-tail",
        "large-forced-single-put",
    ],
)
def test_mc_signed_streaming_single_put(cli, server, s3, bucket, tmp_path, size):
    assert urlsplit(server.endpoint).scheme == "http", "TLS changes mc payload signing"
    # Distinct deterministic binary chunks detect reordering, duplication and truncation.
    expected = random.Random(20261007).randbytes(size)
    source = tmp_path / "signed.bin"
    source.write_bytes(expected)
    key = "signed-single.bin"

    # Do not add --checksum: mc uses it to disable SHA256 payload signing.
    cli.run("mc", "cp", "--disable-multipart", source, f"lamina/{bucket}/{key}")

    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == size
    assert head["ETag"].strip('"') == hashlib.md5(expected).hexdigest(), (
        "--disable-multipart must produce a single PUT ETag even above the multipart threshold"
    )
    with s3.get_object(Bucket=bucket, Key=key)["Body"] as stream:
        assert stream.read() == expected


def test_mc_signed_streaming_multipart(cli, server, s3, bucket, tmp_path):
    assert urlsplit(server.endpoint).scheme == "http", "TLS changes mc payload signing"
    expected = random.Random(20261008).randbytes(70 * MIB + 37)
    source = tmp_path / "signed-multipart.bin"
    source.write_bytes(expected)
    key = "signed-multipart.bin"

    # No --checksum and no --disable-multipart: each UploadPart is signed streaming.
    cli.run("mc", "cp", source, f"lamina/{bucket}/{key}")

    # minio-go v7.0.90 defaults to 16 MiB parts for this known object size:
    # https://github.com/minio/minio-go/blob/v7.0.90/constants.go#L26-L28
    # https://github.com/minio/minio-go/blob/v7.0.90/api-put-object-common.go#L114-L127
    part_size = 16 * MIB
    part_hashes = [
        hashlib.md5(expected[offset : offset + part_size]).digest()
        for offset in range(0, len(expected), part_size)
    ]
    expected_etag = f"{hashlib.md5(b''.join(part_hashes)).hexdigest()}-{len(part_hashes)}"
    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(expected)
    assert head["ETag"].strip('"') == expected_etag, "test must exercise actual multipart"
    with s3.get_object(Bucket=bucket, Key=key)["Body"] as stream:
        assert stream.read() == expected
    assert not s3.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
