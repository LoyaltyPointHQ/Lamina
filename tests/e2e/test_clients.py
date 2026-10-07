"""Real CLI interoperability, including multipart, pagination and destructive sync.

Each invocation uses the harness's isolated credentials/configuration and real HTTP.
CLI semantics: https://rclone.org/commands/ and
https://min.io/docs/minio/linux/reference/minio-mc/mc-mirror.html
"""

import base64
import hashlib
import json
import uuid

from botocore.exceptions import ClientError
import pytest

pytestmark = pytest.mark.cli

CLIENTS = ("aws", "rclone", "mc")
MIB = 1024 * 1024


def payload(size):
    pattern = bytes(range(256))
    return (pattern * ((size + 255) // 256))[:size]


def remote(client, bucket, key=""):
    return {
        "aws": f"s3://{bucket}/{key}",
        "rclone": f"lamina:{bucket}/{key}",
        "mc": f"lamina/{bucket}/{key}",
    }[client]


def copy(cli, client, source, destination):
    if client == "aws":
        cli.run(client, "s3", "cp", str(source), str(destination))
    else:
        cli.run(client, "copyto" if client == "rclone" else "cp", str(source), str(destination))


def read_object(s3, bucket, key):
    with s3.get_object(Bucket=bucket, Key=key)["Body"] as stream:
        return stream.read()


def assert_missing(s3, bucket, key):
    with pytest.raises(ClientError) as caught:
        s3.head_object(Bucket=bucket, Key=key)
    assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def keys(s3, bucket, prefix=""):
    return {
        item["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
    }


@pytest.mark.parametrize("uploader", (*CLIENTS, "boto3"))
@pytest.mark.parametrize(
    "key,body",
    [
        ("small.bin", payload(4099)),
        ("empty.bin", b""),
        ("zażółć/日本語 snow + percent%.bin", b"unicode and reserved URL characters"),
    ],
    ids=["binary", "empty", "unicode"],
)
def test_all_client_interoperability(cli, s3, bucket, tmp_path, uploader, key, body):
    """All 12 directed cross-client pairs, plus each client's own roundtrip."""
    source = tmp_path / key
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(body)
    if uploader == "boto3":
        s3.put_object(Bucket=bucket, Key=key, Body=body)
    else:
        copy(cli, uploader, source, remote(uploader, bucket, key))
    assert read_object(s3, bucket, key) == body
    assert s3.head_object(Bucket=bucket, Key=key)["ContentLength"] == len(body)
    for downloader in CLIENTS:
        destination = tmp_path / downloader / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        copy(cli, downloader, remote(downloader, bucket, key), destination)
        assert destination.read_bytes() == body, f"{uploader} -> {downloader} corrupted {key}"


@pytest.mark.parametrize(
    "client,size", [("aws", 12 * MIB + 37), ("rclone", 12 * MIB + 37), ("mc", 70 * MIB + 37)]
)
def test_cli_multipart_upload(cli, s3, bucket, tmp_path, client, size):
    source = tmp_path / "multipart.bin"
    expected = payload(size)
    source.write_bytes(expected)
    copy(cli, client, source, remote(client, bucket, "multipart.bin"))
    head = s3.head_object(Bucket=bucket, Key="multipart.bin")
    assert head["ContentLength"] == size
    assert "-" in head["ETag"], "test must exercise actual multipart upload, not a single PUT"
    assert read_object(s3, bucket, "multipart.bin") == expected
    destination = tmp_path / "download.bin"
    copy(cli, client, remote(client, bucket, "multipart.bin"), destination)
    assert destination.read_bytes() == expected
    assert not s3.list_multipart_uploads(Bucket=bucket).get("Uploads", [])


@pytest.mark.parametrize("client", CLIENTS)
def test_cli_server_side_copy_move_delete(cli, s3, bucket, tmp_path, client):
    body = payload(256 * 1024 + 13)
    source = tmp_path / "original.bin"
    source.write_bytes(body)
    original = remote(client, bucket, "original.bin")
    copied = remote(client, bucket, "nested/copied.bin")
    moved = remote(client, bucket, "nested/moved.bin")
    copy(cli, client, source, original)
    copy(cli, client, original, copied)
    assert read_object(s3, bucket, "original.bin") == body
    assert read_object(s3, bucket, "nested/copied.bin") == body
    if client == "aws":
        cli.run(client, "s3", "mv", copied, moved)
        cli.run(client, "s3", "rm", original)
    else:
        cli.run(client, "moveto" if client == "rclone" else "mv", copied, moved)
        cli.run(client, "deletefile" if client == "rclone" else "rm", original)
    assert_missing(s3, bucket, "original.bin")
    assert_missing(s3, bucket, "nested/copied.bin")
    assert read_object(s3, bucket, "nested/moved.bin") == body
    assert keys(s3, bucket) == {"nested/moved.bin"}


@pytest.mark.parametrize("client", CLIENTS)
def test_cli_sync_upload_download_and_delete(cli, s3, bucket, tmp_path, client):
    source = tmp_path / "source"
    files = {
        "small.txt": b"first",
        # Empty PUTs have a dedicated interoperability contract above; keep sync
        # independent so that a zero-byte upload failure cannot hide sync defects.
        "removed.txt": b"remove on the second sync",
        "nested/large.bin": payload(9 * MIB + 7),
        "nested/zażółć 日本語.txt": b"unicode",
    }
    for name, body in files.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    s3.put_object(Bucket=bucket, Key="sync/stale.txt", Body=b"delete me")
    s3.put_object(Bucket=bucket, Key="outside-prefix", Body=b"preserve me")

    def sync(src, dst):
        if client == "aws":
            cli.run(client, "s3", "sync", str(src), str(dst), "--delete")
        elif client == "rclone":
            cli.run(client, "sync", str(src), str(dst))
        else:
            cli.run(client, "mirror", "--overwrite", "--remove", str(src), str(dst))

    target = remote(client, bucket, "sync/")
    sync(source, target)
    assert keys(s3, bucket, "sync/") == {f"sync/{name}" for name in files}
    for name, body in files.items():
        assert read_object(s3, bucket, f"sync/{name}") == body

    (source / "small.txt").write_bytes(b"changed content with different length")
    files["small.txt"] = b"changed content with different length"
    (source / "removed.txt").unlink()
    del files["removed.txt"]
    sync(source, target)
    assert keys(s3, bucket, "sync/") == {f"sync/{name}" for name in files}
    destination = tmp_path / "download"
    destination.mkdir()
    (destination / "local-stale").write_bytes(b"delete locally")
    sync(target, destination)
    downloaded = {
        p.relative_to(destination).as_posix(): p.read_bytes()
        for p in destination.rglob("*")
        if p.is_file()
    }
    assert downloaded == files
    assert read_object(s3, bucket, "outside-prefix") == b"preserve me"


@pytest.mark.parametrize("client", CLIENTS)
def test_cli_bucket_lifecycle(cli, s3, client):
    name = f"e2e-cli-{uuid.uuid4().hex}"
    target = remote(client, name)
    if client == "aws":
        cli.run(client, "s3", "mb", target)
    else:
        cli.run(client, "mkdir" if client == "rclone" else "mb", target)
    try:
        assert s3.head_bucket(Bucket=name)["ResponseMetadata"]["HTTPStatusCode"] == 200
        if client == "aws":
            listing = cli.run(client, "s3", "ls")
        else:
            listing = cli.run(
                client,
                "lsd" if client == "rclone" else "ls",
                "lamina:" if client == "rclone" else "lamina",
            )
        assert name in listing
        if client == "aws":
            cli.run(client, "s3", "rb", target)
        else:
            cli.run(client, "rmdir" if client == "rclone" else "rb", target)
        with pytest.raises(ClientError) as caught:
            s3.head_bucket(Bucket=name)
        assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        # Unlike the bucket fixture, this test owns a bucket created by a CLI.
        if name in {item["Name"] for item in s3.list_buckets()["Buckets"]}:
            s3.delete_bucket(Bucket=name)


def test_aws_cli_api_pagination_checksum_metadata(cli, s3, bucket, tmp_path):
    body = payload(4099)
    source = tmp_path / "checksummed.bin"
    source.write_bytes(body)
    checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
    result = json.loads(
        cli.run(
            "aws",
            "s3api",
            "put-object",
            "--bucket",
            bucket,
            "--key",
            "checksummed.bin",
            "--body",
            str(source),
            "--metadata",
            "source=aws-cli,purpose=e2e",
            "--content-type",
            "application/x-lamina-e2e",
            "--checksum-algorithm",
            "SHA256",
            "--checksum-sha256",
            checksum,
            "--output",
            "json",
        )
    )
    assert result["ChecksumSHA256"] == checksum
    head = s3.head_object(Bucket=bucket, Key="checksummed.bin", ChecksumMode="ENABLED")
    assert head["Metadata"] == {"source": "aws-cli", "purpose": "e2e"}
    assert head["ContentType"] == "application/x-lamina-e2e"
    assert head["ChecksumSHA256"] == checksum
    destination = tmp_path / "retrieved.bin"
    result = json.loads(
        cli.run(
            "aws",
            "s3api",
            "get-object",
            "--bucket",
            bucket,
            "--key",
            "checksummed.bin",
            "--checksum-mode",
            "ENABLED",
            "--output",
            "json",
            str(destination),
        )
    )
    assert result["ChecksumSHA256"] == checksum
    assert destination.read_bytes() == body
    expected = [f"page/item-{i:02d}" for i in range(7)]
    for key in expected:
        s3.put_object(Bucket=bucket, Key=key, Body=key.encode())
    listing = json.loads(
        cli.run(
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            "page/",
            "--page-size",
            "2",
            "--output",
            "json",
        )
    )
    assert [item["Key"] for item in listing["Contents"]] == expected
    first = json.loads(
        cli.run(
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            "page/",
            "--max-keys",
            "2",
            "--no-paginate",
            "--output",
            "json",
        )
    )
    assert first["IsTruncated"] is True
    assert [item["Key"] for item in first["Contents"]] == expected[:2]
    second = json.loads(
        cli.run(
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            "page/",
            "--continuation-token",
            first["NextContinuationToken"],
            "--output",
            "json",
        )
    )
    assert [item["Key"] for item in second["Contents"]] == expected[2:]
