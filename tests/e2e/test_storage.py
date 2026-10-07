"""Black-box storage guarantees, including filesystem mutations outside the API.

Filesystem cases deliberately use Lamina's documented data-first extension, not
an AWS feature. Persistence cases exclude volatile data/metadata combinations.
"""

from contextlib import closing
import hashlib
import os
import uuid

from botocore.exceptions import ClientError
import pytest


def require_filesystem(server):
    if server.storage.data != "Filesystem":
        pytest.skip("Direct disk mutation requires filesystem object data")


def require_persistence(server):
    require_filesystem(server)
    if server.storage.metadata == "InMemory":
        pytest.skip("InMemory metadata is intentionally lost on process restart")


def read_object(client, bucket, key):
    response = client.get_object(Bucket=bucket, Key=key)
    with response["Body"] as stream:
        return stream.read()


def listed_keys(client, bucket, **kwargs):
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, **kwargs)
    return {item["Key"] for page in pages for item in page.get("Contents", [])}


@pytest.mark.filesystem
def test_direct_filesystem_create_modify_delete(server, s3, bucket):
    require_filesystem(server)
    key = "external/nested/zażółć.txt"
    path = server.data_dir / bucket / key
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        original = b"created without the S3 API"
        path.write_bytes(original)

        assert read_object(s3, bucket, key) == original
        first = s3.head_object(Bucket=bucket, Key=key)
        assert first["ContentLength"] == len(original)
        assert first["ETag"] == f'"{hashlib.md5(original).hexdigest()}"'
        assert key in listed_keys(s3, bucket)

        replacement = b"changed outside the API; cached metadata must not win"
        previous_mtime = path.stat().st_mtime_ns
        path.write_bytes(replacement)
        # Avoid sleeps and filesystem timestamp granularity races; force a newer mtime.
        modified = max(path.stat().st_mtime_ns, previous_mtime + 2_000_000_000)
        os.utime(path, ns=(modified, modified))
        assert read_object(s3, bucket, key) == replacement
        updated = s3.head_object(Bucket=bucket, Key=key)
        assert updated["ContentLength"] == len(replacement)
        assert updated["ETag"] == f'"{hashlib.md5(replacement).hexdigest()}"'
        assert updated["LastModified"] > first["LastModified"]

        path.unlink()
        assert key not in listed_keys(s3, bucket)
        with pytest.raises(ClientError) as caught:
            s3.get_object(Bucket=bucket, Key=key)
        assert caught.value.response["Error"]["Code"] == "NoSuchKey"
        assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        path.unlink(missing_ok=True)
        # These two directories belong only to this test's out-of-band write.
        path.parent.rmdir()
        path.parent.parent.rmdir()


@pytest.mark.filesystem
def test_internal_files_are_not_objects_or_common_prefixes(server, s3, bucket):
    require_filesystem(server)
    visible = {"visible.txt", "nested/visible.txt"}
    for key in visible:
        s3.put_object(Bucket=bucket, Key=key, Body=b"visible", Metadata={"source": "e2e"})
    hidden = [
        ".lamina-meta/e2e-internal-only.json",
        "nested/.lamina-meta/e2e-internal-only.json",
        ".lamina-tmp-e2e-leftover",
        "nested/.lamina-tmp-e2e-leftover",
        ".lamina-tmp-e2e-directory/hidden.txt",
    ]
    paths = [server.data_dir / bucket / key for key in hidden]
    try:
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"internal test artifact")
        assert listed_keys(s3, bucket, PaginationConfig={"PageSize": 1}) == visible
        for prefix, expected_keys, expected_prefixes in [
            ("", {"visible.txt"}, {"nested/"}),
            ("nested/", {"nested/visible.txt"}, set()),
        ]:
            result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
            assert {item["Key"] for item in result.get("Contents", [])} == expected_keys
            assert {
                item["Prefix"] for item in result.get("CommonPrefixes", [])
            } == expected_prefixes
    finally:
        # Do not leave artificial metadata for the bucket cleanup fixture to encounter.
        for path in paths:
            path.unlink(missing_ok=True)
        for relative in (".lamina-tmp-e2e-directory", "nested/.lamina-meta", ".lamina-meta"):
            directory = server.data_dir / bucket / relative
            # Inline mode may have legitimate metadata here: never remove it.
            if directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()


@pytest.mark.filesystem
def test_delete_bucket_after_external_nested_file_removed(server, s3):
    """Empty filesystem directories are not S3 objects and cannot keep a bucket nonempty."""
    require_filesystem(server)
    bucket = f"e2e-external-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=bucket)
    path = server.data_dir / bucket / "external" / "nested" / "object.txt"
    deleted = False
    try:
        path.parent.mkdir(parents=True)
        path.write_bytes(b"external object")
        assert read_object(s3, bucket, "external/nested/object.txt") == b"external object"
        with pytest.raises(ClientError) as error:
            s3.delete_bucket(Bucket=bucket)
        assert error.value.response["Error"]["Code"] == "BucketNotEmpty"
        assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert path.read_bytes() == b"external object"
        path.unlink()
        assert listed_keys(s3, bucket) == set()
        result = s3.delete_bucket(Bucket=bucket)
        deleted = True
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 204
        assert not (server.data_dir / bucket).exists()
        assert bucket not in {item["Name"] for item in s3.list_buckets()["Buckets"]}
        with pytest.raises(ClientError) as error:
            s3.head_bucket(Bucket=bucket)
        assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        if not deleted:
            path.unlink(missing_ok=True)
            for directory in (path.parent, path.parent.parent):
                if directory.is_dir() and not any(directory.iterdir()):
                    directory.rmdir()
            s3.delete_bucket(Bucket=bucket)


@pytest.mark.persistence
def test_restart_preserves_objects_metadata_tags_and_lifecycle(server, s3, bucket):
    require_persistence(server)
    key = "persistent/nested/object.bin"
    body = bytes(range(256)) * 4096
    metadata = {"source": "restart-e2e", "custom": "preserve me"}
    tags = [{"Key": "purpose", "Value": "restart"}]
    lifecycle = {
        "Rules": [
            {
                "ID": "retain-for-test",
                "Status": "Enabled",
                "Filter": {"Prefix": "persistent/"},
                "Expiration": {"Days": 3650},
            }
        ]
    }
    uploaded = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        Metadata=metadata,
        ContentType="application/x-lamina-e2e",
        Tagging="purpose=restart",
    )
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)
    server.restart()
    with closing(server.client()) as client:
        assert bucket in {item["Name"] for item in client.list_buckets()["Buckets"]}
        assert read_object(client, bucket, key) == body
        head = client.head_object(Bucket=bucket, Key=key)
        assert head["Metadata"] == metadata
        assert head["ContentType"] == "application/x-lamina-e2e"
        assert head["ContentLength"] == len(body)
        assert head["ETag"] == uploaded["ETag"]
        assert client.get_object_tagging(Bucket=bucket, Key=key)["TagSet"] == tags
        assert (
            client.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"] == lifecycle["Rules"]
        )
        assert key in listed_keys(client, bucket)
        client.delete_bucket_lifecycle(Bucket=bucket)


@pytest.mark.persistence
def test_restart_preserves_incomplete_multipart_upload(server, s3, bucket):
    require_persistence(server)
    key = "restart-multipart.bin"
    body = b"multipart data persisted across a real process restart" * 1024
    initiated = s3.create_multipart_upload(
        Bucket=bucket,
        Key=key,
        ContentType="application/x-multipart-e2e",
        Metadata={"source": "restart"},
        Tagging="purpose=multipart",
    )
    upload_id = initiated["UploadId"]
    part = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=body)
    server.restart()
    with closing(server.client()) as client:
        uploads = client.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
        assert any(item["UploadId"] == upload_id and item["Key"] == key for item in uploads)
        parts = client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)["Parts"]
        assert [(p["PartNumber"], p["Size"], p["ETag"]) for p in parts] == [
            (1, len(body), part["ETag"])
        ]
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]},
        )
        assert read_object(client, bucket, key) == body
        head = client.head_object(Bucket=bucket, Key=key)
        assert head["Metadata"] == {"source": "restart"}
        assert head["ContentType"] == "application/x-multipart-e2e"
        assert client.get_object_tagging(Bucket=bucket, Key=key)["TagSet"] == [
            {"Key": "purpose", "Value": "multipart"}
        ]
        assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
