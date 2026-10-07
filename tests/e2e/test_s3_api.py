"""S3 wire-contract checks against a real Lamina process, using unmodified boto3.

References: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
"""

import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
from urllib.request import Request, urlopen
import uuid
import zlib

from botocore.exceptions import ClientError
import pytest

pytestmark = pytest.mark.boto3


@contextmanager
def s3_error(code, status):
    with pytest.raises(ClientError) as caught:
        yield
    response = caught.value.response
    assert response["Error"]["Code"] == code
    assert response["ResponseMetadata"]["HTTPStatusCode"] == status


def read_object(s3, bucket, key, **kwargs):
    response = s3.get_object(Bucket=bucket, Key=key, **kwargs)
    with response["Body"] as stream:
        return stream.read()


def test_bucket_lifecycle(s3):
    name = f"e2e-boto-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=name)
    try:
        assert s3.head_bucket(Bucket=name)["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert name in {item["Name"] for item in s3.list_buckets()["Buckets"]}
        assert s3.get_bucket_location(Bucket=name)["LocationConstraint"] is None
        assert "Status" not in s3.get_bucket_versioning(Bucket=name)
    finally:
        s3.delete_bucket(Bucket=name)
    with s3_error("404", 404):
        s3.head_bucket(Bucket=name)
    with s3_error("NoSuchBucket", 404):
        s3.get_bucket_location(Bucket=name)


def test_nonempty_bucket_cannot_be_deleted(s3, bucket):
    s3.put_object(Bucket=bucket, Key="kept", Body=b"keep me")
    with s3_error("BucketNotEmpty", 409):
        s3.delete_bucket(Bucket=bucket)
    assert read_object(s3, bucket, "kept") == b"keep me"


@pytest.mark.parametrize(
    "key,body",
    [
        ("empty", b""),
        ("binary", bytes(range(256)) * 4096),
        ("zażółć/日本語 snow + percent%.bin", b"unicode key"),
    ],
    ids=["empty", "binary-1MiB", "unicode-key"],
)
def test_object_roundtrip_overwrite_delete(s3, bucket, key, body):
    result = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/octet-stream",
        Metadata={"source": "boto3"},
    )
    expected_etag = f'"{hashlib.md5(body).hexdigest()}"'
    assert result["ETag"] == expected_etag
    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(body)
    assert head["ContentType"] == "application/octet-stream"
    assert head["Metadata"] == {"source": "boto3"}
    assert head["ETag"] == expected_etag
    assert head["LastModified"].tzinfo is not None
    assert read_object(s3, bucket, key) == body
    s3.put_object(Bucket=bucket, Key=key, Body=b"replacement")
    assert read_object(s3, bucket, key) == b"replacement"
    assert s3.head_object(Bucket=bucket, Key=key)["Metadata"] == {}
    s3.delete_object(Bucket=bucket, Key=key)
    s3.delete_object(Bucket=bucket, Key=key)  # idempotent
    with s3_error("NoSuchKey", 404):
        s3.get_object(Bucket=bucket, Key=key)
    with s3_error("404", 404):
        s3.head_object(Bucket=bucket, Key=key)


@pytest.mark.parametrize(
    "byte_range,expected",
    [
        ("bytes=2-5", b"2345"),
        ("bytes=7-", b"789"),
        ("bytes=-3", b"789"),
    ],
)
def test_byte_ranges(s3, bucket, byte_range, expected):
    s3.put_object(Bucket=bucket, Key="range", Body=b"0123456789")
    response = s3.get_object(Bucket=bucket, Key="range", Range=byte_range)
    with response["Body"] as stream:
        assert stream.read() == expected
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert response["ContentLength"] == len(expected)
    assert response["ContentRange"].endswith("/10")


def test_unsatisfiable_range(s3, bucket):
    s3.put_object(Bucket=bucket, Key="range", Body=b"0123456789")
    with s3_error("InvalidRange", 416):
        s3.get_object(Bucket=bucket, Key="range", Range="bytes=20-30")


@pytest.mark.parametrize("operation", ["get_object", "head_object"])
def test_conditional_requests(s3, bucket, operation):
    etag = s3.put_object(Bucket=bucket, Key="conditional", Body=b"value")["ETag"]
    call = getattr(s3, operation)
    response = call(Bucket=bucket, Key="conditional", IfMatch=etag)
    if "Body" in response:
        response["Body"].close()
    # HEAD has no XML error body; botocore uses the numeric status as its error code.
    with s3_error("412" if operation == "head_object" else "PreconditionFailed", 412):
        call(Bucket=bucket, Key="conditional", IfMatch='"not-the-etag"')
    with s3_error("304", 304):
        call(Bucket=bucket, Key="conditional", IfNoneMatch=etag)


@pytest.mark.parametrize("operation", ["list_objects", "list_objects_v2"])
def test_listing_pagination_and_delimiter(s3, bucket, operation):
    keys = ["page/a", "page/b", "page/nested/c", "page/é", "page/日本", "other"]
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=key.encode())
    pages = list(
        s3.get_paginator(operation).paginate(
            Bucket=bucket, Prefix="page/", PaginationConfig={"PageSize": 2}
        )
    )
    actual = [item["Key"] for page in pages for item in page.get("Contents", [])]
    assert actual == sorted(keys[:-1], key=lambda key: key.encode("utf-8"))
    assert len(pages) == 3
    assert pages[0]["IsTruncated"] is True
    assert pages[-1]["IsTruncated"] is False
    delimited = list(
        s3.get_paginator(operation).paginate(
            Bucket=bucket, Prefix="page/", Delimiter="/", PaginationConfig={"PageSize": 2}
        )
    )
    prefixes = [item["Prefix"] for page in delimited for item in page.get("CommonPrefixes", [])]
    objects = [item["Key"] for page in delimited for item in page.get("Contents", [])]
    assert prefixes == ["page/nested/"]
    assert objects == ["page/a", "page/b", "page/é", "page/日本"]
    assert all(
        len(page.get("Contents", [])) + len(page.get("CommonPrefixes", [])) <= 2
        for page in delimited
    )


def test_list_start_after_and_empty_prefix(s3, bucket):
    for key in ["a", "b", "c"]:
        s3.put_object(Bucket=bucket, Key=key, Body=b"")
    page = s3.list_objects_v2(Bucket=bucket, StartAfter="b")
    assert [item["Key"] for item in page["Contents"]] == ["c"]
    empty = s3.list_objects_v2(Bucket=bucket, Prefix="missing/")
    assert empty.get("Contents", []) == []
    assert empty["KeyCount"] == 0
    assert empty["IsTruncated"] is False


def test_batch_delete(s3, bucket):
    keys = [f"delete/{i}" for i in range(5)]
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=b"value")
    result = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in keys]})
    assert result.get("Errors", []) == []
    assert {item["Key"] for item in result["Deleted"]} == set(keys)
    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []


def test_copy_metadata_and_tags(s3, bucket):
    s3.put_object(
        Bucket=bucket,
        Key="source +日本",
        Body=b"copy me",
        Metadata={"original": "yes"},
        ContentType="text/plain",
        Tagging="team=storage",
    )
    source = {"Bucket": bucket, "Key": "source +日本"}
    s3.copy_object(Bucket=bucket, Key="copied", CopySource=source)
    assert read_object(s3, bucket, "copied") == b"copy me"
    assert s3.head_object(Bucket=bucket, Key="copied")["Metadata"] == {"original": "yes"}
    assert s3.get_object_tagging(Bucket=bucket, Key="copied")["TagSet"] == [
        {"Key": "team", "Value": "storage"}
    ]
    s3.copy_object(
        Bucket=bucket,
        Key="replaced",
        CopySource=source,
        MetadataDirective="REPLACE",
        Metadata={"new": "yes"},
        ContentType="application/custom",
        TaggingDirective="REPLACE",
        Tagging="stage=test",
    )
    head = s3.head_object(Bucket=bucket, Key="replaced")
    assert head["Metadata"] == {"new": "yes"}
    assert head["ContentType"] == "application/custom"
    assert s3.get_object_tagging(Bucket=bucket, Key="replaced")["TagSet"] == [
        {"Key": "stage", "Value": "test"}
    ]


def test_object_tagging(s3, bucket):
    s3.put_object(Bucket=bucket, Key="tagged", Body=b"unchanged")
    tags = [{"Key": "team", "Value": "storage"}, {"Key": "unicode", "Value": "zażółć"}]
    s3.put_object_tagging(Bucket=bucket, Key="tagged", Tagging={"TagSet": tags})
    assert (
        sorted(s3.get_object_tagging(Bucket=bucket, Key="tagged")["TagSet"], key=lambda t: t["Key"])
        == tags
    )
    assert s3.head_object(Bucket=bucket, Key="tagged")["TagCount"] == 2
    s3.delete_object_tagging(Bucket=bucket, Key="tagged")
    assert s3.get_object_tagging(Bucket=bucket, Key="tagged")["TagSet"] == []
    assert read_object(s3, bucket, "tagged") == b"unchanged"


def test_invalid_tags_rejected(s3, bucket):
    s3.put_object(Bucket=bucket, Key="tagged", Body=b"value")
    with s3_error("InvalidTag", 400):
        s3.put_object_tagging(
            Bucket=bucket,
            Key="tagged",
            Tagging={"TagSet": [{"Key": f"key{i}", "Value": "value"} for i in range(11)]},
        )
    with s3_error("NoSuchKey", 404):
        s3.get_object_tagging(Bucket=bucket, Key="missing")


def test_bucket_lifecycle_configuration(s3, bucket):
    with s3_error("NoSuchLifecycleConfiguration", 404):
        s3.get_bucket_lifecycle_configuration(Bucket=bucket)
    rules = [
        {
            "ID": "expire",
            "Status": "Enabled",
            "Filter": {"Prefix": "logs/"},
            "Expiration": {"Days": 7},
        },
        {
            "ID": "abort",
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3},
        },
        {
            "ID": "filtered",
            "Status": "Disabled",
            "Filter": {
                "And": {
                    "Prefix": "archive/",
                    "Tags": [{"Key": "archive", "Value": "yes"}],
                    "ObjectSizeGreaterThan": 10,
                    "ObjectSizeLessThan": 1000,
                }
            },
            "Expiration": {"Days": 30},
        },
    ]
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": rules})
    assert s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"] == rules
    s3.delete_bucket_lifecycle(Bucket=bucket)
    with s3_error("NoSuchLifecycleConfiguration", 404):
        s3.get_bucket_lifecycle_configuration(Bucket=bucket)


def test_invalid_lifecycle_rejected(s3, bucket):
    with s3_error("MalformedXML", 400):
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "bad",
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},
                        "Expiration": {"Days": 0},
                    }
                ]
            },
        )


@pytest.mark.parametrize("algorithm", ["CRC32", "SHA1", "SHA256"])
def test_explicit_checksums(s3, bucket, algorithm):
    body = b"checksum payload\x00\xff"
    digest = (
        zlib.crc32(body).to_bytes(4, "big")
        if algorithm == "CRC32"
        else hashlib.new(algorithm.lower(), body).digest()
    )
    checksum = base64.b64encode(digest).decode()
    field = f"Checksum{algorithm}"
    result = s3.put_object(
        Bucket=bucket, Key="checksum", Body=body, ChecksumAlgorithm=algorithm, **{field: checksum}
    )
    assert result[field] == checksum
    response = s3.get_object(Bucket=bucket, Key="checksum", ChecksumMode="ENABLED")
    with response["Body"] as stream:
        assert stream.read() == body
    assert response[field] == checksum


def test_invalid_content_md5_rejected(s3, bucket):
    with s3_error("BadDigest", 400):
        s3.put_object(
            Bucket=bucket,
            Key="bad-md5",
            Body=b"actual",
            ContentMD5=base64.b64encode(hashlib.md5(b"different").digest()).decode(),
        )
    with s3_error("NoSuchKey", 404):
        s3.get_object(Bucket=bucket, Key="bad-md5")


def test_multipart_complete_and_part_pagination(s3, bucket):
    key = "multipart"
    upload = s3.create_multipart_upload(
        Bucket=bucket, Key=key, Metadata={"upload": "multipart"}, Tagging="source=multipart"
    )["UploadId"]
    bodies = [b"a" * (5 * 1024 * 1024), b"last part"]
    parts = []
    for number, body in enumerate(bodies, 1):
        result = s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload, PartNumber=number, Body=body
        )
        parts.append({"PartNumber": number, "ETag": result["ETag"]})
    first = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload, MaxParts=1)
    assert first["IsTruncated"] is True
    assert [part["PartNumber"] for part in first["Parts"]] == [1]
    second = s3.list_parts(
        Bucket=bucket,
        Key=key,
        UploadId=upload,
        PartNumberMarker=first["NextPartNumberMarker"],
        MaxParts=1,
    )
    assert second["IsTruncated"] is False
    assert [part["PartNumber"] for part in second["Parts"]] == [2]
    result = s3.complete_multipart_upload(
        Bucket=bucket, Key=key, UploadId=upload, MultipartUpload={"Parts": parts}
    )
    digest = hashlib.md5(b"".join(hashlib.md5(body).digest() for body in bodies)).hexdigest()
    assert result["ETag"] == f'"{digest}-2"'
    assert read_object(s3, bucket, key) == b"".join(bodies)
    assert s3.head_object(Bucket=bucket, Key=key)["Metadata"] == {"upload": "multipart"}
    assert s3.get_object_tagging(Bucket=bucket, Key=key)["TagSet"] == [
        {"Key": "source", "Value": "multipart"}
    ]
    with s3_error("NoSuchUpload", 404):
        s3.list_parts(Bucket=bucket, Key=key, UploadId=upload)


def test_multipart_abort_and_listing(s3, bucket):
    upload = s3.create_multipart_upload(Bucket=bucket, Key="abort")["UploadId"]
    s3.upload_part(Bucket=bucket, Key="abort", UploadId=upload, PartNumber=1, Body=b"part")
    listed = s3.list_multipart_uploads(Bucket=bucket)
    assert [(item["Key"], item["UploadId"]) for item in listed["Uploads"]] == [("abort", upload)]
    s3.abort_multipart_upload(Bucket=bucket, Key="abort", UploadId=upload)
    assert s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []) == []
    with s3_error("NoSuchUpload", 404):
        s3.list_parts(Bucket=bucket, Key="abort", UploadId=upload)
    with s3_error("NoSuchKey", 404):
        s3.get_object(Bucket=bucket, Key="abort")


def test_multipart_copy_entire_object(s3, bucket):
    body = bytes(range(256)) * 1024 + b"last bytes\x00\xff"
    source = s3.put_object(Bucket=bucket, Key="copy-source", Body=body)
    upload = s3.create_multipart_upload(Bucket=bucket, Key="copy-target")["UploadId"]
    # Deliberately omit CopySourceRange: copy the entire source into one part.
    result = s3.upload_part_copy(
        Bucket=bucket,
        Key="copy-target",
        UploadId=upload,
        PartNumber=1,
        CopySource={"Bucket": bucket, "Key": "copy-source"},
    )
    part_etag = result["CopyPartResult"]["ETag"]
    assert part_etag == source["ETag"]
    listed = s3.list_parts(Bucket=bucket, Key="copy-target", UploadId=upload)
    assert [(part["PartNumber"], part["Size"], part["ETag"]) for part in listed["Parts"]] == [
        (1, len(body), part_etag)
    ]
    completed = s3.complete_multipart_upload(
        Bucket=bucket,
        Key="copy-target",
        UploadId=upload,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_etag}]},
    )
    expected_etag = f'"{hashlib.md5(hashlib.md5(body).digest()).hexdigest()}-1"'
    assert completed["ETag"] == expected_etag
    head = s3.head_object(Bucket=bucket, Key="copy-target")
    assert head["ContentLength"] == len(body)
    assert head["ETag"] == expected_etag
    assert read_object(s3, bucket, "copy-target") == body
    assert read_object(s3, bucket, "copy-source") == body
    assert s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []) == []


def test_multipart_copy_range(s3, bucket):
    s3.put_object(Bucket=bucket, Key="copy-source", Body=b"0123456789")
    upload = s3.create_multipart_upload(Bucket=bucket, Key="copy-target")["UploadId"]
    result = s3.upload_part_copy(
        Bucket=bucket,
        Key="copy-target",
        UploadId=upload,
        PartNumber=1,
        CopySource={"Bucket": bucket, "Key": "copy-source"},
        CopySourceRange="bytes=2-6",
    )
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key="copy-target",
        UploadId=upload,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": result["CopyPartResult"]["ETag"]}]},
    )
    assert read_object(s3, bucket, "copy-target") == b"23456"


def test_multipart_invalid_part_etag(s3, bucket):
    upload = s3.create_multipart_upload(Bucket=bucket, Key="invalid-part")["UploadId"]
    s3.upload_part(Bucket=bucket, Key="invalid-part", UploadId=upload, PartNumber=1, Body=b"part")
    with s3_error("InvalidPart", 400):
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key="invalid-part",
            UploadId=upload,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": '"wrong"'}]},
        )
    s3.abort_multipart_upload(Bucket=bucket, Key="invalid-part", UploadId=upload)


def test_presigned_put_and_get(s3, bucket):
    key = "presigned +日本"
    body = b"presigned request body"
    put_url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": key, "ContentType": "application/octet-stream"},
        ExpiresIn=60,
    )
    request = Request(
        put_url, data=body, method="PUT", headers={"Content-Type": "application/octet-stream"}
    )
    with urlopen(request, timeout=15) as response:
        assert response.status == 200
    get_url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=60
    )
    with urlopen(get_url, timeout=15) as response:
        assert response.status == 200
        assert response.read() == body
    assert read_object(s3, bucket, key) == body


FORM_CONTENT_TYPES = [
    pytest.param("application/x-www-form-urlencoded", id="urlencoded"),
    pytest.param("application/x-www-form-urlencoded; charset=utf-8", id="urlencoded-charset"),
    pytest.param("multipart/form-data; boundary=lamina-test", id="multipart-form"),
    pytest.param("application/octet-stream", id="octet-stream"),
]
FORM_BODY = b"first=value+with+spaces&second=percent%25&binary=\x00\xff\x80=tail"


@pytest.mark.parametrize("content_type", FORM_CONTENT_TYPES)
@pytest.mark.parametrize("transport", ["boto3", "presigned"])
def test_form_content_type_preserves_object_bytes(s3, bucket, transport, content_type):
    key = f"form-body-{transport}"
    # The payload is opaque object data, not a form (even for multipart/form-data).
    body = FORM_BODY
    params = {"Bucket": bucket, "Key": key, "ContentType": content_type}
    if transport == "boto3":
        s3.put_object(**params, Body=body)
    else:
        url = s3.generate_presigned_url("put_object", Params=params, ExpiresIn=60)
        request = Request(url, data=body, method="PUT", headers={"Content-Type": content_type})
        with urlopen(request, timeout=15) as response:
            assert response.status == 200
    assert read_object(s3, bucket, key) == body
    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(body)
    assert head["ContentType"] == content_type
    assert head["ETag"] == f'"{hashlib.md5(body).hexdigest()}"'


@pytest.mark.parametrize("content_type", FORM_CONTENT_TYPES)
def test_multipart_form_content_type_preserves_part_bytes(s3, bucket, content_type):
    key = "form-part"
    body = FORM_BODY
    object_content_type = "application/x-lamina-object"
    upload = s3.create_multipart_upload(Bucket=bucket, Key=key, ContentType=object_content_type)[
        "UploadId"
    ]
    url = s3.generate_presigned_url(
        "upload_part",
        Params={"Bucket": bucket, "Key": key, "UploadId": upload, "PartNumber": 1},
        ExpiresIn=60,
        HttpMethod="PUT",
    )
    request = Request(url, data=body, method="PUT", headers={"Content-Type": content_type})
    with urlopen(request, timeout=15) as response:
        assert response.status == 200
        part_etag = response.headers["ETag"]
    assert part_etag == f'"{hashlib.md5(body).hexdigest()}"'
    listed = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload)
    assert [(part["PartNumber"], part["Size"], part["ETag"]) for part in listed["Parts"]] == [
        (1, len(body), part_etag)
    ]
    completed = s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_etag}]},
    )
    expected_etag = f'"{hashlib.md5(hashlib.md5(body).digest()).hexdigest()}-1"'
    assert completed["ETag"] == expected_etag
    assert read_object(s3, bucket, key) == body
    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(body)
    assert head["ETag"] == expected_etag
    # UploadPart's Content-Type must not replace metadata from initiation.
    assert head["ContentType"] == object_content_type
    assert s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []) == []


def test_copy_between_buckets(s3, bucket):
    destination = f"e2e-copy-{uuid.uuid4().hex}"
    key = "cross-bucket"
    body = b"cross-bucket content"
    s3.put_object(Bucket=bucket, Key=key, Body=body, Metadata={"origin": "source"})
    s3.create_bucket(Bucket=destination)
    try:
        s3.copy_object(Bucket=destination, Key=key, CopySource={"Bucket": bucket, "Key": key})
        assert read_object(s3, destination, key) == body
        assert read_object(s3, bucket, key) == body
        assert s3.head_object(Bucket=destination, Key=key)["Metadata"] == {"origin": "source"}
    finally:
        s3.delete_object(Bucket=destination, Key=key)
        s3.delete_bucket(Bucket=destination)


def test_parallel_independent_objects(s3, bucket):
    def roundtrip(number):
        key = f"parallel/{number}"
        body = bytes([number]) * 32768
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        assert read_object(s3, bucket, key) == body
        return key

    with ThreadPoolExecutor(max_workers=4) as executor:
        keys = list(executor.map(roundtrip, range(12)))
    assert {item["Key"] for item in s3.list_objects_v2(Bucket=bucket)["Contents"]} == set(keys)
