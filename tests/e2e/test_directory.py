"""Lamina Directory bucket extension, not AWS S3 Express CreateSession support.

Lamina selects Directory buckets through signed creation headers and uses stable
opaque d1: continuation tokens, including its ListObjects V1 marker extension.
"""

from contextlib import closing
import uuid

from botocore.exceptions import ClientError
import pytest

pytestmark = pytest.mark.boto3

DIRECTORY_KEYS = (
    "root-a",
    "root-z",
    "folder/a",
    "folder/b",
    "folder/deep/c",
    "other/d",
    "unicode/日本",
)


@pytest.fixture
def directory_bucket(server):
    name = f"e2e-directory-{uuid.uuid4().hex}"

    def directory_headers(request, **kwargs):
        request.headers["x-amz-bucket-type"] = "Directory"
        request.headers["x-amz-storage-class"] = "EXPRESS_ONEZONE"

    # Use an independent client so the creation hook cannot affect other fixtures.
    with closing(server.client()) as client:
        event = "before-sign.s3.CreateBucket"
        client.meta.events.register(event, directory_headers)
        try:
            client.create_bucket(Bucket=name)
        finally:
            client.meta.events.unregister(event, directory_headers)
        try:
            for key in DIRECTORY_KEYS:
                client.put_object(Bucket=name, Key=key, Body=key.encode("utf-8"))
            yield client, name
        finally:
            # Cleanup is independent of listing correctness and only touches owned keys.
            for key in DIRECTORY_KEYS:
                client.delete_object(Bucket=name, Key=key)
            client.delete_bucket(Bucket=name)


def test_directory_head_headers(directory_bucket):
    client, name = directory_bucket
    response = client.head_bucket(Bucket=name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    headers = response["ResponseMetadata"]["HTTPHeaders"]
    assert headers["x-amz-bucket-type"] == "Directory"
    assert headers["x-amz-storage-class"] == "EXPRESS_ONEZONE"


def test_directory_v2_pagination_is_complete_and_stable(directory_bucket):
    client, name = directory_bucket
    orders = []
    for _ in range(2):
        pages = list(
            client.get_paginator("list_objects_v2").paginate(
                Bucket=name, PaginationConfig={"PageSize": 2}
            )
        )
        keys = [item["Key"] for page in pages for item in page.get("Contents", [])]
        assert len(keys) == len(DIRECTORY_KEYS)
        assert set(keys) == set(DIRECTORY_KEYS)
        assert len(pages) == 4
        assert all(len(page.get("Contents", [])) <= 2 for page in pages)
        for page in pages[:-1]:
            assert page["IsTruncated"] is True
            assert page["NextContinuationToken"].startswith("d1:")
        assert pages[-1]["IsTruncated"] is False
        orders.append(keys)
    assert orders[0] == orders[1]


def test_directory_delimiter_prefixes_share_page_limit(directory_bucket):
    client, name = directory_bucket
    pages = list(
        client.get_paginator("list_objects_v2").paginate(
            Bucket=name, Delimiter="/", PaginationConfig={"PageSize": 2}
        )
    )
    keys = [item["Key"] for page in pages for item in page.get("Contents", [])]
    prefixes = [item["Prefix"] for page in pages for item in page.get("CommonPrefixes", [])]
    assert sorted(keys) == ["root-a", "root-z"]
    assert sorted(prefixes) == ["folder/", "other/", "unicode/"]
    assert len(pages) == 3
    for page in pages:
        count = len(page.get("Contents", [])) + len(page.get("CommonPrefixes", []))
        assert count <= 2
        assert page["KeyCount"] == count
    nested = client.list_objects_v2(Bucket=name, Prefix="folder/", Delimiter="/")
    assert {item["Key"] for item in nested["Contents"]} == {"folder/a", "folder/b"}
    assert nested["CommonPrefixes"] == [{"Prefix": "folder/deep/"}]


def test_directory_invalid_listing_arguments(directory_bucket):
    client, name = directory_bucket
    for arguments in (
        {"StartAfter": "root-a"},
        {"Delimiter": ","},
        {"Prefix": "folder", "Delimiter": "/"},
    ):
        with pytest.raises(ClientError) as caught:
            client.list_objects_v2(Bucket=name, **arguments)
        response = caught.value.response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert response["Error"]["Code"] == "InvalidArgument"


def test_directory_v1_opaque_marker_extension(directory_bucket):
    client, name = directory_bucket
    pages = list(
        client.get_paginator("list_objects").paginate(Bucket=name, PaginationConfig={"PageSize": 2})
    )
    keys = [item["Key"] for page in pages for item in page.get("Contents", [])]
    assert len(keys) == len(DIRECTORY_KEYS)
    assert set(keys) == set(DIRECTORY_KEYS)
    assert len(pages) == 4
    for page in pages[:-1]:
        assert page["IsTruncated"] is True
        assert page["NextMarker"].startswith("d1:")
        assert len(page["Contents"]) == 2
    assert pages[-1]["IsTruncated"] is False
    v2 = client.list_objects_v2(Bucket=name)
    assert keys == [item["Key"] for item in v2["Contents"]]
