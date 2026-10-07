"""Real middleware authentication and authorization, never mocked handlers."""

from contextlib import closing
from urllib.request import urlopen

from botocore.exceptions import ClientError
import pytest

pytestmark = pytest.mark.boto3


def test_anonymous_health(server):
    with urlopen(server.endpoint + "/health", timeout=5) as response:
        assert response.status == 200
        assert response.read() == b"Healthy"


@pytest.mark.parametrize(
    "options", [{"anonymous": True}, {"invalid_secret": True}], ids=["unsigned", "wrong-signature"]
)
def test_reject_unauthenticated_object_read(server, s3, bucket, options):
    s3.put_object(Bucket=bucket, Key="private", Body=b"not public")
    with closing(server.client(**options)) as unauthorized:
        with pytest.raises(ClientError) as caught:
            unauthorized.get_object(Bucket=bucket, Key="private")
        assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_readonly_user_can_read_and_list(server, s3, bucket):
    s3.put_object(Bucket=bucket, Key="allowed", Body=b"readable")
    with closing(server.client(readonly=True)) as reader:
        with reader.get_object(Bucket=bucket, Key="allowed")["Body"] as body:
            assert body.read() == b"readable"
        assert reader.head_object(Bucket=bucket, Key="allowed")["ContentLength"] == 8
        assert [o["Key"] for o in reader.list_objects_v2(Bucket=bucket)["Contents"]] == ["allowed"]


@pytest.mark.parametrize(
    "operation", ["put_object", "delete_object", "create_multipart_upload", "put_object_tagging"]
)
def test_readonly_user_cannot_mutate(server, s3, bucket, operation):
    s3.put_object(Bucket=bucket, Key="protected", Body=b"unchanged")
    arguments = {"Bucket": bucket, "Key": "protected"}
    if operation == "put_object":
        arguments["Body"] = b"changed"
    if operation == "put_object_tagging":
        arguments["Tagging"] = {"TagSet": [{"Key": "bad", "Value": "mutation"}]}
    with closing(server.client(readonly=True)) as reader:
        with pytest.raises(ClientError) as caught:
            getattr(reader, operation)(**arguments)
        assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    with s3.get_object(Bucket=bucket, Key="protected")["Body"] as body:
        assert body.read() == b"unchanged"
    assert not s3.get_object_tagging(Bucket=bucket, Key="protected")["TagSet"]
    assert not s3.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
