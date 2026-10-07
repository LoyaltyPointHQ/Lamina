"""Real-process E2E harness. No externally supplied endpoints or credentials.

Every mutable runtime file lives in an owned TemporaryDirectory under /tmp.
Only the child process/container created here is stopped; user storage is never touched.
"""

from contextlib import closing, contextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import time
from urllib.error import URLError
from urllib.request import urlopen
import uuid

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pytest

REPO = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Storage:
    name: str
    data: str
    metadata: str
    mode: str = "Inline"
    provider: str | None = None


STORAGES = {
    item.name: item
    for item in [
        Storage("fs-inline", "Filesystem", "Filesystem"),
        Storage("fs-separate", "Filesystem", "Filesystem", "SeparateDirectory"),
        Storage("fs-xattr", "Filesystem", "Filesystem", "Xattr"),
        Storage("fs-memory", "Filesystem", "InMemory"),
        Storage("fs-sqlite", "Filesystem", "Sql", provider="SQLite"),
        Storage("memory-memory", "InMemory", "InMemory"),
        Storage("memory-inline", "InMemory", "Filesystem"),
        Storage("memory-separate", "InMemory", "Filesystem", "SeparateDirectory"),
        Storage("memory-sqlite", "InMemory", "Sql", provider="SQLite"),
        Storage("fs-postgres", "Filesystem", "Sql", provider="PostgreSQL"),
        Storage("memory-postgres", "InMemory", "Sql", provider="PostgreSQL"),
    ]
}


def pytest_addoption(parser):
    group = parser.getgroup("lamina-e2e")
    group.addoption(
        "--storage",
        action="append",
        choices=tuple(STORAGES),
        help="Repeat for selected combinations; default: fs-inline",
    )
    group.addoption(
        "--storage-matrix",
        choices=("local", "all"),
        help="local: 9 combinations; all: also two PostgreSQL combinations (Podman)",
    )
    group.addoption(
        "--lamina-dll", help="Use an already built Lamina.dll (otherwise build Release once)"
    )
    group.addoption("--container-engine", default="podman", choices=("podman", "docker"))


def pytest_generate_tests(metafunc):
    if "storage" in metafunc.fixturenames:
        config = metafunc.config
        matrix = config.getoption("--storage-matrix")
        selected = config.getoption("--storage")
        if matrix and selected:
            raise pytest.UsageError("Choose --storage or --storage-matrix, not both")
        if matrix:
            selected = [
                name
                for name, item in STORAGES.items()
                if matrix == "all" or item.provider != "PostgreSQL"
            ]
        selected = selected or ["fs-inline"]
        metafunc.parametrize(
            "storage", [STORAGES[name] for name in selected], ids=selected, scope="session"
        )


def pytest_collection_modifyitems(items):
    # Other parametrized tests can otherwise interleave session-scoped profiles,
    # repeatedly rebuilding the same server and PostgreSQL container.
    def profile(item):
        storage = getattr(item, "callspec", None)
        storage = storage.params.get("storage") if storage else None
        return list(STORAGES).index(storage.name) if storage else -1

    items.sort(key=profile)


def clean_env(home):
    # Allowlist rather than inherit AWS profiles, proxies, arbitrary .NET configuration, etc.
    env = {
        key: os.environ[key]
        for key in ("PATH", "DOTNET_ROOT", "DOTNET_ROOT_X64", "LD_LIBRARY_PATH", "SystemRoot")
        if key in os.environ
    }
    env.update(
        HOME=str(home),
        TMPDIR=str(home),
        LANG="C.UTF-8",
        LC_ALL="C.UTF-8",
        NO_PROXY="127.0.0.1,localhost",
        AWS_EC2_METADATA_DISABLED="true",
    )
    return env


@pytest.fixture(scope="session")
def runtime_root():
    with tempfile.TemporaryDirectory(prefix="lamina-e2e-", dir="/tmp") as path:
        yield Path(path)


@pytest.fixture(scope="session", autouse=True)
def isolated_sdk_environment(runtime_root):
    # The SDK and urllib run inside pytest, unlike the isolated CLI subprocesses.
    # Never load a developer's AWS profiles, legacy boto config or proxy settings.
    with pytest.MonkeyPatch.context() as patch:
        for name in tuple(os.environ):
            if name.startswith("AWS_") or name.lower() in {
                "http_proxy",
                "https_proxy",
                "all_proxy",
                "no_proxy",
                "boto_config",
            }:
                patch.delenv(name)
        patch.setenv("AWS_CONFIG_FILE", str(runtime_root / "unused-aws-config"))
        patch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(runtime_root / "unused-credentials"))
        patch.setenv("BOTO_CONFIG", os.devnull)
        patch.setenv("AWS_EC2_METADATA_DISABLED", "true")
        patch.setenv("NO_PROXY", "127.0.0.1,localhost")
        yield


@pytest.fixture
def tmp_path(runtime_root):
    # Override pytest's retained tmp_path: user requested cleanup even after failures.
    with tempfile.TemporaryDirectory(prefix="case-", dir=runtime_root) as path:
        yield Path(path)


@pytest.fixture(scope="session")
def lamina_dll(request):
    supplied = request.config.getoption("--lamina-dll")
    if supplied:
        dll = Path(supplied).resolve()
    else:
        result = subprocess.run(
            ["dotnet", "build", str(REPO / "Lamina/Lamina.csproj"), "-c", "Release", "--nologo"],
            cwd=REPO,
            capture_output=True,
            text=True,
            timeout=240,
        )
        if result.returncode:
            pytest.fail(f"Lamina build failed:\n{result.stdout}\n{result.stderr}")
        dll = REPO / "Lamina/bin/Release/net10.0/Lamina.dll"
    if not dll.is_file():
        pytest.fail(f"Lamina assembly not found: {dll}")
    return dll


@contextmanager
def postgres(storage, root, engine):
    if storage.provider != "PostgreSQL":
        yield None
        return
    if not shutil.which(engine):
        pytest.fail(
            f"{engine} is required for PostgreSQL profiles; use --storage-matrix local without it"
        )
    name = "lamina-e2e-" + uuid.uuid4().hex
    # Private ephemeral trust-auth DB, exposed only to loopback, with storage in tmpfs.
    command = [
        engine,
        "run",
        "--detach",
        "--rm",
        "--name",
        name,
        "--label",
        "lamina.e2e=true",
        "--tmpfs",
        "/tmp/lamina-postgres:rw",
        "--tmpfs",
        "/var/lib/postgresql/data:rw",
        "-e",
        "PGDATA=/tmp/lamina-postgres/data",
        "-e",
        "POSTGRES_HOST_AUTH_METHOD=trust",
        "-e",
        "POSTGRES_DB=lamina",
        "-p",
        "127.0.0.1::5432",
        "docker.io/library/postgres:17.6",
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=180)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            ready = subprocess.run(
                [engine, "exec", name, "pg_isready", "-U", "postgres", "-d", "lamina"],
                capture_output=True,
                timeout=10,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            pytest.fail("E2E PostgreSQL did not become ready in 60 seconds")
        port = (
            subprocess.check_output([engine, "port", name, "5432/tcp"], text=True, timeout=10)
            .strip()
            .rsplit(":", 1)[1]
        )
        yield f"Host=127.0.0.1;Port={port};Database=lamina;Username=postgres"
    finally:
        # Exact UUID-owned name only; never prune volumes or stop unrelated containers.
        result = subprocess.run(
            [engine, "rm", "--force", name], capture_output=True, text=True, timeout=30
        )
        if result.returncode and "no such" not in result.stderr.lower():
            pytest.fail(f"Could not remove E2E container {name}: {result.stderr}")


class Server:
    def __init__(self, dll, root, storage, connection):
        self.dll, self.root, self.storage = dll, root, storage
        self.data_dir, self.metadata_dir = root / "data", root / "metadata"
        self.data_dir.mkdir()
        self.metadata_dir.mkdir()
        self.access_key = "e2e" + secrets.token_hex(8)
        self.secret_key = secrets.token_hex(32)
        self.read_access_key = "e2eread" + secrets.token_hex(8)
        self.read_secret_key = secrets.token_hex(32)
        self.region = "us-east-1"
        self.process = None
        self.log = None
        self.endpoint = None
        self.env = clean_env(root)
        self.env.update(
            {
                "ASPNETCORE_URLS": "http://127.0.0.1:0",
                "ASPNETCORE_ENVIRONMENT": "E2E",
                "ASPNETCORE_CONTENTROOT": str(root),
                "StorageType": storage.data,
                "MetadataStorageType": storage.metadata,
                "FilesystemStorage__DataDirectory": str(self.data_dir),
                "FilesystemStorage__MetadataDirectory": str(self.metadata_dir),
                "FilesystemStorage__MetadataMode": storage.mode,
                "LockManager": "InMemory",
                "Authentication__Enabled": "true",
                "Authentication__Users__0__AccessKeyId": self.access_key,
                "Authentication__Users__0__SecretAccessKey": self.secret_key,
                "Authentication__Users__0__BucketPermissions__0__BucketName": "*",
                "Authentication__Users__0__BucketPermissions__0__Permissions__0": "*",
                "Authentication__Users__1__AccessKeyId": self.read_access_key,
                "Authentication__Users__1__SecretAccessKey": self.read_secret_key,
                "Authentication__Users__1__BucketPermissions__0__BucketName": "*",
                "Authentication__Users__1__BucketPermissions__0__Permissions__0": "read",
                "Authentication__Users__1__BucketPermissions__0__Permissions__1": "list",
                "MetadataCache__Enabled": "true",
                "MultipartUploadCleanup__Enabled": "false",
                "MetadataCleanup__Enabled": "false",
                "TempFileCleanup__Enabled": "false",
                "LifecycleExpiration__Enabled": "false",
                "AutoBucketCreation__Enabled": "false",
                "Logging__LogLevel__Default": "Error",
                "Logging__LogLevel__Microsoft.Hosting.Lifetime": "Information",
                "SqlStorage__EnableSensitiveDataLogging": "false",
            }
        )
        if storage.provider:
            self.env.update(
                {
                    "SqlStorage__Provider": storage.provider,
                    "SqlStorage__MigrateOnStartup": "true",
                    "SqlStorage__ConnectionString": connection
                    or f"Data Source={root / 'metadata.db'}",
                }
            )

    def redact(self, value):
        for sensitive in (
            self.secret_key,
            self.access_key,
            self.read_secret_key,
            self.read_access_key,
        ):
            value = value.replace(sensitive, "[redacted]")
        return re.sub(r"(?i)(X-Amz-Signature=)[a-f0-9]+", r"\1[redacted]", value)

    def start(self):
        log_path = self.root / "server.log"
        self.log = log_path.open("w")
        self.process = subprocess.Popen(
            ["dotnet", str(self.dll)],
            cwd=self.root,
            env=self.env,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            text = log_path.read_text(errors="replace")
            match = re.search(r"Now listening on: (http://127\.0\.0\.1:\d+)", text)
            if match:
                self.endpoint = match.group(1)
                try:
                    with urlopen(self.endpoint + "/health", timeout=1) as response:
                        if response.status == 200:
                            return
                except (OSError, URLError):
                    pass
            if self.process.poll() is not None:
                break
            time.sleep(0.1)
        pytest.fail(
            f"Lamina did not become ready ({self.storage.name}):\n{self.redact(log_path.read_text())}"
        )

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self.log is not None:
            self.log.close()

    def restart(self):
        self.stop()
        self.start()

    def client(self, *, readonly=False, anonymous=False, invalid_secret=False):
        return boto3.Session().client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.read_access_key if readonly else self.access_key,
            aws_secret_access_key=secrets.token_hex(32)
            if invalid_secret
            else (self.read_secret_key if readonly else self.secret_key),
            region_name=self.region,
            config=Config(
                signature_version=UNSIGNED if anonymous else "s3v4",
                proxies={},
                s3={"addressing_style": "path"},
                retries={"max_attempts": 0},
                connect_timeout=5,
                read_timeout=30,
            ),
        )


@pytest.fixture(scope="session")
def server(storage, runtime_root, lamina_dll, request):
    with tempfile.TemporaryDirectory(prefix=storage.name + "-", dir=runtime_root) as path:
        root = Path(path)
        if storage.mode == "Xattr":
            probe = root / "xattr-probe"
            probe.touch()
            try:
                os.setxattr(probe, b"user.lamina_e2e", b"ok")
                assert os.getxattr(probe, b"user.lamina_e2e") == b"ok"
            except (OSError, AttributeError) as exc:
                pytest.fail(f"Selected Xattr profile requires /tmp with user xattrs: {exc}")
            finally:
                probe.unlink()
        with postgres(storage, root, request.config.getoption("--container-engine")) as connection:
            instance = Server(lamina_dll, root, storage, connection)
            try:
                instance.start()
                yield instance
            finally:
                instance.stop()


@pytest.fixture
def s3(server):
    with closing(server.client()) as client:
        yield client


@pytest.fixture
def bucket(server, s3):
    name = "e2e-" + uuid.uuid4().hex
    s3.create_bucket(Bucket=name)
    yield name
    # Use a fresh client: persistence tests may have restarted the process/endpoint.
    with closing(server.client()) as client:
        uploads = list(client.get_paginator("list_multipart_uploads").paginate(Bucket=name))
        for page in uploads:
            for upload in page.get("Uploads", []):
                client.abort_multipart_upload(
                    Bucket=name, Key=upload["Key"], UploadId=upload["UploadId"]
                )
        pages = list(client.get_paginator("list_objects_v2").paginate(Bucket=name))
        for page in pages:
            for item in page.get("Contents", []):
                client.delete_object(Bucket=name, Key=item["Key"])
        client.delete_bucket(Bucket=name)


class Cli:
    def __init__(self, server, root):
        self.server = server
        self.root = root
        self.env = clean_env(root)
        self.env.update(
            AWS_ACCESS_KEY_ID=server.access_key,
            AWS_SECRET_ACCESS_KEY=server.secret_key,
            AWS_DEFAULT_REGION=server.region,
            AWS_CONFIG_FILE=str(root / "aws-config"),
            AWS_SHARED_CREDENTIALS_FILE=str(root / "unused-credentials"),
            AWS_PAGER="",
            AWS_CLI_AUTO_PROMPT="off",
            RCLONE_CONFIG_LAMINA_TYPE="s3",
            RCLONE_CONFIG_LAMINA_PROVIDER="Other",
            RCLONE_CONFIG_LAMINA_ACCESS_KEY_ID=server.access_key,
            RCLONE_CONFIG_LAMINA_SECRET_ACCESS_KEY=server.secret_key,
            RCLONE_CONFIG_LAMINA_ENDPOINT=server.endpoint,
            RCLONE_CONFIG_LAMINA_REGION=server.region,
            RCLONE_CONFIG_LAMINA_FORCE_PATH_STYLE="true",
            RCLONE_CONFIG_LAMINA_UPLOAD_CUTOFF="8Mi",
            RCLONE_CONFIG_LAMINA_CHUNK_SIZE="5Mi",
        )
        (root / "aws-config").write_text(
            "[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n    multipart_threshold = 8MB\n    multipart_chunksize = 5MB\n"
        )
        (root / "rclone.conf").touch()
        mc_dir = root / "mc"
        mc_dir.mkdir()
        config = mc_dir / "config.json"
        config.write_text(
            json.dumps(
                {
                    "version": "10",
                    "aliases": {
                        "lamina": {
                            "url": server.endpoint,
                            "accessKey": server.access_key,
                            "secretKey": server.secret_key,
                            "api": "S3v4",
                            "path": "on",
                        }
                    },
                }
            )
        )
        config.chmod(0o600)

    def run(self, client, *args):
        executable = shutil.which(client)
        if not executable:
            pytest.fail(
                f"Missing {client}; run tests/e2e/install-clients.sh or select -m 'not cli'"
            )
        flags = {
            "aws": [
                "--endpoint-url",
                self.server.endpoint,
                "--no-cli-pager",
                "--cli-connect-timeout",
                "5",
                "--cli-read-timeout",
                "60",
            ],
            "rclone": [
                "--config",
                str(self.root / "rclone.conf"),
                "--contimeout",
                "5s",
                "--timeout",
                "60s",
                "--retries",
                "1",
                "--low-level-retries",
                "1",
            ],
            "mc": ["--config-dir", str(self.root / "mc"), "--disable-pager"],
        }
        try:
            result = subprocess.run(
                [executable, *flags[client], *map(str, args)],
                cwd=self.root,
                env=self.env,
                capture_output=True,
                text=True,
                timeout=180,
            )
        except subprocess.TimeoutExpired:
            pytest.fail(f"{client} exceeded E2E command timeout (180s)")
        if result.returncode:
            pytest.fail(
                f"{client} {args[0]} failed ({result.returncode}):\n{self.server.redact(result.stdout + result.stderr)}"
            )
        return result.stdout


@pytest.fixture
def cli(server, tmp_path):
    return Cli(server, tmp_path)
