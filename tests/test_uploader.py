import shutil
import pytest
import subprocess
import socket
import random
import signal
from unittest.mock import Mock, patch
import sys
from inspect import getsource
import ssl
from anykap import *
from anykap.azure import *

try:
    from azure.storage.blob import BlobServiceClient, generate_container_sas

    has_azure_storage_blob = True
except ImportError:
    has_azure_storage_blob = False


@pytest.fixture(scope="session")
def storage_account():
    return "devstoreaccount1"


@pytest.fixture(scope="session")
def shared_key():
    return "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


@pytest.fixture(scope="session")
def blob_server_cert(storage_account, tmp_path_factory):
    """returns cert and key paths"""
    if not shutil.which("openssl"):
        pytest.skip("openssl not available")
    server_name = f"{storage_account}.blob.core.windows.net"
    path = tmp_path_factory.mktemp("openssl")
    # path = tmp_path / openssl
    key_path = str(path / "server.key")
    cert_path = str(path / "server.crt")
    openssl_args = [
        "openssl",
        "req",
        "-batch",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        key_path,
        "-x509",
        "-days",
        "1",
        "-out",
        cert_path,
        "-subj",
        "/CN=" + server_name,
        # add this so 127.0.0.1 canbe verified
        "-addext",
        "subjectAltName=IP:127.0.0.1",
    ]
    subprocess.run(openssl_args, check=True)
    return server_name, cert_path, key_path


@pytest.fixture
@pytest.mark.skipif(not has_azure_storage_blob, reason="azure.storage.blob is required")
def azurite(
    tmp_path, blob_server_cert, unused_tcp_port_factory, storage_account, shared_key
):
    if not shutil.which("azurite"):
        pytest.skip("azureite not installed")
    blob_port = unused_tcp_port_factory()
    queue_port = unused_tcp_port_factory()
    table_port = unused_tcp_port_factory()
    # we use the default account in azurite
    # https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio%2Cblob-storage#well-known-storage-account-and-key

    server_name, cert, key = blob_server_cert
    azurite_path = str(tmp_path / "azurite")
    azurite_args = [
        "azurite",
        "-l",
        azurite_path,
        "--blobPort",
        str(blob_port),
        "--queuePort",
        str(queue_port),
        "--tablePort",
        str(table_port),
        "--cert",
        cert,
        "--key",
        key,
    ]

    p = subprocess.Popen(azurite_args)
    yield server_name, "127.0.0.1", cert, blob_port, queue_port, table_port

    p.terminate()
    p.wait(timeout=10)  # it takes quite long for azurite to terminate


@pytest.fixture
def blob_container(azurite, shared_key, storage_account):
    server_name, server_ip, cert, blob_port, queue_port, table_port = azurite
    blob_service_client = BlobServiceClient(
        f"https://{server_ip}:{blob_port}/{storage_account}",
        credential=shared_key,
        connection_verify=str(cert),
        connection_timeout=1,
        read_timeout=1,
        initial_backoff=1,
        increment_base=0.9,
    )
    container_name = "foobar"  # we use a hard coded container for test now
    now = datetime.datetime.now(datetime.timezone.utc)
    sas = generate_container_sas(
        account_name=storage_account,
        container_name=container_name,
        account_key=shared_key,
        permission="racw",
        start=now,
        expiry=now + datetime.timedelta(hours=1),
    )

    with blob_service_client:
        blob_service_client.create_container(container_name, timeout=1, retry_total=5)
        container_client = blob_service_client.get_container_client(container_name)
        with container_client:
            yield container_client, container_name, sas


@pytest.fixture
def ssl_context(blob_server_cert):
    server_name, cert_path, key_path = blob_server_cert
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cafile=cert_path)
    return context


def test_azureblob_upload_working(
    blob_container, storage_account, azurite, tmp_path, ssl_context
):
    server_name, server_ip, cert, blob_port, queue_port, table_port = azurite
    container_client, container_name, sas = blob_container
    blobfile = tmp_path / "blobdata_working"
    outname = "output.data"
    data = getsource(sys.modules[__name__]).encode()
    # print(f'lines: {lines}')
    blobfile.write_bytes(data)
    uploader = AzureBlobUploader(
        account=storage_account,
        container=container_name,
        sas=sas,
        url=f"https://127.0.0.1:{blob_port}",
        urlopen_options={"context": ssl_context},
    )
    result = uploader.upload_sync(blobfile, outname)
    downloader = container_client.download_blob(outname)
    assert downloader.readall() == data
    assert (
        result
        == f"https://127.0.0.1:{blob_port}/{storage_account}/{container_name}/{outname}"
    )


def test_azureblob_upload_container_notfound(
    blob_container, storage_account, azurite, tmp_path, ssl_context
):
    server_name, server_ip, cert, blob_port, queue_port, table_port = azurite
    container_client, container_name, sas = blob_container
    another_container = "yetanothercontainer"
    blobfile = tmp_path / "blobdata_notfound"
    data = getsource(sys.modules[__name__])
    blobfile.write_text(data)

    uploader = AzureBlobUploader(
        account=storage_account,
        container=another_container,
        sas=sas,
        url=f"https://127.0.0.1:{blob_port}",
        urlopen_options={"context": ssl_context},
    )
    with pytest.raises(urllib.error.HTTPError):
        uploader.upload_sync(blobfile, "output.data")


def test_make_azureblob_url():
    assert (
        azureblob_make_url("foobar.zip", url="http://127.0.0.1/foo/bar/baz")
        == "http://127.0.0.1/foo/bar/baz/foobar.zip"
    )
    assert (
        azureblob_make_url(
            "foobar.zip",
            url="https://127.0.0.1:33333/",
            account="myaccount",
            container="mycontainer",
        )
        == "https://127.0.0.1:33333/myaccount/mycontainer/foobar.zip"
    )
    assert (
        azureblob_make_url(
            "foobar.zip",
            account="myaccount",
            container="mycontainer",
            sas="foobartoken",
        )
        == "https://myaccount.blob.core.windows.net/mycontainer/foobar.zip?foobartoken"
    )

    assert (
        azureblob_make_url(
            "foobar.zip",
            account="myaccount.blob.core.windows.net",
            url="/foobar",
            sas="foobartoken",
        )
        == "https://myaccount.blob.core.windows.net/foobar/foobar.zip?foobartoken"
    )
    assert (
        azureblob_make_url(
            "foobar.zip",
            url="https://myaccount.blob.core.windows.net/foobar?foobartoken",
        )
        == "https://myaccount.blob.core.windows.net/foobar/foobar.zip?foobartoken"
    )
    # forgot to input account
    with pytest.raises(ValueError):
        azureblob_make_url("foobar.zip", url="/foobar", sas="foobartoken")

    with pytest.raises(ValueError):
        azureblob_make_url(
            "foobar.zip", account="contoso.com", url="/foobar", sas="foobartoken"
        )
    with pytest.raises(ValueError):
        azureblob_make_url(
            "foobar.zip",
            account="foobar.blob.core.chinacloudapi.cn",
            url="foobar.blob.core.windows.net",
            container="mycontainer",
        )


def test_copy_uploader(tmp_path):
    data = getsource(sys.modules[__name__]).encode()
    srcpath = tmp_path / "src"
    dstpath = tmp_path / "dst"
    dstpath.mkdir()
    outname = "foobar.data"
    srcpath.write_bytes(data)
    uploader = CopyUploader(dstpath)
    result = uploader.upload_sync(srcpath, outname)
    assert (dstpath / outname).read_bytes() == data
    assert result.removeprefix("file://") == str(dstpath / outname)
