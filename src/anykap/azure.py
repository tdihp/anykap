import re
import urllib.request
import urllib.parse
import ipaddress
import datetime
from pathlib import Path
import logging
from . import Uploader, USER_AGENT

logger = logging.getLogger(__name__)

VALID_DOMAINS = [
    ".blob.core.windows.net",
    ".blob.core.chinacloudapi.cn",
    ".blob.core.usgovcloudapi.net",
    ".blob.core.cloudapi.de",
]

AZUREBLOB_ACCOUNT_PATTERN = r"[a-z0-9]{3,24}"
# https://stackoverflow.com/a/35130142/1000290
AZUREBLOB_CONTAINER_PATTERN = r"[a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]"
AZUREBLOB_FQDN_PATTERN = (
    r"(?P<account>"
    + AZUREBLOB_ACCOUNT_PATTERN
    + r")"
    + "|".join(map(re.escape, VALID_DOMAINS))
)
# a best-effort IP addr matching
IPPORT_PATTERN = r"(?P<addr>.*?)(:(?P<port>\d+))?$"


def azureblob_make_url(
    filename: str,
    url=None,
    scheme=None,
    account=None,
    container=None,
    directory=None,
    sas=None,
):
    """
    netloc can be FQDN or ip:port, ip:port should only be for local testing
    account can be either account name or FQDN
    """
    defaults = {  # the default setting
        "scheme": "https",
        "directory": "",
        "sas": "",
    }
    config = {}

    def set_config(k, v):
        if v:
            if config.get(k, v) != v:
                raise ValueError(
                    f"inconsistent setting for {k}: " f"{config[k]!r} != {v!r}"
                )
            config[k] = v

    set_config("scheme", scheme)
    if account:
        if re.fullmatch(AZUREBLOB_ACCOUNT_PATTERN, account):
            set_config("account", account)
        else:
            m = re.fullmatch(AZUREBLOB_FQDN_PATTERN, account)
            if m:
                d = m.groupdict()
                set_config("account", d["account"])
                set_config("fqdn", account)
            else:
                raise ValueError(
                    f"account {account} is not valid Azure storage account"
                )

    set_config("container", container)
    set_config("directory", directory)
    if sas:
        sas = sas.lstrip("?")
    set_config("sas", sas)

    if url:
        parsed = urllib.parse.urlparse(url)
        set_config("scheme", parsed.scheme)
        if parsed.netloc:
            m = re.fullmatch(AZUREBLOB_FQDN_PATTERN, parsed.netloc)
            if m:
                d = m.groupdict()
                set_config("account", d["account"])
                set_config("fqdn", parsed.netloc)
            else:
                m = re.fullmatch(IPPORT_PATTERN, parsed.netloc)
                if not m:
                    raise ValueError(
                        f"failed to parse netloc {parsed.netloc} as ip:port"
                    )
                d = m.groupdict()
                try:
                    ipaddress.ip_address(d["addr"])
                except ValueError as e:
                    raise ValueError(f'unable to parse addr {d["addr"]}') from e
                set_config("ipport", parsed.netloc)

        if parsed.fragment or parsed.params:
            raise ValueError(f"unrecognizable url {url}")
        if parsed.query:
            set_config("sas", parsed.query)
        path = [None] * 2
        if parsed.path:
            path = parsed.path.strip("/").split("/")
            if "ipport" in config:
                if path:
                    set_config("account", path.pop(0))
            if path:
                set_config("container", path.pop(0))
            if path:
                set_config("directory", "/".join(path))

    config = defaults | config
    try:
        if not re.fullmatch(AZUREBLOB_ACCOUNT_PATTERN, config["account"]):
            raise ValueError(f"invalid account {account}")
        if not re.fullmatch(AZUREBLOB_CONTAINER_PATTERN, config["container"]):
            raise ValueError(f"invalid container {container}")
        path = "/" + config["container"] + "/"
        if config["directory"]:
            path += config["directory"] + "/"
        path += filename
        if "ipport" in config:
            path = "/" + config["account"] + path
            return urllib.parse.urlunparse(
                (config["scheme"], config["ipport"], path, "", config["sas"], "")
            )
        elif "fqdn" not in config:
            config["fqdn"] = config["account"] + ".blob.core.windows.net"
        return urllib.parse.urlunparse(
            (config["scheme"], config["fqdn"], path, "", config["sas"], "")
        )
    except KeyError as e:
        (key,) = e.args
        raise ValueError(f"key {key} not provided in upload setting")


def urlopen_worker(request, options):
    logger.debug("sending request with headers: %s", request.headers)
    with urllib.request.urlopen(request, **options) as f:
        logger.debug("url %s got status: %s, headers: %s", f.url, f.status, f.headers)
        logger.debug("body: %r", f.read())


class AzureBlobUploader(Uploader):
    def __init__(self, urlopen_options=None, **kwargs):
        super().__init__()
        self.urlopen_options = urlopen_options
        # validate the storage option just to be sure
        try:
            azureblob_make_url("contoso", **kwargs)
        except Exception as e:
            raise ValueError("Invalid azure blob configuration") from e
        self.blob_options = kwargs
        self.urlopen_options = urlopen_options or {}

    def upload_sync(self, path, name):
        path = Path(path)
        if not path.is_file():
            raise RuntimeError(f"path {path} not a file")

        upload_url = azureblob_make_url(name, **self.blob_options)
        logger.debug("uploading using url %s", upload_url)
        # caller should guarantee sure no further change to the file
        size = path.stat().st_size
        with path.open("rb") as f:
            # https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob
            now = datetime.datetime.now(datetime.timezone.utc)
            request = urllib.request.Request(
                url=upload_url,
                method="PUT",
                data=f,
                headers={
                    "Content-Length": str(size),
                    "Content-Type": "application/octet-stream",
                    "User-Agent": USER_AGENT,
                    "Date": now.strftime("%a, %d %b %Y %H:%M:%S GMT"),
                    "x-ms-version": "2023-11-03",
                    "x-ms-blob-type": "BlockBlob",
                },
            )
            # we don't want loop to be blocked, therefore using thread pool.
            urlopen_worker(request, self.urlopen_options)
        return upload_url.split("?", 1)[0]  # we skip queries for the result
