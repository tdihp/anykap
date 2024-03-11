# ruff: noqa
import urllib

AZUREBLOB_ACCOUNT_PATTERN = r"[a-z0-9]{3,24}"


# I don't want to throw this away, it is rather better IMO than the official SDK
# today
def azureblob_sign_request(method, url, headers):
    """returns Authorization header content"""
    import hmac
    import hashlib

    # https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#blob-queue-and-file-services-shared-key-authorization
    HEADERS_TO_SIGN = [
        "Content-Encoding",
        "Content-Language",
        "Content-Length",
        "Content-MD5",
        "Content-Type",
        "Date",
        "If-Modified-Since",
        "If-Match",
        "If-None-Match",
        "If-Unmodified-Since",
        "Range",
    ]
    sign_lines = [method.upper()]
    sign_lines += [headers.get(k, "") for k in HEADERS_TO_SIGN]
    sign_lines += [
        lk + ":" + sv
        for lk, sv in sorted(
            (k.lower(), headers[k].strip())
            for k in headers.keys()
            if k.lower().startswith("x-ms-")
        )
    ]
    # For our use case CanonicalizedResource should only be
    # /account/container/blob (hence /account + url.path)
    parsed_url = urllib.parse.urlparse(url)
    account = AZUREBLOB_ACCOUNT_PATTERN.match(parsed_url.netloc).groupdict()["account"]
    sign_lines.append("/" + "account" + parsed_url.path)
    sign_lines.append("")  # the final new line
    result = "\n".join(sign_lines).encode("utf-8")
    signed_hmac_sha256 = hmac.HMAC(key, string_to_sign, hashlib.sha256)
