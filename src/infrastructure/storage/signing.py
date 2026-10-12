"""AWS Signature Version 4, for talking to MinIO.

Written out rather than pulled in. `boto3` would be one line, and it brings
`botocore` -- tens of megabytes of service models -- into an image that per-service images
sized by walking every reachable import. The signing algorithm itself is public,
stable since 2012, and about eighty lines; the trade is a small amount of code
here against a large dependency in every image that carries this module.

The failure mode also argues for it: a wrong signature is answered with
`403 SignatureDoesNotMatch` on the first request, which is loud and immediate.
This is not encryption and nothing is kept secret by it -- the secret key is
used to derive a signing key and never travels.

Reference: AWS "Signature Version 4 signing process", the `AWS4-HMAC-SHA256`
scheme that MinIO implements.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime
from urllib.parse import quote

ALGORITHM = "AWS4-HMAC-SHA256"
#: Signing an empty body still requires its hash; this is sha256(b"").
EMPTY_PAYLOAD_SHA256 = hashlib.sha256(b"").hexdigest()


def _sign(key: bytes, message: str) -> bytes:
    """Подписать запрос к S3 по методу, адресу и содержимому тела."""
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    """Получить ключ подписи SigV4 для даты, региона и сервиса."""
    key = _sign(f"AWS4{secret_key}".encode(), date_stamp)
    key = _sign(key, region)
    key = _sign(key, service)
    return _sign(key, "aws4_request")


def _canonical_query(params: dict[str, str]) -> str:
    # Sorted by key, each part percent-encoded. S3 is strict about this: an
    # unsorted query string produces a valid-looking signature that the server
    # computes differently and rejects.
    """Отсортировать и закодировать параметры для подписи запроса S3."""
    return "&".join(
        f"{quote(name, safe='-_.~')}={quote(value, safe='-_.~')}"
        for name, value in sorted(params.items())
    )


def canonical_uri(key: str) -> str:
    """Percent-encode an object key for the request line.

    `/` stays literal because it separates path segments; everything else is
    encoded, including the characters a key is allowed to contain and a URL is
    not.
    """
    return quote(key, safe="/-_.~")


def sign_request(
    *,
    method: str,
    host: str,
    uri: str,
    query: dict[str, str],
    region: str,
    access_key_id: str,
    secret_key: str,
    now: datetime,
    payload_sha256: str = EMPTY_PAYLOAD_SHA256,
    service: str = "s3",
) -> dict[str, str]:
    """Headers that authenticate one request, including the signature itself.

    `now` is passed in rather than read here so a test can pin it: a signature
    is a function of the clock, and a signing routine that reads the clock
    cannot be compared against a known-good value.
    """
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    canonical_headers = f"""host:{host}
x-amz-content-sha256:{payload_sha256}
x-amz-date:{amz_date}
"""
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        [
            method,
            uri,
            _canonical_query(query),
            canonical_headers,
            signed_headers,
            payload_sha256,
        ]
    )

    scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            ALGORITHM,
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(secret_key, date_stamp, region, service),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    authorization = (
        f"{ALGORITHM} Credential={access_key_id}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return {
        "Authorization": authorization,
        "x-amz-content-sha256": payload_sha256,
        "x-amz-date": amz_date,
    }
