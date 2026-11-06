"""Read an operator-admitted offline projection; no Gateway/harness imports.

Only the environment selects this shared public identity. Report content,
preferences and tenant files cannot override it. Python 3.10 and shipped Pillow
are sufficient. The immutable result owns all bytes for one report operation.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import stat
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from PIL import Image

MAX_BYTES = 2 * 1024 * 1024
MAX_ASSET = 256 * 1024
MEMBER = "product-identity.json"
UNAVAILABLE = "Shared product identity is unavailable or invalid; configured reports were not published."


@dataclass(frozen=True)
class ProductIdentity:
    digest: str
    product_name: str
    product_logo: str
    provider_name: str
    provider_logo: str
    support_url: str
    record: bytes


def _canonical(value) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _object(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError("fields")
    return value


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def _name(value):
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 40
        or value != value.strip()
        or any(unicodedata.category(c) in {"Cc", "Cs", "Cf", "Zl", "Zp"} for c in value)
    ):
        raise ValueError("name")


def _support(value):
    if (
        not isinstance(value, str)
        or len(value) > 2048
        or "\\" in value
        or any(
            c.isspace() or unicodedata.category(c) in {"Cc", "Cs", "Cf"} for c in value
        )
    ):
        raise ValueError("support")
    url = urlsplit(value)
    if (
        url.scheme != "https"
        or not url.hostname
        or url.username is not None
        or url.password is not None
        or "%" in url.netloc
        or (url.port is not None and not 0 < url.port <= 65535)
    ):
        raise ValueError("support")


def _image(value, *, icon=False) -> tuple[bytes, bytes]:
    prefix = "data:image/png;base64,"
    if (
        not isinstance(value, str)
        or not value.startswith(prefix)
        or len(value) > len(prefix) + 4 * ((MAX_ASSET + 2) // 3)
    ):
        raise ValueError("inline PNG")
    data = base64.b64decode(value[len(prefix) :], validate=True)
    if (
        len(data) > MAX_ASSET
        or base64.b64encode(data).decode("ascii") != value[len(prefix) :]
        or not data.startswith(b"\x89PNG\r\n\x1a\n")
    ):
        raise ValueError("PNG bounds")
    # Gateway projections contain re-encoded RGBA pixels only. Reject ancillary
    # metadata, animation, active/trailing content and forged final IEND chunks.
    offset = 8
    kinds = []
    while offset < len(data):
        if len(data) - offset < 12:
            raise ValueError("PNG framing")
        length = int.from_bytes(data[offset : offset + 4], "big")
        kind = data[offset + 4 : offset + 8]
        kinds.append(kind)
        offset += 12 + length
        if (
            kind not in {b"IHDR", b"IDAT", b"IEND"}
            or offset > len(data)
            or (kind == b"IEND" and (length or offset != len(data)))
        ):
            raise ValueError("PNG content")
    if (
        not kinds
        or kinds[0] != b"IHDR"
        or kinds[-1] != b"IEND"
        or kinds.count(b"IHDR") != 1
        or b"IDAT" not in kinds
    ):
        raise ValueError("PNG framing")
    with Image.open(io.BytesIO(data), formats=["PNG"]) as image:
        width, height = image.size
        if (
            not (1 <= width <= 1024 and 1 <= height <= 1024)
            or image.mode != "RGBA"
            or getattr(image, "n_frames", 1) != 1
        ):
            raise ValueError("PNG pixels")
        if icon and (width != height or not 32 <= width <= 256):
            raise ValueError("icon")
        image.verify()
    with Image.open(io.BytesIO(data), formats=["PNG"]) as image:
        # Actually decode all pixels; a filename/header/CRC check is insufficient.
        clean = Image.frombytes("RGBA", image.size, image.convert("RGBA").tobytes())
        sanitized = io.BytesIO()
        clean.save(sanitized, format="PNG")
        if len(sanitized.getvalue()) > MAX_ASSET:
            raise ValueError("processed PNG bounds")
    # Retain the verified, already sanitized source bytes so different shipped
    # Pillow encoders cannot change the source identity's content binding.
    return data, sanitized.getvalue()


def _read(selected: str) -> bytes:
    path = Path(selected)
    if not path.is_absolute() or ".." in path.parts or str(path) != selected:
        raise ValueError("absolute normalized path required")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
            )
            os.close(fd)
            fd = child
        source = os.open(
            path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd
        )
        with os.fdopen(source, "rb") as stream:
            info = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_size > MAX_BYTES
            ):
                raise ValueError("bounded regular snapshot required")
            data = stream.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise ValueError("snapshot size")
            return data
    finally:
        os.close(fd)


def load_product_identity() -> ProductIdentity | None:
    selected = os.environ.get("HARTMESH_REPORT_PRODUCT_IDENTITY")
    expected = os.environ.get("HARTMESH_REPORT_PRODUCT_SHA256")
    if (
        selected is None
        and expected is None
        and "HARTMESH_PRODUCT_PAYLOAD" not in os.environ
    ):
        return None
    try:
        if (
            not selected
            or not isinstance(expected, str)
            or re.fullmatch(r"[0-9a-f]{64}", expected) is None
        ):
            raise ValueError("explicit selection and binding required")
        record = _object(
            json.loads(_read(selected).decode("utf-8"), object_pairs_hook=_unique),
            {"schema_version", "identity", "sha256"},
        )
        if type(record["schema_version"]) is not int or record["schema_version"] != 1:
            raise ValueError("projection version")
        identity = _object(
            record["identity"],
            {"schema_version", "mode", "name", "product", "provider"},
        )
        if (
            type(identity["schema_version"]) is not int
            or identity["schema_version"] != 1
            or identity["mode"] != "configured"
        ):
            raise ValueError("identity version/mode")
        product = _object(identity["product"], {"name", "logo", "browser_icon"})
        provider = _object(identity["provider"], {"name", "logo", "support_url"})
        _name(product["name"])
        _name(provider["name"])
        if identity["name"] != product["name"]:
            raise ValueError("product alias")
        _support(provider["support_url"])
        images = {
            _image(product["logo"]),
            _image(product["browser_icon"], icon=True),
            _image(provider["logo"]),
        }
        if any(
            sum(len(data) for data in {pair[index] for pair in images}) > 512 * 1024
            for index in (0, 1)
        ):
            raise ValueError("aggregate PNG bounds")
        digest = hashlib.sha256(_canonical(identity)).hexdigest()
        if record["sha256"] != digest or digest != expected:
            raise ValueError("identity binding")
        return ProductIdentity(
            digest,
            product["name"],
            product["logo"],
            provider["name"],
            provider["logo"],
            provider["support_url"],
            _canonical(record) + b"\n",
        )
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        SyntaxError,
        Image.DecompressionBombError,
        RecursionError,
        OverflowError,
    ):
        raise ValueError(UNAVAILABLE) from None
