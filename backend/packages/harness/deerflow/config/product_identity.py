"""Deployment-owned public identity, frozen as one validated process snapshot.

No tenant data, credentials, remote assets or arbitrary configuration overlays.
The environment selects a read-only directory; a restart activates a new payload.
"""

from __future__ import annotations

import base64
import copy
import json
import os
import re
import stat
import unicodedata
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from threading import RLock
from typing import Literal

from PIL import Image
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from deerflow.config.ui_config import DEFAULT_PRODUCT_NAME, first_control_or_reordering_character

MAX_ASSET = 256 * 1024


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class Product(StrictModel):
    name: str = Field(min_length=1, max_length=40)
    logo: str
    browser_icon: str

    @field_validator("name")
    @classmethod
    def plain_name(cls, value: str) -> str:
        if value != value.strip() or first_control_or_reordering_character(value, allow_newlines=False) or any(unicodedata.category(c) in {"Cc", "Cs", "Cf", "Zl", "Zp"} for c in value):
            raise ValueError("name must be a bounded plain line")
        return value

    @field_validator("logo", "browser_icon")
    @classmethod
    def asset_path(cls, value: str) -> str:
        if not re.fullmatch(r"assets/[A-Za-z0-9_-]{1,80}\.png", value):
            raise ValueError("asset must be an assets/ PNG basename")
        return value


class Provider(StrictModel):
    name: str = Field(min_length=1, max_length=40)
    logo: str
    support_url: str = Field(max_length=2048)

    _name = field_validator("name")(Product.plain_name.__func__)
    _logo = field_validator("logo")(Product.asset_path.__func__)

    @field_validator("support_url")
    @classmethod
    def destination(cls, value: str) -> str:
        from deerflow.config.tenant_bundle import _valid_support_url

        if not _valid_support_url(value) or any(unicodedata.category(c) in {"Cc", "Cs", "Cf"} for c in value):
            raise ValueError("support must be an HTTPS URL without credentials")
        return value


class Payload(StrictModel):
    schema_version: Literal[1]
    public: Literal[True]
    product: Product
    provider: Provider

    @model_validator(mode="before")
    @classmethod
    def literal_types(cls, value):
        if isinstance(value, dict) and (type(value.get("schema_version")) is not int or value.get("public") is not True):
            raise ValueError("schema_version must be integer 1 and public must be true")
        return value


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate payload field")
        result[key] = value
    return result


def _read(root_fd: int, path: str, limit: int) -> bytes:
    parts = path.split("/")
    parent = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
                raise ValueError("payload must contain bounded regular files without links")
            data = source.read(limit + 1)
            if len(data) > limit:
                raise ValueError("payload exceeds byte limit")
            return data
    finally:
        os.close(parent)


def _raster(data: bytes, *, icon: bool) -> str:
    if not data.startswith(b"\x89PNG\r\n\x1a\n") or not data.endswith(b"\x00\x00\x00\x00IEND\xaeB`\x82"):
        raise ValueError("asset must be a complete static PNG")
    offset = 8
    while offset < len(data):
        if len(data) - offset < 12:
            raise ValueError("truncated PNG chunk")
        length = int.from_bytes(data[offset : offset + 4], "big")
        kind = data[offset + 4 : offset + 8]
        offset += 12 + length
        if offset > len(data) or (kind == b"IEND" and (length != 0 or offset != len(data))):
            raise ValueError("PNG contains trailing or truncated content")
    # Verify framing/CRC, decode with explicit dimensions, then strip metadata.
    with Image.open(BytesIO(data), formats=["PNG"]) as image:
        width, height = image.size
        if not (1 <= width <= 1024 and 1 <= height <= 1024) or getattr(image, "n_frames", 1) != 1:
            raise ValueError("asset dimensions or animation unsupported")
        if icon and (width != height or not 32 <= width <= 256):
            raise ValueError("browser icon must be square, 32 through 256 pixels")
        image.verify()
    with Image.open(BytesIO(data), formats=["PNG"]) as image:
        clean = Image.frombytes("RGBA", image.size, image.convert("RGBA").tobytes())
        out = BytesIO()
        clean.save(out, format="PNG")
        processed = out.getvalue()
    if len(processed) > MAX_ASSET:
        raise ValueError("processed PNG exceeds byte limit")
    return "data:image/png;base64," + base64.b64encode(processed).decode("ascii")


_snapshot_lock = RLock()


def mounted_identity() -> dict | None:
    with _snapshot_lock:
        return _mounted_identity()


@lru_cache(maxsize=1)
def _mounted_identity() -> dict | None:
    """Successful first load freezes all admitted bytes; failed loads never default."""
    selected = os.environ.get("HARTMESH_PRODUCT_PAYLOAD")
    if selected is None:
        return None
    try:
        root = Path(selected)
        if not root.is_absolute():
            raise ValueError("payload directory must be absolute")
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in root.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            payload = Payload.model_validate(json.loads(_read(fd, "product.json", 16 * 1024).decode("utf-8"), object_pairs_hook=_unique))
            paths = {payload.product.logo, payload.product.browser_icon, payload.provider.logo}
            if set(os.listdir(fd)) != {"product.json", "assets"}:
                raise ValueError("undeclared payload files")
            assets_fd = os.open("assets", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            try:
                if set(os.listdir(assets_fd)) != {p.split("/")[1] for p in paths}:
                    raise ValueError("undeclared asset files")
            finally:
                os.close(assets_fd)
            raw = {p: _read(fd, p, MAX_ASSET) for p in paths}
            if sum(map(len, raw.values())) > 512 * 1024:
                raise ValueError("aggregate asset limit exceeded")
            images = {p: _raster(data, icon=p == payload.product.browser_icon) for p, data in raw.items()}
            if sum(len(base64.b64decode(v.split(",")[1])) for v in images.values()) > 512 * 1024:
                raise ValueError("aggregate processed asset limit exceeded")
            return dict(
                schema_version=1,
                mode="configured",
                name=payload.product.name,
                product=dict(name=payload.product.name, logo=images[payload.product.logo], browser_icon=images[payload.product.browser_icon]),
                provider=dict(name=payload.provider.name, logo=images[payload.provider.logo], support_url=payload.provider.support_url),
            )
        finally:
            os.close(fd)
    except (OSError, ValueError, SyntaxError, Image.DecompressionBombError, RecursionError, OverflowError):
        # Diagnostics never include input values, paths, URLs or unapproved data.
        raise ValueError("Public product payload is unavailable or invalid") from None


def identity_for(config) -> dict:
    snapshot = mounted_identity()
    ui = getattr(config, "ui", None)
    legacy = getattr(ui, "product_name", DEFAULT_PRODUCT_NAME)
    if snapshot is None:
        return dict(schema_version=1, mode="default", name=legacy, product=dict(name=legacy, logo=None, browser_icon=None), provider=None)
    if ui is not None and "product_name" in ui.model_fields_set and legacy != snapshot["name"]:
        raise ValueError("ui.product_name conflicts with fixed product identity")
    old_env = os.environ.get("HARTMESH_PRODUCT_NAME", "").strip()
    if old_env and old_env != snapshot["name"]:
        raise ValueError("HARTMESH_PRODUCT_NAME conflicts with fixed product identity")
    tenant = getattr(getattr(config, "tenant_bundle", None), "path", None)
    if tenant and os.path.lexists(Path(tenant) / "provider.json"):
        raise ValueError("tenant provider.json is unauthorized in configured product mode; use the public payload")
    return copy.deepcopy(snapshot)


mounted_identity.cache_clear = _mounted_identity.cache_clear
