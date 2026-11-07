"""Bounded, content-bound workspace presentation over the mounted tenant interface.

This reader never writes inputs, chooses a tenant, or coordinates activation.
Operators stage immutable inputs and restart every affected Gateway worker.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Literal

from PIL import Image
from pydantic import Field, field_validator, model_validator

from deerflow.config.product_identity import MAX_ASSET, StrictModel, _raster, _read, _unique
from deerflow.config.tenant_bundle import TenantBundleConfig

MAX_MANIFEST = 16 * 1024
MAX_RESPONSE = 384 * 1024
UNAVAILABLE = "Tenant presentation unavailable or invalid"


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def content_digest(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


class Logo(StrictModel):
    path: str = Field(pattern=r"^[A-Za-z0-9_-]{1,80}\.png$")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class Tenant(StrictModel):
    name: str = Field(min_length=1, max_length=80)
    logo: Logo

    @field_validator("name")
    @classmethod
    def plain_name(cls, value: str) -> str:
        if value != value.strip() or any(unicodedata.category(c) in {"Cc", "Cs", "Cf", "Zl", "Zp"} for c in value):
            raise ValueError("name must be one bounded plain line")
        return value


class Manifest(StrictModel):
    schema_version: Literal[1]
    mode: Literal["product-only", "tenant"]
    product_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    tenant: Tenant | None

    @model_validator(mode="before")
    @classmethod
    def complete_mode(cls, value):
        if not isinstance(value, dict) or type(value.get("schema_version")) is not int or ((value.get("mode") == "tenant") != (value.get("tenant") is not None)):
            raise ValueError("presentation requires an explicit complete mode")
        return value


@dataclass(frozen=True)
class PresentationSnapshot:
    record: bytes

    def projection(self) -> dict:
        return json.loads(self.record)


def capture_presentation(path: str, expected: str, product: dict) -> PresentationSnapshot:
    """Read every source once, verify binding, and retain only admitted bytes."""
    try:
        if not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError("missing manifest binding")
        root = Path(path)
        if not root.is_absolute() or str(root) != path or ".." in root.parts:
            raise ValueError("presentation path must be normalized and absolute")
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in root.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            value = json.loads(_read(fd, "presentation.json", MAX_MANIFEST).decode("utf-8"), object_pairs_hook=_unique)
            manifest = Manifest.model_validate(value)
            if content_digest(value) != expected or manifest.product_sha256 != content_digest(product):
                raise ValueError("presentation binding mismatch")
            tenant = None
            if manifest.tenant is not None:
                raw = _read(fd, manifest.tenant.logo.path, MAX_ASSET)
                if hashlib.sha256(raw).hexdigest() != manifest.tenant.logo.sha256:
                    raise ValueError("logo binding mismatch")
                # Only raster/color chunks; reject active/metadata carriers before
                # full CRC/decode/re-encode validation strips the source encoding.
                offset = 8
                while offset < len(raw):
                    if raw[offset + 4 : offset + 8] not in {b"IHDR", b"PLTE", b"tRNS", b"IDAT", b"IEND"}:
                        raise ValueError("unsupported PNG chunk")
                    offset += 12 + int.from_bytes(raw[offset : offset + 4], "big")
                tenant = dict(name=manifest.tenant.name, logo=_raster(raw, icon=False))
            projection = dict(schema_version=1, mode=manifest.mode, product_sha256=manifest.product_sha256, tenant=tenant)
            projection["snapshot_id"] = content_digest(projection)
            record = canonical_bytes(projection)
            if len(record) > MAX_RESPONSE:
                raise ValueError("presentation response exceeds limit")
            return PresentationSnapshot(record)
        finally:
            os.close(fd)
    except (OSError, ValueError, TypeError, AttributeError, SyntaxError, Image.DecompressionBombError, RecursionError, OverflowError):
        raise ValueError(UNAVAILABLE) from None


class PresentationReader:
    """One worker's immutable selection; failed capture never becomes a default."""

    def __init__(self, settings: TenantBundleConfig, product: dict):
        self.settings = settings.model_copy(deep=True)
        self.product = json.loads(canonical_bytes(product))
        self._lock = RLock()
        self._snapshot: PresentationSnapshot | None = None

    def read(self) -> dict:
        with self._lock:
            if self.settings.presentation == "legacy":
                return dict(schema_version=1, mode="legacy")
            if self._snapshot is None:
                self._snapshot = capture_presentation(self.settings.path, self.settings.presentation_sha256, self.product)
            return self._snapshot.projection()


def main() -> int:
    """Read-only operator validation of the selected config and mounted input."""
    import sys

    from deerflow.config import get_app_config
    from deerflow.config.product_identity import identity_for

    try:
        config = get_app_config()
        projection = PresentationReader(config.tenant_bundle, identity_for(config)).read()
        # This explicit operator command emits protected metadata. Never use it
        # in public logs or an anonymous readiness route.
        sys.stdout.write(canonical_bytes(projection).decode("ascii") + "\n")
        return 0
    except (OSError, ValueError):
        sys.stderr.write(UNAVAILABLE + "\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
