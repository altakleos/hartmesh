"""One offline presentation selection, independent of report facts and live paths.

New reports use the tenant bundle. Historical records retain their original mode.
Retained records and assets are ordinary integrity-checked bundle members.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import unicodedata
from dataclasses import dataclass, replace
from pathlib import Path

from PIL import Image

from business_report_common import DEFAULT_BRAND, InputError, _one_plain_line, is_color, load_brand
from business_report_product import (
    MAX_ASSET,
    ProductIdentity,
    _canonical,
    _image,
    _object,
    _read,
    _unique,
    load_product_identity,
    parse_product_identity,
)

MEMBER = "presentation.json"
LOGO = "presentation/tenant.png"
MAX_RECORD = 16 * 1024
UNAVAILABLE = "Report presentation is unavailable, incompatible or invalid; no new output was published."
_UNSET = object()


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _name(value, *, optional=False, legacy=False):
    if (
        not isinstance(value, str)
        or len(value) > 80
        or (not optional and not value)
        or value != value.strip()
        or (
            not _one_plain_line(value)
            if legacy
            else any(
                unicodedata.category(c) in {"Cc", "Cs", "Cf", "Zl", "Zp"} for c in value
            )
        )
    ):
        raise ValueError("presentation name")


def _palette(primary, secondary, *, strict):
    if not is_color(primary) or not is_color(secondary):
        raise ValueError("Unsupported report palette/preferences")
    if strict:
        components = [int(primary[i : i + 2], 16) / 255 for i in (1, 3, 5)]
        linear = [
            c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4
            for c in components
        ]
        luminance = sum(a * b for a, b in zip(linear, (0.2126, 0.7152, 0.0722)))
        if 1.05 / (luminance + 0.05) < 4.5:
            raise ValueError(
                "Report palette/preferences need readable primary text and white table headings"
            )


def _legacy_logo(path):
    if not path:
        return None
    try:
        raw = _read(str(Path(path).absolute()), 2 * 1024 * 1024)
        with Image.open(io.BytesIO(raw), formats=["PNG", "JPEG"]) as image:
            if getattr(image, "n_frames", 1) != 1 or max(image.size) > 4096:
                return None
            image.load()
            pixels = image.convert("RGBA")
            pixels.thumbnail((1024, 1024))
            clean = Image.frombytes("RGBA", pixels.size, pixels.tobytes())
            out = io.BytesIO()
            clean.save(out, format="PNG")
            return out.getvalue() if len(out.getvalue()) <= MAX_ASSET else None
    except (OSError, ValueError, Image.DecompressionBombError):
        return None


@dataclass(frozen=True)
class Presentation:
    product_identity: ProductIdentity | None
    tenant_name: str
    logo: bytes | None
    primary: str = DEFAULT_BRAND["primary"]
    secondary: str = DEFAULT_BRAND["secondary"]

    def brand(self):
        return {
            "company": self.tenant_name,
            "logo": None,
            "logo_bytes": self.logo,
            "primary": self.primary,
            "secondary": self.secondary,
        }

    def with_preferences(self, preferences):
        if preferences is None:
            return self
        from business_report_settings import SettingsError, validate

        try:
            brand = validate(preferences).get("brand", {})
            primary, secondary = (
                brand.get("primary", self.primary),
                brand.get("secondary", self.secondary),
            )
            _palette(primary, secondary, strict=False)
        except (ValueError, TypeError, SettingsError):
            raise ValueError(
                "Unsupported or conflicting report palette/preferences"
            ) from None
        return replace(self, primary=primary, secondary=secondary)

    def value(self):
        tenant = None
        if self.tenant_name or self.logo:
            tenant = {
                "name": self.tenant_name,
                "logo": LOGO if self.logo else None,
                "sha256": hashlib.sha256(self.logo).hexdigest() if self.logo else None,
            }
        value = {
            "schema_version": 1,
            "mode": "legacy",
            "source_id": None,
            "product_sha256": self.product_identity.digest
            if self.product_identity is not None
            else None,
            "tenant": tenant,
            "palette": {"primary": self.primary, "secondary": self.secondary},
        }
        value["snapshot_id"] = _digest(value)
        return value

    @property
    def digest(self):
        return self.value()["snapshot_id"]

    def write(self, directory):
        raw = _canonical(self.value()) + b"\n"
        if len(raw) > MAX_RECORD:
            raise ValueError(UNAVAILABLE)
        (directory / MEMBER).write_bytes(raw)
        if self.logo:
            (directory / "presentation").mkdir(exist_ok=True)
            (directory / LOGO).write_bytes(self.logo)


def check_tenant_selectors():
    retired = ("HARTMESH_REPORT_TENANT_PRESENTATION", "HARTMESH_REPORT_TENANT_SHA256")
    if any(key in os.environ for key in retired):
        raise InputError(
            f"Unset {' and '.join(retired)}. Reports read tenant branding "
            "from --tenant or /mnt/tenant/brand.json."
        )


def capture_presentation(tenant_dir, *, product_identity=_UNSET):
    check_tenant_selectors()
    product = load_product_identity() if product_identity is _UNSET else product_identity
    brand = load_brand(tenant_dir)
    return Presentation(
        product, brand["company"], _legacy_logo(brand["logo"]), brand["primary"], brand["secondary"]
    )


def validate_retained(directory, members):
    """Refuse malformed historical evidence even when its outer hashes match."""
    try:

        def member(name, limit):
            raw = _read(str(directory.resolve() / name), limit)
            if hashlib.sha256(raw).hexdigest() != members.get(name):
                raise ValueError("member binding")
            return raw

        value = _object(
            json.loads(
                member(MEMBER, MAX_RECORD).decode("utf-8"), object_pairs_hook=_unique
            ),
            {
                "schema_version",
                "mode",
                "source_id",
                "product_sha256",
                "tenant",
                "palette",
                "snapshot_id",
            },
        )
        identity = value.pop("snapshot_id")
        if (
            type(value["schema_version"]) is not int
            or value["schema_version"] != 1
            or value["mode"] not in {"legacy", "tenant", "product-only"}
            or _digest(value) != identity
        ):
            raise ValueError("retained version/binding")
        palette = _object(value["palette"], {"primary", "secondary"})
        _palette(**palette, strict=value["mode"] != "legacy")
        if value["product_sha256"] is not None:
            parse_product_identity(
                member("product-identity.json", 2 * 1024 * 1024),
                value["product_sha256"],
            )
        elif "product-identity.json" in members or value["mode"] != "legacy":
            raise ValueError("retained product binding")
        projected_tenant = None
        if value["tenant"] is not None:
            tenant = _object(value["tenant"], {"name", "logo", "sha256"})
            _name(
                tenant["name"],
                optional=value["mode"] == "legacy",
                legacy=value["mode"] == "legacy",
            )
            if tenant["logo"] is not None:
                if tenant["logo"] != LOGO:
                    raise ValueError("relative asset")
                raw = member(LOGO, MAX_ASSET)
                if hashlib.sha256(raw).hexdigest() != tenant["sha256"]:
                    raise ValueError("asset binding")
                inline = "data:image/png;base64," + base64.b64encode(raw).decode(
                    "ascii"
                )
                _image(inline)
                projected_tenant = {"name": tenant["name"], "logo": inline}
            elif tenant["sha256"] is not None or value["mode"] != "legacy":
                raise ValueError("incomplete asset")
        elif value["mode"] == "tenant":
            raise ValueError("incomplete tenant")
        if value["mode"] != "legacy":
            if value["mode"] == "product-only" and value["tenant"] is not None:
                raise ValueError("product-only tenant")
            projected = {
                "schema_version": 1,
                "mode": value["mode"],
                "product_sha256": value["product_sha256"],
                "tenant": projected_tenant,
            }
            if _digest(projected) != value["source_id"]:
                raise ValueError("retained source binding")
        elif value["source_id"] is not None:
            raise ValueError("legacy is not a strict projection")
        value["snapshot_id"] = identity
        return value
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
