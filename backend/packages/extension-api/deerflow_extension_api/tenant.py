"""Safe, immutable tenant references exposed to extensions.

The operator-readable tenant identifier deliberately stays in the host.  This
module contains only the pseudonymous reference that is safe to persist and
hand to extension contributors.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Self

_LOWERCASE_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_PUBLIC_REF = re.compile(r"^tenant-[0-9a-f]{16}$", re.ASCII)


@dataclass(frozen=True)
class TenantReferenceV1:
    """Bounded pseudonymous reference to one server-owned tenant identity."""

    version: Literal[1]
    public_ref: str
    digest: str

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("tenant reference version must be 1")
        if _PUBLIC_REF.fullmatch(self.public_ref) is None:
            raise ValueError("tenant public_ref must be tenant- plus 16 lowercase hex characters")
        if _LOWERCASE_SHA256.fullmatch(self.digest) is None:
            raise ValueError("tenant digest must be a lowercase SHA-256 digest")
        if self.public_ref != f"tenant-{self.digest[:16]}":
            raise ValueError("tenant public_ref must match the digest prefix")

    def to_json(self) -> dict[str, object]:
        return {
            "version": self.version,
            "public_ref": self.public_ref,
            "digest": self.digest,
        }

    @classmethod
    def from_json(cls, value: object) -> Self:
        if not isinstance(value, Mapping) or set(value) != {
            "version",
            "public_ref",
            "digest",
        }:
            raise ValueError("tenant reference has unknown or missing fields")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            public_ref=value["public_ref"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
        )


__all__ = ["TenantReferenceV1"]
