"""Host-owned native-channel source bindings.

Bindings are created only after the host authenticates and resolves a native
source.  The opaque reference is safe correlation evidence; it is not a
credential and cannot be supplied through invocation request metadata.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum

_MAX_BINDING_REFERENCE_BYTES = 320
_ROUTE_REFERENCE = re.compile(r"route:v1:sha256:[0-9a-f]{64}\Z")


class InternalVerifiedNativeBindingKind(StrEnum):
    """Finite host-owned native source-binding kinds."""

    connection = "connection"
    webhook_route = "webhook_route"


def _bounded_reference(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    if len(value.encode("utf-8")) > _MAX_BINDING_REFERENCE_BYTES:
        raise ValueError(f"{field} must not exceed {_MAX_BINDING_REFERENCE_BYTES} UTF-8 bytes")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ValueError(f"{field} must not contain control characters")
    return value


@dataclass(frozen=True, slots=True)
class InternalVerifiedNativeBinding:
    """One immutable source binding verified by a host-owned adapter."""

    kind: InternalVerifiedNativeBindingKind
    reference: str

    def __post_init__(self) -> None:
        try:
            kind = InternalVerifiedNativeBindingKind(self.kind)
        except (TypeError, ValueError) as exc:
            raise ValueError("unsupported verified native binding kind") from exc
        reference = _bounded_reference(self.reference, field="verified native binding reference")
        if kind is InternalVerifiedNativeBindingKind.webhook_route and _ROUTE_REFERENCE.fullmatch(reference) is None:
            raise ValueError("verified webhook route binding reference is malformed")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "reference", reference)


def build_verified_webhook_route_binding(
    *,
    provider: str,
    installation_reference: str | int,
    owner_user_id: str,
    agent_id: str,
    repository_reference: str,
) -> InternalVerifiedNativeBinding:
    """Derive an opaque route binding from server-resolved coordinates."""

    coordinates = {
        "agent_id": _bounded_reference(agent_id, field="route agent id"),
        "installation_reference": _bounded_reference(
            str(installation_reference),
            field="route installation reference",
        ),
        "owner_user_id": _bounded_reference(owner_user_id, field="route owner user id"),
        "provider": _bounded_reference(provider, field="route provider"),
        "repository_reference": _bounded_reference(
            repository_reference,
            field="route repository reference",
        ),
    }
    canonical = json.dumps(
        {
            "domain": "deerflow-native-webhook-route-binding-v1",
            "coordinates": coordinates,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return InternalVerifiedNativeBinding(
        kind=InternalVerifiedNativeBindingKind.webhook_route,
        reference=f"route:v1:sha256:{hashlib.sha256(canonical).hexdigest()}",
    )
