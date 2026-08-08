"""Host-internal external-key and canonical request identity primitives."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

REQUEST_DIGEST_VERSION = "sha256-canonical-json-v1"
CALLER_INTENT_DIGEST_VERSION = "caller-intent-canonical-json-v1"
SYSTEM_TASK_OWNER = "__deerflow_system__"
_RAW_EXTERNAL_KEY_MAX_BYTES = 255
_ATTACHMENT_FIELDS = frozenset(
    {
        "upload_id",
        "artifact_id",
        "file_id",
        "content_digest",
        "sha256",
        "filename",
        "media_type",
        "mime_type",
        "size",
        "is_image",
    }
)


def _require_external_string(value: str, *, field: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    if not value and not allow_empty:
        raise ValueError(f"{field} must not be empty")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ValueError(f"{field} must not contain control characters")
    return value


def normalize_external_key(value: str) -> str:
    """Return the unambiguous bounded representation stored on ``RunRow``."""

    value = _require_external_string(value, field="external key")
    encoded = value.encode("utf-8")
    if len(encoded) <= _RAW_EXTERNAL_KEY_MAX_BYTES:
        return f"raw:{value}"
    return f"sha256:utf8:{hashlib.sha256(encoded).hexdigest()}"


def _scope(source: str, parts: list[str]) -> str:
    canonical = json.dumps(
        {"domain": "deerflow-invocation-scope-v1", "tuple": parts},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return f"{source}:v1:sha256:{hashlib.sha256(canonical).hexdigest()}"


def scope_for_http(principal_kind: str, server_subject_id: str) -> str:
    return _scope(
        "http",
        [
            "http",
            _require_external_string(principal_kind, field="principal kind"),
            _require_external_string(server_subject_id, field="server subject id"),
        ],
    )


def scope_for_channel(
    provider: str,
    connection_id: str,
    workspace_or_empty: str,
    chat_id: str,
) -> str:
    return _scope(
        "channel",
        [
            "channel",
            _require_external_string(provider, field="provider"),
            _require_external_string(connection_id, field="connection id"),
            _require_external_string(workspace_or_empty, field="workspace", allow_empty=True),
            _require_external_string(chat_id, field="chat id"),
        ],
    )


def scope_for_scheduler(owner_id: str, task_id: str) -> str:
    return _scope(
        "scheduler",
        [
            "scheduler",
            _require_external_string(owner_id, field="scheduler owner"),
            _require_external_string(task_id, field="scheduled task id"),
        ],
    )


def scope_for_service(authenticated_service_id: str) -> str:
    """Return the host-owned scope for one authenticated embedded service."""

    return _scope(
        "service",
        [
            "service",
            _require_external_string(
                authenticated_service_id,
                field="authenticated service id",
            ),
        ],
    )


def canonical_request_digest(value: Any) -> str:
    """Hash canonical UTF-8 JSON with an explicit projector version tag."""

    normalized = canonical_request_value(value)
    canonical = json.dumps(
        {"version": REQUEST_DIGEST_VERSION, "request": normalized},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def canonical_request_value(value: Any, *, attachment: bool = False) -> Any:
    """Convert accepted request data into finite credential-free JSON."""

    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if isinstance(value, Mapping):
        items = value.items()
        if attachment:
            items = ((key, item) for key, item in items if key in _ATTACHMENT_FIELDS)
        return {
            str(key): canonical_request_value(
                item,
                attachment=str(key) in {"files", "attachments"},
            )
            for key, item in items
        }
    if isinstance(value, (list, tuple)):
        return [canonical_request_value(item, attachment=attachment) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ValueError(f"request value of type {type(value).__name__} cannot be projected")


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


@dataclass(frozen=True)
class CanonicalCallerIntent:
    """Immutable v1 projection of only the caller-controlled execution intent."""

    value: Mapping[str, Any]
    digest: str = field(init=False)
    digest_version: str = field(default=CALLER_INTENT_DIGEST_VERSION, init=False)

    def __post_init__(self) -> None:
        normalized = canonical_request_value(self.value)
        if not isinstance(normalized, dict):  # pragma: no cover - type contract
            raise TypeError("caller intent must be an object")
        persisted = {
            "kind": "caller_intent",
            "version": 1,
            "value": normalized,
        }
        object.__setattr__(self, "value", _freeze_json(normalized))
        object.__setattr__(self, "digest", canonical_request_digest(persisted))

    def to_persisted(self) -> dict[str, Any]:
        return {
            "kind": "caller_intent",
            "version": 1,
            "value": _thaw_json(self.value),
        }

    @classmethod
    def from_persisted(cls, value: Mapping[str, Any]) -> CanonicalCallerIntent:
        if set(value) != {"kind", "version", "value"} or value.get("kind") != "caller_intent" or value.get("version") != 1 or not isinstance(value.get("value"), Mapping):
            raise ValueError("unsupported caller-intent projection")
        return cls(value=value["value"])


@dataclass(frozen=True)
class EffectiveExecutionProjection:
    """Immutable accepted execution projection with resolved and pinned facts."""

    value: Mapping[str, Any]
    digest: str = field(init=False)
    digest_version: str = field(default=REQUEST_DIGEST_VERSION, init=False)

    def __post_init__(self) -> None:
        normalized = canonical_request_value(self.value)
        if not isinstance(normalized, dict):  # pragma: no cover - type contract
            raise TypeError("effective execution projection must be an object")
        object.__setattr__(self, "value", _freeze_json(normalized))
        object.__setattr__(self, "digest", canonical_request_digest(normalized))

    def to_persisted(self) -> dict[str, Any]:
        return _thaw_json(self.value)


__all__ = [
    "CALLER_INTENT_DIGEST_VERSION",
    "CanonicalCallerIntent",
    "EffectiveExecutionProjection",
    "REQUEST_DIGEST_VERSION",
    "SYSTEM_TASK_OWNER",
    "canonical_request_digest",
    "canonical_request_value",
    "normalize_external_key",
    "scope_for_channel",
    "scope_for_http",
    "scope_for_scheduler",
    "scope_for_service",
]
