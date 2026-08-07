"""Host-internal external-key and canonical request identity primitives."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Mapping
from typing import Any

REQUEST_DIGEST_VERSION = "sha256-canonical-json-v1"
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


def canonical_request_digest(value: Any) -> str:
    """Hash canonical UTF-8 JSON with an explicit projector version tag."""

    canonical = json.dumps(
        {"version": REQUEST_DIGEST_VERSION, "request": value},
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


__all__ = [
    "REQUEST_DIGEST_VERSION",
    "SYSTEM_TASK_OWNER",
    "canonical_request_digest",
    "canonical_request_value",
    "normalize_external_key",
    "scope_for_channel",
    "scope_for_http",
    "scope_for_scheduler",
]
