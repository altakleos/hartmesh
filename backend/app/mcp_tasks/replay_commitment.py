"""Private exact-request commitments for durable MCP idempotency.

The public lineage intentionally contains only a safe structural projection.
This module supplies the separate equality proof used to decide whether an
idempotency replay carries exactly the same execution inputs.  Secret key
material is process-local and the database stores only its public key id plus
the HMAC output.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

MCP_TASK_REQUEST_COMMITMENT_VERSION = 1
MCP_TASK_REPLAY_KEYRING_CONFIRMATION_VERSION = 1
MCP_TASK_REPLAY_HMAC_KEYS_ENV = "MCP_TASK_REPLAY_HMAC_KEYS"
MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID_ENV = "MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID"

_DOMAIN = b"hartmesh.mcp-task.request-commitment/v1\0"
_KEY_CONFIRMATION_DOMAIN = b"hartmesh.mcp-task.replay-key-confirmation/v1\0"
_KEYRING_CONFIRMATION_DOMAIN = b"hartmesh.mcp-task.replay-keyring-confirmation/v1\0"
_KEY_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,31}\Z")
_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_MAX_KEYRING_ENV_BYTES = 16_384
_MAX_KEYS = 8
_MIN_KEY_BYTES = 32
_MAX_KEY_BYTES = 128
_MAX_COMMITMENT_INPUT_BYTES = 1_048_576


class McpTaskReplayCommitmentError(ValueError):
    """A bounded machine-code failure at the private replay boundary."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class McpTaskRequestCommitment:
    version: int
    key_id: str
    digest: str


@dataclass(frozen=True, slots=True)
class McpTaskReplayKeyringConfirmation:
    """Versioned non-secret proof that two replicas froze one keyring."""

    version: int
    digest: str

    def __post_init__(self) -> None:
        if self.version != MCP_TASK_REPLAY_KEYRING_CONFIRMATION_VERSION:
            raise ValueError("unsupported MCP replay keyring confirmation version")
        if _SHA256_RE.fullmatch(self.digest) is None:
            raise ValueError("MCP replay keyring confirmation digest is invalid")


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _decode_key(value: object) -> bytes:
    if not isinstance(value, str) or not value:
        raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
    try:
        encoded = value.encode("ascii")
        decoded = base64.b64decode(
            encoded + b"=" * (-len(encoded) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, ValueError) as exc:
        raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid") from exc
    if not _MIN_KEY_BYTES <= len(decoded) <= _MAX_KEY_BYTES:
        raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if canonical != value:
        raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
    return decoded


def _canonical_exact_value(value: object, *, depth: int = 0) -> object:
    """Return lossless finite JSON without public-projection redaction."""

    if depth > 32:
        raise McpTaskReplayCommitmentError("mcp_task_request_commitment_invalid")
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_invalid")
        return {key: _canonical_exact_value(item, depth=depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_exact_value(item, depth=depth + 1) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise McpTaskReplayCommitmentError("mcp_task_request_commitment_invalid")


class McpTaskReplayKeyring:
    """Immutable bounded keyring with one active write key."""

    __slots__ = ("_active_key_id", "_keys")

    def __init__(self, *, active_key_id: str, keys: Mapping[str, bytes]) -> None:
        copied: dict[str, bytes] = {}
        if not isinstance(active_key_id, str) or _KEY_ID_RE.fullmatch(active_key_id) is None:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
        if not 1 <= len(keys) <= _MAX_KEYS:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
        for key_id, secret in keys.items():
            if not isinstance(key_id, str) or _KEY_ID_RE.fullmatch(key_id) is None:
                raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
            if not isinstance(secret, bytes) or not _MIN_KEY_BYTES <= len(secret) <= _MAX_KEY_BYTES:
                raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
            copied[key_id] = bytes(secret)
        if active_key_id not in copied:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
        self._active_key_id = active_key_id
        self._keys = MappingProxyType(copied)

    @classmethod
    def from_environment(
        cls,
        *,
        required: bool,
        environ: Mapping[str, str] | None = None,
    ) -> McpTaskReplayKeyring | None:
        values = os.environ if environ is None else environ
        raw_keys = values.get(MCP_TASK_REPLAY_HMAC_KEYS_ENV)
        active_key_id = values.get(MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID_ENV)
        if raw_keys is None and active_key_id is None:
            if required:
                raise McpTaskReplayCommitmentError("mcp_task_request_commitment_unavailable")
            return None
        if raw_keys is None or active_key_id is None or len(raw_keys.encode("utf-8")) > _MAX_KEYRING_ENV_BYTES:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
        try:
            parsed = json.loads(raw_keys, object_pairs_hook=_object_without_duplicate_keys)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid") from exc
        if not isinstance(parsed, dict):
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_configuration_invalid")
        return cls(
            active_key_id=active_key_id,
            keys={key_id: _decode_key(value) for key_id, value in parsed.items()},
        )

    @property
    def active_key_id(self) -> str:
        return self._active_key_id

    def confirmation(self) -> McpTaskReplayKeyringConfirmation:
        """Return a redacted equality proof covering every key and the active id."""

        keys = [
            {
                "key_id": key_id,
                "confirmation": hmac.new(
                    secret,
                    _KEY_CONFIRMATION_DOMAIN + key_id.encode("ascii"),
                    hashlib.sha256,
                ).hexdigest(),
            }
            for key_id, secret in sorted(self._keys.items())
        ]
        canonical = json.dumps(
            {
                "version": MCP_TASK_REPLAY_KEYRING_CONFIRMATION_VERSION,
                "active_key_id": self._active_key_id,
                "keys": keys,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        digest = hashlib.sha256(_KEYRING_CONFIRMATION_DOMAIN + canonical).hexdigest()
        return McpTaskReplayKeyringConfirmation(
            version=MCP_TASK_REPLAY_KEYRING_CONFIRMATION_VERSION,
            digest=f"sha256:{digest}",
        )

    def commit(
        self,
        value: object,
        *,
        key_id: str | None = None,
        version: int = MCP_TASK_REQUEST_COMMITMENT_VERSION,
    ) -> McpTaskRequestCommitment:
        if version != MCP_TASK_REQUEST_COMMITMENT_VERSION:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_invalid")
        selected_key_id = self._active_key_id if key_id is None else key_id
        secret = self._keys.get(selected_key_id)
        if secret is None:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_key_unavailable")
        try:
            canonical = json.dumps(
                {
                    "version": MCP_TASK_REQUEST_COMMITMENT_VERSION,
                    "request": _canonical_exact_value(value),
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
        except McpTaskReplayCommitmentError:
            raise
        except (TypeError, ValueError) as exc:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_invalid") from exc
        if len(canonical) > _MAX_COMMITMENT_INPUT_BYTES:
            raise McpTaskReplayCommitmentError("mcp_task_request_commitment_invalid")
        digest = hmac.new(secret, _DOMAIN + canonical, hashlib.sha256).hexdigest()
        return McpTaskRequestCommitment(
            version=version,
            key_id=selected_key_id,
            digest=digest,
        )


__all__ = [
    "MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID_ENV",
    "MCP_TASK_REPLAY_HMAC_KEYS_ENV",
    "MCP_TASK_REPLAY_KEYRING_CONFIRMATION_VERSION",
    "MCP_TASK_REQUEST_COMMITMENT_VERSION",
    "McpTaskReplayCommitmentError",
    "McpTaskReplayKeyring",
    "McpTaskReplayKeyringConfirmation",
    "McpTaskRequestCommitment",
]
