"""Typed paging contract for the authoritative invocation lifecycle journal."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any

_CURSOR_VERSION = "deerflow.lifecycle.cursor/v1"


class InvalidLifecycleCursor(ValueError):
    """The opaque lifecycle cursor is malformed or uses another version."""


class CursorGap(ValueError):
    """The requested cursor is older than retained lifecycle evidence."""

    def __init__(self, minimum_available_cursor: str) -> None:
        self.minimum_available_cursor = minimum_available_cursor
        super().__init__("lifecycle cursor is older than retained evidence")


class CursorAhead(ValueError):
    """The requested cursor is newer than the current committed fence."""

    def __init__(self, read_fence_cursor: str) -> None:
        self.read_fence_cursor = read_fence_cursor
        super().__init__("lifecycle cursor is ahead of committed evidence")


class LifecycleOrderingCorruption(RuntimeError):
    """Lifecycle events and their global cursor metadata cannot be reconciled."""


def encode_lifecycle_cursor(cursor: int) -> str:
    if type(cursor) is not int or cursor < 0:
        raise ValueError("lifecycle cursor must be a non-negative integer")
    payload = json.dumps(
        {"cursor": cursor, "version": _CURSOR_VERSION},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    encoded = base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")
    return f"lc1.{encoded}"


def decode_lifecycle_cursor(token: str) -> int:
    if not isinstance(token, str) or not token.startswith("lc1."):
        raise InvalidLifecycleCursor("invalid lifecycle cursor")
    encoded = token[4:]
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        payload = json.loads(raw)
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise InvalidLifecycleCursor("invalid lifecycle cursor") from exc
    if not isinstance(payload, dict) or set(payload) != {"cursor", "version"}:
        raise InvalidLifecycleCursor("invalid lifecycle cursor fields")
    if payload["version"] != _CURSOR_VERSION:
        raise InvalidLifecycleCursor("unsupported lifecycle cursor version")
    cursor = payload["cursor"]
    if type(cursor) is not int or cursor < 0:
        raise InvalidLifecycleCursor("invalid lifecycle cursor value")
    return cursor


@dataclass(frozen=True)
class LifecycleQuery:
    """One invocation or context query after authorization and visibility."""

    run_id: str | None = None
    thread_id: str | None = None
    owner_scope: str | None = None
    cursor: str | None = None
    limit: int = 100
    include_snapshot: bool = True

    def __post_init__(self) -> None:
        if (self.run_id is None) == (self.thread_id is None):
            raise ValueError("exactly one lifecycle query target is required")
        if type(self.limit) is not int or not 1 <= self.limit <= 1000:
            raise ValueError("lifecycle query limit must be between 1 and 1000")
        if self.cursor is not None:
            decode_lifecycle_cursor(self.cursor)


@dataclass(frozen=True)
class LifecyclePage:
    snapshots: tuple[dict[str, Any], ...]
    events: tuple[dict[str, Any], ...]
    next_cursor: str
    minimum_available_cursor: str
    read_fence_cursor: str


def validate_cursor_window(cursor: str | None, *, pruned_through: int, last_cursor: int) -> int:
    requested = pruned_through if cursor is None else decode_lifecycle_cursor(cursor)
    if requested < pruned_through:
        raise CursorGap(encode_lifecycle_cursor(pruned_through))
    if requested > last_cursor:
        raise CursorAhead(encode_lifecycle_cursor(last_cursor))
    return requested


__all__ = [
    "CursorAhead",
    "CursorGap",
    "InvalidLifecycleCursor",
    "LifecycleOrderingCorruption",
    "LifecyclePage",
    "LifecycleQuery",
    "decode_lifecycle_cursor",
    "encode_lifecycle_cursor",
    "validate_cursor_window",
]
