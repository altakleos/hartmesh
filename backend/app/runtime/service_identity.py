"""Host-owned validation for service identities persisted as run owners."""

from __future__ import annotations

MAX_PERSISTED_SERVICE_ID_CHARACTERS = 64
MAX_PERSISTED_SERVICE_ID_BYTES = 256


def validate_persisted_service_id(value: object) -> str:
    """Return one exact service ID that fits every persisted owner column."""

    if not isinstance(value, str) or not value:
        raise ValueError("persisted service id must be a non-empty string")
    if len(value) > MAX_PERSISTED_SERVICE_ID_CHARACTERS or len(value.encode("utf-8")) > MAX_PERSISTED_SERVICE_ID_BYTES:
        raise ValueError("persisted service id must be at most 64 characters and 256 UTF-8 bytes")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError("persisted service id must not contain control characters")
    return value
