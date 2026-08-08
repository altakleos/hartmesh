"""Canonical thread identifier validation shared across DeerFlow backends."""

from __future__ import annotations

import uuid
from typing import Annotated

from deerflow_extension_api.identifiers import (
    THREAD_IDENTIFIER_PATTERN,
    validate_thread_identifier,
)
from pydantic import AfterValidator, StringConstraints

THREAD_ID_PATTERN = rf"^{THREAD_IDENTIFIER_PATTERN}$"


def validate_thread_id(thread_id: str) -> str:
    """Return a valid thread ID or raise ``ValueError``.

    Thread IDs are caller-defined opaque identifiers, not necessarily UUIDs,
    but they must be safe for every persistence and filesystem backend.
    """
    try:
        return validate_thread_identifier(thread_id, field_name="thread_id")
    except ValueError as exc:
        raise ValueError("Invalid thread_id: expected 1-64 ASCII letters, digits, hyphens, or underscores") from exc


def resolve_thread_id(thread_id: str | None) -> str:
    """Validate a supplied ID, generating a UUID only when it is ``None``."""
    if thread_id is None:
        return str(uuid.uuid4())
    return validate_thread_id(thread_id)


ThreadId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=64, pattern=THREAD_ID_PATTERN),
    AfterValidator(validate_thread_id),
]
