"""Closed parsers for provider-owned evidence policy configuration."""

from __future__ import annotations

import math
from collections.abc import Mapping

from deerflow.retrieval.contracts import RetrievalProviderError

_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def configured_domains(
    extras: Mapping[str, object],
    key: str,
) -> tuple[str, ...]:
    value = extras.get(key)
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)) or len(value) > 64 or any(not isinstance(item, str) or not item.strip() for item in value):
        raise RetrievalProviderError("configuration_error")
    return tuple(item.strip() for item in value)


def configured_int(
    extras: Mapping[str, object],
    key: str,
    *,
    default: int,
    minimum: int = 1,
    maximum: int,
) -> int:
    value = extras.get(key, default)
    if type(value) is int:
        parsed = value
    elif isinstance(value, str) and value.strip() == value and value.isascii() and value.isdecimal():
        parsed = int(value)
    else:
        raise RetrievalProviderError("configuration_error")
    if parsed < minimum or parsed > maximum:
        raise RetrievalProviderError("configuration_error")
    return parsed


def configured_timeout_ms(
    extras: Mapping[str, object],
    *,
    default_seconds: float,
) -> int:
    value = extras.get("timeout", default_seconds)
    if isinstance(value, bool):
        raise RetrievalProviderError("configuration_error")
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        raise RetrievalProviderError("configuration_error") from None
    if not math.isfinite(seconds) or not 0 < seconds <= 120:
        raise RetrievalProviderError("configuration_error")
    return max(1, int(seconds * 1_000))


def validate_response_body_size(
    response: object,
    *,
    max_response_bytes: int,
) -> None:
    """Reject an absent or over-budget buffered HTTP response body."""

    if type(max_response_bytes) is not int or not 1 <= max_response_bytes <= _MAX_RESPONSE_BYTES:
        raise RetrievalProviderError("configuration_error")
    content = getattr(response, "content", None)
    if not isinstance(content, (bytes, bytearray)):
        raise RetrievalProviderError("unsafe_response")
    if len(content) > max_response_bytes:
        raise RetrievalProviderError("oversized_response")


def validate_json_content_type(response: object) -> None:
    """Accept JSON media types only, without copying provider headers."""

    headers = getattr(response, "headers", None)
    try:
        raw_content_type = headers.get("content-type")
    except (AttributeError, TypeError):
        raise RetrievalProviderError("unsafe_response") from None
    if not isinstance(raw_content_type, str):
        raise RetrievalProviderError("unsafe_response")
    media_type = raw_content_type.partition(";")[0].strip().lower()
    if media_type != "application/json" and not (media_type.startswith("application/") and media_type.endswith("+json")):
        raise RetrievalProviderError("unsafe_response")


__all__ = [
    "configured_domains",
    "configured_int",
    "configured_timeout_ms",
    "validate_json_content_type",
    "validate_response_body_size",
]
