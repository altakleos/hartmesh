"""Transient channel-neutral preview syntax, never terminal output evidence."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields

PREVIEW_SPAN_KEY = "deerflow_preview_span"


class PreviewProtocolError(RuntimeError):
    def __init__(self):
        super().__init__("Channel preview refused")


def text_digest(value: str) -> str:
    if type(value) is not str:
        raise PreviewProtocolError() from None
    try:
        return hashlib.sha256(value.encode("utf-8", "strict")).hexdigest()
    except UnicodeError:
        raise PreviewProtocolError() from None


@dataclass(frozen=True)
class PreviewSpan:
    version: int
    owner_user_id: str
    thread_id: str
    run_id: str
    publication_id: str
    invocation_id: str
    model_call_id: str
    message_id: str
    sequence: int
    offset: int
    text_sha256: str
    prefix_sha256: str

    def __post_init__(self):
        if type(self.version) is not int or self.version != 1 or type(self.sequence) is not int or self.sequence <= 0 or type(self.offset) is not int or self.offset < 0:
            raise PreviewProtocolError() from None
        for name in ("owner_user_id", "thread_id", "run_id", "invocation_id", "model_call_id", "message_id"):
            value = getattr(self, name)
            if type(value) is not str or not value or value != value.strip() or not value.isprintable():
                raise PreviewProtocolError() from None
        for name, size in (("publication_id", 32), ("text_sha256", 64), ("prefix_sha256", 64)):
            value = getattr(self, name)
            if type(value) is not str or re.fullmatch(r"[0-9a-f]{" + str(size) + "}", value) is None:
                raise PreviewProtocolError() from None

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, Mapping) or set(value) != {field.name for field in fields(cls)}:
            raise PreviewProtocolError() from None
        try:
            return cls(**value)
        except Exception:
            raise PreviewProtocolError() from None


def strip_preview_spans(value):
    """Remove every caller/provider copy before any fresh server stamp."""
    if isinstance(value, Mapping):
        return {key: strip_preview_spans(item) for key, item in value.items() if key != PREVIEW_SPAN_KEY}
    if isinstance(value, (tuple, list)):
        return [strip_preview_spans(item) for item in value]
    return value


def attach_preview_span(payload, span: PreviewSpan | None):
    payload = strip_preview_spans(payload)
    if span is not None and isinstance(payload, list) and len(payload) == 2 and isinstance(payload[1], dict):
        payload[1][PREVIEW_SPAN_KEY] = asdict(span)
    return payload
