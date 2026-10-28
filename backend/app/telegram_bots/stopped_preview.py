"""Opaque host-only stopped-presentation handoff; no final-output authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

_ISSUER = object()
PREVIEW_SOURCE_UNITS = 3500


def preview_source_slices(text, *, start=0):
    """Split literal raw source, keeping codepoint offsets and UTF16 budget."""
    if type(text) is not str or type(start) is not int or not 0 <= start <= len(text):
        _refuse()
    result, cursor = [], start
    while cursor < len(text):
        end, units, paragraph = cursor, 0, None
        while end < len(text):
            char = text[end]
            if 0xD800 <= ord(char) <= 0xDFFF:
                _refuse()
            cost = 2 if ord(char) > 0xFFFF else 1
            if units + cost > PREVIEW_SOURCE_UNITS:
                break
            units += cost
            end += 1
            if text[max(cursor, end - 2) : end] == "\n\n":
                paragraph = end
        if end < len(text) and paragraph is not None and paragraph > cursor:
            end = paragraph
        result.append((cursor, end, text[cursor:end]))
        cursor = end
    return tuple(result)


def _refuse():
    raise RuntimeError("Stopped preview refused") from None


def _freeze(value):
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            _refuse()
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(item) for item in value)
    if value is None or type(value) in {str, int, bool}:
        return value
    _refuse()


def _canonical(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=lambda item: dict(item) if isinstance(item, Mapping) else _refuse())
    except Exception:
        _refuse()


@dataclass(frozen=True)
class StoppedSegment:
    span_id: str
    message_id: str
    sequence: int
    text: str
    text_sha256: str
    mode: str
    anchor_message_id: int | None
    draft_id: int | None
    part_index: int
    part_id: str
    source_start: int
    source_end: int

    def __post_init__(self):
        for value in (self.span_id, self.message_id, self.part_id):
            if type(value) is not str or not value or value != value.strip() or not value.isprintable():
                _refuse()
        if (
            type(self.text) is not str
            or type(self.sequence) is not int
            or self.sequence <= 0
            or any(type(value) is not int or value < 0 for value in (self.part_index, self.source_start, self.source_end))
            or self.source_end - self.source_start != len(self.text)
            or self.mode not in {"draft", "edit"}
            or (self.anchor_message_id is None) != (self.mode == "draft")
            or (self.draft_id is None) != (self.mode == "edit")
            or any(type(value) is not int or value <= 0 for value in (self.anchor_message_id, self.draft_id) if value is not None)
        ):
            _refuse()
        try:
            if self.text_sha256 != hashlib.sha256(self.text.encode("utf-8", "strict")).hexdigest():
                _refuse()
        except UnicodeError:
            _refuse()


@dataclass(frozen=True)
class StoppedPreviewData:
    publication_identity: Mapping
    run_id: str
    stop_id: str
    segments: tuple[StoppedSegment, ...]


@dataclass(frozen=True)
class ShownPreviewData:
    publication_identity: Mapping
    run_id: str
    segments: tuple[StoppedSegment, ...]


class StoppedPreview:
    __slots__ = ("_data", "_current", "_taken")

    def __init__(self, issuer, data, current):
        if issuer is not _ISSUER:
            _refuse()
        self._data, self._current, self._taken = data, current, False

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __reduce_ex__(self, protocol):
        _refuse()


class ShownPreview(StoppedPreview):
    """Acknowledged display only; it conveys no cancellation authority."""


def _issue_shown_preview(*, publication_identity, run_id, segments, check_current):
    checked = _issue_stopped_preview(publication_identity=publication_identity, run_id=run_id, stop_id="shown-only", segments=segments, check_current=check_current)
    data = checked._data
    return ShownPreview(_ISSUER, ShownPreviewData(data.publication_identity, data.run_id, data.segments), check_current)


def check_shown_preview(capability, *, publication_identity, run_id):
    if type(capability) is not ShownPreview:
        _refuse()
    _check(capability, publication_identity=publication_identity, run_id=run_id)
    return capability._data


def take_shown_preview(capability, *, publication_identity, run_id):
    check_shown_preview(capability, publication_identity=publication_identity, run_id=run_id)
    if capability._taken:
        _refuse()
    capability._taken = True
    return capability._data


def _issue_stopped_preview(*, publication_identity, run_id, stop_id, segments, check_current):
    """Only the observer calls this after real Stop/source/preview confirmation."""
    if not isinstance(publication_identity, Mapping) or not callable(check_current) or any(type(value) is not str or not value or not value.isprintable() for value in (run_id, stop_id)):
        _refuse()
    if type(segments) is not tuple or any(type(segment) is not StoppedSegment for segment in segments) or len({segment.part_id for segment in segments}) != len(segments):
        _refuse()
    return StoppedPreview(_ISSUER, StoppedPreviewData(_freeze(publication_identity), run_id, stop_id, segments), check_current)


def check_stopped_preview(capability, *, publication_identity, run_id):
    if type(capability) is not StoppedPreview:
        _refuse()
    _check(capability, publication_identity=publication_identity, run_id=run_id)


def _check(capability, *, publication_identity, run_id):
    try:
        if capability._current() is not True or capability._data.run_id != run_id or _canonical(capability._data.publication_identity) != _canonical(publication_identity):
            _refuse()
    except Exception:
        _refuse()


def take_stopped_preview(capability, *, publication_identity, run_id) -> StoppedPreviewData:
    check_stopped_preview(capability, publication_identity=publication_identity, run_id=run_id)
    if capability._taken:
        _refuse()
    capability._taken = True
    return capability._data
