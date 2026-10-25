"""Generic immutable output evidence, independent of transports and business rules."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

from .schemas import RunStatus

OUTPUT_CAPTURE_KEY = "output_capture"
OUTPUT_CAPTURE_DISCONNECT_KEY = "output_capture_on_disconnect"
OUTPUT_EVIDENCE_KEY = "output_evidence"


class OutputEvidenceError(RuntimeError):
    def __init__(self):
        super().__init__("Run output evidence is unavailable")


class OutputEvidenceScopeError(OutputEvidenceError):
    """Observed stored authority belongs to a different requested scope."""


class _Versioned(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")

    version: Literal[1]

    @field_validator("version", mode="before")
    @classmethod
    def literal_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("Unsupported output evidence version")
        return value


class OutputCaptureIntent(BaseModel):
    """A requested capability; it supplies no result or source authority."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")

    version: Literal[1, 2]

    @field_validator("version", mode="before")
    @classmethod
    def literal_version(cls, value):
        if type(value) is not int or value not in (1, 2):
            raise ValueError("Unsupported output capture version")
        return value


class _ScopedReceipt(_Versioned):
    owner_user_id: str = Field(min_length=1, max_length=128)
    thread_id: str = Field(min_length=1, max_length=64)
    run_id: str = Field(min_length=1, max_length=128)
    input_message_id: str | None

    @field_validator("owner_user_id", "thread_id", "run_id", "input_message_id")
    @classmethod
    def exact_identity(cls, value):
        if value is not None and not value:
            raise ValueError("Invalid output evidence identity")
        return value


class TextOutputReceipt(_ScopedReceipt):
    kind: Literal["text"]
    assistant_message_id: str = Field(min_length=1)
    event_seq: int = Field(strict=True, ge=1)
    text_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("assistant_message_id")
    @classmethod
    def exact_message_identity(cls, value):
        return cls.exact_identity(value)


type OutputUnavailableReason = Literal["unsupported_input", "unsupported_output", "human_input_required", "ambiguous_source", "missing_message_identity", "failed", "cancelled"]


class UnavailableOutputReceipt(_ScopedReceipt):
    kind: Literal["unavailable"]
    reason: OutputUnavailableReason


type OutputReceipt = TextOutputReceipt | UnavailableOutputReceipt
_RECEIPT = TypeAdapter(Annotated[OutputReceipt, Field(discriminator="kind")])


def validate_receipt(value: object) -> OutputReceipt:
    try:
        if isinstance(value, Mapping):
            value = dict(value)
        return _RECEIPT.validate_python(value, strict=True)
    except Exception:
        raise OutputEvidenceError() from None


def receipt_to_dict(value: object) -> dict[str, Any]:
    """Serialize validated immutable primitives, without a string fallback."""
    return validate_receipt(value).model_dump(mode="json")


def validate_receipt_status(value: object, status: RunStatus | str) -> OutputReceipt:
    """Bind an immutable receipt to the actual row's terminal outcome."""
    receipt = validate_receipt(value)
    status_value = status.value if type(status) is RunStatus else status
    if type(status_value) is not str:
        raise OutputEvidenceError()
    allowed = {"success"}
    if isinstance(receipt, UnavailableOutputReceipt):
        if receipt.reason == "failed":
            allowed = {"error", "timeout"}
        elif receipt.reason == "cancelled":
            allowed = {"interrupted", "error"}
    if status_value not in allowed:
        raise OutputEvidenceError()
    return receipt


def capture_from_kwargs(kwargs: object) -> OutputCaptureIntent | None:
    try:
        if not isinstance(kwargs, Mapping):
            raise OutputEvidenceError()
        if OUTPUT_CAPTURE_KEY not in kwargs:
            return None
        value = kwargs[OUTPUT_CAPTURE_KEY]
        return OutputCaptureIntent.model_validate(dict(value) if isinstance(value, Mapping) else value)
    except Exception:
        raise OutputEvidenceError() from None


def receipt_from_kwargs(kwargs: object) -> OutputReceipt | None:
    try:
        if not isinstance(kwargs, Mapping):
            raise OutputEvidenceError()
        if OUTPUT_EVIDENCE_KEY not in kwargs:
            return None
        return validate_receipt(kwargs[OUTPUT_EVIDENCE_KEY])
    except Exception:
        raise OutputEvidenceError() from None


def canonical_text_digest(text: str) -> str:
    try:
        if type(text) is not str:
            raise OutputEvidenceError()
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
    except Exception:
        raise OutputEvidenceError() from None


def _freeze_json(value):
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise OutputEvidenceError()
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    if value is None or type(value) in {str, bool, int} or type(value) is float and math.isfinite(value):
        return value
    raise OutputEvidenceError()


@dataclass(frozen=True)
class RunOutputSnapshot:
    run_id: str
    thread_id: str
    owner_user_id: str
    assistant_id: str | None
    status: RunStatus
    kwargs: Mapping[str, Any]
    local_finalizing: bool

    def __post_init__(self):
        try:
            for value in (self.run_id, self.thread_id, self.owner_user_id):
                if type(value) is not str or not value:
                    raise OutputEvidenceError()
            if self.assistant_id is not None and type(self.assistant_id) is not str or type(self.status) is not RunStatus or type(self.local_finalizing) is not bool or not isinstance(self.kwargs, Mapping):
                raise OutputEvidenceError()
            object.__setattr__(self, "kwargs", _freeze_json(self.kwargs))
        except Exception:
            raise OutputEvidenceError() from None
