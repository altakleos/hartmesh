"""Generic immutable output evidence, independent of transports and business rules."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_serializer, model_validator
from pydantic.json_schema import SkipJsonSchema

from .schemas import RunStatus

OUTPUT_CAPTURE_KEY = "output_capture"
OUTPUT_CAPTURE_DISCONNECT_KEY = "output_capture_on_disconnect"
OUTPUT_EVIDENCE_KEY = "output_evidence"


class OutputEvidenceError(RuntimeError):
    def __init__(self):
        super().__init__("Run output evidence is unavailable")


class OutputEvidenceScopeError(OutputEvidenceError):
    """Observed stored authority belongs to a different requested scope."""


_TOOL_REQUEST_ARGUMENTS = {"tool_use": ("input", Mapping), "tool_call": ("args", Mapping), "function_call": ("arguments", str)}


def content_block_role(block: Mapping, tool_calls=()) -> Literal["text", "reasoning", "tool_request", "other"]:
    """What one provider content block is to a reader.

    Providers add bookkeeping keys such as `index`, `id`, `signature` and
    `partial_json`, and those never change a block's role. A tool request is
    known by the call it names in `tool_calls`, so an unmatched one stays `other`.
    """
    kind = block.get("type")
    if kind == "text":
        return "text"
    if kind in ("reasoning", "thinking", "redacted_thinking"):
        return "reasoning"
    if type(kind) is str and kind in _TOOL_REQUEST_ARGUMENTS:
        key, shape = _TOOL_REQUEST_ARGUMENTS[kind]
        identity = block.get("call_id" if kind == "function_call" else "id")
        name = block.get("name")
        if type(identity) is str and identity and type(name) is str and name and isinstance(block.get(key), shape):
            if any(isinstance(call, Mapping) and call.get("id") == identity and call.get("name") == name for call in tool_calls):
                return "tool_request"
    return "other"


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
    inventory_document_version: Literal[2] | SkipJsonSchema[None] = Field(default=None, json_schema_extra=lambda schema: schema.pop("default", None))

    @model_validator(mode="wrap")
    @classmethod
    def exact_selector(cls, value, handler):
        # Revalidation must distinguish an omitted default from explicit null,
        # including copies of valid instances and model_copy/construct bypasses.
        if isinstance(value, cls):
            if value.model_fields_set - cls.model_fields.keys():
                raise ValueError("Unsupported output capture request")
            supplied = dict(vars(value))
            if supplied.get("inventory_document_version") is None and "inventory_document_version" not in value.model_fields_set:
                supplied.pop("inventory_document_version", None)
            value = supplied
        if isinstance(value, Mapping):
            value = dict(value)
            if "inventory_document_version" in value and (type(value["inventory_document_version"]) is not int or value["inventory_document_version"] != 2 or value.get("version") != 2):
                raise ValueError("Unsupported inventory document version")
        return handler(value)

    @property
    def document_version(self) -> Literal[1, 2]:
        return 1 if self.inventory_document_version is None else 2

    @model_serializer(mode="plain")
    def canonical_request(self):
        return capture_intent_to_dict(self)

    @field_validator("version", mode="before")
    @classmethod
    def literal_version(cls, value):
        if type(value) is not int or value not in (1, 2):
            raise ValueError("Unsupported output capture version")
        return value


def capture_intent_to_dict(value: object) -> dict[str, Any]:
    """Canonical request bytes, preserving omitted legacy selector defaults."""
    try:
        intent = OutputCaptureIntent.model_validate(value)
        result = {"version": intent.version}
        if intent.document_version == 2:
            result["inventory_document_version"] = 2
        return result
    except Exception:
        raise OutputEvidenceError() from None


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
