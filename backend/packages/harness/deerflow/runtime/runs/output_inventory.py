"""Closed terminal presentation attestations; shape is never lifecycle authority."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator

from .output import OutputEvidenceError, OutputReceipt, validate_receipt, validate_receipt_status
from .schemas import RunStatus

INVENTORY_EVIDENCE_KEY = "inventory_evidence"
INVENTORY_EVENT_TYPE = "run.output_inventory"
INVENTORY_EVENT_CATEGORY = "outputs"
MAX_INVENTORY_DOCUMENT_BYTES = 262144
MAX_INVENTORY_DESCRIPTORS = 256
MAX_INVENTORY_FRAGMENT_DEPTH = 12
MAX_INVENTORY_FRAGMENT_NODES = 8192


class InventoryLimitExceeded(OutputEvidenceError):
    """A known pure representation bound, never an I/O outcome."""


def _plain(value):
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python")
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise OutputEvidenceError()
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return tuple(_plain(item) for item in value)
    if value is None or type(value) in {str, bool, int} or type(value) is float and math.isfinite(value):
        return value
    raise OutputEvidenceError()


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _scalar_size(value, remaining):
    if type(value) is str:
        # Exact UTF-8/escaping follows a cheap lower bound. No giant scalar is
        # handed to the encoder; even the temporary escaped string is bounded.
        if len(value) + 2 > remaining:
            raise InventoryLimitExceeded()
        return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))
    if value is None:
        return 4
    if type(value) is bool:
        return 4 if value else 5
    if type(value) is int:
        if value.bit_length() > 4 * remaining:
            raise InventoryLimitExceeded()
        return len(str(value))
    if type(value) is float and math.isfinite(value):
        return len(json.dumps(value, allow_nan=False))
    raise OutputEvidenceError()


def _check_document_size(value):
    size = 0

    def charge(amount):
        nonlocal size
        size += amount
        if size > MAX_INVENTORY_DOCUMENT_BYTES:
            raise InventoryLimitExceeded()

    def visit(item):
        if isinstance(item, BaseModel):
            # Read immutable declared fields without materializing potentially
            # overbound model_dump arrays or serialization-warning payloads.
            item = {name: getattr(item, name) for name in type(item).model_fields}
        if isinstance(item, Mapping):
            charge(2)
            for index, (key, child) in enumerate(item.items()):
                if type(key) is not str:
                    raise OutputEvidenceError()
                charge(1 + bool(index))
                visit(key)
                visit(child)
        elif isinstance(item, (tuple, list)):
            charge(2)
            for index, child in enumerate(item):
                charge(bool(index))
                visit(child)
        else:
            charge(_scalar_size(item, MAX_INVENTORY_DOCUMENT_BYTES - size))

    visit(value)


class _Version1(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, revalidate_instances="always")
    version: Literal[1]

    @model_validator(mode="before")
    @classmethod
    def detached(cls, value):
        return _plain(value)

    @field_validator("version", mode="before")
    @classmethod
    def literal_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("Invalid inventory version")
        return value


class _Closed(BaseModel):
    model_config = _Version1.model_config

    @model_validator(mode="before")
    @classmethod
    def detached(cls, value):
        return _plain(value)


class _Scoped(_Version1):
    owner_user_id: str = Field(min_length=1, max_length=128)
    thread_id: str = Field(min_length=1, max_length=64)
    run_id: str = Field(min_length=1, max_length=128)
    input_message_id: str | None
    output_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("input_message_id")
    @classmethod
    def genuine_input(cls, value):
        if value == "":
            raise ValueError("Invalid input identity")
        return value


class EventInventoryEvidence(_Scoped):
    kind: Literal["event"]
    inventory_event_seq: int = Field(strict=True, ge=1)
    inventory_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


type InventoryUnavailableReason = Literal["prestart_not_enumerated", "lease_takeover_not_enumerated", "replacement_not_enumerated", "cancellation_after_inventory_not_enumerated"]


class UnavailableInventoryEvidence(_Scoped):
    kind: Literal["unavailable"]
    reason: InventoryUnavailableReason


type InventoryEvidence = EventInventoryEvidence | UnavailableInventoryEvidence
_EVIDENCE = TypeAdapter(Annotated[InventoryEvidence, Field(discriminator="kind")])


class InventoryScanScope(_Version1):
    kind: Literal["managed_outputs_regular_changes"]
    virtual_root: Literal["/mnt/user-data/outputs"]
    excluded_dir_names: tuple[str, ...]

    @field_validator("excluded_dir_names")
    @classmethod
    def exact_exclusions(cls, values):
        if any(not item for item in values) or tuple(sorted(set(values))) != values:
            raise ValueError("Invalid exclusions")
        return values


class CompleteInventoryScan(_Closed):
    state: Literal["complete"]
    source: Literal["managed_outputs"]
    scope: InventoryScanScope
    before_root: Literal["opened"]
    after_root: Literal["opened"]
    produced_paths: tuple[str, ...]

    @field_validator("produced_paths")
    @classmethod
    def exact_paths(cls, values):
        if any(not item for item in values) or len(set(values)) != len(values):
            raise ValueError("Invalid produced paths")
        return values


class IncompleteInventoryScan(_Closed):
    state: Literal["incomplete"]
    source: Literal["managed_outputs"]
    reason: Literal["unavailable", "truncated", "disabled"]
    scope: InventoryScanScope | None
    before_root: Literal["opened", "missing", "unavailable"] | None
    after_root: Literal["opened", "missing", "unavailable"] | None


class MessageEventInventoryWitness(_Closed):
    kind: Literal["message_event"]
    event_seq: int = Field(strict=True, ge=1)
    message_id: str | None

    @field_validator("message_id")
    @classmethod
    def exact_message(cls, value):
        if value == "":
            raise ValueError("Invalid message identity")
        return value


def _uuid(value):
    UUID(value)
    return value


class CallbackInventoryWitness(_Closed):
    kind: Literal["callback"]
    callback_run_id: str
    _valid_uuid = field_validator("callback_run_id")(_uuid)


class RootSnapshotInventoryWitness(_Closed):
    kind: Literal["root_snapshot"]
    root_epoch: str
    message_id: str | None
    _valid_uuid = field_validator("root_epoch")(_uuid)
    _valid_message = field_validator("message_id")(MessageEventInventoryWitness.exact_message.__func__)


class OutputScanInventoryWitness(_Closed):
    kind: Literal["output_scan"]


type InventoryWitness = Annotated[MessageEventInventoryWitness | CallbackInventoryWitness | RootSnapshotInventoryWitness | OutputScanInventoryWitness, Field(discriminator="kind")]


class FileInventoryDescriptor(_Closed):
    kind: Literal["file"]
    path: str = Field(min_length=1)
    surfaces: tuple[Literal["output_change", "artifact_reference", "presentation_reference"], ...] = Field(min_length=1)
    witnesses: tuple[InventoryWitness, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def no_duplicates(self):
        if len(set(self.surfaces)) != len(self.surfaces) or len({_encode(item.model_dump(mode="json")) for item in self.witnesses}) != len(self.witnesses):
            raise ValueError("Duplicate inventory witness")
        return self


class StructuredInventoryDescriptor(_Closed):
    kind: Literal["structured"]
    carrier: Literal["message_artifact", "command_output", "terminal_field"]
    value_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    witness: InventoryWitness


type InventoryDescriptor = Annotated[FileInventoryDescriptor | StructuredInventoryDescriptor, Field(discriminator="kind")]


class _Document(_Scoped):
    profile: Literal["terminal-presentation-v1"] | None
    root_epoch: str | None

    @field_validator("root_epoch")
    @classmethod
    def exact_epoch(cls, value):
        return _uuid(value) if value is not None else value

    @model_validator(mode="after")
    def joined_witnesses(self):
        items = self.items if self.coverage == "complete" else self.observed_items
        if len(items) > MAX_INVENTORY_DESCRIPTORS:
            raise InventoryLimitExceeded()
        canonical = [_encode(item.model_dump(mode="json")) for item in items]
        if len(set(canonical)) != len(canonical):
            raise ValueError("Duplicate inventory descriptor")
        paths = [item.path for item in items if isinstance(item, FileInventoryDescriptor)]
        if len(paths) != len(set(paths)):
            raise ValueError("Duplicate file reference")
        if self.coverage == "complete" and not set(self.scan.produced_paths).issubset({item.path for item in items if isinstance(item, FileInventoryDescriptor) and "output_change" in item.surfaces}):
            raise ValueError("Incomplete output delta inventory")
        for item in items:
            witnesses = item.witnesses if isinstance(item, FileInventoryDescriptor) else (item.witness,)
            for witness in witnesses:
                if isinstance(witness, RootSnapshotInventoryWitness) and witness.root_epoch != self.root_epoch:
                    raise ValueError("Inventory root differs")
                if isinstance(witness, CallbackInventoryWitness) and self.root_epoch is None:
                    raise ValueError("Missing inventory root")
                if isinstance(witness, OutputScanInventoryWitness) and not isinstance(self.scan, CompleteInventoryScan):
                    raise ValueError("Missing complete scan")
            if isinstance(item, FileInventoryDescriptor) and "output_change" in item.surfaces and (not isinstance(self.scan, CompleteInventoryScan) or item.path not in self.scan.produced_paths):
                raise ValueError("Output delta differs")
        return self


class CompleteInventoryDocument(_Document):
    profile: Literal["terminal-presentation-v1"]
    root_epoch: str
    coverage: Literal["complete"]
    reason: None
    items: tuple[InventoryDescriptor, ...]
    scan: CompleteInventoryScan


class IncompleteInventoryDocument(_Document):
    coverage: Literal["incomplete"]
    reason: Literal["scan_incomplete", "carrier_incomplete", "terminal_membership_unknown", "enumeration_overflow", "execution_stopped"]
    observed_items: tuple[InventoryDescriptor, ...]
    scan: Annotated[CompleteInventoryScan | IncompleteInventoryScan, Field(discriminator="state")] | None


class UnsupportedInventoryDocument(_Document):
    coverage: Literal["unsupported"]
    reason: Literal["unsupported_profile", "unsupported_surface", "multiple_epochs"]
    observed_items: tuple[InventoryDescriptor, ...]
    scan: Annotated[CompleteInventoryScan | IncompleteInventoryScan, Field(discriminator="state")] | None


type InventoryDocument = CompleteInventoryDocument | IncompleteInventoryDocument | UnsupportedInventoryDocument
_DOCUMENT = TypeAdapter(Annotated[InventoryDocument, Field(discriminator="coverage")])


def validate_inventory_evidence(value: object) -> InventoryEvidence:
    try:
        result = _EVIDENCE.validate_python(_plain(value))
        _encode(result.model_dump(mode="json"))
        return result
    except Exception:
        raise OutputEvidenceError() from None


def inventory_evidence_from_kwargs(kwargs: object) -> InventoryEvidence | None:
    try:
        if not isinstance(kwargs, Mapping):
            raise OutputEvidenceError()
        return validate_inventory_evidence(kwargs[INVENTORY_EVIDENCE_KEY]) if INVENTORY_EVIDENCE_KEY in kwargs else None
    except Exception:
        raise OutputEvidenceError() from None


def inventory_evidence_to_dict(value: object) -> dict:
    return validate_inventory_evidence(value).model_dump(mode="json")


def validate_inventory_document(value: object) -> InventoryDocument:
    try:
        values = value if isinstance(value, Mapping) else None
        if values is not None:
            for key in ("items", "observed_items"):
                if isinstance(values.get(key), (tuple, list)) and len(values[key]) > MAX_INVENTORY_DESCRIPTORS:
                    raise InventoryLimitExceeded()
        _check_document_size(value)
        result = _DOCUMENT.validate_python(_plain(value))
        if len(_encode(result.model_dump(mode="json"))) > MAX_INVENTORY_DOCUMENT_BYTES:
            raise InventoryLimitExceeded()
        return result
    except InventoryLimitExceeded:
        raise InventoryLimitExceeded() from None
    except Exception:
        raise OutputEvidenceError() from None


def inventory_document_to_dict(value: object) -> dict:
    return validate_inventory_document(value).model_dump(mode="json")


def canonical_inventory_document_bytes(document: object) -> bytes:
    return _encode(inventory_document_to_dict(document))


def inventory_document_digest(document: object) -> str:
    return hashlib.sha256(canonical_inventory_document_bytes(document)).hexdigest()


def output_receipt_digest(receipt: object) -> str:
    try:
        return hashlib.sha256(_encode(validate_receipt(receipt).model_dump(mode="json"))).hexdigest()
    except Exception:
        raise OutputEvidenceError() from None


def presentation_fragment_digest(fragment: object) -> str:
    nodes = size = 0

    def charge(amount):
        nonlocal size
        size += amount
        if size > MAX_INVENTORY_DOCUMENT_BYTES:
            raise InventoryLimitExceeded()

    def visit(value, depth=0):
        nonlocal nodes
        nodes += 1
        if nodes > MAX_INVENTORY_FRAGMENT_NODES or depth > MAX_INVENTORY_FRAGMENT_DEPTH:
            raise InventoryLimitExceeded()
        if isinstance(value, Mapping):
            charge(2)
            result = {}
            for index, (key, item) in enumerate(value.items()):
                if type(key) is not str:
                    raise OutputEvidenceError()
                charge(1 + bool(index))  # colon and optional comma
                visit(key, depth + 1)
                result[key] = visit(item, depth + 1)
            return result
        if isinstance(value, (list, tuple)):
            charge(2)
            result = []
            for index, item in enumerate(value):
                charge(bool(index))
                result.append(visit(item, depth + 1))
            return result
        charge(_scalar_size(value, MAX_INVENTORY_DOCUMENT_BYTES - size))
        return value

    try:
        return hashlib.sha256(_encode(visit(fragment))).hexdigest()
    except InventoryLimitExceeded:
        raise InventoryLimitExceeded() from None
    except Exception:
        raise OutputEvidenceError() from None


def _scope(value):
    return value.owner_user_id, value.thread_id, value.run_id, value.input_message_id


def validate_inventory_pair(output_receipt: object, inventory_evidence: object, *, status: RunStatus | str) -> tuple[OutputReceipt, InventoryEvidence]:
    try:
        output = validate_receipt_status(output_receipt, status)
        evidence = validate_inventory_evidence(inventory_evidence)
        if _scope(output) != _scope(evidence) or evidence.output_receipt_sha256 != output_receipt_digest(output):
            raise OutputEvidenceError()
        if isinstance(evidence, UnavailableInventoryEvidence):
            if output.kind != "unavailable" or output.reason not in {"failed", "cancelled"}:
                raise OutputEvidenceError()
            if evidence.reason in {"lease_takeover_not_enumerated", "replacement_not_enumerated"} and (output.reason != "failed" or (status.value if isinstance(status, RunStatus) else status) != "error"):
                raise OutputEvidenceError()
            if evidence.reason == "cancellation_after_inventory_not_enumerated" and output.reason != "cancelled":
                raise OutputEvidenceError()
        return output, evidence
    except Exception:
        raise OutputEvidenceError() from None


def build_unavailable_inventory_evidence(output_receipt: object, *, status: RunStatus | str, reason: InventoryUnavailableReason) -> UnavailableInventoryEvidence:
    try:
        output = validate_receipt_status(output_receipt, status)
        evidence = UnavailableInventoryEvidence(
            version=1, kind="unavailable", owner_user_id=output.owner_user_id, thread_id=output.thread_id, run_id=output.run_id, input_message_id=output.input_message_id, output_receipt_sha256=output_receipt_digest(output), reason=reason
        )
        return validate_inventory_pair(output, evidence, status=status)[1]
    except Exception:
        raise OutputEvidenceError() from None


def build_event_inventory_evidence(output_receipt: object, *, status: RunStatus | str, inventory_event_seq: int, document: object) -> EventInventoryEvidence:
    try:
        output = validate_receipt_status(output_receipt, status)
        doc = validate_inventory_document(document)
        if _scope(output) != _scope(doc) or doc.output_receipt_sha256 != output_receipt_digest(output):
            raise OutputEvidenceError()
        evidence = EventInventoryEvidence(
            version=1,
            kind="event",
            owner_user_id=output.owner_user_id,
            thread_id=output.thread_id,
            run_id=output.run_id,
            input_message_id=output.input_message_id,
            output_receipt_sha256=output_receipt_digest(output),
            inventory_event_seq=inventory_event_seq,
            inventory_sha256=inventory_document_digest(doc),
        )
        return validate_inventory_pair(output, evidence, status=status)[1]
    except InventoryLimitExceeded:
        raise InventoryLimitExceeded() from None
    except Exception:
        raise OutputEvidenceError() from None
