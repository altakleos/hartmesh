"""Bounded exact singleton inventory reads, never live producer attestation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from deerflow.runtime.events.store.base import RunEventStore
from deerflow.utils.file_io import await_drained

from .capture_io import CaptureIOClosed, CaptureIOUnsupported, run_capture_io
from .output import OutputEvidenceError, OutputReceipt, validate_receipt
from .output_inventory import (
    INVENTORY_EVENT_CATEGORY,
    INVENTORY_EVENT_TYPE,
    EventInventoryEvidence,
    FileInventoryDescriptor,
    InventoryDocument,
    MessageEventInventoryWitness,
    inventory_document_digest,
    output_receipt_digest,
    validate_inventory_document,
    validate_inventory_evidence,
)


@dataclass(frozen=True)
class InventorySourceAudit:
    state: Literal["matched", "missing", "changed"]
    document: InventoryDocument | None


def _evidence(value, output):
    result = validate_inventory_evidence(value)
    if not isinstance(result, EventInventoryEvidence) or (result.owner_user_id, result.thread_id, result.run_id, result.input_message_id, result.output_receipt_sha256) != (
        output.owner_user_id,
        output.thread_id,
        output.run_id,
        output.input_message_id,
        output_receipt_digest(output),
    ):
        raise OutputEvidenceError()
    return result


def audit_inventory_source_event(event: object, *, evidence: EventInventoryEvidence, output_receipt: OutputReceipt) -> InventorySourceAudit:
    """Audit a present stored envelope; the caller owns proof of its actual read."""
    try:
        output = validate_receipt(output_receipt)
        evidence = _evidence(evidence, output)
        if not isinstance(event, Mapping) or any(event.get(key) != expected for key, expected in (("thread_id", output.thread_id), ("run_id", output.run_id), ("user_id", output.owner_user_id), ("category", INVENTORY_EVENT_CATEGORY))):
            raise OutputEvidenceError()
        seq = event.get("seq")
        if type(seq) is not int or seq <= 0 or type(event.get("event_type")) is not str or not event["event_type"] or not isinstance(event.get("metadata"), Mapping) or type(event.get("created_at")) is not str:
            raise OutputEvidenceError()
        stamp = datetime.fromisoformat(event["created_at"])
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise OutputEvidenceError()
        document = validate_inventory_document(event.get("content"))
        if (document.owner_user_id, document.thread_id, document.run_id, document.input_message_id) != (output.owner_user_id, output.thread_id, output.run_id, output.input_message_id):
            raise OutputEvidenceError()
        items = document.items if document.coverage == "complete" else document.observed_items
        for item in items:
            witnesses = item.witnesses if isinstance(item, FileInventoryDescriptor) else (item.witness,)
            if any(isinstance(witness, MessageEventInventoryWitness) and witness.event_seq >= seq for witness in witnesses):
                raise OutputEvidenceError()
        if event["event_type"] != INVENTORY_EVENT_TYPE or seq != evidence.inventory_event_seq or document.output_receipt_sha256 != evidence.output_receipt_sha256 or inventory_document_digest(document) != evidence.inventory_sha256:
            return InventorySourceAudit("changed", None)
        return InventorySourceAudit("matched", document)
    except Exception:
        raise OutputEvidenceError() from None


async def read_durable_inventory_source(event_store: RunEventStore, *, evidence: EventInventoryEvidence, output_receipt: OutputReceipt) -> InventorySourceAudit:
    """One explicit owner-scoped singleton read, retaining actual I/O settlement."""
    try:
        output = validate_receipt(output_receipt)
        evidence = _evidence(evidence, output)
        if getattr(event_store, "supports_durable_output_inventory", False) is not True:
            raise OutputEvidenceError()
    except Exception:
        raise OutputEvidenceError() from None

    async def read():
        try:
            rows = await run_capture_io(lambda: event_store.list_events(output.thread_id, output.run_id, event_types=[INVENTORY_EVENT_TYPE], limit=2, user_id=output.owner_user_id))
            if not isinstance(rows, list) or len(rows) > 1:
                raise OutputEvidenceError()
            if not rows:
                return InventorySourceAudit("missing", None)
            return audit_inventory_source_event(rows[0], evidence=evidence, output_receipt=output)
        except (CaptureIOClosed, CaptureIOUnsupported):
            raise
        except Exception:
            raise OutputEvidenceError() from None

    return await await_drained(read())
