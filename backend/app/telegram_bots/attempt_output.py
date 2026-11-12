"""Read only the successful run's sealed exact assistant source."""

from app.telegram_bots.notifications import aware
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.runs.output import OUTPUT_CAPTURE_KEY, OUTPUT_EVIDENCE_KEY, OutputEvidenceError, TextOutputReceipt, canonical_text_digest, capture_from_kwargs, receipt_from_kwargs, validate_receipt_status
from deerflow.runtime.runs.output_inventory import inventory_evidence_from_kwargs, validate_inventory_pair
from deerflow.runtime.runs.output_source import audit_output_source_event


def output_identity(run):
    """Freeze the seal independently of source availability or display access."""
    if run is None or type(run.kwargs_json) is not dict:
        return None
    return {key: run.kwargs_json.get(key) for key in (OUTPUT_CAPTURE_KEY, OUTPUT_EVIDENCE_KEY)}


def attempt_output(run, source):
    """Rows must come from one recipient-authorized final SQL snapshot.

    No cache, latest-message scan or unavailable source can supply an excerpt.
    A malformed or retained-without-source seal leaves the ordinary notice.
    """
    if run is None or source is None:
        return None
    try:
        intent = capture_from_kwargs(run.kwargs_json)
        receipt = receipt_from_kwargs(run.kwargs_json)
        if intent is None or not isinstance(receipt, TextOutputReceipt) or run.operation_kind != "run":
            return None
        validate_receipt_status(receipt, run.status)
        if (receipt.run_id, receipt.thread_id, receipt.owner_user_id) != (run.run_id, run.thread_id, run.user_id):
            return None
        if intent.version == 2:
            validate_inventory_pair(receipt, inventory_evidence_from_kwargs(run.kwargs_json), status=run.status)
        if source.content_sha256 != canonical_text_digest(source.content):
            return None
        event = DbRunEventStore._row_to_dict(source)
        event["created_at"] = aware(source.created_at).isoformat()
        audit = audit_output_source_event(event, receipt=receipt)
        return audit.text if audit.state == "matched" else None
    except (OutputEvidenceError, ValueError, TypeError, AttributeError):
        return None
