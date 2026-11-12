"""Canonical source signatures for unsolicited Work question deliveries."""

import hashlib
import json

from app.telegram_bots.producer import ProductSourceRetired


def event_signature(event, receipt, intent):
    """A hash of canonical source data, never a replacement for current access."""
    fields = ("id", "instance_id", "work_id", "actor_id", "actor_kind", "operation_id", "action", "revision", "assignment_revision", "request", "result")
    event_data = {key: getattr(event, key) for key in fields}
    receipt_data = None if receipt is None else {key: getattr(receipt, key) for key in ("id", "instance_id", "request_id", "actor_id", "actor_kind", "operation_id", "action", "revision", "request", "receipt")}
    return hashlib.sha256(json.dumps((event_data, receipt_data, intent), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()


def unsolicited_event(item, basis):
    return "work_event_intent" in item.payload and basis.get("version") in {1, 2, 5, 6} and not basis.get("reveal") and not basis.get("reply_origin") and not basis.get("choice_origin")


def require_event_source(item, source, *, event=None, receipt=None, final=False):
    intent, proof = item.payload.get("work_event_intent"), item.payload.get("work_event_source")
    if (
        type(intent) is not dict
        or type(proof) is not dict
        or set(proof) != {"version", "signature"}
        or type(proof["version"]) is not int
        or proof["version"] != 1
        or type(proof["signature"]) is not str
        or len(proof["signature"]) != 64
        or any(value not in "0123456789abcdef" for value in proof["signature"])
        or type(intent.get("request_revision")) is not int
        or type(intent.get("assignment_revision")) is not int
        or (intent["request_revision"], intent["assignment_revision"]) != (source.seen["request_revision"], source.seen["assignment_revision"])
    ):
        raise ProductSourceRetired()
    if final and (event is None or event_signature(event, receipt, intent) != proof["signature"]):
        raise ProductSourceRetired()
