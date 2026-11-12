"""Ordered result parts and explicit same-source media replacement receipts.

This ledger qualifies presentation history only. The Work source owner must
also qualify the recipient, canonical outcome and exact current delivery claim.
"""

from copy import deepcopy
from datetime import datetime

from app.telegram_bots.work_actions import same_body

_MEDIA = {"document", "photo", "album"}
_RECEIPTS = {
    "part_id",
    "presentation_version",
    "ordinal",
    "state",
    "message_ids",
    "sent_at",
    "dispatched_at",
    "callback_group_id",
    "plain_fallback",
    "url_button_fallback",
    "invalid_token_verification_pending",
    "result_file_hashes",
}


def _key(value, length=32):
    return type(value) is str and len(value) == length and all(char in "0123456789abcdef" for char in value)


def _clean(part):
    return {key: deepcopy(value) for key, value in part.items() if key not in _RECEIPTS}


def initial_ledger(parts):
    return {"version": 1, "roots": [part["part_id"] for part in parts], "events": []}


def _warning(parent, key, file):
    if key not in {"file.no_access", "file.out_failed", "file.too_large_out"}:
        raise ValueError("invalid-result-file-warning")
    return {
        "kind": "text",
        "source_ref": deepcopy(parent["source_ref"]),
        "result_file_warning": {"key": key, "file": deepcopy(file)},
        "silent": True,
        "protect_content": parent["protect_content"],
    }


def _media_recipe(parent, kind, files):
    if kind not in _MEDIA or type(files) is not list or not (2 <= len(files) <= 10 if kind == "album" else len(files) == 1):
        raise ValueError("invalid-result-media-replacement")
    if kind in {"photo", "album"} and any(file["source"] is None or not file["filename"].lower().endswith((".jpg", ".jpeg", ".png")) for file in files):
        raise ValueError("invalid-result-photo-source")
    return {**_clean(parent), "kind": kind, "files": deepcopy(files)}


def qualify_result_parts(item, expected, *, selected=None, acknowledged=False, carried=None, known_hashes=None):
    """Replay every rewrite, validate typed ACKs, and require the shown prefix."""
    try:
        ledger, version = item.payload["work_result_ledger"], item.payload["presentation_version"]
        if type(version) is not int or version < 0 or type(ledger) is not dict or set(ledger) != {"version", "roots", "events"} or type(ledger["version"]) is not int or ledger["version"] != 1:
            raise ValueError()
        roots = ledger["roots"]
        if type(roots) is not list or len(roots) != len(expected) or len(set(roots)) != len(roots) or any(not _key(identity) for identity in roots) or type(ledger["events"]) is not list:
            raise ValueError()
        parts = [part for part in item.parts if part["presentation_version"] == version]
        rows = {part["part_id"]: part for part in parts}
        if len(rows) != len(parts) or not set(roots) <= rows.keys():
            raise ValueError()
        recipes, order, rewritten, covered = dict(zip(roots, expected, strict=True)), list(roots), set(), set()
        for event in ledger["events"]:
            if type(event) is not dict or set(event) != {"old", "new", "reason"} | ({"key"} if event.get("reason") == "unavailable" else {"receipts"} if event.get("reason") == "carry" else set()):
                raise ValueError()
            identity, children = event["old"], event["new"]
            if (
                identity not in recipes
                or identity in rewritten
                or type(children) is not list
                or (not children and event["reason"] != "carry")
                or len(set(children)) != len(children)
                or any(child in recipes or child not in rows for child in children)
            ):
                raise ValueError()
            parent, fresh = rows[identity], [rows[child] for child in children]
            if parent["kind"] not in _MEDIA or parent.get("callback_group_id"):
                raise ValueError()
            if event["reason"] == "carry":
                if parent["state"] != "retired" or type(event["receipts"]) is not list or not event["receipts"] or carried is None:
                    raise ValueError()
                indices = set()
                for receipt in event["receipts"]:
                    if type(receipt) is not dict or set(receipt) != {"part_id", "source_index"} or type(receipt["source_index"]) is not int or carried.get(receipt["source_index"]) != receipt or receipt["source_index"] in indices | covered:
                        raise ValueError()
                    indices.add(receipt["source_index"])
                covered.update(indices)
                if not indices <= {file["source_index"] for file in parent["files"]}:
                    raise ValueError()
                allowed = [_media_recipe(parent, "document", [file]) for file in parent["files"] if file["source_index"] not in indices]
                for part in fresh:
                    for file in part["files"]:
                        previous = parent.get("result_file_hashes", {}).get(str(file["source_index"]))
                        if previous is not None and part.get("result_file_hashes", {}).get(str(file["source_index"])) != previous:
                            raise ValueError()
            elif event["reason"] == "unavailable":
                if parent["state"] != "unavailable":
                    raise ValueError()
                allowed = [_warning(parent, event["key"], file) for file in parent["files"]]
            elif event["reason"] in {"photo-classification", "photo-rejected", "capacity"}:
                if parent["state"] != "retired" or parent["kind"] not in {"photo", "album"} or [file for part in fresh for file in part["files"]] != parent["files"]:
                    raise ValueError()
                if event["reason"] == "photo-rejected" and any(part["kind"] != "document" for part in fresh):
                    raise ValueError()
                allowed = [_media_recipe(parent, part["kind"], part["files"]) for part in fresh]
                for part in fresh:
                    for file in part["files"]:
                        previous = parent.get("result_file_hashes", {}).get(str(file["source_index"]))
                        if previous is not None and part.get("result_file_hashes", {}).get(str(file["source_index"])) != previous:
                            raise ValueError()
            else:
                raise ValueError()
            if len(fresh) != len(allowed):
                raise ValueError()
            recipes.update(zip(children, allowed, strict=True))
            position = order.index(identity) + 1
            order[position:position] = children
            rewritten.add(identity)
        if [part["part_id"] for part in parts] != order or set(rows) != set(recipes):
            raise ValueError()
        expected_indices = {file["source_index"] for recipe in expected for file in recipe.get("files", [])}
        if covered != set(carried or {}) & expected_indices:
            raise ValueError()
        messages, selected_index = set(), None
        if selected is not None:
            selected_index = next(index for index, part in enumerate(parts) if part == selected)
            if selected["state"] != ("sent" if acknowledged else "sending"):
                raise ValueError()
        for ordinal, part in enumerate(parts):
            identity = part["part_id"]
            if not _key(identity) or type(part["ordinal"]) is not int or part["ordinal"] != ordinal or not same_body(_clean(part), recipes[identity]):
                raise ValueError()
            if part["state"] not in {"pending", "sending", "sent", "retired", "unavailable"} or (part["state"] in {"retired", "unavailable"}) != (identity in rewritten):
                raise ValueError()
            if part.get("callback_group_id") is not None and ordinal != len(parts) - 1:
                raise ValueError()
            if any(type(part[key]) is not bool for key in ("plain_fallback", "url_button_fallback") if key in part):
                raise ValueError()
            hashes = part.get("result_file_hashes", {})
            indices = {str(file["source_index"]) for file in part.get("files", []) if file["source"] is not None}
            if type(hashes) is not dict or not hashes.keys() <= indices or any(not _key(digest, 64) for digest in hashes.values()):
                raise ValueError()
            for file in part.get("files", []):
                index = file["source_index"]
                if part["state"] in {"pending", "sending", "sent"} and index in (carried or {}):
                    raise ValueError()
                if file["source"] is not None and index in (known_hashes or {}) and hashes.get(str(index)) != known_hashes[index]:
                    raise ValueError()
            ids = part["message_ids"]
            if type(ids) is not list or any(type(identity) is not int or identity <= 0 for identity in ids) or len(set(ids)) != len(ids) or messages.intersection(ids):
                raise ValueError()
            if part["state"] == "sent":
                if len(ids) != (len(part["files"]) if part["kind"] == "album" else 1) or datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
                    raise ValueError()
                if part["kind"] in _MEDIA and (len(indices) != len(part["files"]) or set(hashes) != indices):
                    raise ValueError()
                messages.update(ids)
            elif ids or "sent_at" in part:
                raise ValueError()
            if (acknowledged or selected_index is not None and ordinal < selected_index) and part["state"] not in {"sent", "retired", "unavailable"}:
                raise ValueError()
        return recipes[selected["part_id"]] if selected is not None else None
    except (KeyError, TypeError, ValueError, IndexError, StopIteration):
        raise ValueError("invalid-work-result-history") from None


def _replace(item, part_id, build, *, reason, key=None):
    from app.telegram_bots.producer import Outbox

    parts = deepcopy(item.parts)
    version = item.payload["presentation_version"]
    position = next(index for index, part in enumerate(parts) if part["part_id"] == part_id)
    parent = parts[position]
    if parent["presentation_version"] != version or parent["state"] != "sending" or parent["kind"] not in _MEDIA or parent.get("callback_group_id"):
        raise ValueError("result-file-claim-required")
    fresh = Outbox._parts(build(parent), version)
    for part in fresh:
        hashes = {str(file["source_index"]): parent["result_file_hashes"][str(file["source_index"])] for file in part.get("files", []) if str(file["source_index"]) in parent.get("result_file_hashes", {})}
        if hashes:
            part["result_file_hashes"] = hashes
    parent["state"] = "unavailable" if key is not None else "retired"
    parts[position + 1 : position + 1] = fresh
    current = [part for part in parts if part["presentation_version"] == version]
    for ordinal, part in enumerate(current):
        part["ordinal"] = ordinal
    ledger = deepcopy(item.payload["work_result_ledger"])
    ledger["events"].append({"old": part_id, "new": [part["part_id"] for part in fresh], "reason": reason, **({"key": key} if key is not None else {})})
    item.parts, item.payload = parts, {**item.payload, "work_result_ledger": ledger}


def replace_media(item, part_id, templates, *, reason):
    if reason not in {"photo-classification", "photo-rejected", "capacity"}:
        raise ValueError("invalid-result-media-reason")

    def build(parent):
        if parent["kind"] not in {"photo", "album"} or [file for _kind, files in templates for file in files] != parent["files"]:
            raise ValueError("invalid-result-media-replacement")
        if reason == "photo-rejected" and any(kind != "document" for kind, _files in templates):
            raise ValueError("invalid-result-media-replacement")
        return [_media_recipe(parent, kind, files) for kind, files in templates]

    _replace(item, part_id, build, reason=reason)


def replace_unavailable(item, part_id, *, key):
    _replace(item, part_id, lambda parent: [_warning(parent, key, file) for file in parent["files"]], reason="unavailable", key=key)


def carry_media(item, carried, hashes, *, documents=()):
    """Retire fresh roots covered by real earlier ACKs; never copy a receipt."""
    from app.telegram_bots.producer import Outbox

    version = item.payload["presentation_version"]
    parts, ledger = deepcopy(item.parts), deepcopy(item.payload["work_result_ledger"])
    current = [part for part in parts if part["presentation_version"] == version]
    if any(part["state"] != "pending" or part["message_ids"] for part in current) or ledger["events"]:
        raise ValueError("fresh-result-plan-required")
    for parent in current:
        if parent["kind"] not in _MEDIA:
            continue
        remembered = {str(file["source_index"]): hashes[file["source_index"]] for file in parent["files"] if file["source"] is not None and file["source_index"] in hashes}
        if remembered:
            parent["result_file_hashes"] = remembered
        receipts = [deepcopy(carried[file["source_index"]]) for file in parent["files"] if file["source_index"] in carried]
        fallback = parent["kind"] in {"photo", "album"} and any(file["source_index"] in documents for file in parent["files"])
        if not receipts and not fallback:
            continue
        remaining = [file for file in parent["files"] if file["source_index"] not in carried]
        children = Outbox._parts([_media_recipe(parent, "document" if receipts or file["source_index"] in documents else "photo", [file]) for file in remaining], version) if remaining else []
        for child in children:
            inherited = {str(file["source_index"]): remembered[str(file["source_index"])] for file in child["files"] if str(file["source_index"]) in remembered}
            if inherited:
                child["result_file_hashes"] = inherited
        parent["state"] = "retired"
        position = parts.index(parent) + 1
        parts[position:position] = children
        ledger["events"].append({"old": parent["part_id"], "new": [part["part_id"] for part in children], "reason": "carry" if receipts else "photo-classification", **({"receipts": receipts} if receipts else {})})
    for ordinal, part in enumerate(part for part in parts if part["presentation_version"] == version):
        part["ordinal"] = ordinal
    item.parts, item.payload = parts, {**item.payload, "work_result_ledger": ledger}
