"""Ownership of deferred Work prompts; source dispatch remains separately fenced."""

import json
from datetime import datetime, timedelta

from app.telegram_bots.callbacks import CallbackRefused, _aware, _plain, _visibility

PURPOSES = {"answer": "answer", "cannot": "cannot_provide", "wrong": "wrong_recipient", "decision": "decide_own", "decision_note": "decide_note", "changes": "request_changes"}


def prompt_parts(item):
    """Identify canonical prompt parts for attachment/ACK, never authorize a send."""
    try:
        basis = item.payload["work_request_basis"]
        if item.class_ != "reply" or basis["version"] != 3 or basis["interaction"]["view"] != "prompt" or basis["interaction"]["purpose"] not in PURPOSES or basis["keyboard"]:
            raise ValueError()
        identity = basis["prompt_id"]
        if type(identity) is not str or len(identity) != 32 or any(ch not in "0123456789abcdef" for ch in identity):
            raise ValueError()
        parts = tuple(part for part in item.parts if part["presentation_version"] == item.payload["presentation_version"])
        if not parts or len(parts) != len(basis["parts"]):
            raise ValueError()
        for index, (part, recipe) in enumerate(zip(parts, basis["parts"], strict=True)):
            if part["ordinal"] != index or part.get("kind") != "text" or any(part.get(key) != value for key, value in recipe.items()) or part.get("stored_keyboard") or part.get("callback_group_id"):
                raise ValueError()
            if bool(part.get("force_reply")) != (index == len(parts) - 1):
                raise ValueError()
        return parts
    except (KeyError, TypeError, ValueError):
        raise CallbackRefused() from None


def validate_prompt(item, prompt):
    parts = prompt_parts(item)
    basis = item.payload["work_request_basis"]
    expected = {"purpose": PURPOSES[basis["interaction"]["purpose"]], "request_id": item.request_id, "work_id": item.work_id, "seen": basis["source"]["seen"], "interaction": basis["interaction"]}
    if prompt is None or prompt.id != basis["prompt_id"] or prompt.kind != "prompt" or json.dumps({key: _plain(prompt.payload.get(key)) for key in expected}, sort_keys=True) != json.dumps(expected, sort_keys=True):
        raise CallbackRefused()
    visibility = _visibility(prompt.payload.get("prompt_visibility"))
    if prompt.target_kind is not None or prompt.target_id is not None or visibility["state"] == "pending" and (prompt.message_id is not None or prompt.expires_at is not None):
        raise CallbackRefused()
    if (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != (item.link_id, item.chat_id, item.binding_generation, item.message_thread_id):
        raise CallbackRefused()
    return parts


def shown_prompt_messages(item, *, accepted_at=None):
    """Every alias belongs to the complete, typed and ordered prompt ACK."""
    messages = []
    try:
        for part in prompt_parts(item):
            if (
                part["state"] != "sent"
                or len(part["message_ids"]) != 1
                or type(part["message_ids"][0]) is not int
                or part["message_ids"][0] <= 0
                or part["message_ids"][0] in messages
                or datetime.fromisoformat(part["sent_at"]).utcoffset() is None
            ):
                raise CallbackRefused()
            if accepted_at is not None:
                dispatched = datetime.fromisoformat(part["dispatched_at"])
                if dispatched.utcoffset() is None or dispatched > _aware(accepted_at):
                    raise CallbackRefused()
            messages.append(part["message_ids"][0])
    except (KeyError, TypeError, ValueError):
        raise CallbackRefused() from None
    return tuple(messages)


def validate_shown_prompt(item, prompt):
    """Match prompt visibility to its exact final ACK; return that ACK time."""
    parts = validate_prompt(item, prompt)
    shown_prompt_messages(item)
    final = parts[-1]
    visibility = _visibility(prompt.payload.get("prompt_visibility"))
    try:
        acknowledged_at = _aware(datetime.fromisoformat(final["sent_at"]))
        if (
            visibility["state"] != "visible"
            or visibility["part_id"] != final["part_id"]
            or final["state"] != "sent"
            or not final["message_ids"]
            or type(prompt.message_id) is not int
            or prompt.message_id != final["message_ids"][-1]
            or type(prompt.payload.get("presentation_version")) is not int
            or prompt.payload["presentation_version"] != item.payload["presentation_version"]
            or prompt.payload.get("anchor_outbox_id") != item.id
            or _aware(prompt.created_at) != acknowledged_at
            or prompt.expires_at is None
            or _aware(prompt.expires_at) != acknowledged_at + timedelta(seconds=visibility["ttl_seconds"])
        ):
            raise CallbackRefused()
    except (KeyError, TypeError, ValueError, OverflowError):
        raise CallbackRefused() from None
    return acknowledged_at
