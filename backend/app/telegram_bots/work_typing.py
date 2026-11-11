"""Ownership of deferred Work prompts; source dispatch remains separately fenced."""

import json

from app.telegram_bots.callbacks import CallbackRefused, _plain, _visibility

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
