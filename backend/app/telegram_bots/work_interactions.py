"""Complete, unsubmitted Work interaction views; presentation performs no action."""

from copy import deepcopy
from html import escape

from pydantic import TypeAdapter

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_requests import WorkRequestPresentation, present_request
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.work_contract import Statement

_STATEMENT = TypeAdapter(Statement)
_FIELDS = {
    "request": {"view"},
    "current": {"view"},
    "cannot": {"view"},
    "decision": {"view", "decision", "note"},
    "accept": {"view"},
    "changes": {"view", "note"},
    "answer": {"view", "text"},
    "voice": {"view", "text"},
    "prompt": {"view", "purpose"},
}
_PROMPTS = {
    "comment": (None, "ask.type_prompt"),
    "answer": ("information", "ask.type_prompt"),
    "cannot": ("information", "ask.cant_prompt"),
    "wrong": ("information", "ask.cant_prompt"),
    "decision": ("decision", "decide.own_prompt"),
    "decision_note": ("decision", "decide.note_prompt"),
    "changes": ("review", "review.changes_prompt"),
}


def interaction_state(value, source):
    """Closed JSON state that can be retained without authorizing submission."""
    if type(value) is not dict or type(value.get("view")) is not str or value["view"] not in _FIELDS:
        raise ValueError("invalid-work-interaction")
    state = deepcopy(value)
    view = state["view"]
    fields = _FIELDS[view] | ({"decision"} if view == "prompt" and state.get("purpose") == "decision_note" else set())
    if view == "decision" and state.get("refreshed") is True:
        fields = fields | {"refreshed"}
    if view == "changes" and state.get("replaced") is True:
        fields = fields | {"replaced"}
    if set(state) != fields:
        raise ValueError("invalid-work-interaction")
    for key in {"text", "note", "decision"} & fields:
        if key == "note" and view == "decision" and state[key] is None:
            continue
        state[key] = _STATEMENT.validate_python(state[key])
    purpose = source.request["purpose"]
    if view == "prompt":
        if type(state["purpose"]) is not str or state["purpose"] not in _PROMPTS:
            raise ValueError("invalid-work-prompt")
        expected = _PROMPTS[state["purpose"]][0]
        if state["purpose"] == "comment" and purpose in {"decision", "review"}:
            expected = purpose
    else:
        expected = {"cannot": "information", "decision": "decision", "accept": "review", "changes": "review", "answer": "information", "voice": purpose, "request": purpose, "current": purpose}[view]
    if purpose != expected:
        raise ValueError("wrong-work-interaction-purpose")
    if source.request["state"] == "closed" or source.request["needs_routing"]:
        raise AgentConflict("The request is no longer available")
    responding = view in {"request", "current"} and purpose == "information" or view in {"cannot", "answer", "voice"} or view == "prompt" and state["purpose"] in {"answer", "cannot", "wrong", "comment"}
    if not source.request["can_respond" if responding else "can_manage"]:
        raise AgentDenied("The interaction is unavailable")
    if view == "decision":
        _STATEMENT.validate_python(state["decision"] + ("\n\n" + state["note"] if state["note"] else ""))
    return state


def present_interaction(source, value, *, locale, expires_at=None):
    state = interaction_state(value, source)
    view = state["view"]
    base = present_request(source, locale=locale)
    if view == "request":
        return base
    if view == "current":
        html = render("stale.changed_resend", locale) + "\n\n" + "".join(part["html"] for part in base.parts)
        return WorkRequestPresentation(
            tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for chunk in split_html(html)),
            base.keyboard,
        )
    payload = {"request_id": source.request["id"], "work_id": source.work["id"], "seen": deepcopy(source.seen), "interaction": state}

    def button(kind, label):
        return {"label": label, "kind": kind, "target_kind": "request", "target_id": source.request["id"], "payload": deepcopy(payload)}

    suffix, keyboard = "", []
    if view == "cannot":
        keyboard = [[button(kind, label)] for kind, label in (("hi_cannot", "label.cannot_provide"), ("hi_wrong", "label.wrong_recipient"), ("back", "label.back"))]
    elif view == "decision":
        suffix = render("decide.confirm_note" if state["note"] else "decide.confirm", locale, decision=state["decision"], note=state["note"])
        keyboard = [[button("decide_confirm", "label.confirm")]]
        keyboard.append(([button("decide_note", "label.add_note")] if not state["note"] else []) + [button("back", "label.back")])
    elif view == "accept":
        suffix = render("review.accept_confirm", locale, name=source.request["instance_name"])
        keyboard = [[button("accept_confirm", "label.yes_accept"), button("back", "label.back")]]
    elif view == "changes":
        suffix = render("review.changes_confirm", locale, note=state["note"])
        keyboard = [[button(kind, label)] for kind, label in (("changes_confirm", "label.request_these_changes"), ("changes_open", "label.edit_note"), ("dismiss", "label.discard"))]
        for line in keyboard:
            for control in line:
                control["expires_at"] = expires_at
    elif view in {"answer", "voice"}:
        suffix = escape(state["text"]) + "\n\n" + render("ask.is_this_answer", locale) if view == "answer" else render("voice.confirm_answer", locale, text=state["text"])
        keyboard = [[button("answer_yes" if view == "answer" else "voice_save_answer", "label.save_answer")], [button("answer_no" if view == "answer" else "voice_discard", "label.new_message" if view == "answer" else "label.discard")]]
    else:
        suffix = render(_PROMPTS[state["purpose"]][1], locale, question=source.request["question"], name=source.request["instance_name"])
        if state["purpose"] == "decision_note":
            suffix = escape(state["decision"]) + "\n\n" + suffix
    html = "".join(part["html"] for part in base.parts) + ("\n\n" + suffix if suffix else "")
    if view == "changes" or view == "decision" and state.get("refreshed") is True:
        html = render("stale.replaced" if state.get("replaced") is True else "stale.changed_resend", locale) + "\n\n" + html
    return WorkRequestPresentation(
        tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for chunk in split_html(html)),
        tuple(tuple(line) for line in canonical_keyboard(keyboard)),
    )
