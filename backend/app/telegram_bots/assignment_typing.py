"""Deferred assignment prompts use their complete ordered Telegram ACK."""

from datetime import datetime, timedelta
from types import SimpleNamespace

from app.telegram_bots.assignment_prompt_sources import PURPOSES
from app.telegram_bots.callbacks import CallbackRefused, _aware, _visibility
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_views import WorkViewPresentation


def present_prompt(source, basis):
    intent = source.work["intent"]
    text = render("assign.edit_prompt" if source.key == "management.prompt" else PURPOSES[intent["purpose"]], basis["locale"], name=source.values["name"])
    parts = [{"kind": "text", "html": html, "visible_text": visible_text(html), "protect_content": source.protected} for html in split_html(text)]
    parts[-1]["force_reply"] = True
    return WorkViewPresentation(tuple(parts), ())


def prompt_parts(item):
    from app.telegram_bots.work_news_delivery import frozen_news

    try:
        _basis, source, _part = frozen_news(SimpleNamespace(item=item, link=None, part=None))
        if source.key not in {"assignment.prompt", "management.prompt"}:
            raise ValueError()
        parts = tuple(part for part in item.parts if part["presentation_version"] == item.payload["presentation_version"])
        if not parts or any(part.get("stored_keyboard") or part.get("callback_group_id") or bool(part.get("force_reply")) != (index == len(parts) - 1) for index, part in enumerate(parts)):
            raise ValueError()
        return parts
    except (KeyError, TypeError, ValueError):
        raise CallbackRefused() from None


def validate_prompt(item, prompt, *, attached=False):
    parts = prompt_parts(item)
    intent = item.payload["management_prompt_intent"] if "management_prompt_intent" in item.payload else item.payload["assignment_prompt_intent"]
    if (
        prompt is None
        or prompt.kind != "prompt"
        or prompt.id != intent["prompt_id"]
        or (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != (item.link_id, item.chat_id, item.binding_generation, item.message_thread_id)
        or prompt.payload.get("purpose") != intent["purpose"]
        or prompt.payload.get("draft_id") != intent["draft_id"]
        or prompt.target_kind is not None
        or prompt.target_id is not None
    ):
        raise CallbackRefused()
    visibility = _visibility(prompt.payload.get("prompt_visibility"))
    if visibility["state"] == "pending" and (prompt.message_id is not None or prompt.expires_at is not None):
        raise CallbackRefused()
    if attached and (visibility["part_id"] != parts[-1]["part_id"] or prompt.payload.get("anchor_outbox_id") != item.id or prompt.payload.get("presentation_version") != item.payload["presentation_version"]):
        raise CallbackRefused()
    return parts


def shown_prompt(item, prompt, *, accepted_at=None):
    parts = validate_prompt(item, prompt)
    messages = []
    try:
        for part in parts:
            sent = datetime.fromisoformat(part["sent_at"])
            dispatched = datetime.fromisoformat(part["dispatched_at"])
            if (
                part["state"] != "sent"
                or len(part["message_ids"]) != 1
                or type(part["message_ids"][0]) is not int
                or part["message_ids"][0] <= 0
                or part["message_ids"][0] in messages
                or sent.utcoffset() is None
                or dispatched.utcoffset() is None
                or accepted_at is not None
                and dispatched > _aware(accepted_at)
            ):
                raise ValueError()
            messages.append(part["message_ids"][0])
        final, visibility = parts[-1], _visibility(prompt.payload["prompt_visibility"])
        acknowledged = _aware(datetime.fromisoformat(final["sent_at"]))
        if (
            visibility["state"] != "visible"
            or visibility["part_id"] != final["part_id"]
            or prompt.message_id != messages[-1]
            or prompt.payload.get("anchor_outbox_id") != item.id
            or prompt.payload.get("presentation_version") != item.payload["presentation_version"]
            or _aware(prompt.created_at) != acknowledged
            or prompt.expires_at is None
            or _aware(prompt.expires_at) != acknowledged + timedelta(seconds=visibility["ttl_seconds"])
        ):
            raise ValueError()
        return acknowledged, tuple(messages)
    except (KeyError, TypeError, ValueError):
        raise CallbackRefused() from None
