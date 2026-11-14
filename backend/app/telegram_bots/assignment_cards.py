"""Pure full assignment draft cards; source readers own all consent authority."""

from dataclasses import replace

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_view_sources import present_view
from app.telegram_bots.work_views import WorkViewPresentation

ASSIGNMENT_KEYS = frozenset({"assignment.card"})


def present_assignment(source, basis, link):
    if source.key not in ASSIGNMENT_KEYS:
        raise ValueError("unsupported-assignment-card")
    data, locale = source.work, basis["locale"]
    phase, keyboard = data["phase"], []
    if phase in {"discarded", "replaced", "answering", "expired"}:
        html = render("stale.draft" if phase == "expired" else "assign." + phase, locale)
    elif phase == "saved":
        record = data["detail"]["record"]
        html = render("assign.saved_line", locale, name=source.values["name"]) + "\n\n" if not record.get("attempt") and not record.get("activation_pending") else ""
        current = present_view(replace(source, key="view.detail", values=data["detail_dates"]), basis, link)
        html += "".join(part["html"] for part in current.parts)
        if data["due_note"]:
            html += "\n" + render("assign.due_note", locale, name=source.values["name"])
        keyboard = [[button for button in line if button.get("kind") == "work_start" or "url" in button] for line in current.keyboard]
        keyboard = [line for line in keyboard if line]
    else:
        draft = data["draft"]
        title = render("assign.card_title_proposed" if data["source"] == "tool" else "assign.card_title", locale, name=source.values["name"])
        # The title is already escaped; render the canonical fields separately
        # so the whole objective and criteria survive without double escaping.
        html = title + "\n" + render("assign.card", locale, objective_full=draft["objective"], success_criteria=draft["success_criteria"])
        if source.values.get("due") is not None:
            html += "\n" + render("assign.due", locale, due=source.values["due"])
        if data["due_note"]:
            html += "\n" + render("assign.due_note", locale, name=source.values["name"])
        if phase == "draft":

            def button(kind, label):
                return {"label": label, "kind": kind, "target_kind": "draft", "target_id": data["draft_id"], "payload": {"draft_id": data["draft_id"]}, "expires_at": data["expires_at"]}

            primary = []
            if data["can_start"]:
                primary.append(button("assign_start", "label.start_now"))
            if data["can_save"]:
                primary.append(button("assign_save", "label.save_later"))
            keyboard = [
                *([primary] if primary else []),
                [button("assign_edit", "label.edit"), button("assign_answer", "label.just_answer") if data["source"] == "tool" else button("assign_discard", "label.discard")],
            ]
        elif phase in {"saving", "unconfirmed", "refused"}:
            html = render(data["notice"]["key"], locale, **source.values) + "\n\n" + html
            notice, act = data["notice"], data["act"]
            if notice["cancel"] or notice["retry"]:
                keyboard = [
                    [{"label": "label.cancel" if notice["cancel"] else "label.retry_action", "kind": "cancel_wait" if notice["cancel"] else "act_check", "target_kind": "act", "target_id": act["id"], "payload": {"act_id": act["id"]}}]
                ]
        else:
            raise ValueError("unsupported-assignment-phase")
    parts = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for chunk in split_html(html))
    if phase == "draft" and "heard" in source.values:
        from app.telegram_bots.voice import _echo

        heard = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for text in source.values["heard"] for chunk in split_html(render("voice.heard", locale, text=_echo(text))))
        parts = heard + parts
    return WorkViewPresentation(parts, tuple(tuple(line) for line in canonical_keyboard(keyboard)))
