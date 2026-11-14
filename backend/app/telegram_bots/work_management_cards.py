"""Pure saved-assignment previews; effect authority belongs to their owners."""

from dataclasses import replace

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_view_sources import present_view
from app.telegram_bots.work_views import WorkViewPresentation

MANAGEMENT_KEYS = frozenset({"management.edit", "management.withdraw"})


def present_management(source, basis, link):
    if source.key not in MANAGEMENT_KEYS:
        raise ValueError("unsupported-management-card")
    data, locale = source.work, basis["locale"]
    phase, keyboard = data["phase"], []

    def button(kind, label, target_kind, target_id):
        return {"label": label, "kind": kind, "target_kind": target_kind, "target_id": target_id, "payload": {"draft_id" if target_kind == "draft" else "work_id": target_id}, "expires_at": data["expires_at"]}

    if phase in {"discarded", "replaced", "expired"}:
        html = render("stale.draft" if phase == "expired" else "assign." + phase, locale)
    elif phase == "editing":
        html = render("assign.edit_prompt", locale)
    elif source.key == "management.edit" and phase in {"draft", "unchanged"}:
        draft, identity = data["draft"], data["draft_id"]
        html = render("work.change_title", locale, name=source.values["name"]) + "\n" + render("assign.card", locale, objective_full=draft["objective"], success_criteria=draft["success_criteria"])
        if source.values.get("due"):
            html += "\n" + render("assign.due", locale, due=source.values["due"])
        if data.get("edit_failed"):
            html += "\n" + render("assign.draft_failed", locale)
        if phase == "unchanged":
            html += "\n" + render("work.change_none", locale)
        else:
            keyboard.append([button("work_change_save", "label.save_changes", "draft", identity)])
        keyboard.append([button("assign_edit", "label.edit", "draft", identity), button("assign_discard", "label.discard", "draft", identity)])
    elif source.key == "management.withdraw" and phase == "confirm":
        html = render("work.withdraw_confirm_running" if data["running"] else "work.withdraw_confirm", locale, name=source.values["name"], objective_full=data["objective"])
        keyboard = [[button("work_withdraw_confirm", "label.yes_withdraw", "work", source.work_id), button("back", "label.back", "work", source.work_id)]]
    elif source.key == "management.withdraw" and phase == "withdrawn":
        html = render("work.withdrawn", locale, objective_full=data["objective"])
        from app.telegram_bots.pages import page_address

        address = page_address("web.work", link, work_id=source.work_id, permissions=3, public_base_url=basis["public_base_url"])
        if address:
            keyboard = [[{"label": "label.open_assignment", "url": address}]]
    elif phase in {"saved", "changed", "back"}:
        view = present_view(replace(source, key="view.detail", values=data["detail_dates"]), basis, link)
        notice = render("stale.changed_resend" if phase == "changed" else "work.change_saved_running" if data["running"] else "work.change_saved", locale, name=source.values["name"])
        html = "".join(part["html"] for part in view.parts) + ("\n\n" + notice if phase != "back" else "")
        keyboard = [list(row) for row in view.keyboard]
    elif phase in {"saving", "refused", "cancelled"}:
        key = data["notice"]["key"] or ("denied.change" if source.key == "management.edit" else "denied.withdraw")
        html = render(key, locale, name=source.values["name"])
    else:
        raise ValueError("unsupported-management-phase")
    notice = data.get("notice", {})
    if notice.get("key") == "work.stop_unconfirmed":
        html += "\n\n" + render(notice["key"], locale)
    if notice.get("cancel") or notice.get("retry"):
        act = data["act"]
        keyboard.insert(
            0, [{"label": "label.cancel" if notice["cancel"] else "label.retry_action", "kind": "cancel_wait" if notice["cancel"] else "act_check", "target_kind": "act", "target_id": act["id"], "payload": {"act_id": act["id"]}}]
        )
    if source.values.get("heard") is not None and phase in {"draft", "unchanged"}:
        html = render("voice.heard", locale, text=source.values["heard"]) + "\n\n" + html
    parts = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for chunk in split_html(html))
    return WorkViewPresentation(parts, tuple(tuple(row) for row in canonical_keyboard(keyboard)))
