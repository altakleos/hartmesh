"""Pure own-action replies, with complete current previews before fresh Starts."""

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.pages import page_address
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_view_sources import detail_read, present_view
from app.telegram_bots.work_views import WorkViewPresentation


def present_execution(source, basis, link):
    notice, act = source.work["notice"], source.work["act"]
    locale = basis["locale"]
    wait = render_plain("start.wait_two_minutes", locale) if source.work["wait_seconds"] == 120 else render_plain("start.wait_seconds", locale, n=source.work["wait_seconds"])
    html = source.values.get("capacity_html") if notice["key"] == "busy.work_limit" else None
    html = html or render(notice["key"], locale, **source.values, wait=wait, product=source.work["product"])
    keyboard = []
    if notice["cancel"] or notice["retry"]:
        keyboard.append([dict(label="label.cancel" if notice["cancel"] else "label.retry_action", kind="cancel_wait" if notice["cancel"] else "act_check", target_kind="act", target_id=act["id"], payload={"act_id": act["id"]})])
    if notice["preview"]:
        # Reuse the canonical detail renderer. Its final source suppresses Starts
        # for unresolved promises and preserves the full assignment before controls.
        from dataclasses import replace

        detail = detail_read(source.work["detail"])
        view = present_view(replace(source, key="view.detail", values=source.work["detail_dates"]), basis, link)
        html += "\n\n" + "".join(part["html"] for part in view.parts)
        keyboard.extend([list(line) for line in view.keyboard])
        if detail.record["id"] != source.work_id:
            raise ValueError("execution-preview-work-mismatch")
    else:
        address = page_address("web.work", link, work_id=source.work_id, permissions=3, public_base_url=basis["public_base_url"])
        if address:
            keyboard.append([{"label": "label.open_assignment", "url": address}])
    parts = tuple(dict(kind="text", html=chunk, visible_text=visible_text(chunk), protect_content=source.protected) for chunk in split_html(html))
    return WorkViewPresentation(parts, tuple(tuple(line) for line in canonical_keyboard(keyboard)))
