"""Bounded summary navigation; full previews remain owned by their sources."""

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import _clip, render, render_plain
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.summary import SUMMARY_BUTTONS_MAX, SUMMARY_LINES_PER_PART
from app.telegram_bots.work_digest_text import _identity
from app.telegram_bots.work_requests import WorkRequestPresentation


def present_summary(sections, *, locale, protected, concealed, outbox_id, link_id, time, work_url):
    if not _identity(outbox_id) or not _identity(link_id) or type(protected) is not bool or type(concealed) is not bool or concealed and not protected:
        raise ValueError("invalid-summary-presentation")
    if set(sections) != {"finished", "working", "interrupted", "waiting"}:
        raise ValueError("invalid-summary-sections")
    if concealed:
        html = render("concealed.summary", locale)
        keyboard = [[{"label": "label.show", "kind": "reveal", "target_kind": "outbox", "target_id": outbox_id, "payload": {"outbox_id": outbox_id}}]]
    else:
        lines, keyboard = [render("summary.header", locale, time=time)], []
        for kind in ("finished", "working", "interrupted", "waiting"):
            items = sections[kind]
            if not items:
                continue
            shown = items[:SUMMARY_LINES_PER_PART]
            if kind == "waiting":
                text = "; ".join(render_plain("summary.item." + item["kind"], locale, objective=" ".join(item["objective"].split())) for item in shown)
                lines.append(render("summary.waiting", locale, items=text))
            else:
                lines.extend(render("summary." + kind, locale, objective=" ".join(item["objective"].split())) for item in shown)
            if len(items) > len(shown):
                lines.append(render("summary.more", locale, n=len(items) - len(shown)))
        requests = [item for item in sections["waiting"] if item["kind"] in {"question", "decision", "review"}]
        for item in requests[:SUMMARY_BUTTONS_MAX]:
            if not _identity(item["request_id"]):
                raise ValueError("invalid-summary-request")
            label = _clip(render_plain("summary.button." + item["kind"], locale, objective=" ".join(item["objective"].split())), 32)
            keyboard.append(
                [{"label": "label.choice", "label_values": {"label": label}, "kind": "digest_item", "target_kind": "request", "target_id": item["request_id"], "payload": {"summary_id": outbox_id, "request_id": item["request_id"]}}]
            )
        if len(sections["waiting"]) > SUMMARY_BUTTONS_MAX and work_url is not None:
            keyboard.append([{"label": "label.open_work", "url": work_url}])
        keyboard.append([{"label": "label.choice", "label_values": {"label": render_plain("summary.button.off", locale)}, "kind": "summary_off", "target_kind": "link", "target_id": link_id, "payload": {}}])
        html = "\n".join(lines)
    if len(split_html(html)) != 1:
        raise ValueError("summary-navigation-too-large")
    return WorkRequestPresentation(({"kind": "text", "html": html, "visible_text": visible_text(html), "protect_content": protected},), tuple(tuple(row) for row in canonical_keyboard(keyboard)))
