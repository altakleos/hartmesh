"""Pure colleague-event notices; source authority and links belong to callers."""

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_requests import WorkRequestPresentation

NEWS_KEYS = frozenset(
    {
        "request.answered",
        "decision.recorded",
        "result.accepted",
        "result.changes",
        "work.cancelled",
        "work.changed",
        "work.reopened",
        "attempt.stopped",
        "attempt.stopped_changed",
        "attempt.stopped_withdrawn",
        "work.proposed",
        "remind.ready_to_resume",
    }
)


def present_news(key, *, values, locale, protected, concealed=False, outbox_id=None):
    if key not in NEWS_KEYS:
        raise ValueError("unsupported-work-news")
    keyboard = []
    if concealed:
        if type(outbox_id) is not str or len(outbox_id) != 32 or any(char not in "0123456789abcdef" for char in outbox_id):
            raise ValueError("concealed-source-required")
        html = render("concealed.proposed" if key == "work.proposed" else "concealed.info", locale)
        keyboard = [[{"label": "label.show", "kind": "reveal", "target_kind": "outbox", "target_id": outbox_id, "payload": {"outbox_id": outbox_id}}]]
    else:
        html = render(key, locale, **values)
        if key == "decision.recorded" and values.get("note"):
            html += "\n" + render("decision.note", locale, note=values["note"])
    parts = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": protected} for chunk in split_html(html))
    return WorkRequestPresentation(parts, tuple(tuple(row) for row in canonical_keyboard(keyboard)))
