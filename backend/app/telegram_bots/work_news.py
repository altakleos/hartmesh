"""Pure colleague-event notices; source authority and links belong to callers."""

from app.telegram_bots.attempt_text import ATTEMPT_KEYS, attempt_html, concealed_attempt
from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_requests import WorkRequestPresentation
from app.telegram_bots.work_status_text import STATUS_KEYS, status_html

NEWS_KEYS = (
    frozenset(
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
    | ATTEMPT_KEYS
)


def news_class(key):
    if key == "capacity.offer":
        return "service"
    return (
        "reply"
        if key in {"view.list", "view.detail", "execution.activate", "execution.stop", "assignment.card", "assignment.prompt", "management.edit", "management.withdraw", "management.prompt"}
        else "progress"
        if key in STATUS_KEYS
        else "reminder"
        if key == "remind.ready_to_resume"
        else "interrupt"
        if key in ATTEMPT_KEYS
        else "info"
    )


def news_owner(payload):
    """Select only a known source owner; malformed persisted bases stay refused."""
    basis = payload.get("work_news_basis")
    if type(basis) is not dict or type(basis.get("source")) is not dict or type(basis["source"].get("key")) is not str:
        return None
    if basis["source"]["key"] in {"execution.activate", "execution.stop"}:
        return "work_execution_sources"
    if basis["source"]["key"] == "capacity.offer":
        return "work_capacity_sources"
    if basis["source"]["key"] == "assignment.card":
        return "assignment_sources"
    if basis["source"]["key"] == "assignment.prompt":
        return "assignment_prompt_sources"
    if basis["source"]["key"] in {"management.edit", "management.withdraw"}:
        return "work_management_sources"
    if basis["source"]["key"] == "management.prompt":
        return "work_management_prompt_sources"
    return "work_view_sources" if basis["source"]["key"] in {"view.list", "view.detail"} else "work_status_sources" if basis["source"]["key"].startswith("status.") else "work_news_sources"


def present_news(key, *, values, locale, protected, concealed=False, outbox_id=None):
    if key not in NEWS_KEYS | STATUS_KEYS:
        raise ValueError("unsupported-work-news")
    keyboard = []
    if concealed:
        if type(outbox_id) is not str or len(outbox_id) != 32 or any(char not in "0123456789abcdef" for char in outbox_id):
            raise ValueError("concealed-source-required")
        html = render("concealed.progress" if key in STATUS_KEYS else concealed_attempt(key, values) if key in ATTEMPT_KEYS else "concealed.proposed" if key == "work.proposed" else "concealed.info", locale)
        keyboard = [[{"label": "label.show", "kind": "reveal", "target_kind": "outbox", "target_id": outbox_id, "payload": {"outbox_id": outbox_id}}]]
    else:
        html = status_html(key, values, locale) if key in STATUS_KEYS else attempt_html(key, values, locale) if key in ATTEMPT_KEYS else render(key, locale, **values)
        if key == "decision.recorded" and values.get("note"):
            html += "\n" + render("decision.note", locale, note=values["note"])
    parts = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": protected} for chunk in split_html(html))
    return WorkRequestPresentation(parts, tuple(tuple(row) for row in canonical_keyboard(keyboard)))
