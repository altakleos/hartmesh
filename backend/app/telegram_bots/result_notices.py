"""Full result notices for recipients who are not being asked to review."""

from html import escape

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_requests import WorkRequestPresentation
from app.telegram_bots.work_result_files import result_files, result_media


def present_result(work, *, key, name, person, protected, locale, capacity, max_bytes, concealed=False, outbox_id=None):
    """Pure rendering; callers must separately qualify the canonical source."""
    if key not in {"result.done", "result.waiting_review"} or key == "result.waiting_review" and (type(person) is not str or not person):
        raise ValueError("invalid-result-notice")
    files = result_files(work, locale=locale)
    media = result_media(files, capacity=capacity, max_bytes=max_bytes, protected=protected)
    keyboard = []
    if concealed:
        if type(outbox_id) is not str or len(outbox_id) != 32 or any(ch not in "0123456789abcdef" for ch in outbox_id):
            raise ValueError("concealed-source-required")
        html = render("concealed.result", locale)
        keyboard = [[{"label": "label.show", "kind": "reveal", "target_kind": "outbox", "target_id": outbox_id, "payload": {"outbox_id": outbox_id}}]]
    else:
        html = render(key, locale, objective=work["objective"], name=name, statement=work["outcome"]["statement"], person=person or "")
        if work.get("suggestion"):
            html += "\n\n" + render("result.suggestion", locale, name=name, suggestion=work["suggestion"]["statement"])
        if files:
            html += "\n\n" + render("result.files_above", locale) + "\n" + "\n".join(escape(file["filename"]) for file in files)
    parts = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": protected} for chunk in split_html(html))
    return WorkRequestPresentation((() if concealed else media) + parts, tuple(tuple(row) for row in canonical_keyboard(keyboard)))
