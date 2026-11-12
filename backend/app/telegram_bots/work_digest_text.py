"""Pure bounded digest navigation; source and callback authority live elsewhere."""

from dataclasses import dataclass
from html import escape

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.digests import category_for
from app.telegram_bots.render import _clip, render, render_plain
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_requests import WorkRequestPresentation


@dataclass(frozen=True)
class DigestEntry:
    outbox_id: str
    class_: str
    kind: str
    objective: str = ""
    first_line: str = ""


def _identity(value):
    return type(value) is str and len(value) == 32 and all(char in "0123456789abcdef" for char in value)


def present_digest(entries, *, locale, protected, outbox_id, concealed=False):
    if not _identity(outbox_id) or type(protected) is not bool or type(concealed) is not bool or concealed and not protected or not 1 <= len(entries) <= 10:
        raise ValueError("invalid-digest-presentation")
    if any(not isinstance(entry, DigestEntry) or not _identity(entry.outbox_id) or entry.kind not in {"question", "decision", "review", "news"} for entry in entries):
        raise ValueError("invalid-digest-entry")
    if len({entry.outbox_id for entry in entries}) != len(entries) or len({category_for(entry.class_) for entry in entries}) != 1:
        raise ValueError("invalid-digest-membership")
    questions = all(entry.kind != "news" for entry in entries)
    if concealed:
        html = render("concealed.digest" if questions else "concealed.digest_news", locale, n=len(entries))
        keyboard = [[{"label": "label.show", "kind": "reveal", "target_kind": "outbox", "target_id": outbox_id, "payload": {"outbox_id": outbox_id}}]]
    else:
        lines = [render("digest.header" if questions else "digest.header_news", locale, n=len(entries))]
        keyboard = []
        for entry in entries:
            if type(entry.objective) is not str or type(entry.first_line) is not str:
                raise ValueError("invalid-digest-navigation-text")
            if entry.kind == "news":
                first = entry.first_line.splitlines()[0] if entry.first_line else ""
                line = _clip(first.strip(), 60, at_word=True)
                label = line
            else:
                objective = " ".join(entry.objective.split())
                if not objective:
                    raise ValueError("empty-digest-objective")
                line = render_plain("summary.item." + entry.kind, locale, objective=objective)
                label = render_plain("summary.button." + entry.kind, locale, objective=objective)
            if not line:
                raise ValueError("empty-digest-line")
            lines.append(escape(line, quote=True))
            keyboard.append(
                [{"label": "label.choice", "label_values": {"label": _clip(label, 32)}, "kind": "digest_item", "target_kind": "outbox", "target_id": entry.outbox_id, "payload": {"digest_id": outbox_id, "outbox_id": entry.outbox_id}}]
            )
        html = "\n".join(lines)
    chunks = split_html(html)
    # Ten bounded navigation lines fit one Telegram message in either locale.
    # Full member content is rendered separately after the saved item press.
    if len(chunks) != 1:
        raise ValueError("digest-navigation-too-large")
    parts = ({"kind": "text", "html": html, "visible_text": visible_text(html), "protect_content": protected},)
    return WorkRequestPresentation(parts, tuple(tuple(row) for row in canonical_keyboard(keyboard)))
