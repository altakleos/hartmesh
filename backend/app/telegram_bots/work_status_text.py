"""Pure Work status copy; starting and stopping remain web actions."""

from app.telegram_bots.render import render

STATUS_KEYS = frozenset({"status.starting", "status.working", "status.waiting_you", "status.waiting_answer", "status.checking", "status.stopping", "status.stopped", "status.finished", "status.ended"})


def status_html(key, values, locale):
    if key not in STATUS_KEYS:
        raise ValueError("unsupported-work-status")
    text = render(key, locale, **values)
    return text.rsplit("\n", 1)[0] if key == "status.working" and not values["progress"] else text


def render_status(source, *, locale):
    return status_html(source.key, source.values, locale)
