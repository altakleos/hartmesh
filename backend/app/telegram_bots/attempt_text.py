"""Pure interruption wording; provenance and navigation belong to the source."""

from html import escape

from app.telegram_bots.render import _clip, render

ATTEMPT_KEYS = frozenset({"interrupted", "attempt.failed", "attempt.no_result", "attempt.stopped_restart"})
CONTAINED_REASONS = frozenset({"container-absent", "run-orphaned", "run-never-started", "start-failed", "outcome-uncertain", "idle", "lifecycle", "storage-recovery", "operator", "host-replaced"})
FAILED_REASONS = frozenset({"busy", "run-limit", "restarting", "starting-elsewhere", "start-failed", "files-full", "storage-busy"})
NOT_STARTED = frozenset({"run-never-started", "start-failed"})
FILES_REASONS = frozenset({"outcome-uncertain", "host-replaced", "storage-recovery"})


def concealed_attempt(key, values):
    if key == "interrupted" and values.get("reason") in NOT_STARTED:
        return "concealed.not_started"
    return "concealed.interrupted" if key in {"interrupted", "attempt.stopped_restart"} else "concealed.no_result"


def attempt_html(key, values, locale):
    if key == "interrupted":
        reason = values.get("reason")
        title = "interrupted.title_not_started" if reason in NOT_STARTED else "interrupted"
        lines = [render(title, locale, **values), render("interrupted.reason." + (reason if reason in CONTAINED_REASONS else "unknown"), locale, **values)]
        if reason in CONTAINED_REASONS - NOT_STARTED - {"storage-recovery", "host-replaced"}:
            lines.append(render("attempt.work_kept", locale))
        return "\n".join(lines)
    if key == "attempt.failed":
        reason = values.get("reason")
        if reason not in FAILED_REASONS:
            return render("attempt.failed.other", locale, **values)
        form = "files-full_ask" if reason == "files-full" and values.get("reader_is_manager") is not True else reason
        return render(key, locale, **values) + "\n" + render("attempt.failed." + form, locale, **values)
    if key == "attempt.no_result":
        excerpt = values.get("excerpt")
        lines = [render("attempt.no_result_excerpt" if excerpt else key, locale, **values)]
        if values.get("progress"):
            lines.append(render("attempt.latest", locale, **values))
        if excerpt:
            lines.extend((render("attempt.wrote_last", locale, **values), escape(_clip(excerpt, 600, at_word=True), quote=True)))
        return "\n".join(lines)
    if key == "attempt.stopped_restart":
        return render(key, locale, **values)
    raise ValueError("unsupported-attempt-notice")
