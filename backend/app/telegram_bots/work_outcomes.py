"""Pure wording for a qualified Work action; no read, command or retry authority."""

from dataclasses import dataclass

from app.telegram_bots.errors import WORK_DENIALS, key_for_reader
from deerflow.agent_instances.work import UNRESOLVED


@dataclass(frozen=True)
class WorkOutcomeText:
    key: str
    echo: str | None = None
    open_assignment: bool = False
    retry_action: bool = False
    refresh_request: bool = False
    route_question: bool = False


def outcome_text(act, *, purpose, permissions, attempt_status, request_changed=False, continued=False):
    """The caller must qualify the exact receipt and current reader before display."""
    if act.state == "uncertain":
        return WorkOutcomeText("act.unconfirmed", retry_action=True)
    if act.state in {"pending", "waiting"}:
        if act.reason == "attempt-unresolved":
            return WorkOutcomeText("decide.wait" if act.kind in {"decide", "accept"} else "tap.failed")
        if act.reason == "actions-paused":
            return WorkOutcomeText("link.limited")
        return None
    if act.state == "refused":
        return WorkOutcomeText("tap.failed" if act.reason in {"invalid", "storage-busy"} else WORK_DENIALS.get(act.kind, "denied.other"))
    if act.state == "conflict":
        refresh = act.kind != "route" or request_changed
        return WorkOutcomeText("stale.changed_resend" if refresh else "tap.failed", refresh_request=refresh)
    if act.state != "done":
        raise ValueError("unsupported-work-outcome")
    if act.kind == "respond":
        disposition = act.body.get("disposition", "supplied")
        if disposition == "wrong_recipient":
            return WorkOutcomeText("ask.cant_saved", route_question=permissions & 6 == 6)
        if disposition == "cannot_provide":
            return WorkOutcomeText(key_for_reader("ask.cant_saved", permissions), open_assignment=bool(permissions & 4))
        if purpose in {"decision", "review"}:
            return WorkOutcomeText("decide.comment_saved")
        unresolved = attempt_status in UNRESOLVED
        key = "ask.saved" if continued else "ask.saved_running" if attempt_status == "running" else "ask.saved_waiting" if unresolved else "ask.saved_next"
        return WorkOutcomeText(key, echo=act.body.get("text"), open_assignment=not unresolved)
    if act.kind == "decide":
        return WorkOutcomeText("decide.saved" if continued else "decide.saved_next", open_assignment=True)
    if act.kind == "accept":
        return WorkOutcomeText("review.done_line")
    if act.kind == "changes_requested":
        return WorkOutcomeText("review.changes_recorded" if continued else "review.changes_saved", open_assignment=True)
    if act.kind == "route":
        return WorkOutcomeText("request.routed")
    raise ValueError("unsupported-work-outcome")


@dataclass(frozen=True)
class WorkOutcomePresentation:
    text: WorkOutcomeText
    parts: tuple[dict, ...]


def present_outcome(act, *, purpose, permissions, attempt_status, locale, values, request_changed=False, protected=False, continued=False, file_answer=None):
    """Render complete text only; the receipt owner separately installs controls."""
    from app.telegram_bots.render import render
    from app.telegram_bots.split import split_html, visible_text

    shown = outcome_text(act, purpose=purpose, permissions=permissions, attempt_status=attempt_status, request_changed=request_changed, continued=continued)
    if shown is None:
        return None
    supplied = dict(values)
    if act.kind == "changes_requested" and act.state == "done":
        supplied["text"] = act.body["note"]
    html = render(shown.key, locale, **supplied)
    if shown.echo is not None:
        if file_answer is None:
            html = render("ask.saved_echo", locale, text=shown.echo) + "\n\n" + html
        else:
            names = file_answer["files"] if "files" in file_answer else [file_answer["file"]]
            prefix = "\n".join(render("ask.saved_file", locale, file=name, name=values["name"]) for name in names)
            if file_answer["caption"]:
                prefix += "\n\n" + render("ask.saved_echo", locale, text=file_answer["caption"])
            html = prefix + "\n\n" + html
    return WorkOutcomePresentation(shown, tuple({"kind": "text", "html": part, "visible_text": visible_text(part), "protect_content": protected} for part in split_html(html)))
