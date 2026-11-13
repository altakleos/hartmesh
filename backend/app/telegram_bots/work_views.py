"""Complete Work previews and finite button descriptions, without execution."""

from dataclasses import dataclass

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import _clip, render, render_plain
from app.telegram_bots.split import split_html, visible_text
from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.work import UNRESOLVED


@dataclass(frozen=True)
class WorkViewPresentation:
    parts: tuple[dict, ...]
    keyboard: tuple[tuple[dict, ...], ...]


def _button(label, kind, target_kind, target_id, payload):
    return {"label": label, "kind": kind, "target_kind": target_kind, "target_id": target_id, "payload": payload}


def work_buttons(record, *, owner_id, permissions, held, request=None, lifecycle_pending=False, can_reuse_conversation=False):
    """Caller supplies a source-qualified record and current owner ceilings."""
    if type(permissions) is not int or not 1 <= permissions <= 7 or permissions & 3 != 3 or "agents:read" not in held:
        raise AgentDenied("Work details require current Use and Inspect")
    work_id = record["id"]
    writable = "agents:write" in held
    manager = writable and permissions & 6 == 6
    attempt = record.get("attempt") or {}
    active = attempt.get("status") in UNRESOLVED
    closed = record["status"] in {"completed", "cancelled"}
    waiting = request is not None and request["state"] == "pending"
    eligible = (
        waiting
        and not record["needs_mandate_reconciliation"]
        and request.get("recipient_id") == owner_id
        and not request["needs_routing"]
        and (request["can_respond"] if request["purpose"] == "information" else manager and request["can_manage"])
    )
    keyboard = []
    if not closed and eligible:
        request_id = request["id"]
        payload = {"work_id": work_id, "request_id": request_id}
        if request["purpose"] == "review" and record["status"] == "submitted":
            keyboard.extend([[_button("label.accept", "accept_open", "request", request_id, payload)], [_button("label.request_changes", "changes_open", "request", request_id, payload)]])
        elif request["purpose"] != "review":
            keyboard.append([_button("label.show_question", "digest_item", "request", request_id, payload)])
    if not closed and active:
        if writable and "runs:cancel" in held and attempt["status"] in {"starting", "running"} and (manager or attempt.get("requester_id") == owner_id):
            keyboard.append([_button("label.stop", "stop_attempt", "attempt", attempt["id"], {"work_id": work_id, "attempt_id": attempt["id"]})])
    elif (
        not waiting
        and not record.get("activation_pending")
        and record["status"] in {"open", "blocked"}
        and record["work_enabled"]
        and record["execution_available"]
        and not record["needs_mandate_reconciliation"]
        and not lifecycle_pending
        and writable
        and "runs:create" in held
        and ("threads:write" in held or can_reuse_conversation is True and attempt.get("thread_id"))
    ):
        keyboard.append([_button("label.resume" if attempt else "label.start_now", "work_start", "work", work_id, {"work_id": work_id, "assignment_revision": record["assignment_revision"]})])
    if manager and not closed and (not active or "runs:cancel" in held):
        changes = []
        if record["status"] in {"open", "blocked"}:
            changes.append(_button("label.change", "work_change", "work", work_id, {"work_id": work_id}))
        if record["status"] in {"open", "blocked", "submitted"}:
            changes.append(_button("label.withdraw", "work_withdraw", "work", work_id, {"work_id": work_id}))
        if changes:
            keyboard.append(changes)
    return canonical_keyboard(keyboard)


def work_status_key(record, *, owner_id, request=None):
    if record["needs_mandate_reconciliation"]:
        return "work.status.needs_confirmation"
    if record["status"] == "completed":
        return "work.status.done"
    if record["status"] == "cancelled":
        return "work.status.withdrawn"
    if request is not None and request["state"] == "pending" and not request["needs_routing"]:
        own = request.get("recipient_id") == owner_id
        return {
            "information": "work.status.waiting_you_answer" if own else "work.status.waiting_answer",
            "decision": "work.status.waiting_you_decision" if own else "work.status.waiting_decision",
            "review": "work.status.review_you" if own else "work.status.review",
        }[request["purpose"]]
    if record["status"] == "submitted":
        return "work.status.review"
    attempt = record.get("attempt") or {}
    return {
        "starting": "work.status.starting",
        "running": "work.status.working",
        "stopping": "work.status.stopping",
        "uncertain": "work.status.checking",
        "failed": "work.status.failed",
        "cancelled": "work.status.stopped",
        "succeeded": "work.status.no_result",
    }.get(attempt.get("status"), "work.status.not_started")


def present_work_detail(record, *, owner_id, permissions, held, locale, person, date, request=None, recipient=None, due=None, protected=False, lifecycle_pending=False, can_reuse_conversation=False, link=None, public_base_url=None):
    keyboard = work_buttons(record, owner_id=owner_id, permissions=permissions, held=held, request=request, lifecycle_pending=lifecycle_pending, can_reuse_conversation=can_reuse_conversation)
    status = render_plain(work_status_key(record, owner_id=owner_id, request=request), locale, person=recipient)
    lines = [render("work.detail", locale, objective_full=record["objective"], status=status)]
    if record.get("progress"):
        lines.append(render("work.detail.latest", locale, progress=record["progress"]))
    lines.append(render("work.detail.criteria", locale, success_criteria=record["success_criteria"]))
    if due is not None:
        lines.append(render("work.detail.due", locale, due=due))
    lines.append(render("work.detail.given", locale, person=person, date=date))
    if link is not None:
        from app.telegram_bots.pages import page_address

        address = page_address("web.work", link, work_id=record["id"], permissions=permissions, public_base_url=public_base_url)
        if address is not None:
            keyboard.append([{"label": "label.open_assignment", "url": address}])
    chunks = split_html("\n".join(lines))
    parts = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": protected, **({"stored_keyboard": True} if keyboard and index == len(chunks) - 1 else {})} for index, chunk in enumerate(chunks))
    return WorkViewPresentation(parts, tuple(tuple(row) for row in canonical_keyboard(keyboard)))


def present_work_page(frame, *, link, owner_id, name, locale, protected=False, public_base_url=None):
    """Preserve this page's saved item order; all data was read as its owner."""
    rows = frame["work"]
    if not rows and frame["pages"] == 1:
        html = render("work.empty", locale, name=name)
        return WorkViewPresentation(tuple({"kind": "text", "html": part, "visible_text": visible_text(part), "protect_content": protected} for part in split_html(html)), ())
    requests = {request["work_id"]: request for request in frame["requests"] if request["state"] != "closed"}
    waiting = {request["work_id"]: request for request in frame["pending"] + frame["routing"]}
    lines, keyboard, section = [render("work.list", locale, name=name)], [], None
    if not rows:
        lines.append(render("work.empty", locale, name=name))
    for index, row in enumerate(rows, start=(frame["page"] - 1) * 10 + 1):
        index = frame.get("positions", {}).get(row["id"], index)
        request = requests.get(row["id"])
        attempt = row.get("attempt") or {}
        if row["id"] in waiting:
            group = "work.list.waiting"
            status = render_plain("work.needs." + {"information": "answer", "decision": "decision", "review": "review"}[waiting[row["id"]]["purpose"]], locale)
        elif attempt.get("status") in UNRESOLVED:
            group = "work.list.working"
            status = _clip(" ".join(row.get("progress", "").split()), 60, at_word=True) or render_plain(work_status_key(row, owner_id=owner_id, request=request), locale, person=None)
        elif request is not None and request["state"] == "answered" and request.get("recipient_id") == owner_id and row["status"] in {"open", "blocked"}:
            group, status = "work.list.waiting", render_plain("work.needs.resume", locale)
        else:
            group, status = "work.list.other", render_plain(work_status_key(row, owner_id=owner_id, request=request), locale, person=None)
        if group != section:
            lines.append(render(group, locale))
            section = group
        lines.append(render("work.list.row", locale, n=index, objective=" ".join(row["objective"].split()), status=status))
        button = _button("label.work_number", "work_details", "work", row["id"], {"work_id": row["id"]})
        keyboard.append([{**button, "label_values": {"number": index}, "expires_at": frame["expires_at"]}])
    lines.append(render("work.list_page", locale, page=frame["page"], pages=frame["pages"]))
    if frame["next"] is not None:
        lines.append(render("work.list_more", locale))
    for token, label in ((frame["previous"], "label.previous_assignments"), (frame["next"], "label.next_assignments")):
        if token is not None:
            keyboard.append([{**_button(label, "work_list_page", "link", link.id, {"cursor": token}), "expires_at": frame["expires_at"]}])
    if public_base_url is not None:
        from app.telegram_bots.pages import page_address

        address = page_address("web.work", link, permissions=3, public_base_url=public_base_url)
        if address is not None:
            keyboard.append([{"label": "label.open_work", "url": address}])
    chunks = split_html("\n".join(lines))
    parts = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": protected, **({"stored_keyboard": True} if index == len(chunks) - 1 else {})} for index, chunk in enumerate(chunks))
    return WorkViewPresentation(parts, tuple(tuple(row) for row in canonical_keyboard(keyboard)))
