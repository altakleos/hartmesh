"""Finite controls under the current, source-qualified attempt status."""

from app.telegram_bots.work_views import WorkViewPresentation, present_work_detail, work_buttons


def present_status_controls(source, basis, link, view):
    from app.telegram_bots.work_view_sources import detail_read

    if basis["concealed"] or "detail" not in source.work:
        return view
    detail = detail_read(source.work["detail"])
    if (detail.record.get("attempt") or {}).get("id") != source.work["attempt_id"]:
        return view
    allowed = {"work_start"} if source.key == "status.stopped" else {"stop_attempt"} if source.key in {"status.starting", "status.working", "status.waiting_you", "status.waiting_answer"} else set()
    controls = work_buttons(
        detail.record, owner_id=link.owner_user_id, permissions=detail.permissions, held=detail.held, request=detail.request, lifecycle_pending=detail.lifecycle_pending, can_reuse_conversation=detail.can_reuse_conversation
    )
    keyboard = tuple(tuple(button for button in line if button["kind"] in allowed) for line in controls)
    keyboard = tuple(line for line in keyboard if line)
    if not keyboard:
        return view
    if source.key != "status.stopped":
        return WorkViewPresentation(view.parts, keyboard)
    preview = present_work_detail(
        detail.record,
        owner_id=link.owner_user_id,
        permissions=detail.permissions,
        held=detail.held,
        locale=basis["locale"],
        person=detail.person,
        recipient=detail.recipient,
        request=detail.request,
        date=source.work["detail_dates"]["date"],
        due=source.work["detail_dates"]["due"],
        protected=source.protected,
        lifecycle_pending=detail.lifecycle_pending,
        can_reuse_conversation=detail.can_reuse_conversation,
    )
    from app.telegram_bots.split import split_html, visible_text

    html = "\n\n".join("".join(part["html"] for part in shown.parts) for shown in (view, preview))
    parts = tuple(dict(kind="text", html=chunk, visible_text=visible_text(chunk), protect_content=source.protected) for chunk in split_html(html))
    return WorkViewPresentation(parts, keyboard)
