"""Full current Work previews beneath receipt-owned result messages."""

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_views import present_work_detail, work_buttons


def native_outcome(link, data, shown, *, locale, timezone):
    detail = data["detail"]
    record = detail["record"]
    controls = work_buttons(
        record,
        owner_id=link.owner_user_id,
        permissions=detail["permissions"],
        held=frozenset(detail["held"]),
        request=detail["request"],
        lifecycle_pending=detail["lifecycle_pending"],
        can_reuse_conversation=detail["can_reuse_conversation"],
    )
    allowed = {"work_change"} if shown.text.key == "ask.cant_saved_manager" else {"work_start"} if shown.text.open_assignment else set()
    keyboard = [[button for button in line if button.get("kind") in allowed] for line in controls]
    keyboard = [line for line in keyboard if line]
    html = "".join(part["html"] for part in shown.parts)
    if keyboard and allowed == {"work_start"}:
        preview = present_work_detail(
            record,
            owner_id=link.owner_user_id,
            permissions=detail["permissions"],
            held=frozenset(detail["held"]),
            locale=locale,
            person=detail["person"],
            recipient=detail["recipient"],
            request=detail["request"],
            date=data["detail_dates"]["date"],
            due=data["detail_dates"]["due"],
            protected=data["protected"],
            lifecycle_pending=detail["lifecycle_pending"],
            can_reuse_conversation=detail["can_reuse_conversation"],
            link=link,
            public_base_url=None,
        )
        html += "\n\n" + "".join(part["html"] for part in preview.parts)
    chunks = split_html(html)
    parts = [
        {
            "kind": "text",
            "html": chunk,
            "visible_text": visible_text(chunk),
            "protect_content": data["protected"],
            "source_ref": {"key": "work.action.outcome", "values": {"act_id": data["act"]["id"], "part": index}},
            **({"stored_keyboard": True} if keyboard and index == len(chunks) - 1 else {}),
        }
        for index, chunk in enumerate(chunks)
    ]
    return parts, canonical_keyboard(keyboard), shown.text
