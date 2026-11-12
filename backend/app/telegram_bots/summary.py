"""Local summary scheduling and complete-set selection; never starts Work."""

from datetime import timedelta

from app.telegram_bots.notifications import aware

SUMMARY_LINES_PER_PART = 5
SUMMARY_BUTTONS_MAX = 5


def due_date(now, settings, zone):
    """Return the scheduled local day, including a delay across midnight."""
    if not settings.summary_enabled:
        return None
    local = aware(now).astimezone(zone)
    day, minute = local.date(), settings.summary_minute
    if settings.quiet_enabled:
        start, end = settings.quiet_start_minute, settings.quiet_end_minute
        if start > end and minute >= start:
            day, minute = day - timedelta(days=1), end
        else:
            minute = max(minute, end)
    return day.isoformat() if local.hour * 60 + local.minute >= minute else None


def _time(value):
    from datetime import datetime

    return aware(datetime.fromisoformat(value)).timestamp()


def sections(attention, *, owner_user_id, since):
    """Rank all authorized records before a renderer applies its display cap."""
    work = {row["id"]: row for row in attention["work"]}
    output = {key: [] for key in ("finished", "working", "interrupted", "waiting")}
    threshold = aware(since).timestamp()

    def entry(row, kind, *, request=None):
        return {"kind": kind, "work_id": row["id"], "request_id": request["id"] if request else None, "objective": row["objective"]}

    waiting = []
    for request in attention["pending"]:
        row = work.get(request["work_id"])
        if row is not None and request["recipient_id"] == owner_user_id and request["state"] == "pending" and not request["needs_routing"]:
            kind = {"information": "question", "decision": "decision", "review": "review"}[request["purpose"]]
            waiting.append((0, _time(request["created_at"]), request["id"], entry(row, kind, request=request)))
    answered = set()
    for request in attention["requests"]:
        row = work.get(request["work_id"])
        blocker = row.get("blocker") if row is not None else None
        if (
            request["state"] == "answered"
            and request["purpose"] == "information"
            and isinstance(blocker, dict)
            and isinstance(blocker.get("id"), str)
            and request["assignment_revision"] == row["assignment_revision"]
            and (request["basis_id"], request["basis_revision"]) == (blocker["id"], blocker.get("revision"))
        ):
            answered.add(row["id"])

    for row in work.values():
        attempt = row.get("attempt") or {}
        settled = _time(attempt["settled_at"]) if attempt.get("settled_at") else None
        if row["status"] in {"completed", "submitted"} and settled is not None and settled >= threshold:
            output["finished"].append((settled, row["id"], entry(row, "finished")))
        if attempt.get("status") in {"starting", "running"}:
            output["working"].append((_time(row["created_at"]), row["id"], entry(row, "working")))
        if attempt.get("status") == "uncertain" or attempt.get("status") == "failed" and settled is not None and settled >= threshold:
            output["interrupted"].append((_time(row["created_at"]), row["id"], entry(row, "interrupted")))
        if row["status"] == "blocked" and row["id"] in answered and settled is not None and attempt.get("status") in {"succeeded", "failed", "cancelled"} and attempt.get("assignment_revision") == row["assignment_revision"]:
            waiting.append((1, _time(row["created_at"]), row["id"], entry(row, "resume")))
        elif row["status"] == "open" and not attempt and row["creator_id"] == owner_user_id:
            waiting.append((2, _time(row["created_at"]), row["id"], entry(row, "start")))
    for key in ("finished", "working", "interrupted"):
        ordered = sorted(output[key], key=lambda value: (-value[0] if key == "finished" else value[0], value[1]))
        output[key] = [value[-1] for value in ordered]
    output["waiting"] = [value[-1] for value in sorted(waiting, key=lambda value: value[:3])]
    return output
