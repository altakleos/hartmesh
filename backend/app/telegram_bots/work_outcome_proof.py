"""Immutable original action and canonical receipt facts for native outcomes."""

from datetime import datetime

from app.telegram_bots.notifications import aware
from app.telegram_bots.work_actions import _ACT_SCOPE, thaw


def outcome_key(value):
    """Decode only the producer's base identity and canonical retry suffix."""
    if type(value) is not str or not value.startswith("notice:work-outcome:"):
        return None
    retry = value.removeprefix("notice:work-outcome:").split("~")
    if len(retry) > 2 or len(retry) == 2 and (not retry[1].isascii() or not retry[1].isdigit() or retry[1].startswith("0")):
        return None
    parts = retry[0].split(".")
    if len(parts) != 2 or any(len(part) != 32 or any(ch not in "0123456789abcdef" for ch in part) for part in parts):
        return None
    return tuple(parts)


def event_facts(row):
    if row is None:
        return None
    return {column.name: aware(value).isoformat() if isinstance(value := getattr(row, column.name), datetime) else value for column in row.__table__.columns}


def native_proof(act, event, effect, answer):
    return thaw({"act": {key: getattr(act, key) for key in ("id", *_ACT_SCOPE, "body", "state", "reason", "attempts", "result")}, "event": event_facts(event), "effect": event_facts(effect), "answer": event_facts(answer)})
