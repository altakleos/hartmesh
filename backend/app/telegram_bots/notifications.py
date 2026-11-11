"""Notification choices and discretionary budgets, independent of Work execution.

Budget admission borrows the sender's writer. The per-owner PostgreSQL advisory
lock serializes different bots; SQLite already owns its single writer. Existing
part dispatch receipts reserve a logical notification even after an unknown send.
No counter or delivery is inferred from process-local memory.
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, or_, select

from app.telegram_bots.settings import _snapshot, local_now
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained

ESSENTIAL_DELIVERY_CLASSES = frozenset({"reply", "question", "result", "interrupt", "progress", "service", "reminder"})
DISCRETIONARY_DELIVERY_CLASSES = frozenset({"info", "summary"})
DELIVERY_CLASSES = ESSENTIAL_DELIVERY_CLASSES | DISCRETIONARY_DELIVERY_CLASSES | {"digest"}
OUTBOX_INFO_MAX_AGE_HOURS = 24


def aware(value):
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def preference_skip(item, settings, now):
    """Return a fixed disposition, never content or authority."""
    kind = item.class_
    if kind not in DELIVERY_CLASSES:
        raise ValueError("unsupported-notification-class")
    if kind == "reply":
        return None
    if kind == "digest":
        from types import SimpleNamespace

        from app.telegram_bots.digests import validate

        _identities, kinds, category = validate(item)
        for member_class, stamp in zip(kinds, item.payload["member_created_at"], strict=True):
            reason = preference_skip(SimpleNamespace(class_=member_class, state=item.state, dedupe_key="digest-member", created_at=datetime.fromisoformat(stamp)), settings, now)
            if reason:
                return reason
        return None
    if kind == "summary":
        if not settings.summary_enabled:
            return "notifications-off"
    elif kind in {"question", "reminder"}:
        if settings.notify not in {"all", "questions"}:
            return "notifications-off"
    elif kind in {"result", "interrupt", "info"} and settings.notify != "all":
        return "notifications-off"
    if item.state == "pending" and kind in DISCRETIONARY_DELIVERY_CLASSES and not item.dedupe_key.startswith("edit:") and now - aware(item.created_at) >= timedelta(hours=OUTBOX_INFO_MAX_AGE_HOURS):
        return "expired"
    return None


def discretionary(row):
    if row.class_ == "digest":
        from app.telegram_bots.digests import validate

        return validate(row)[2] == "discretionary"
    return row.class_ in DISCRETIONARY_DELIVERY_CLASSES and not row.dedupe_key.startswith("edit:")


def _stored_stamp(value):
    if type(value) is not str:
        return None
    try:
        return aware(datetime.fromisoformat(value))
    except (ValueError, OverflowError):
        return None


def first_dispatch(parts):
    if type(parts) not in {list, tuple}:
        return None
    stamps = [stamp for part in parts if type(part) is dict for stamp in (_stored_stamp(part.get("sent_at")) or _stored_stamp(part.get("dispatched_at")),) if stamp is not None]
    return min(stamps) if stamps else None


def _day_window(settings, now):
    local = local_now(settings, now)
    start = local.replace(hour=0, minute=0, second=0, microsecond=0, fold=0)
    return start.astimezone(UTC), (start + timedelta(days=1)).astimezone(UTC)


async def budget_retry_at(session, row, *, now, config):
    """Reserve within the same transaction that writes the first part dispatch.

    Replies, essential delivery and edits never acquire the budget lock. Retrying
    a logical notification preserves its existing reservation; Telegram transport
    pacing still applies to every actual send, including ambiguous retries.
    """
    if not discretionary(row):
        return None
    if row.payload.get("notification_reserved_at") is not None:
        return None
    previous = first_dispatch(row.parts)
    if previous is not None:
        row.payload = {**row.payload, "notification_reserved_at": previous.isoformat()}
        return None
    if session.get_bind().dialect.name == "postgresql":
        key = int.from_bytes(hashlib.blake2b(("telegram-notifications:" + row.owner_user_id).encode(), digest_size=8).digest(), signed=True)
        await session.execute(select(func.pg_advisory_xact_lock(key)))
    preferences = await session.get(TelegramPersonSettingsRow, row.owner_user_id, populate_existing=True)
    if preferences is None:
        return now + timedelta(seconds=1)
    start, end = await await_drained(asyncio.to_thread(_day_window, _snapshot(preferences), now))
    cutoff = min(start, now - timedelta(hours=1))
    candidates = await session.scalars(
        select(TelegramOutboxRow).where(
            TelegramOutboxRow.owner_user_id == row.owner_user_id,
            TelegramOutboxRow.class_.in_(DISCRETIONARY_DELIVERY_CLASSES | {"digest"}),
            TelegramOutboxRow.id != row.id,
            or_(TelegramOutboxRow.created_at >= cutoff, TelegramOutboxRow.closed_at >= cutoff, TelegramOutboxRow.closed_at.is_(None)),
        )
    )
    link_times, day_times = [], []
    for candidate in candidates:
        try:
            optional = discretionary(candidate)
        except ValueError:
            # A damaged historical capsule cannot erase its charged dispatch.
            optional = True
        if not optional:
            continue
        reservation = candidate.payload.get("notification_reserved_at") if type(candidate.payload) is dict else None
        stamp = _stored_stamp(reservation) or first_dispatch(candidate.parts)
        if stamp is None:
            continue
        if stamp >= start:
            day_times.append(stamp)
        if candidate.link_id == row.link_id:
            link_times.append(stamp)
    retry = []
    for seconds, maximum in ((60, config.per_link_per_minute), (3600, config.per_link_per_hour)):
        active = sorted(stamp for stamp in link_times if stamp > now - timedelta(seconds=seconds))
        if len(active) >= maximum:
            retry.append(active[-maximum] + timedelta(seconds=seconds))
    if len(day_times) >= config.per_person_per_day:
        retry.append(end)
    if retry:
        return max(retry)
    row.payload = {**row.payload, "notification_reserved_at": now.isoformat()}
    return None
