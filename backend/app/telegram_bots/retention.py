"""Delete settled Telegram rows by age, keeping anything a live control still needs.

Each table is swept in its own short transaction. Outbox candidates are locked
before their dependencies are read, so the check holds where the deletion runs.

Stage one writes neither ``stop_dispatch`` nor ``continuation`` on an act, so a
row carrying either is simply kept, and it produces no digest and no attempt
status message, so neither pins an outbox row yet.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, text

from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY, callback_publication_pending
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkEventRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow

INBOUND_RETENTION_DAYS = 7
OUTBOX_RETENTION_DAYS = 30
ACT_RETENTION_DAYS = 400
LINK_EVENT_RETENTION_DAYS = 400
_SETTLED_ACTS = ("done", "refused", "conflict")
_BATCH = 500


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


@asynccontextmanager
async def _writer(sessions):
    async with sessions() as session, session.begin():
        if session.get_bind().dialect.name == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        yield session


def _strings(*values) -> set[str]:
    return {value for value in values if type(value) is str}


def _owner_deliveries(payload: Mapping) -> set[str]:
    """What an unfinished submission or retried publication still needs."""
    owner = payload.get(CALLBACK_PUBLICATION_KEY)
    if not isinstance(owner, Mapping) or not callback_publication_pending(payload):
        return set()
    listed = owner.get("delivery_ids")
    return _strings(owner.get("source_outbox_id"), owner.get("outbox_id"), *(listed if isinstance(listed, list) else ()))


async def _first_replies(session, anchors: set[str]) -> set[str]:
    """The ``reply:`` key of each anchor's turn: a form's later steps read that row."""
    keys: set[str] = set()
    ordered = sorted(anchors)
    for start in range(0, len(ordered), _BATCH):
        for payload in await session.scalars(select(TelegramOutboxRow.payload).where(TelegramOutboxRow.id.in_(ordered[start : start + _BATCH]))):
            basis = payload.get("reply_basis") if isinstance(payload, Mapping) else None
            publication = basis.get("publication") if isinstance(basis, Mapping) else None
            turn = publication.get("turn_id") if isinstance(publication, Mapping) else None
            if type(turn) is str:
                keys.add("reply:" + turn)
    return keys


async def _pinned_deliveries(session, now: datetime, callback_max_age_days: int) -> set[str]:
    """The ids and ``reply:`` keys of outbox rows a live control or unfinished delivery needs."""
    generations = dict((await session.execute(select(TelegramBotLinkRow.id, TelegramBotLinkRow.binding_generation))).all())
    pinned: set[str] = set()
    anchors: set[str] = set()
    callback = TelegramCallbackRow
    for row in await session.execute(select(callback.link_id, callback.binding_generation, callback.target_kind, callback.target_id, callback.payload, callback.expires_at, callback.consumed_at, callback.created_at)):
        payload = row.payload if isinstance(row.payload, Mapping) else {}
        pinned |= _owner_deliveries(payload)
        unexpired = row.expires_at is None or _aware(row.expires_at) > now
        if row.consumed_at is None and unexpired and now - _aware(row.created_at) < timedelta(days=callback_max_age_days) and generations.get(row.link_id) == row.binding_generation:
            anchors |= _strings(payload.get("anchor_outbox_id"))
            pinned |= _strings(row.target_id if row.target_kind == "outbox" else None)
    for delivery_ids, outbox_id in await session.execute(select(TelegramInboundMarkRow.delivery_ids, TelegramInboundMarkRow.outbox_id).where(TelegramInboundMarkRow.state != "done")):
        pinned |= _strings(outbox_id, *delivery_ids)
    return pinned | anchors | await _first_replies(session, anchors)


async def _sweep_outbox(sessions, now: datetime, callback_max_age_days: int) -> int:
    async with _writer(sessions) as session:
        # A row someone holds is in use; it waits for the next sweep.
        settled = select(TelegramOutboxRow.id, TelegramOutboxRow.dedupe_key).where(TelegramOutboxRow.closed_at < now - timedelta(days=OUTBOX_RETENTION_DAYS), TelegramOutboxRow.state.not_in(("pending", "sending")))
        candidates = dict((await session.execute(settled.with_for_update(skip_locked=True))).all())
        if not candidates:
            return 0
        pinned = await _pinned_deliveries(session, now, callback_max_age_days)
        expired = sorted(identity for identity, key in candidates.items() if identity not in pinned and key not in pinned)
        for start in range(0, len(expired), _BATCH):
            await session.execute(delete(TelegramOutboxRow).where(TelegramOutboxRow.id.in_(expired[start : start + _BATCH])))
        return len(expired)


async def _sweep(sessions, statement) -> int:
    async with _writer(sessions) as session:
        return (await session.execute(statement)).rowcount


async def sweep_rows(sessions, now: datetime, *, callback_max_age_days: int) -> dict[str, int]:
    now = _aware(now)
    mark = TelegramInboundMarkRow
    open_act = select(TelegramActRow.id).where(TelegramActRow.link_id == mark.link_id, TelegramActRow.chat_id == mark.chat_id, TelegramActRow.message_id == mark.message_id, TelegramActRow.state.not_in(_SETTLED_ACTS))
    # A mark is done only once every delivery it lists is sent or skipped.
    inbound = await _sweep(sessions, delete(mark).where(mark.closed_at < now - timedelta(days=INBOUND_RETENTION_DAYS), mark.state == "done", ~open_act.exists()))
    outbox = await _sweep_outbox(sessions, now, callback_max_age_days)
    acts = await _sweep(
        sessions,
        delete(TelegramActRow).where(
            TelegramActRow.created_at < now - timedelta(days=ACT_RETENTION_DAYS),
            TelegramActRow.state.in_(_SETTLED_ACTS),
            TelegramActRow.continuation.is_(None),
            TelegramActRow.stop_dispatch.is_(None),
        ),
    )
    events = await _sweep(sessions, delete(TelegramBotLinkEventRow).where(TelegramBotLinkEventRow.created_at < now - timedelta(days=LINK_EVENT_RETENTION_DAYS)))
    return {"inbound": inbound, "outbox": outbox, "acts": acts, "events": events}
