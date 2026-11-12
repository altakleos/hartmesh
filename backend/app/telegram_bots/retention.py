"""Delete settled Telegram rows by age, keeping anything a live control still needs.

Each table is swept in its own short transaction. Outbox candidates are locked
before their dependencies are read, so the check holds where the deletion runs.

Unknown Act continuations and stop dispatches are retained. A validated settled
ordinary file journal expires normally; an unfinished Work file input retains
its native receipt until the explicit input is settled.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from sqlalchemy import String, and_, cast, delete, or_, select, text

from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY, callback_publication_pending
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkEventRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPendingFileRow

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
    pinned |= anchors | await _first_replies(session, anchors)
    delivery = TelegramOutboxRow
    # Until album collection classifies its quotes, removing a candidate card
    # could turn a Work answer into an ordinary message at restart.
    mark = TelegramInboundMarkRow
    unresolved_album = (
        select(mark.message_id)
        .where(
            mark.link_id == delivery.link_id,
            mark.chat_id == delivery.chat_id,
            mark.binding_generation == delivery.binding_generation,
            mark.message_thread_id.is_not_distinct_from(delivery.message_thread_id),
            mark.route_kind == "work_album_pending",
            mark.route_complete.is_(False),
        )
        .correlate(delivery)
        .exists()
    )
    pinned.update(await session.scalars(select(delivery.id).where(delivery.request_id.is_not(None), unresolved_album)))
    # Compact dependency edges preserve complete digests behind live controls.
    records = await session.execute(
        select(
            delivery.id,
            delivery.class_,
            delivery.state,
            delivery.payload["member_ids"],
            delivery.payload["digest_id"].as_string(),
            delivery.payload["work_request_basis"]["cleanup_id"].as_string(),
            delivery.payload["work_request_basis"]["origin"]["anchor_outbox_id"].as_string(),
            delivery.payload["work_outcome_basis"]["origin"]["anchor_outbox_id"].as_string(),
            delivery.payload["work_outcome_basis"]["refresh"]["anchor_outbox_id"].as_string(),
        ).where(or_(delivery.class_ == "digest", delivery.state.in_(("pending", "sending", "failed"))))
    )
    graph = {}
    for identity, kind, state, members, reservation, cleanup, origin, outcome_origin, refresh_origin in records:
        if state in {"pending", "sending"}:
            pinned.add(identity)
            pinned |= _strings(reservation)
        if state in {"pending", "sending", "failed"}:
            pinned |= _strings(cleanup, origin, outcome_origin, refresh_origin)
        if kind == "digest" and isinstance(members, list):
            graph[identity] = _strings(*members)
    pending = list(pinned)
    while pending:
        children = graph.get(pending.pop(), set()) - pinned
        pinned |= children
        pending.extend(children)
    return pinned


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


async def sweep_pending_files(sessions, now: datetime, *, ttl_hours: int) -> int:
    """A file sent without a caption leaves its name and Telegram id until then."""
    return await _sweep(sessions, delete(TelegramPendingFileRow).where(TelegramPendingFileRow.created_at <= _aware(now) - timedelta(hours=ttl_hours)))


async def _sweep_acts(sessions, now):
    from app.telegram_bots.file_puts import settled_file_journal

    async with _writer(sessions) as session:
        delivery = TelegramOutboxRow
        pending_outcome = (
            select(delivery.id)
            .where(
                delivery.link_id == TelegramActRow.link_id,
                delivery.binding_generation == TelegramActRow.binding_generation,
                delivery.state.in_(("pending", "sending", "failed")),
                delivery.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == TelegramActRow.id,
            )
            .exists()
        )
        expired = (~pending_outcome, TelegramActRow.created_at < now - timedelta(days=ACT_RETENTION_DAYS), TelegramActRow.state.in_(_SETTLED_ACTS), TelegramActRow.stop_dispatch.is_(None))
        ordinary = await session.execute(delete(TelegramActRow).where(*expired, TelegramActRow.kind != "file_put", TelegramActRow.continuation.is_(None)))
        mark = TelegramInboundMarkRow
        work_file_input = (
            select(mark.message_id)
            .where(
                mark.link_id == TelegramActRow.link_id,
                mark.chat_id == TelegramActRow.chat_id,
                mark.binding_generation == TelegramActRow.binding_generation,
                mark.message_id == TelegramActRow.message_id,
                mark.route_complete.is_(False),
                or_(
                    and_(cast(mark.route_basis["work_input"]["version"].as_string(), String) == "4", cast(TelegramActRow.continuation["version"].as_string(), String) == "4"),
                    and_(mark.route_basis["work_album"].as_string().is_not(None), cast(TelegramActRow.continuation["version"].as_string(), String) == "4", cast(TelegramActRow.continuation["work"]["version"].as_string(), String) == "2"),
                    and_(mark.route_kind == "work_file_new", cast(TelegramActRow.continuation["version"].as_string(), String) == "3"),
                ),
            )
            .exists()
        )
        from types import SimpleNamespace

        from app.telegram_bots.file_puts import file_act_id
        from app.telegram_bots.inbox import _callback_owner, chat_input_parts

        # Callback publication snapshots outlive the original inbox rows. Read
        # exact file dependencies from those validated owners, including Retry
        # successors, rather than pinning every file in a busy chat.
        group_files = set()
        callback = TelegramCallbackRow
        live_retry_parents = set()
        retries = await session.execute(
            select(callback.link_id, callback.chat_id, callback.binding_generation, callback.message_thread_id, callback.payload).where(
                callback.kind == "retry_message", callback.consumed_at.is_(None), or_(callback.expires_at.is_(None), callback.expires_at > now)
            )
        )
        for link_id, chat_id, generation, topic, payload in retries:
            if isinstance(payload, Mapping) and type(payload.get("anchor_outbox_id")) is str:
                live_retry_parents.add((link_id, chat_id, generation, topic, payload["anchor_outbox_id"]))
        owners = await session.execute(select(callback.id, callback.link_id, callback.chat_id, callback.binding_generation, callback.message_thread_id, callback.payload).where(callback.payload["publication_owner"].as_string().is_not(None)))
        for identity, link_id, chat_id, generation, topic, payload in owners:
            try:
                _data, stage = _callback_owner(payload, identity)
                if stage.state == "done" and (link_id, chat_id, generation, topic, _data["outbox_id"]) not in live_retry_parents:
                    continue
                for origin in stage.origins:
                    if "work_album_handoff" not in origin.route_basis:
                        continue
                    if (origin.link_id, origin.chat_id, origin.binding_generation, origin.message_thread_id) != (link_id, chat_id, generation, topic):
                        continue
                    _text, files = chat_input_parts(origin)
                    group_files.update(file_act_id(SimpleNamespace(id=link_id), file) for file in files)
            except (ValueError, TypeError, KeyError, AttributeError):
                continue
        candidates = list(await session.scalars(select(TelegramActRow).where(*expired, TelegramActRow.kind == "file_put", ~work_file_input).with_for_update(skip_locked=True)))
        identities = [row.id for row in candidates if row.id not in group_files and settled_file_journal(row)]
        for start in range(0, len(identities), _BATCH):
            await session.execute(delete(TelegramActRow).where(TelegramActRow.id.in_(identities[start : start + _BATCH])))
        from app.telegram_bots.work_actions import WORK_ACT_KINDS
        from app.telegram_bots.work_outcome_delivery import delivery_receipt

        outcomes = list(await session.scalars(select(TelegramActRow).where(*expired, TelegramActRow.kind.in_(WORK_ACT_KINDS), TelegramActRow.continuation.is_not(None)).with_for_update(skip_locked=True)))
        settled = []
        for row in outcomes:
            try:
                receipt = delivery_receipt(row.continuation)
            except (ValueError, TypeError, KeyError):
                continue
            from app.telegram_bots.work_outcome_transfer import transferred

            if receipt is not None or transferred(row):
                settled.append(row.id)
        for start in range(0, len(settled), _BATCH):
            await session.execute(delete(TelegramActRow).where(TelegramActRow.id.in_(settled[start : start + _BATCH])))
        return ordinary.rowcount + len(identities) + len(settled)


def _live_followup_input(mark, now, callback_max_age_days):
    """Keep only the exact completed input a live or unfinished reply needs."""
    delivery, callback, link = TelegramOutboxRow, TelegramCallbackRow, TelegramBotLinkRow
    basis = delivery.payload["work_outcome_basis"]
    live = (
        select(callback.id)
        .where(
            callback.link_id == delivery.link_id,
            callback.binding_generation == delivery.binding_generation,
            callback.chat_id == delivery.chat_id,
            callback.message_thread_id.is_(None),
            callback.payload["anchor_outbox_id"].as_string() == delivery.id,
            callback.payload["presentation_version"].as_string() == delivery.payload["presentation_version"].as_string(),
            callback.kind == "route_to",
            callback.consumed_at.is_(None),
            callback.created_at > now - timedelta(days=callback_max_age_days),
            or_(callback.expires_at.is_(None), callback.expires_at > now),
        )
        .correlate(delivery)
        .exists()
    )
    return (
        select(delivery.id)
        .join(link, and_(link.id == delivery.link_id, link.owner_user_id == delivery.owner_user_id, link.binding_generation == delivery.binding_generation))
        .where(
            delivery.link_id == mark.link_id,
            delivery.chat_id == mark.chat_id,
            delivery.binding_generation == mark.binding_generation,
            cast(basis["version"].as_string(), String).in_(("3", "4")),
            mark.route_kind == "work_input",
            basis["input"]["link_id"].as_string() == mark.link_id,
            cast(basis["input"]["chat_id"].as_string(), String) == cast(mark.chat_id, String),
            cast(basis["input"]["message_id"].as_string(), String) == cast(mark.message_id, String),
            cast(basis["input"]["update_id"].as_string(), String) == cast(mark.update_id, String),
            cast(basis["input"]["binding_generation"].as_string(), String) == cast(mark.binding_generation, String),
            basis["input"]["operation_id"].as_string() == mark.operation_ids[0].as_string(),
            or_(delivery.state.in_(("pending", "sending", "failed")), live),
        )
        .correlate(mark)
        .exists()
    )


async def sweep_rows(sessions, now: datetime, *, callback_max_age_days: int) -> dict[str, int]:
    now = _aware(now)
    mark = TelegramInboundMarkRow
    open_act = select(TelegramActRow.id).where(TelegramActRow.link_id == mark.link_id, TelegramActRow.chat_id == mark.chat_id, TelegramActRow.message_id == mark.message_id, TelegramActRow.state.not_in(_SETTLED_ACTS))
    # A mark is done only once every delivery it lists is sent or skipped.
    inbound = await _sweep(sessions, delete(mark).where(mark.closed_at < now - timedelta(days=INBOUND_RETENTION_DAYS), mark.state == "done", ~open_act.exists(), ~_live_followup_input(mark, now, callback_max_age_days)))
    outbox = await _sweep_outbox(sessions, now, callback_max_age_days)
    acts = await _sweep_acts(sessions, now)
    events = await _sweep(sessions, delete(TelegramBotLinkEventRow).where(TelegramBotLinkEventRow.created_at < now - timedelta(days=LINK_EVENT_RETENTION_DAYS)))
    return {"inbound": inbound, "outbox": outbox, "acts": acts, "events": events}
