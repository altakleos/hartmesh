"""Server-owned Work ordering and opaque cursors, independent of execution."""

import math
import re
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.attention_scope import attention_proof
from app.telegram_bots.notifications import aware
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow
from deerflow.utils.file_io import await_drained

WORK_LIST_LINES = 10


def _key(value):
    return type(value) is str and re.fullmatch(r"[0-9a-f]{32}", value) is not None


def _snapshot(row, link, cursor, now):
    if (
        row is None
        or (row.kind, row.link_id, row.binding_generation, row.chat_id, row.group_id, row.target_kind, row.target_id, row.message_id, row.message_thread_id, row.consumed_at, row.closed_reason)
        != ("work_list_snapshot", link.id, link.binding_generation, link.chat_id, row.id, None, None, None, None, None, None)
        or row.expires_at is None
        or aware(row.expires_at) <= now
    ):
        raise ValueError("work-list-unavailable")
    data = row.payload
    if (
        type(data) is not dict
        or set(data) != {"version", "owner_user_id", "instance_id", "as_of", "work_ids", "cursors"}
        or type(data["version"]) is not int
        or data["version"] != 1
        or data["owner_user_id"] != link.owner_user_id
        or data["instance_id"] != link.agent_ref
        or type(data["as_of"]) is not str
        or type(data["work_ids"]) is not list
        or not all(_key(identity) for identity in data["work_ids"])
        or len(set(data["work_ids"])) != len(data["work_ids"])
        or type(data["cursors"]) is not dict
        or cursor not in data["cursors"]
    ):
        raise ValueError("work-list-unavailable")
    positions = data["cursors"]
    count = max(1, math.ceil(len(data["work_ids"]) / WORK_LIST_LINES))
    if not all(_key(token) and type(position) is int and 0 <= position < count for token, position in positions.items()) or len(positions) != count or set(positions.values()) != set(range(count)) or positions.get(row.id) != 0:
        raise ValueError("work-list-unavailable")
    try:
        horizon = datetime.fromisoformat(data["as_of"])
        if horizon.utcoffset() is None or horizon > aware(row.created_at):
            raise ValueError()
    except ValueError:
        raise ValueError("work-list-unavailable") from None
    return row.id, deepcopy(data), horizon.astimezone(UTC), aware(row.expires_at)


class WorkListMethods:
    async def open_list(self, link):
        async def perform():
            now = self.clock()
            collected = await self.collect(link, as_of=now)
            identity = uuid4().hex
            identities = [record["id"] for record in collected["work"]]
            count = max(1, math.ceil(len(identities) / WORK_LIST_LINES))
            cursors = {identity: 0, **{uuid4().hex: position for position in range(1, count)}}
            expires = now + timedelta(hours=self.links.config.callbacks.draft_ttl_hours)
            admission = await prepare_attention(self, link)
            async with self.links._transaction() as writer:
                await writer.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).with_for_update().execution_options(populate_existing=True))
                await admission(session=writer)
                writer.add(
                    TelegramCallbackRow(
                        id=identity,
                        group_id=identity,
                        link_id=link.id,
                        chat_id=link.chat_id,
                        binding_generation=link.binding_generation,
                        message_id=None,
                        message_thread_id=None,
                        kind="work_list_snapshot",
                        target_kind=None,
                        target_id=None,
                        payload={"version": 1, "owner_user_id": link.owner_user_id, "instance_id": link.agent_ref, "as_of": collected["as_of"], "work_ids": identities, "cursors": cursors},
                        expires_at=expires,
                        created_at=now,
                    )
                )
                await writer.flush()
                await admission(session=writer)
                if self.clock() >= expires:
                    raise ValueError("work-list-unavailable")
            return identity

        return await await_drained(perform())

    async def _list_snapshot(self, link, cursor):
        if not _key(cursor):
            raise ValueError("work-list-unavailable")
        row = TelegramCallbackRow
        async with self.links.session_factory() as reader:
            result = (
                await reader.execute(
                    select(row).where(
                        row.kind == "work_list_snapshot",
                        row.link_id == link.id,
                        row.binding_generation == link.binding_generation,
                        row.chat_id == link.chat_id,
                        or_(row.id == cursor, row.payload["cursors"][cursor].as_integer().is_not(None)),
                    )
                )
            ).scalar_one_or_none()
            return _snapshot(result, link, cursor, self.clock())

    async def page(self, link, cursor):
        async def perform():
            await self._current(link)
            identity, captured, horizon, expires = await self._list_snapshot(link, cursor)
            # Reuse the complete service scan for current visibility. Rank is
            # fixed by the original snapshot; current state only changes rows.
            collected = await self.collect(link, as_of=self.clock())
            current = {record["id"]: record for record in collected["work"]}
            position = captured["cursors"][cursor]
            pages = len(captured["cursors"])
            if position >= pages:
                raise ValueError("work-list-page-unavailable")
            # Fixed original slots prevent a deletion on another page from
            # moving unseen rows across a cursor boundary.
            slots = captured["work_ids"][position * WORK_LIST_LINES : (position + 1) * WORK_LIST_LINES]
            records = [current[identity] for identity in slots if identity in current]
            ids = {record["id"] for record in records}
            tokens = {value: key for key, value in captured["cursors"].items()}
            expected = identity, captured, horizon, expires
            with attention_proof(lambda: _ListSnapshotProof(link, cursor, self.clock, expected)):
                await self._current(link)
            if self.clock() >= expires:
                raise ValueError("work-list-unavailable")
            return {
                "snapshot_id": identity,
                "cursor": cursor,
                "as_of": captured["as_of"],
                "expires_at": expires.isoformat(),
                "page": position + 1,
                "pages": pages,
                "previous": tokens.get(position - 1),
                "next": tokens.get(position + 1) if position + 1 < pages else None,
                "work": records,
                "positions": {identity: position * WORK_LIST_LINES + index + 1 for index, identity in enumerate(slots) if identity in current},
                "requests": [request for request in collected["requests"] if request["work_id"] in ids],
                "pending": [request for request in collected["pending"] if request["work_id"] in ids],
                "routing": [request for request in collected["routing"] if request["work_id"] in ids],
            }

        return await await_drained(perform())


class _ListSnapshotProof:
    """Expiry and immutable cursor custody share the provider-final SQL read."""

    def __init__(self, link, cursor, clock, expected):
        self.link, self.cursor, self.clock, self.expected = link, cursor, clock, expected

    def extend(self, query):
        self.column = len(query.column_descriptions)
        row = aliased(TelegramCallbackRow)
        return query.add_columns(row).outerjoin(
            row,
            (row.kind == "work_list_snapshot")
            & (row.link_id == TelegramBotLinkRow.id)
            & (row.binding_generation == TelegramBotLinkRow.binding_generation)
            & (row.chat_id == TelegramBotLinkRow.chat_id)
            & or_(row.id == self.cursor, row.payload["cursors"][self.cursor].as_integer().is_not(None)),
        )

    def check(self, rows):
        snapshots = {row[self.column].id: row[self.column] for row in rows if row[self.column] is not None}
        if len(snapshots) != 1 or _snapshot(next(iter(snapshots.values())), self.link, self.cursor, self.clock()) != self.expected:
            raise ValueError("work-list-unavailable")
