"""Dormant coalesced edits to the acknowledged status of one Work attempt."""

from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import _PART_IDENTITY, _TRANSPORT_RECEIPTS, DeliveryClaimRetired, DeliveryRetry, ProductSourceRetired, _item
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_news_delivery import frozen_news
from app.telegram_bots.work_news_sources import same_news_origin
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_status import STATUS_KEYS, StatusProjection, WorkStatusRead, render_status
from app.telegram_bots.work_status_delivery import notice
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def recipes(source, target):
    status = WorkStatusRead(**source["status"])
    chunks = split_html(render_status(status, locale=source["locale"]))
    previous = target["messages"]
    if type(target.get("replace")) is not bool:
        raise ProductSourceRetired()
    edit = not target["replace"] and len(chunks) == len(previous) and all(message["protected"] or not status.protected for message in previous)
    result = []
    for index, chunk in enumerate(chunks):
        part = {
            "kind": "edit" if edit else "text",
            "html": chunk,
            "visible_text": visible_text(chunk),
            "protect_content": status.protected or any(message["protected"] for message in previous),
            "silent": True,
            "source_ref": {"key": "work.status.update", "values": {"attempt_id": status.attempt_id, "part": index}},
        }
        if edit:
            part.update(target_message_id=previous[index]["id"], target_protect_content=previous[index]["protected"])
        result.append(part)
    return result


def compatible(previous, current, item):
    return same_news_origin(notice(WorkStatusRead(**previous["status"]), item), notice(WorkStatusRead(**current["status"]), item))


def validate_update(row, *, acknowledged=False):
    try:
        return _validate_update(row, acknowledged=acknowledged)
    except (TypeError, ValueError, KeyError, IndexError, AttributeError):
        raise ProductSourceRetired() from None


def _validate_update(row, *, acknowledged=False):
    basis = row.payload["work_status_update"]
    if type(basis) is not dict or set(basis) != {"version", "source", "anchor", "parts", "history"} or type(basis["version"]) is not int or basis["version"] != 1:
        raise ProductSourceRetired()
    version, history = row.payload["presentation_version"], basis["history"]
    if type(version) is not int or version < 0 or type(history) is not list or len(history) != version:
        raise ProductSourceRetired()
    ids, messages = set(), set()
    for prior in range(version + 1):
        saved = basis if prior == version else history[prior]["basis"]
        if set(saved) != {"version", "source", "anchor", "parts", "history"} or type(saved["version"]) is not int or saved["version"] != 1:
            raise ProductSourceRetired()
        source, target = saved["source"], saved["anchor"]
        if type(source) is not dict or set(source) != {"status", "revision", "work_id", "locale"} or type(source["revision"]) is not int or source["revision"] < 1 or source["work_id"] != row.work_id:
            raise ProductSourceRetired()
        status = WorkStatusRead(**source["status"])
        if status.key not in STATUS_KEYS or type(status.protected) is not bool or type(status.terminal) is not bool or type(status.values) is not dict or type(status.event_proofs) is not dict or not status.event_proofs:
            raise ProductSourceRetired()
        if type(target) is not dict or set(target) != {"outbox_id", "source_signature", "version", "message_thread_id", "messages", "edited_at", "replace"} or type(target["version"]) is not int or target["version"] < 0:
            raise ProductSourceRetired()
        if (
            type(target["messages"]) is not list
            or not target["messages"]
            or any(type(message) is not dict or set(message) != {"id", "protected"} or type(message["id"]) is not int or message["id"] <= 0 or type(message["protected"]) is not bool for message in target["messages"])
        ):
            raise ProductSourceRetired()
        if len({message["id"] for message in target["messages"]}) != len(target["messages"]) or datetime.fromisoformat(target["edited_at"]).utcoffset() is None:
            raise ProductSourceRetired()
        if prior < version and (set(history[prior]) != {"basis", "parts"} or saved.get("history") != [] or not compatible(saved["source"], basis["source"], row)):
            raise ProductSourceRetired()
        expected = recipes(saved["source"], saved["anchor"])
        actual = [part for part in row.parts if part["presentation_version"] == prior]
        if not same_body(expected, saved["parts"]) or len(actual) != len(expected) or prior < version and not same_body(actual, history[prior]["parts"]):
            raise ProductSourceRetired()
        messages.update(message["id"] for message in saved["anchor"]["messages"])
        pending = False
        for index, (part, recipe) in enumerate(zip(actual, expected, strict=True)):
            identity = part.get("part_id")
            if (
                type(identity) is not str
                or len(identity) != 32
                or any(ch not in "0123456789abcdef" for ch in identity)
                or identity in ids
                or set(part) - set(recipe) - _PART_IDENTITY - _TRANSPORT_RECEIPTS - {"callback_group_id", "sent_protect_content"}
                or any(type(part[key]) is not bool for key in ("plain_fallback", "url_button_fallback", "sent_protect_content") if key in part)
                or type(part.get("presentation_version")) is not int
                or type(part.get("ordinal")) is not int
                or part["ordinal"] != index
                or any(not same_body(part.get(key), value) for key, value in recipe.items())
                or part.get("callback_group_id")
                or part["state"] not in ({"sent", "retired"} if prior < version else {"pending", "sending", "sent"})
                or acknowledged
                and prior == version
                and part["state"] != "sent"
            ):
                raise ProductSourceRetired()
            ids.add(identity)
            if part["state"] == "sent":
                try:
                    stamp = datetime.fromisoformat(part["sent_at"])
                    if stamp.utcoffset() is None:
                        raise ValueError()
                except (KeyError, TypeError, ValueError):
                    raise ProductSourceRetired() from None
                if pending or type(part.get("message_ids")) is not list or len(part["message_ids"]) != 1 or type(part["message_ids"][0]) is not int or part["message_ids"][0] <= 0:
                    raise ProductSourceRetired()
                message = part["message_ids"][0]
                if part["kind"] == "edit" and message != recipe["target_message_id"] or part["kind"] == "text" and message in messages:
                    raise ProductSourceRetired()
                messages.add(message)
            else:
                pending = True
                if part.get("message_ids") != [] or part.get("sent_at") is not None:
                    raise ProductSourceRetired()
    if len(ids) != len(row.parts):
        raise ProductSourceRetired()
    return basis


def anchor(link, parent, source, moved=None, *, replacement=False):
    if parent is None or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.work_id, parent.dedupe_key) != (
        link.id,
        link.owner_user_id,
        link.binding_generation,
        link.chat_id,
        source["work_id"],
        "progress:" + source["status"]["attempt_id"],
    ):
        raise ProductSourceRetired()
    if parent.state == "skipped":
        raise ProductSourceRetired()
    if parent.state != "sent":
        raise DeliveryRetry(timedelta(seconds=5))
    item = _item(parent)
    basis, original, _part = frozen_news(SimpleNamespace(link=link, item=item, part=item.parts[-1]), acknowledged=True)
    if basis["concealed"] or not same_news_origin(original, notice(WorkStatusRead(**source["status"]), item)):
        raise ProductSourceRetired()
    parts = [part for part in parent.parts if part["presentation_version"] == parent.payload["presentation_version"]]
    identity = {"outbox_id": parent.id, "source_signature": digest(basis), "version": parent.payload["presentation_version"], "message_thread_id": parent.message_thread_id}
    pointer = parent.payload.get("status_anchor")
    acknowledged_revision = parent.payload.get("status_acked_revision")
    if (pointer is None) != (acknowledged_revision is None) or acknowledged_revision is not None and (type(acknowledged_revision) is not int or acknowledged_revision < 1 or not pointer):
        raise ProductSourceRetired()
    if pointer:
        if type(pointer) is not dict or set(pointer) != {"outbox_id", "part_id"} or moved is None or moved.id != pointer["outbox_id"] or moved.state != "sent":
            raise ProductSourceRetired()
        saved = validate_update(moved, acknowledged=True)
        if saved["source"]["revision"] != acknowledged_revision or not compatible(saved["source"], source, moved):
            raise ProductSourceRetired()
        if (
            any(not same_body(saved["anchor"].get(key), value) for key, value in identity.items())
            or (moved.link_id, moved.owner_user_id, moved.binding_generation, moved.chat_id, moved.work_id) != (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.work_id)
            or moved.parts[-1]["part_id"] != pointer["part_id"]
        ):
            raise ProductSourceRetired()
        parts = [part for part in moved.parts if part["presentation_version"] == moved.payload["presentation_version"]]
    return {**identity, "messages": [{"id": part["message_ids"][0], "protected": part["protect_content"]} for part in parts], "edited_at": max(part["sent_at"] for part in parts), "replace": replacement}


async def acknowledge_status_update(session, row):
    basis = validate_update(row, acknowledged=True)
    parent = await session.get(TelegramOutboxRow, basis["anchor"]["outbox_id"], with_for_update=True, populate_existing=True)
    if parent is None or parent.payload.get("status_update") != {"revision": basis["source"]["revision"], "outbox_id": row.id}:
        raise ProductSourceRetired()
    revision = basis["source"]["revision"]
    previous = parent.payload.get("status_acked_revision", 0)
    target = {"outbox_id": row.id, "part_id": row.parts[-1]["part_id"]}
    if type(previous) is not int or previous > revision or previous == revision and parent.payload.get("status_anchor") != target:
        raise ProductSourceRetired()
    parent.payload = {**parent.payload, "status_anchor": target, "status_acked_revision": revision}


class WorkStatusUpdates:
    def __init__(self, plans):
        self.plans, self.reader, self.outbox = plans, plans.reader, plans.outbox

    async def materialize(self, link, identity):
        return await await_drained(self._materialize(link, identity))

    async def _materialize(self, link, identity):
        clock = await self.reader.prepare(link)
        admission = await prepare_attention(self.reader, link)
        async with self.outbox._writer(link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
            if row is None or row.payload.get("work_event_intent", {}).get("key") not in STATUS_KEYS or not row.dedupe_key.startswith("edit:"):
                return False
            if not self.outbox._binding(binding, link) or any(getattr(row, name) is not None for name in ("claim_id", "claim_owner", "claimed_at")) or row.state != "pending":
                raise ProductSourceRetired()
            if row.parts:
                validate_update(row)
                return True
            projection = _SourceProjection(link, _item(row), clock)
            await admission(session=writer, projection=projection)
            source = projection.context
            parent = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "progress:" + projection.source.attempt_id).with_for_update())
            if parent is None:
                raise ProductSourceRetired()
            if parent.state in {"pending", "sending", "failed"}:
                return False
            previous = parent.payload.get("status_update", {})
            if previous.get("revision", 0) >= source["revision"]:
                raise ProductSourceRetired()
            if previous:
                prior = await writer.get(TelegramOutboxRow, previous["outbox_id"], populate_existing=True)
                if prior is None or prior.state not in {"sent", "skipped"}:
                    return False
                if prior.state == "sent" and parent.payload.get("status_anchor") != {"outbox_id": prior.id, "part_id": prior.parts[-1]["part_id"]}:
                    raise ProductSourceRetired()
            pointer = parent.payload.get("status_anchor")
            moved = await writer.get(TelegramOutboxRow, pointer["outbox_id"], populate_existing=True) if pointer else None
            target = anchor(link, parent, source, moved)
            due = aware(datetime.fromisoformat(target["edited_at"])) + timedelta(seconds=self.outbox.config.outbox.progress_edit_seconds)
            if self.outbox.clock() < due:
                return False
            parts = recipes(source, target)
            row.payload = {**row.payload, "work_status_update": {"version": 1, "source": source, "anchor": target, "parts": parts, "history": []}}
            row.parts = self.outbox._parts(parts, 0)
            row.message_thread_id = target["message_thread_id"]
            parent.payload = {**parent.payload, "status_update": {"revision": source["revision"], "outbox_id": row.id}}
            await writer.flush()
            await admission(session=writer, projection=_SendProjection(self, link, _item(row), clock))
            return True

    async def read_part(self, captured):
        async def perform():
            basis = validate_update(captured.item)
            clock = await self.reader.prepare(captured.link)
            admission = await prepare_attention(self.reader, captured.link)
            source = _SourceProjection(captured.link, captured.item, clock)
            await admission(projection=source)
            if not compatible(basis["source"], source.context, captured.item):
                raise ProductSourceRetired()
            if not same_body(basis["source"], source.context):
                await self.refresh(captured, source.context, clock, admission)
                raise DeliveryClaimRetired()
            await admission(projection=_SendProjection(self, captured.link, captured.item, clock, captured=captured))
            return captured.part["html"], captured.part["visible_text"]

        return await await_drained(perform())

    async def refresh(self, captured, source, clock, admission):
        async with self.outbox._writer(captured.link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True, populate_existing=True)
            current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
            if current != captured:
                raise DeliveryClaimRetired()
            old = validate_update(row)
            parent = await writer.get(TelegramOutboxRow, old["anchor"]["outbox_id"], with_for_update=True, populate_existing=True)
            if parent is None or parent.payload.get("status_update") != {"revision": old["source"]["revision"], "outbox_id": row.id} or not compatible(old["source"], source, row):
                raise ProductSourceRetired()
            projection = _SourceProjection(captured.link, _item(row), clock, WorkStatusRead(**source["status"]))
            await admission(session=writer, projection=projection)
            pointer = parent.payload.get("status_anchor")
            moved = await writer.get(TelegramOutboxRow, pointer["outbox_id"], populate_existing=True) if pointer else None
            target = anchor(captured.link, parent, source, moved, replacement=old["anchor"]["replace"])
            if not same_body(target, old["anchor"]):
                raise ProductSourceRetired()
            self._replace(row, parent, source, target)
            await writer.flush()
            await admission(session=writer, projection=_SendProjection(self, captured.link, _item(row), clock))

    def _replace(self, row, parent, source, target):
        old = validate_update(row)
        version = row.payload["presentation_version"]
        historical = deepcopy(row.parts)
        for part in historical:
            if part["state"] in {"pending", "sending"}:
                part["state"] = "retired"
        archived = {**deepcopy(old), "history": []}
        history = [*deepcopy(old["history"]), {"basis": archived, "parts": [part for part in historical if part["presentation_version"] == version]}]
        parts = recipes(source, target)
        row.parts = [*historical, *self.outbox._parts(parts, version + 1)]
        row.payload = {**row.payload, "presentation_version": version + 1, "work_status_update": {"version": 1, "source": source, "anchor": target, "parts": parts, "history": history}}
        row.state, row.not_before = "pending", self.outbox.clock()
        row.failed_reason = row.closed_at = row.skip_reason = row.sent_at = None
        row.claim_id = row.claim_owner = row.claimed_at = None
        parent.payload = {**parent.payload, "status_update": {"revision": source["revision"], "outbox_id": row.id}}

    async def repair(self, link, identity, *, guard=None):
        async def perform():
            if guard is not None:
                await prepare_publication_guard(guard)
            clock = await self.reader.prepare(link)
            admission = await prepare_attention(self.reader, link)
            async with self.outbox._writer(link.id) as (writer, binding):
                if guard is not None:
                    await guard.check(writer)
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                if (
                    row is None
                    or not self.outbox._binding(binding, link)
                    or row.state != "failed"
                    or row.failed_reason != "work-status-refresh-required"
                    or any(getattr(row, name) is not None for name in ("claim_id", "claim_owner", "claimed_at", "message_thread_id"))
                ):
                    return False
                old = validate_update(row)
                projection = _SourceProjection(link, _item(row), clock)
                await admission(session=writer, projection=projection)
                source = projection.context
                if not compatible(old["source"], source, row):
                    raise ProductSourceRetired()
                parent = await writer.get(TelegramOutboxRow, old["anchor"]["outbox_id"], with_for_update=True, populate_existing=True)
                if parent is None or parent.payload.get("status_update") != {"revision": old["source"]["revision"], "outbox_id": row.id}:
                    raise ProductSourceRetired()
                pointer = parent.payload.get("status_anchor")
                moved = await writer.get(TelegramOutboxRow, pointer["outbox_id"], populate_existing=True) if pointer else None
                target = anchor(link, parent, source, moved, replacement=old["anchor"]["replace"])
                if not same_body(target, old["anchor"]):
                    raise ProductSourceRetired()
                self._replace(row, parent, source, {**target, "replace": True})
                await writer.flush()
                await admission(session=writer, projection=_SendProjection(self, link, _item(row), clock))
                if guard is not None:
                    await guard.check(writer)
                return True

        return await await_drained(perform())

    async def authorize(self, captured, *, silent, now):
        async def read():
            if silent is not True:
                raise DeliveryClaimRetired()
            try:
                await self.read_part(captured)
            except ProductSourceRetired:
                # No SDK call passed the final fence. Only mutable display drift
                # with fresh source authority may keep this delivery for retry.
                basis = validate_update(captured.item)
                clock = await self.reader.prepare(captured.link)
                admission = await prepare_attention(self.reader, captured.link)
                source = _SourceProjection(captured.link, captured.item, clock)
                await admission(projection=source)
                if compatible(basis["source"], source.context, captured.item) and not same_body(basis["source"], source.context):
                    raise DeliveryRetry(timedelta(seconds=1)) from None
                raise

        return await await_drained(read())


class _SourceProjection(StatusProjection):
    def check(self, rows):
        super().check(rows)
        self.context = {"status": asdict(self.source), "revision": rows[0][self.column].revision, "work_id": self.item.work_id, "locale": self.clock.settings.locale}


class _SendProjection(_SourceProjection):
    def __init__(self, host, link, item, clock, *, captured=None):
        self.basis = validate_update(item)
        super().__init__(link, item, clock, WorkStatusRead(**self.basis["source"]["status"]))
        self.host, self.captured = host, captured

    def extend(self, query):
        result = super().extend(query)
        self.anchor_column = len(result.column_descriptions)
        parent, moved = aliased(TelegramOutboxRow), aliased(TelegramOutboxRow)
        return result.add_columns(parent, moved).outerjoin(parent, parent.id == self.basis["anchor"]["outbox_id"]).outerjoin(moved, moved.id == parent.payload["status_anchor"]["outbox_id"].as_string())

    def check(self, rows):
        super().check(rows)
        row, parent, moved = rows[0][self.column + 4], rows[0][self.anchor_column], rows[0][self.anchor_column + 1]
        if parent is None or not same_body(self.context, self.basis["source"]) or parent.payload.get("status_update") != {"revision": self.context["revision"], "outbox_id": row.id}:
            raise ProductSourceRetired()
        target = anchor(self.link, parent, self.context, moved, replacement=self.basis["anchor"]["replace"])
        if not same_body(target, self.basis["anchor"]) or row.message_thread_id != target["message_thread_id"]:
            raise ProductSourceRetired()
        due = aware(datetime.fromisoformat(target["edited_at"])) + timedelta(seconds=self.host.outbox.config.outbox.progress_edit_seconds)
        now = self.host.outbox.clock()
        if now < due:
            raise DeliveryRetry(due - now)
        if self.captured is not None:
            saved = self.captured
            current = self.host.outbox._sending(rows[0][0], row, link_id=self.link.id, part_id=saved.part["part_id"], claim_owner=saved.claim_owner, claim_id=saved.claim_id, token_revision=saved.token_revision)
            if current != saved:
                raise DeliveryClaimRetired()
