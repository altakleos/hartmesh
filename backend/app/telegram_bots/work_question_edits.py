"""Dormant source-owned updates to acknowledged question messages."""

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import _PART_IDENTITY, _TRANSPORT_RECEIPTS, DeliveryClaimRetired, DeliveryRetry, ProductSourceRetired, _item
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_question_updates import UPDATE_KEYS, QuestionUpdateProjection
from app.telegram_bots.work_request_source import _references, frozen
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_stop_news import NewsClock
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def anchor(link, row, source, moved=None):
    if (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.work_id, row.request_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, source["work_id"], source["request_id"]):
        raise ProductSourceRetired()
    if row.state == "skipped":
        raise ProductSourceRetired()
    if row.state != "sent":
        raise DeliveryRetry(timedelta(seconds=5))
    item = _item(row)
    current = [part for part in item.parts if part["presentation_version"] == item.payload["presentation_version"]]
    if not current:
        raise ProductSourceRetired()
    last = current[-1]
    basis, request, _recipe = frozen(SimpleNamespace(link=link, item=item, part=last), acknowledged=True)
    if (request.seen["request_revision"], request.seen["assignment_revision"], request.request["id"]) != (source["request_revision"], source["assignment_revision"], source["request_id"]):
        raise ProductSourceRetired()
    if len(last["message_ids"]) != 1 or type(last["message_ids"][0]) is not int:
        raise ProductSourceRetired()
    target = {
        "outbox_id": row.id,
        "version": item.payload["presentation_version"],
        "part_id": last["part_id"],
        "message_id": last["message_ids"][0],
        "original_message_id": last["message_ids"][0],
        "protected": bool(last.get("protect_content")),
        "concealed": basis["concealed"],
        "html": last["html"],
        "group_id": last.get("callback_group_id"),
        "message_thread_id": row.message_thread_id,
        "source_signature": digest(basis),
        "space_ids": sorted(set(_references((request.request, request.work, request.responses)))),
    }
    pointer = row.payload.get("question_anchor")
    if pointer is not None:
        if type(pointer) is not dict or set(pointer) != {"outbox_id", "part_id"} or moved is None or moved.id != pointer["outbox_id"] or moved.state != "sent":
            raise ProductSourceRetired()
        saved = validate_update(moved, acknowledged=True)
        if (moved.link_id, moved.owner_user_id, moved.binding_generation, moved.chat_id, moved.work_id, moved.request_id) != (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.work_id, row.request_id) or any(
            not same_body(saved["anchor"][key], target[key]) for key in ("outbox_id", "version", "source_signature", "original_message_id", "space_ids")
        ):
            raise ProductSourceRetired()
        if saved["anchor"]["html"] not in {"", target["html"]}:
            raise ProductSourceRetired()
        last = [part for part in moved.parts if part["presentation_version"] == moved.payload["presentation_version"]][-1]
        if last["part_id"] != pointer["part_id"] or len(last["message_ids"]) != 1:
            raise ProductSourceRetired()
        concealed = target["concealed"] or saved["anchor"]["concealed"] or saved["source"]["protected"] and not target["protected"]
        # Overflow keeps the original body intact and moves only the footer.
        body = saved["anchor"]["html"] if last["kind"] == "edit" and not concealed else ""
        target.update(part_id=last["part_id"], message_id=last["message_ids"][0], protected=last["protect_content"], concealed=concealed, html=body)
    return target


def recipes(source, target):
    # An unopened teaser contains no question text to retain. A closure removes
    # Show without exposing the old question or a new recipient's identity.
    upgrade = source["protected"] and not target["protected"]
    html = render("stale.not_waiting", source["locale"]) if target["concealed"] or upgrade else target["html"] + "\n\n" + render(source["key"], source["locale"], **source["values"])
    protected = source["protected"] or target["protected"]
    parts = []
    if upgrade:
        parts.append(
            {
                "kind": "edit",
                "html": "",
                "visible_text": "",
                "protect_content": True,
                "silent": True,
                "remove_keyboard": True,
                "target_message_id": target["message_id"],
                "target_protect_content": target["protected"],
                "source_ref": {"key": "work.question.update", "values": {"request_id": source["request_id"], "part": 0}},
            }
        )
    chunks = split_html(html)
    if not upgrade and len(chunks) > 1:
        # Preserve every character of the acknowledged body; only the short
        # closure line needs a new message when it cannot fit beside that body.
        parts.append(
            {
                "kind": "edit",
                "html": "",
                "visible_text": "",
                "protect_content": protected,
                "silent": True,
                "remove_keyboard": True,
                "target_message_id": target["message_id"],
                "target_protect_content": target["protected"],
                "source_ref": {"key": "work.question.update", "values": {"request_id": source["request_id"], "part": 0}},
            }
        )
        chunks = split_html(render(source["key"], source["locale"], **source["values"]))
    for index, chunk in enumerate(chunks):
        part = {
            "kind": "text",
            "html": chunk,
            "visible_text": visible_text(chunk),
            "protect_content": protected,
            "source_ref": {"key": "work.question.update", "values": {"request_id": source["request_id"], "part": len(parts)}},
            "silent": True,
        }
        if index == 0 and not parts:
            part.update(kind="edit", target_message_id=target["message_id"], target_protect_content=target["protected"])
        parts.append(part)
    return parts


async def acknowledge_question_update(session, row):
    basis = validate_update(row, acknowledged=True)
    parent = await session.get(TelegramOutboxRow, basis["anchor"]["outbox_id"], with_for_update=True, populate_existing=True)
    if parent is None or parent.payload.get("question_update") != {"event_revision": basis["source"]["event_revision"], "outbox_id": row.id}:
        raise ProductSourceRetired()
    last = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]][-1]
    parent.payload = {**parent.payload, "question_anchor": {"outbox_id": row.id, "part_id": last["part_id"]}}


async def moved_anchor(session, parent):
    pointer = parent.payload.get("question_anchor")
    return await session.get(TelegramOutboxRow, pointer["outbox_id"], populate_existing=True) if pointer else None


def validate_update(row, *, acknowledged=False):
    basis = row.payload["work_question_update"]
    if type(basis) is not dict or set(basis) != {"version", "source", "anchor", "parts", "history"} or type(basis["version"]) is not int or basis["version"] != 1:
        raise ProductSourceRetired()
    version = row.payload["presentation_version"]
    history = basis["history"]
    if type(version) is not int or version < 0 or type(history) is not list or len(history) != version:
        raise ProductSourceRetired()
    ids = set()
    messages = {basis["anchor"]["original_message_id"]}
    for prior in range(version + 1):
        saved = basis if prior == version else history[prior]["basis"]
        messages.add(saved["anchor"]["message_id"])
        if prior < version and (set(history[prior]) != {"basis", "parts"} or saved.get("history") != [] or saved["source"]["event_signature"] != basis["source"]["event_signature"]):
            raise ProductSourceRetired()
        expected = recipes(saved["source"], saved["anchor"])
        actual = [part for part in row.parts if part["presentation_version"] == prior]
        if not same_body(expected, saved["parts"]) or len(actual) != len(expected) or prior < version and not same_body(actual, history[prior]["parts"]):
            raise ProductSourceRetired()
        unsent = False
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
                    if stamp.tzinfo is None or stamp.utcoffset() is None:
                        raise ValueError()
                except (KeyError, TypeError, ValueError):
                    raise ProductSourceRetired() from None
                if (
                    unsent
                    or type(part.get("message_ids")) is not list
                    or len(part["message_ids"]) != 1
                    or type(part["message_ids"][0]) is not int
                    or part["message_ids"][0] <= 0
                    or part["kind"] == "edit"
                    and part["message_ids"] != [recipe["target_message_id"]]
                    or part["kind"] == "text"
                    and part["message_ids"][0] in messages
                ):
                    raise ProductSourceRetired()
                messages.add(part["message_ids"][0])
            else:
                unsent = True
                if part.get("message_ids") != [] or part.get("sent_at") is not None:
                    raise ProductSourceRetired()
    if len(ids) != len(row.parts):
        raise ProductSourceRetired()
    return basis


def retired_without_delivery(link, row, source):
    intent = row.payload.get("work_event_intent", {})
    return (
        (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.instance_id, row.work_id, row.request_id)
        == (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref, source["work_id"], source["request_id"])
        and row.state == "skipped"
        and row.skip_reason == "superseded"
        and row.dedupe_key == f"event:{row.event_id}:question"
        and not row.parts
        and row.message_id is None
        and all(getattr(row, field) is None for field in ("claim_owner", "claim_id", "claimed_at"))
        and type(intent.get("version")) is int
        and intent["version"] == 1
        and intent.get("key") in {"ask.info", "ask.decision", "review.ready", "request.unrouted"}
        and type(intent.get("request_revision")) is int
        and intent["request_revision"] == source["request_revision"]
        and type(intent.get("assignment_revision")) is int
        and intent["assignment_revision"] == source["assignment_revision"]
    )


class WorkQuestionEdits:
    def __init__(self, plans):
        self.plans, self.reader, self.outbox, self.settings = plans, plans.reader, plans.outbox, plans.settings

    async def clock(self, link):
        settings = await self.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        return NewsClock(settings, await asyncio.to_thread(ZoneInfo, settings.timezone), self.outbox.clock)

    async def materialize(self, link, identity):
        return await await_drained(self._materialize(link, identity))

    async def _materialize(self, link, identity):
        clock = await self.clock(link)
        admission = await prepare_attention(self.reader, link)
        async with self.outbox._writer(link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
            if row is None or row.payload.get("work_event_intent", {}).get("key") not in UPDATE_KEYS:
                return False
            if not self.outbox._binding(binding, link) or row.claim_id is not None or row.state != "pending":
                raise ProductSourceRetired()
            if row.parts:
                return "work_question_update" in row.payload
            projection = QuestionUpdateProjection(link, _item(row), clock)
            await admission(session=writer, projection=projection)
            source = projection.source
            parents = list(
                await writer.scalars(
                    select(TelegramOutboxRow)
                    .where(
                        TelegramOutboxRow.link_id == link.id,
                        TelegramOutboxRow.binding_generation == link.binding_generation,
                        TelegramOutboxRow.request_id == row.request_id,
                        TelegramOutboxRow.class_ == "question",
                        ~TelegramOutboxRow.dedupe_key.startswith("edit:"),
                    )
                    .order_by(TelegramOutboxRow.created_at.desc(), TelegramOutboxRow.id.desc())
                    .with_for_update()
                )
            )
            matching = [parent for parent in parents if parent.payload.get("work_request_basis", {}).get("source", {}).get("seen", {}).get("request_revision") == source["request_revision"]]
            if not matching:
                # A conclusively retired raw question has no Telegram message
                # to edit. Missing or ambiguous anchors must remain held.
                relevant = [parent for parent in parents if parent.payload.get("work_event_intent", {}).get("request_revision") == source["request_revision"]]
                if relevant and all(retired_without_delivery(link, parent, source) for parent in relevant):
                    row.state, row.skip_reason, row.closed_at = "skipped", "superseded", self.outbox.clock()
                    await writer.flush()
                    # Recapture authority and the complete original intent in
                    # the same transaction after changing only disposition.
                    await admission(session=writer, projection=QuestionUpdateProjection(link, _item(row), clock, source=source))
                    return True
                if any(parent.state in {"pending", "sending", "failed"} for parent in parents):
                    return False
                raise ProductSourceRetired()
            parent = matching[0]
            previous = parent.payload.get("question_update", {})
            if previous.get("event_revision", 0) >= source["event_revision"]:
                raise ProductSourceRetired()
            if previous:
                prior = await writer.get(TelegramOutboxRow, previous["outbox_id"], populate_existing=True)
                if prior is None or prior.state not in {"sent", "skipped"}:
                    raise DeliveryRetry(timedelta(seconds=5))
            target = anchor(link, parent, source, await moved_anchor(writer, parent))
            parts = recipes(source, target)
            basis = {"version": 1, "source": source, "anchor": target, "parts": parts, "history": []}
            row.payload = {**row.payload, "work_question_update": basis}
            row.message_thread_id = target["message_thread_id"]
            row.parts = self.outbox._parts(parts, row.payload["presentation_version"])
            parent.payload = {**parent.payload, "question_update": {"event_revision": source["event_revision"], "outbox_id": row.id}}
            controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == target["group_id"]).with_for_update()))
            try:
                _validate_rows(parent, controls)
            except CallbackRefused:
                raise ProductSourceRetired() from None
            original = [part for part in parent.parts if part["presentation_version"] == parent.payload["presentation_version"]][-1]
            if any(original["message_ids"] != [control.message_id] for control in controls):
                raise ProductSourceRetired()
            for control in controls:
                if control.consumed_at is None:
                    control.consumed_at, control.closed_reason = self.outbox.clock(), "superseded"
            await writer.flush()
            await admission(session=writer, projection=_EditProjection(self, link, _item(row), clock))
            return True

    async def read_part(self, captured):
        async def read():
            clock = await self.clock(captured.link)
            admission = await prepare_attention(self.reader, captured.link)
            current = QuestionUpdateProjection(captured.link, captured.item, clock)
            await admission(projection=current)
            old = validate_update(captured.item)
            if current.source["event_signature"] != old["source"]["event_signature"]:
                raise ProductSourceRetired()
            if not same_body(current.source, old["source"]):
                await self.refresh(captured, clock, admission)
                raise DeliveryClaimRetired()
            projection = _EditProjection(self, captured.link, captured.item, clock, captured=captured)
            await admission(projection=projection)
            part = projection.basis["parts"][captured.part["ordinal"]]
            return part["html"], part["visible_text"]

        return await await_drained(read())

    async def refresh(self, captured, clock, admission):
        async with self.outbox._writer(captured.link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True, populate_existing=True)
            current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
            if current != captured:
                raise DeliveryClaimRetired()
            await self._refresh(writer, row, captured.link, clock, admission)

    async def _refresh(self, writer, row, link, clock, admission):
        basis = validate_update(row)
        source = QuestionUpdateProjection(link, _item(row), clock)
        await admission(session=writer, projection=source)
        if source.source["event_signature"] != basis["source"]["event_signature"]:
            raise ProductSourceRetired()
        parent = await writer.get(TelegramOutboxRow, basis["anchor"]["outbox_id"], with_for_update=True, populate_existing=True)
        if parent is None or parent.payload.get("question_update") != {"event_revision": source.source["event_revision"], "outbox_id": row.id}:
            raise ProductSourceRetired()
        target = anchor(link, parent, source.source, await moved_anchor(writer, parent))
        version = row.payload["presentation_version"]
        old = deepcopy(row.parts)
        for part in old:
            if part["state"] in {"pending", "sending"}:
                part["state"] = "retired"
        history = deepcopy(basis["history"])
        history.append({"basis": {**deepcopy(basis), "history": []}, "parts": [part for part in old if part["presentation_version"] == version]})
        parts = recipes(source.source, target)
        row.parts = old + self.outbox._parts(parts, version + 1)
        row.payload = {**row.payload, "presentation_version": version + 1, "work_question_update": {"version": 1, "source": source.source, "anchor": target, "parts": parts, "history": history}}
        row.state, row.not_before = "pending", self.outbox.clock()
        row.claim_owner = row.claim_id = row.claimed_at = None
        row.failed_reason = row.closed_at = None
        await writer.flush()
        await admission(session=writer, projection=_EditProjection(self, link, _item(row), clock))

    async def repair(self, link, identity, *, guard=None):
        async def write():
            if guard is not None:
                await prepare_publication_guard(guard)
            clock = await self.clock(link)
            admission = await prepare_attention(self.reader, link)
            async with self.outbox._writer(link.id) as (writer, binding):
                if guard is not None:
                    await guard.check(writer)
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                if row is None or not self.outbox._binding(binding, link) or row.state != "failed" or row.failed_reason != "work-question-refresh-required":
                    raise ProductSourceRetired()
                if row.claim_owner is not None or row.claim_id is not None or row.claimed_at is not None:
                    raise DeliveryClaimRetired()
                await self._refresh(writer, row, link, clock, admission)
                if guard is not None:
                    await guard.check(writer)
                return True

        return await await_drained(write())

    async def authorize(self, captured, *, silent, now):
        async def read():
            if silent is not True:
                raise DeliveryClaimRetired()
            projection = _EditProjection(self, captured.link, captured.item, await self.clock(captured.link), captured=captured)
            admission = await prepare_attention(self.reader, captured.link)
            try:
                await admission(projection=projection)
            except ProductSourceRetired:
                fresh = QuestionUpdateProjection(captured.link, captured.item, await self.clock(captured.link))
                await admission(projection=fresh)
                old = projection.basis["source"]
                if fresh.source["event_signature"] == old["event_signature"] and not same_body(fresh.source, old):
                    raise DeliveryRetry(timedelta(seconds=1)) from None
                raise

        return await await_drained(read())


class _EditProjection(QuestionUpdateProjection):
    def __init__(self, host, link, item, clock, *, captured=None):
        self.basis = validate_update(item)
        super().__init__(link, item, clock, source=self.basis["source"])
        self.captured = captured
        from app.telegram_bots.work_source_spaces import ReferencedSpaces

        source = SimpleNamespace(request=[{"kind": "space_file", "space_id": identity} for identity in self.basis["anchor"]["space_ids"]], work={}, responses=[])
        self.references = ReferencedSpaces(host.reader, host.outbox.sessions, source)

    def extend(self, query):
        result = super().extend(query)
        self.anchor_column = len(result.column_descriptions)
        parent = aliased(TelegramOutboxRow)
        moved = aliased(TelegramOutboxRow)
        return result.add_columns(parent, moved, self.references.expression()).outerjoin(parent, parent.id == self.basis["anchor"]["outbox_id"]).outerjoin(moved, moved.id == parent.payload["question_anchor"]["outbox_id"].as_string())

    def check(self, rows):
        super().check(rows)
        row, parent = rows[0][self.column + 2], rows[0][self.anchor_column]
        if parent is None or parent.payload.get("question_update") != {"event_revision": self.source["event_revision"], "outbox_id": row.id}:
            raise ProductSourceRetired()
        target = anchor(self.link, parent, self.source, rows[0][self.anchor_column + 1])
        self.references.check(rows[0][self.anchor_column + 2])
        parts = recipes(self.source, target)
        current = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        if not same_body(target, self.basis["anchor"]) or not same_body(parts, self.basis["parts"]) or len(current) != len(parts):
            raise ProductSourceRetired()
        if row.message_thread_id != target["message_thread_id"]:
            raise ProductSourceRetired()
        for index, (part, recipe) in enumerate(zip(current, parts, strict=True)):
            if part.get("ordinal") != index or part.get("presentation_version") != row.payload["presentation_version"] or any(not same_body(part.get(key), value) for key, value in recipe.items()) or part.get("callback_group_id"):
                raise ProductSourceRetired()
            if part.get("state") == "sent" and (not part.get("sent_at") or not part.get("message_ids") or index == 0 and part["message_ids"] != [target["message_id"]]):
                raise ProductSourceRetired()
        if self.captured is not None:
            saved = self.captured
            if (row.state, row.claim_owner, row.claim_id, rows[0][0].token_revision) != ("sending", saved.claim_owner, saved.claim_id, saved.token_revision):
                raise DeliveryClaimRetired()
            if saved.part not in row.parts:
                raise DeliveryClaimRetired()
