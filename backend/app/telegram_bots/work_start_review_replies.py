"""An actual refused Start may open its current recipient's full review."""

import logging
from dataclasses import dataclass
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item, keyboard_cleanup_row
from app.telegram_bots.work_digest_projection import DigestProjection
from app.telegram_bots.work_navigation_origin import NavigationRead, NavigationTarget, validate_origin
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_result_recovery import WorkResultRecovery, _ResultRecoveryProjection
from app.telegram_bots.work_start_controls import displayed_start
from app.telegram_bots.work_start_refresh import UnadmittedStartProjection, prepare_start_carrier, refresh_origin_projection, require_unreserved, unadmitted_start
from app.telegram_bots.work_start_review_acts import JournalStartProof
from app.telegram_bots.work_view_origins import anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class _ReviewIneligible(Exception):
    """A proven review-only restriction permits the qualified Work-link fallback."""


def intent_for(item):
    value = item.payload.get("work_start_refusal_intent")
    if (
        type(value) is not dict
        or set(value) != {"version", "mode", "reason", "gesture_id", "callback_id", "anchor_outbox_id", "presentation_version", "anchor_signature", "cleanup_id"} | ({"act_id", "act_proof"} if value.get("mode") == "act" else set())
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["reason"] != "awaiting-review"
        or value["mode"] not in {"unadmitted", "act"}
        or type(value["presentation_version"]) is not int
        or value["presentation_version"] < 0
    ):
        raise ProductSourceRetired()
    keys = ["gesture_id", "callback_id", "anchor_outbox_id", "anchor_signature", *(["act_id", "act_proof"] if value["mode"] == "act" else ["cleanup_id"])]
    if value["mode"] == "act" and value["cleanup_id"] is not None:
        raise ProductSourceRetired()
    for key in keys:
        size = 64 if key in {"anchor_signature", "act_proof"} else 32
        if type(value[key]) is not str or len(value[key]) != size or any(char not in "0123456789abcdef" for char in value[key]):
            raise ProductSourceRetired()
    return value


@dataclass(frozen=True)
class StartReviewSnapshot:
    link: object
    item: object
    locale: str
    projection: object


@dataclass(frozen=True)
class StartReviewRead:
    owner: object
    snapshot: StartReviewSnapshot
    anchor: object
    value: dict
    target: NavigationTarget

    authorize = NavigationRead.authorize


class WorkStartReviewReplies:
    def __init__(self, chat):
        self.chat, self.plans = chat, chat.runtime.reviews
        self.outbox, self.callbacks = self.plans.outbox, self.plans.callbacks
        self.recovery = WorkResultRecovery(self.plans)

    async def open(self, link, callback_id, *, query_id, chat_id, message_id, message_thread_id, guard):
        return await await_drained(self._open(link, callback_id, query_id=query_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard))

    async def from_act(self, link, act_id, *, guard):
        try:
            return await await_drained(self._from_act(link, act_id, guard=guard))
        except _ReviewIneligible:
            return None

    async def _from_act(self, link, act_id, *, guard):
        if guard is None:
            return None
        await prepare_publication_guard(guard)
        key = "notice:work-navigation:" + digest([link.id, link.binding_generation, "act-review", act_id])
        async with self.outbox.sessions() as reader:
            existing = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.binding_generation == link.binding_generation, TelegramOutboxRow.dedupe_key == key))
            if existing is not None:
                return existing.id
        try:
            proof = await JournalStartProof.read(self.chat, link, act_id)
            detail = await self.chat.views.reader.detail(link, proof.act.work_id)
            if detail.request is None:
                return None
            source, _settings = await self.recovery._source(link, detail.request["id"])
            require_review(link, source)
        except (ProductSourceRetired, ValueError):
            return None
        if source.work["id"] != proof.act.work_id:
            raise ProductSourceRetired()
        admission = await prepare_attention(self.plans.reader, link)
        creation = DigestProjection(link, [proof.projection, _ResultRecoveryProjection(source)])
        async with self.callbacks.writer(link, guard) as writer:
            held, _ = await admission(session=writer, projection=creation)
            if "agents:write" not in held:
                raise _ReviewIneligible()
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
            if existing is not None:
                return existing.id
            selected = await writer.get(TelegramCallbackRow, proof.callback_id, populate_existing=True)
            self._original_age(selected)
            identity = uuid4().hex
            gesture_id = digest([identity, "start-review-origin"])[:32]
            intent = {
                "version": 1,
                "mode": "act",
                "reason": "awaiting-review",
                "gesture_id": gesture_id,
                "act_id": act_id,
                "act_proof": proof.signature,
                "callback_id": proof.callback_id,
                "anchor_outbox_id": proof.parent_id,
                "presentation_version": selected.payload["presentation_version"],
                "anchor_signature": proof.anchor_signature,
                "cleanup_id": None,
            }
            row = TelegramOutboxRow(
                id=identity,
                link_id=link.id,
                owner_user_id=link.owner_user_id,
                binding_generation=link.binding_generation,
                chat_id=link.chat_id,
                message_thread_id=selected.message_thread_id,
                instance_id=link.agent_ref,
                work_id=source.work["id"],
                request_id=source.request["id"],
                dedupe_key=key,
                class_="reply",
                payload={"presentation_version": 0, "work_start_refusal_intent": intent},
                parts=[],
                state="pending",
                silent=True,
                created_at=self.outbox.clock(),
                not_before=self.outbox.clock(),
            )
            expires = aware(selected.created_at) + timedelta(days=self.callbacks.config.callbacks.max_age_days)
            if selected.expires_at is not None:
                expires = min(expires, aware(selected.expires_at))
            witness = TelegramCallbackRow(
                id=gesture_id,
                group_id=gesture_id,
                link_id=link.id,
                binding_generation=link.binding_generation,
                chat_id=link.chat_id,
                message_id=selected.message_id,
                message_thread_id=selected.message_thread_id,
                kind="work_start_review_origin",
                target_kind="outbox",
                target_id=identity,
                payload=intent,
                expires_at=expires,
                created_at=self.outbox.clock(),
            )
            writer.add_all([row, witness])
            await writer.flush()
            held, _ = await admission(session=writer, projection=creation)
            if "agents:write" not in held:
                raise _ReviewIneligible()
            self._original_age(selected)
        try:
            await self.materialize(link, identity, guard=guard)
        except Exception as error:
            # The committed handoff remains owned and can be reopened; a
            # generic execution notice must not replace this saved reply.
            logger.info("Start review materialization deferred (%s)", type(error).__name__)
        return identity

    def _original_age(self, selected):
        try:
            self.callbacks._unexpired(selected)
        except CallbackRefused:
            raise _ReviewIneligible() from None

    async def _open(self, link, callback_id, *, query_id, chat_id, message_id, message_thread_id, guard):
        if type(query_id) is not str or not 1 <= len(query_id) <= 256:
            raise CallbackRefused()
        await prepare_publication_guard(guard)
        key = digest([link.id, link.binding_generation, callback_id, query_id, "start-review"])
        dedupe = "notice:work-navigation:" + key
        async with self.outbox.sessions() as reader:
            selected = await reader.get(TelegramCallbackRow, callback_id)
            parent = await reader.get(TelegramOutboxRow, selected.payload.get("anchor_outbox_id")) if selected else None
            if selected is None or parent is None:
                raise CallbackRefused()
            original, displayed = displayed_start(link, parent, selected)
        original_proof = await prepare_start_carrier(self.chat.views.reader, link, parent, original)
        detail = await self.chat.views.reader.detail(link, displayed["id"])
        request = detail.request
        if detail.record["status"] != "submitted" or request is None or request["purpose"] != "review" or request["state"] != "pending":
            raise ProductSourceRetired()
        source, _settings = await self.recovery._source(link, request["id"])
        require_review(link, source)
        admission = await prepare_attention(self.plans.reader, link)
        creation = DigestProjection(link, [original_proof, _ResultRecoveryProjection(source)])
        async with self.callbacks.writer(link, guard) as writer:
            held, _target = await admission(session=writer, projection=creation)
            if "agents:write" not in held:
                raise ProductSourceRetired()
            row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe).with_for_update())
            if row is None:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                original, current = displayed_start(link, parent, group.selected)
                if current["id"] != source.work["id"]:
                    raise ProductSourceRetired()
                require_unreserved(group.rows)
                await unadmitted_start(writer, link, callback_id, source.work["id"])
                await self.callbacks.consume_group(group, reason="start-review", session=writer)
                cleanup = keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=self.outbox.clock())
                row = TelegramOutboxRow(
                    id=uuid4().hex,
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    binding_generation=link.binding_generation,
                    chat_id=chat_id,
                    message_thread_id=message_thread_id,
                    instance_id=link.agent_ref,
                    work_id=source.work["id"],
                    request_id=source.request["id"],
                    dedupe_key=dedupe,
                    class_="reply",
                    payload={
                        "presentation_version": 0,
                        "work_start_refusal_intent": {
                            "version": 1,
                            "mode": "unadmitted",
                            "reason": "awaiting-review",
                            "callback_id": callback_id,
                            "anchor_outbox_id": parent.id,
                            "presentation_version": group.presentation_version,
                            "anchor_signature": anchor_signature(original),
                            "cleanup_id": cleanup.id,
                        },
                    },
                    parts=[],
                    state="pending",
                    silent=True,
                    not_before=self.outbox.clock(),
                    created_at=cleanup.created_at + timedelta(microseconds=1),
                )
                cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id}
                gesture_id = digest([row.id, "start-review-origin"])[:32]
                row.payload = {**row.payload, "work_start_refusal_intent": {**row.payload["work_start_refusal_intent"], "gesture_id": gesture_id}}
                expires = aware(group.selected.created_at) + timedelta(days=self.callbacks.config.callbacks.max_age_days)
                if group.selected.expires_at is not None:
                    expires = min(expires, aware(group.selected.expires_at))
                witness = TelegramCallbackRow(
                    id=gesture_id,
                    group_id=gesture_id,
                    link_id=link.id,
                    binding_generation=link.binding_generation,
                    chat_id=chat_id,
                    message_id=message_id,
                    message_thread_id=message_thread_id,
                    kind="work_start_review_origin",
                    target_kind="outbox",
                    target_id=row.id,
                    payload=row.payload["work_start_refusal_intent"],
                    expires_at=expires,
                    created_at=self.outbox.clock(),
                )
                writer.add_all([cleanup, row, witness])
                await writer.flush()
            item = _item(row)
            if intent_for(item)["callback_id"] != callback_id or (item.chat_id, item.message_thread_id) != (chat_id, message_thread_id):
                raise ProductSourceRetired()
            held, _target = await admission(session=writer, projection=creation)
            if "agents:write" not in held:
                raise ProductSourceRetired()
            await unadmitted_start(writer, link, callback_id, source.work["id"])
            self.callbacks._unexpired(selected)
        return await self.materialize(link, item.id, guard=guard)

    async def materialize(self, link, identity, *, guard):
        return await await_drained(self._materialize(link, identity, guard=guard))

    async def _materialize(self, link, identity, *, guard):
        await prepare_publication_guard(guard)
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            if row is None:
                raise ProductSourceRetired()
            item = _item(row)
            intent = intent_for(item)
            selected = await reader.get(TelegramCallbackRow, intent["callback_id"])
            parent = await reader.get(TelegramOutboxRow, intent["anchor_outbox_id"])
            if selected is None or parent is None:
                raise ProductSourceRetired()
            original = None
            if intent["mode"] == "unadmitted":
                original, _ = displayed_start(link, parent, selected)
            value = validate_origin(
                {
                    "version": 1,
                    "kind": "start",
                    "start_id": item.id,
                    "start_proof": digest(intent),
                    "anchor_outbox_id": parent.id,
                    "anchor_proof": anchor_signature(original) if original is not None else intent["anchor_signature"],
                    "presentation_version": intent["presentation_version"],
                    "message_id": selected.message_id,
                    "callback_id": selected.id,
                    "member_id": item.request_id,
                    "query_key": item.dedupe_key.removeprefix("notice:work-navigation:"),
                }
            )
        origin = await self.restore(link, value)
        settings = await self.plans.settings.get(link.owner_user_id)
        if settings is None or settings.locale != origin.snapshot.locale:
            raise ProductSourceRetired()
        async with self.callbacks.writer(link, guard) as writer:
            row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
            await origin.authorize(session=writer)
            if not row.parts:
                await self.plans._freeze(writer, link, row, origin.target.source, settings, pending=True, navigation_origin=value)
                await inherit_age(writer, row, selected.created_at)
            await origin.authorize(session=writer)
            return row.id

    async def restore(self, link, value):
        value = validate_origin(value)

        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, value["start_id"])
                if row is None:
                    raise ProductSourceRetired()
                item = _item(row)
                intent = intent_for(item)
                parent = await reader.get(TelegramOutboxRow, intent["anchor_outbox_id"])
                selected = await reader.get(TelegramCallbackRow, intent["callback_id"])
                if parent is None or selected is None:
                    raise ProductSourceRetired()
                original = None
                if intent["mode"] == "unadmitted":
                    original, _ = displayed_start(link, parent, selected)
            journal = None
            if intent["mode"] == "act":
                journal = await JournalStartProof.read(self.chat, link, intent["act_id"])
                if journal.signature != intent["act_proof"] or (journal.callback_id, journal.parent_id, journal.anchor_signature) != (intent["callback_id"], parent.id, intent["anchor_signature"]):
                    raise ProductSourceRetired()
                original = journal.original
            if value != {
                "version": 1,
                "kind": "start",
                "start_id": item.id,
                "start_proof": digest(intent),
                "anchor_outbox_id": parent.id,
                "anchor_proof": journal.anchor_signature if journal is not None else anchor_signature(original),
                "presentation_version": intent["presentation_version"],
                "message_id": selected.message_id,
                "callback_id": selected.id,
                "member_id": item.request_id,
                "query_key": item.dedupe_key.removeprefix("notice:work-navigation:"),
            }:
                raise ProductSourceRetired()
            source, _settings = await self.recovery._source(link, item.request_id)
            require_review(link, source)
            if journal is not None and journal.act.work_id != source.work["id"]:
                raise ProductSourceRetired()
            settings = await self.plans.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            original_projection = journal.projection if journal is not None else await refresh_origin_projection(self.chat.views.reader, link, item, origin=intent)
            projection = StartReviewProjection(self, link, item, source, original_projection)
            snapshot = StartReviewSnapshot(link, item, settings.locale, projection)
            result = StartReviewRead(self, snapshot, original, value, NavigationTarget(source, "work_request_basis", source.work["id"], source.request["id"], None, review=True))
            await result.authorize()
            return result

        return await await_drained(perform())


def require_review(link, source):
    if (
        source.work["status"] != "submitted"
        or source.request["purpose"] != "review"
        or source.request["state"] != "pending"
        or source.request["needs_routing"]
        or source.request["recipient_id"] != link.owner_user_id
        or not source.request["can_manage"]
    ):
        raise ProductSourceRetired()


async def inherit_age(writer, row, created_at):
    groups = {part["callback_group_id"] for part in row.parts if "callback_group_id" in part}
    for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups))):
        control.created_at = created_at


class StartReviewProjection:
    def __init__(self, owner, link, item, source, original):
        self.owner, self.link, self.item, self.source, self.original = owner, link, item, source, original
        self.intent = intent_for(item)
        self.review = _ResultRecoveryProjection(source)
        self.composed = DigestProjection(link, [original, self.review])
        self.absence = UnadmittedStartProjection(link, self.intent, item) if self.intent["mode"] == "unadmitted" else None

    def extend(self, query):
        query = self.composed.extend(query)
        if self.absence is not None:
            query = self.absence.extend(query)
        self.column = len(query.column_descriptions)
        delivery, parent, control, witness = aliased(TelegramOutboxRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(TelegramCallbackRow)
        self.callback_column = self.column + 2
        return (
            query.add_columns(delivery, parent, control, witness)
            .join(delivery, delivery.id == self.item.id)
            .join(parent, parent.id == self.intent["anchor_outbox_id"])
            .outerjoin(control, control.group_id == self.owner_group)
            .outerjoin(witness, witness.id == self.intent["gesture_id"])
        )

    @property
    def owner_group(self):
        # The original parent's group is a stored, SQL-selected SDK fact.
        return select(TelegramCallbackRow.group_id).where(TelegramCallbackRow.id == self.intent["callback_id"]).scalar_subquery()

    def check_with_permissions(self, rows, held):
        if "agents:write" not in held:
            raise ProductSourceRetired()
        self.composed.check_with_permissions(rows, held)
        if self.absence is not None:
            self.absence.check(rows)
        delivery, parent, _, witness = rows[0][self.column : self.column + 4]
        if (
            intent_for(_item(delivery)) != self.intent
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.message_thread_id, delivery.instance_id, delivery.work_id, delivery.request_id, delivery.event_id, delivery.class_)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.item.chat_id, self.item.message_thread_id, self.link.agent_ref, self.source.work["id"], self.source.request["id"], None, "reply")
            or delivery.dedupe_key != self.item.dedupe_key
        ):
            raise ProductSourceRetired()
        if (
            witness is None
            or witness.id != digest([delivery.id, "start-review-origin"])[:32]
            or (witness.id, witness.group_id, witness.kind, witness.target_kind, witness.target_id) != (self.intent["gesture_id"], self.intent["gesture_id"], "work_start_review_origin", "outbox", delivery.id)
            or (witness.link_id, witness.binding_generation, witness.chat_id, witness.message_thread_id) != (delivery.link_id, delivery.binding_generation, delivery.chat_id, delivery.message_thread_id)
            or witness.payload != self.intent
        ):
            raise ProductSourceRetired()
        controls = list({control.id: control for row in rows if (control := row[self.callback_column]) is not None}.values())
        selected = next((control for control in controls if control.id == self.intent["callback_id"]), None)
        if selected is None:
            raise ProductSourceRetired()
        expires = aware(selected.created_at) + timedelta(days=self.owner.callbacks.config.callbacks.max_age_days)
        if selected.expires_at is not None:
            expires = min(expires, aware(selected.expires_at))
        if witness.expires_at is None or aware(witness.expires_at) != expires:
            raise ProductSourceRetired()
        if self.intent["mode"] == "act":
            if selected.message_thread_id != delivery.message_thread_id or witness.message_id != selected.message_id:
                raise ProductSourceRetired()
            self.owner.callbacks._unexpired(selected)
            return
        original, displayed = displayed_start(self.link, parent, selected)
        _validate_rows(original, controls)
        require_unreserved(controls)
        if (
            anchor_signature(original) != self.intent["anchor_signature"]
            or displayed["id"] != delivery.work_id
            or selected.message_thread_id != delivery.message_thread_id
            or witness.message_id != selected.message_id
            or selected.message_id != self.absence.item.payload.get("work_request_basis", {}).get("navigation_origin", {}).get("message_id", selected.message_id)
            or any(control.consumed_at is None or control.closed_reason != "start-review" for control in controls)
        ):
            raise ProductSourceRetired()
        self.owner.callbacks._unexpired(selected)
