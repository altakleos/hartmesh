"""Input-owned confirmation views retain text while requiring a fresh explicit tap."""

import hashlib
import json
from copy import deepcopy
from datetime import timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackGroup, CallbackRefused, _aware, _row
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired, _item, keyboard_cleanup_row
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_input_capture import accepted_text
from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans, acknowledged_source
from app.telegram_bots.work_interactions import interaction_state
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryProjection
from app.telegram_bots.work_typing import shown_prompt_messages, validate_shown_prompt
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.json_compat import json_match
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def input_identity(mark):
    if "work_action_id" in mark.route_basis and mark.route_basis["work_action_id"] != mark.operation_ids[0]:
        raise CallbackRefused()
    return {"link_id": mark.link_id, "chat_id": mark.chat_id, "message_id": mark.message_id, "update_id": mark.update_id, "binding_generation": mark.binding_generation, "operation_id": mark.operation_ids[0]}


def validate_input_basis(basis):
    value = basis["input"]
    if type(value) is not dict or set(value) != {"link_id", "chat_id", "message_id", "update_id", "binding_generation", "operation_id"}:
        raise ValueError("invalid-work-input-origin")
    for key in ("link_id", "operation_id"):
        if type(value[key]) is not str or len(value[key]) != 32 or any(ch not in "0123456789abcdef" for ch in value[key]):
            raise ValueError("invalid-work-input-origin")
    for key in ("chat_id", "message_id", "update_id", "binding_generation"):
        if type(value[key]) is not int or value[key] < (1 if key in {"chat_id", "message_id"} else 0):
            raise ValueError("invalid-work-input-origin")
    if basis["interaction"]["view"] not in {"decision", "changes"} or basis["cleanup_id"] is not None:
        raise ValueError("invalid-work-input-view")


def input_state(data, source):
    if data["work_id"] != source.work["id"]:
        raise CallbackRefused()
    if data["purpose"] == "request_changes":
        if same_body(data["seen"], source.seen):
            raise CallbackRefused()
        value = {"view": "changes", "note": data["text"]}
        if data["request_id"] != source.request["id"]:
            value["replaced"] = True
    elif data["purpose"] in {"decide_own", "decide_note"} and data["request_id"] == source.request["id"]:
        value = {"view": "decision", "decision": data["text"] if data["purpose"] == "decide_own" else data["interaction"]["decision"], "note": None if data["purpose"] == "decide_own" else data["text"]}
        if not same_body(data["seen"], source.seen):
            value["refreshed"] = True
    else:
        raise CallbackRefused()
    return interaction_state(value, source)


def blocked_input_actions(identity):
    """Unknown or completed operations from this input forbid a fresh intent."""
    act, callback, delivery = aliased(TelegramActRow), aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
    active = (
        select(act.id)
        .outerjoin(callback, callback.id == act.id)
        .outerjoin(delivery, delivery.id == callback.payload["anchor_outbox_id"].as_string())
        .where(
            act.link_id == identity["link_id"],
            act.binding_generation == identity["binding_generation"],
            act.state.in_({"pending", "waiting", "uncertain", "done"}),
            or_(
                act.id == identity["operation_id"],
                delivery.payload["work_request_basis"]["input"]["operation_id"].as_string() == identity["operation_id"],
            ),
        )
        .exists()
    )
    reserved, parent, saved = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramActRow)
    orphaned_control = (
        select(reserved.id)
        .join(parent, parent.id == reserved.payload["anchor_outbox_id"].as_string())
        .outerjoin(saved, and_(saved.id == reserved.payload["work_action_id"].as_string(), saved.link_id == reserved.link_id, saved.binding_generation == reserved.binding_generation))
        .where(
            reserved.link_id == identity["link_id"],
            reserved.binding_generation == identity["binding_generation"],
            or_(reserved.payload["work_action_id"].as_string().is_not(None), json_match(reserved.payload, "work_action_id", None)),
            saved.id.is_(None),
            parent.payload["work_request_basis"]["input"]["operation_id"].as_string() == identity["operation_id"],
        )
        .exists()
    )
    mark, original = aliased(TelegramInboundMarkRow), aliased(TelegramActRow)
    orphaned_input = (
        select(mark.link_id)
        .outerjoin(original, and_(original.id == mark.route_basis["work_action_id"].as_string(), original.link_id == mark.link_id, original.binding_generation == mark.binding_generation))
        .where(
            mark.link_id == identity["link_id"],
            mark.chat_id == identity["chat_id"],
            mark.message_id == identity["message_id"],
            mark.binding_generation == identity["binding_generation"],
            or_(mark.route_basis["work_action_id"].as_string().is_not(None), json_match(mark.route_basis, "work_action_id", None)),
            or_(original.id.is_(None), mark.route_basis["work_action_id"].as_string() != identity["operation_id"]),
        )
        .exists()
    )
    return or_(active, orphaned_control, orphaned_input)


def check_input_origin(link, parent, callbacks, mark, blocked, basis, source, *, draft_ttl_hours):
    """Final joined SQL facts prove the immutable accepted input and shown prompt."""
    try:
        _check_input_origin(link, parent, callbacks, mark, blocked, basis, source, draft_ttl_hours=draft_ttl_hours)
    except (KeyError, TypeError, ValueError, IndexError, AttributeError, CallbackRefused):
        raise ProductSourceRetired() from None


def _check_input_origin(link, parent, callbacks, mark, blocked, basis, source, *, draft_ttl_hours):
    if mark is None or blocked or mark.state == "done" or mark.route_complete or mark.route_kind != "work_input" or len(mark.operation_ids) != 1 or not same_body(input_identity(mark), basis["input"]):
        raise ProductSourceRetired()
    if basis["interaction"]["view"] == "changes":
        expiry = (_aware(mark.accepted_at) + timedelta(hours=draft_ttl_hours)).isoformat()
        if any(control.get("expires_at") != expiry for line in basis["keyboard"] for control in line):
            raise ProductSourceRetired()
    origin = basis["origin"]
    previous = acknowledged_source(link, parent)
    prompt = next((row for row in callbacks if row.id == origin["callback_id"]), None)
    if parent.id != origin["anchor_outbox_id"] or parent.payload["presentation_version"] != origin["presentation_version"] or prompt is None or prompt.consumed_at is None or prompt.closed_reason != "answered":
        raise ProductSourceRetired()
    acknowledged_at = validate_shown_prompt(_item(parent), prompt)
    if prompt.message_id != origin["message_id"] or mark.route_target_id != previous.request["id"] or (mark.link_id, mark.chat_id, mark.binding_generation) != (link.id, link.chat_id, link.binding_generation):
        raise ProductSourceRetired()
    data = {
        "version": 1,
        "prompt_id": prompt.id,
        "source_outbox_id": parent.id,
        "presentation_version": parent.payload["presentation_version"],
        "request_id": previous.request["id"],
        "work_id": previous.work["id"],
        "purpose": prompt.payload["purpose"],
        "interaction": prompt.payload["interaction"],
        "seen": previous.seen,
        "text": accepted_text(mark, prompt, acknowledged_at, message_ids=shown_prompt_messages(_item(parent), accepted_at=mark.accepted_at)),
    }
    if not same_body(data, mark.route_basis.get("work_input")) or not same_body(input_state(data, source), basis["interaction"]):
        raise ProductSourceRetired()


async def lock_input_origin(writer, link, parent, source, *, draft_ttl_hours):
    """Retain the accepted input through confirmation's borrowed service writer."""
    basis = parent.payload["work_request_basis"]
    if basis["version"] != 4:
        return
    identity = basis["input"]
    # Lock delivery before callback, as the publication paths do.
    original = await writer.get(TelegramOutboxRow, basis["origin"]["anchor_outbox_id"], with_for_update=True, populate_existing=True)
    prompt = await writer.get(TelegramCallbackRow, basis["origin"]["callback_id"], with_for_update=True, populate_existing=True)
    mark = await writer.get(TelegramInboundMarkRow, (identity["link_id"], identity["message_id"], identity["chat_id"]), with_for_update=True, populate_existing=True)
    blocked = await writer.scalar(select(blocked_input_actions(identity)))
    check_input_origin(link, original, [prompt] if prompt is not None else [], mark, blocked, basis, source, draft_ttl_hours=draft_ttl_hours)


class WorkInputPreviews:
    def __init__(self, plans, captures):
        self.plans, self.captures = plans, captures
        self.recovery, self.interactions = WorkRequestRecovery(plans), WorkInteractionPlans(plans)
        if captures.inbox.session_factory.kw.get("bind") is not plans.outbox.sessions.kw.get("bind"):
            raise ValueError("work-input-requires-shared-storage")

    async def open(self, link, origin, *, guard, request_id=None, conclusive_act=None):
        async def perform():
            if origin.link_id != link.id:
                raise CallbackRefused()
            async with self.plans.outbox.sessions() as reader:
                mark = (await self.captures.inbox._rows(reader, [origin]))[0]
                if mark.route_kind != "work_input":
                    raise CallbackRefused()
                prompt_id = mark.route_basis["work_input"]["prompt_id"]
            captured = await self.captures.capture(link, origin, prompt_id=prompt_id, guard=guard)
            data = captured.route_basis["work_input"]
            source, settings = await self.recovery._source(link, request_id or data["request_id"])
            state = input_state(data, source)
            admission = await prepare_attention(self.plans.reader, link)
            composed = _InputGuard(guard, admission, source, captured, conclusive_act=conclusive_act)
            async with self.plans.callbacks.writer(link, composed) as writer:
                latest = await self.captures.capture(link, origin, prompt_id=prompt_id, guard=composed, session=writer)
                if not same_body(latest.route_basis, captured.route_basis) or latest.operation_ids != captured.operation_ids:
                    raise CallbackRefused()
                return await self._publish(writer, link, captured, source, settings, state, guard=composed)

        return await await_drained(perform())

    async def _publish(self, writer, link, captured, source, settings, state, *, guard):
        """Publish already-qualified input inside its owning guarded writer."""
        data, identity = captured.route_basis["work_input"], input_identity(captured)
        digest = hashlib.sha256(json.dumps((source.seen, state, source.protected, settings.locale), sort_keys=True).encode()).hexdigest()[:32]
        key = f"notice:work-input-preview:{captured.message_id}:{digest}"
        existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
        if existing is not None:
            if not same_body(existing.payload.get("work_request_basis", {}).get("input"), identity):
                raise CallbackRefused()
            return existing.id
        await self._retire_previous(writer, link, identity)
        parent = await writer.get(TelegramOutboxRow, data["source_outbox_id"], with_for_update=True, populate_existing=True)
        prompt = await writer.get(TelegramCallbackRow, data["prompt_id"], with_for_update=True, populate_existing=True)
        group = CallbackGroup(_row(prompt), (_row(prompt),), parent.id, parent.payload["presentation_version"])
        result = await self.interactions._publish(
            writer,
            link,
            group,
            parent,
            source,
            settings,
            state,
            key=key,
            chat_id=captured.chat_id,
            message_id=prompt.message_id,
            message_thread_id=captured.message_thread_id,
            guard=guard,
            input_origin=identity,
        )
        await self.captures.inbox.register_delivery([captured], outbox_id=result, primary=False, finish=False, session=writer)
        return result

    async def _retire_previous(self, writer, link, identity):
        rows = await writer.scalars(
            select(TelegramOutboxRow)
            .where(
                TelegramOutboxRow.link_id == link.id,
                TelegramOutboxRow.binding_generation == link.binding_generation,
                TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == identity["operation_id"],
            )
            .order_by(TelegramOutboxRow.id)
            .with_for_update()
        )
        for row in rows:
            # An in-flight send must settle before its keyboard can be removed.
            if row.claim_id is not None:
                raise DeliveryRetry(timedelta(seconds=5))
            groups = {part.get("callback_group_id") for part in row.parts} - {None}
            controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups)).order_by(TelegramCallbackRow.id).with_for_update()))
            cleared = set()
            for control in controls:
                if control.consumed_at is not None:
                    continue
                if control.message_id is not None and control.group_id not in cleared:
                    cleared.add(control.group_id)
                    cleanup = keyboard_cleanup_row(control, owner_user_id=link.owner_user_id, now=self.plans.outbox.clock(), dedupe_key="notice:work-input-cleanup:" + control.group_id)
                    cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id}
                    writer.add(cleanup)
                control.consumed_at, control.closed_reason = self.plans.outbox.clock(), "superseded"
            if row.state not in {"sent", "skipped"}:
                row.state, row.skip_reason, row.closed_at = "skipped", "superseded", self.plans.outbox.clock()
                parts = deepcopy(row.parts)
                for part in parts:
                    if part["state"] not in {"sent", "retired", "unavailable"}:
                        part["state"] = "retired"
                row.parts = parts


class _InputGuard:
    def __init__(self, base, admission, source, captured, *, conclusive_act=None):
        self.base, self.admission, self.source, self.captured = base, admission, source, captured
        self.conclusive_act = conclusive_act

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        held, _target = await self.admission(session=writer, projection=_InputProjection(self.source, self.captured, conclusive_act=self.conclusive_act))
        if "agents:write" not in held:
            raise AgentDenied("The preview is unavailable")
        self.base.current()
        return current


class _InputProjection(_RecoveryProjection):
    def __init__(self, source, captured, *, conclusive_act=None):
        super().__init__(source)
        self.captured, self.conclusive_act = captured, conclusive_act

    def extend(self, query):
        mark = TelegramInboundMarkRow
        query = (
            super()
            .extend(query)
            .add_columns(mark, blocked_input_actions(input_identity(self.captured)))
            .join(mark, and_(mark.link_id == self.captured.link_id, mark.chat_id == self.captured.chat_id, mark.message_id == self.captured.message_id))
        )
        if self.conclusive_act is not None:
            saved = aliased(TelegramActRow)
            query = query.add_columns(saved).outerjoin(saved, saved.id == self.conclusive_act.id)
        return query

    def check(self, rows):
        super().check(rows)
        self._check_mark(rows)

    def _check_mark(self, rows, *, complete=False):
        mark, blocked = rows[0][11:13]
        if self.conclusive_act is not None:
            saved, expected = rows[0][13], self.conclusive_act
            if (
                saved is None
                or saved.state != "conflict"
                or expected.state != "conflict"
                or saved.attempts != expected.attempts
                or any(getattr(saved, field) != getattr(expected, field) for field in _ACT_SCOPE)
                or not same_body(saved.body, expected.body)
            ):
                raise ProductSourceRetired()
        if (
            blocked
            or mark.state == "done"
            and not complete
            or mark.route_complete is not complete
            or not same_body(mark.route_basis, self.captured.route_basis)
            or not same_body(mark.sanitized_input, self.captured.sanitized_input)
            or mark.operation_ids != self.captured.operation_ids
            or mark.binding_generation != self.captured.binding_generation
            or (mark.route_kind, mark.route_target_id, mark.message_thread_id, mark.update_id) != (self.captured.route_kind, self.captured.route_target_id, self.captured.message_thread_id, self.captured.update_id)
            or _aware(mark.accepted_at) != _aware(self.captured.accepted_at)
        ):
            raise ProductSourceRetired()
