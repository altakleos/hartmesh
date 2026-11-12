"""Retain ambiguous text until an actual answer or new-message choice."""

from copy import deepcopy
from dataclasses import replace

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_input_capture import _TEXT
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_outcome_delivery import acknowledged_parts
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryProjection
from app.telegram_bots.work_request_source import _RequestProjection, same_displayed_basis
from app.telegram_bots.work_untied_delivery import WorkAnswerNotice, answer_data, answer_input, answer_keyboard, answer_parts, answer_source_group, answer_source_input, callback_facts, notice_recipes, same_input
from app.telegram_bots.work_untied_source import WorkUntiedSource
from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkUntiedInputs:
    def __init__(self, services, controls):
        self.services, self.controls = services, controls
        self.plans, self.callbacks = controls.plans, services.callbacks
        self.inbox, self.locator = services.outbox.inbox, WorkUntiedSource(self.plans)
        self.recovery = WorkRequestRecovery(self.plans)
        self.notice = WorkAnswerNotice(self)

    async def offer(self, link, origin, *, guard):
        return await await_drained(self._offer(link, origin, guard=guard))

    async def _offer(self, link, origin, *, guard):
        async with self.callbacks.writer(link, guard) as writer:
            mark = (await self.inbox._rows(writer, [origin]))[0]
            self.inbox._current(link, mark)
            if mark.route_kind is not None:
                return mark.route_basis.get("work_answer", {}).get("notice_id")
            found = await self.locator.find(link, origin, guard=guard, session=writer)
            if found is None:
                return None
            text = _TEXT.validate_python(mark.sanitized_input["text"])
            data = {
                "version": 1,
                "source_outbox_id": found.outbox_id,
                "presentation_version": found.presentation_version,
                "group_id": found.group_id,
                "request_id": found.source.request["id"],
                "work_id": found.source.work["id"],
                "seen": found.source.seen,
                "text": text,
            }
            await self.inbox.register_delivery([InboxRow.from_row(mark)], outbox_id=found.outbox_id, primary=False, finish=False, session=writer)
            await self.inbox.route(InboxRow.from_row(mark), kind="work_answer", target_id=data["request_id"], basis={"work_answer": data}, operation_ids=[], session=writer)
            return await self.publish_choice(link, mark, data, guard=guard, writer=writer)

    async def publish_choice(self, link, mark, data, *, guard, writer):
        identity = await self.services.outbox.plan_product_keyboard(
            link,
            source_id="work-answer:" + str(mark.message_id),
            parts=answer_parts(data, mark.message_id),
            keyboard=answer_keyboard(data, mark.message_id),
            callbacks=self.callbacks,
            guard=guard,
            inbox_rows=[InboxRow.from_row(mark)],
            finish_route=False,
            message_thread_id=mark.message_thread_id,
            session=writer,
        )
        mark.route_basis = {**mark.route_basis, "work_answer": {**data, "notice_id": identity}}
        await writer.flush()
        return identity

    async def handle(self, channel, update, *, link, replay_only=False):
        async def perform():
            message = update.message
            guard = self.services.commands._guard(channel, link)
            async with self.callbacks.writer(link, guard) as writer:
                mark = await writer.get(TelegramInboundMarkRow, (link.id, message.message_id, message.chat_id))
                if mark is not None and "work_answer" in mark.route_basis:
                    # The persisted classification wins over a later prompt.
                    saved = InboxRow.from_row(mark)
                else:
                    saved = None
                if saved is None and (mark is not None and mark.route_kind is not None or replay_only):
                    return False
            if saved is not None:
                return await self.resume(channel, link, saved, guard=guard)
            if message.reply_to_message is not None:
                return False
            origin, _ = await self.inbox.accept(link, update, sanitized_input={"type": "text", "text": message.text})
            return await self.offer(link, origin, guard=guard) is not None

        return await await_drained(perform())

    async def execute_callback(self, channel, row, *, update_id):
        if row.kind not in {"answer_yes", "answer_no", "voice_save_answer", "voice_discard"}:
            return False

        async def perform():
            link = deepcopy(channel.link)
            guard = self.services.commands._guard(channel, link)
            async with self.callbacks.writer(link, guard) as writer:
                parent = await writer.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id"))
                origins = parent.payload.get("product_basis", {}).get("origins") if parent else None
                if type(origins) is not list or len(origins) != 1 or len(origins[0]) != 3 or origins[0][0] != link.id or origins[0][2] != link.chat_id:
                    return False
                mark = await writer.get(TelegramInboundMarkRow, tuple(origins[0]))
                if mark is None or mark.route_basis.get("work_answer", {}).get("notice_id") != parent.id:
                    return False
                answer_data(mark)
                origin = InboxRow.from_row(mark)
            if row.kind == "voice_discard":
                await self.discard_voice(link, origin, row, guard=guard)
                return True
            if row.kind == "answer_no":
                async with self.callbacks.writer(link, guard) as writer:
                    group = await self.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                    mark = (await self.inbox._rows(writer, [origin]))[0]
                    self.inbox._current(link, mark)
                    if any("work_action_id" in member.payload for member in group.rows):
                        raise CallbackRefused()
                    if not any("publication_owner" in member.payload for member in group.rows):
                        if mark.route_kind != "work_answer" or mark.route_complete:
                            raise CallbackRefused()
                        mark.route_kind, mark.route_target_id = "turn", link.agent_ref
                        mark.route_basis = {**mark.route_basis, "input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}
                        await writer.flush()
                    stage = await self.inbox.stage_held_message(
                        link, kind="answer_no", callback_id=row.id, update_id=update_id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, callbacks=self.callbacks, guard=guard, session=writer
                    )
                await self.services.turns.accept_staged(channel, stage)
                return True
            try:
                staged = await self.stage(link, origin, row, guard=guard)
            except ProductSourceRetired:
                await self._closed(channel, link, origin, row, guard=guard)
                return True
            if staged.attempts == 0:
                expected = {"id": staged.id, "kind": staged.kind, "body": staged.body, "attempts": staged.attempts}
                result = await self.controls.acts.check(staged.id, expected=expected, attempt_current=lambda: channel._callback_current(link))
            else:
                result = await self.controls.acts.reconcile_receipt(staged.id)
            await self.services.work_callbacks._observe(link, result.id, guard=guard)
            return True

        return await await_drained(perform())

    async def discard_voice(self, link, origin, row, *, guard):
        async with self.callbacks.writer(link, guard) as writer:
            group = await self.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            mark = (await self.inbox._rows(writer, [origin]))[0]
            self.inbox._current(link, mark)
            data = answer_data(mark)
            if data["version"] != 2 or data["notice_id"] != group.anchor_outbox_id or mark.route_kind != "work_answer" or mark.route_complete or mark.operation_ids or any("work_action_id" in member.payload for member in group.rows):
                raise CallbackRefused()
            await self.services.acts.finish_voice_note(link, message_id=mark.message_id, duration=data["original"]["voice"]["duration"], used_as="discarded", session=writer)
            await self.callbacks.consume_group(group, session=writer)
            for member in group.rows:
                saved = await writer.get(TelegramCallbackRow, member.id)
                saved.payload = {key: value for key, value in saved.payload.items() if key != "transcript"}
            parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
            parent.payload = {**parent.payload, "product_keyboard_basis": answer_keyboard(data, mark.message_id, cleared=True)}
            await self.services.outbox.plan_product_keyboard(
                link,
                source_id="work-voice-discard:" + row.id,
                parts=[{"kind": "edit", "source_ref": {"key": "voice.discarded", "values": {}}, "target_message_id": row.message_id}],
                keyboard=[],
                callbacks=self.callbacks,
                guard=guard,
                inbox_rows=[InboxRow.from_row(mark)],
                finish_route=True,
                message_thread_id=mark.message_thread_id,
                session=writer,
            )

    async def resume(self, channel, link, origin, *, guard):
        return await await_drained(self._resume(channel, link, origin, guard=guard))

    async def _resume(self, channel, link, origin, *, guard):
        async with self.callbacks.writer(link, guard) as writer:
            mark = (await self.inbox._rows(writer, [origin]))[0]
            self.inbox._current(link, mark)
            if "work_answer" not in mark.route_basis:
                return False
            if mark.route_kind == "work_answer" or mark.route_complete:
                return True
            if mark.route_kind != "work_input" or len(mark.operation_ids) != 1:
                return True
            row = await writer.get(TelegramCallbackRow, mark.operation_ids[0])
            action = await writer.get(TelegramActRow, mark.operation_ids[0])
            if row is None or action is None:
                return True
            captured = InboxRow.from_row(mark)
        # Stored input is no new gesture, even when an admitted Act has not
        # reached its first attempt. The worker owns that attempt.
        await self.stage(link, captured, row, guard=guard)
        result = await self.controls.acts.reconcile_receipt(row.id)
        await self.services.work_callbacks._observe(link, result.id, guard=guard)
        return True

    async def _closed(self, channel, link, origin, row, *, guard):
        async with self.callbacks.writer(link, guard) as writer:
            group = await self.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            mark = (await self.inbox._rows(writer, [origin]))[0]
            self.inbox._current(link, mark)
            if mark.route_kind != "work_answer" or mark.route_complete or any("work_action_id" in member.payload or "publication_owner" in member.payload for member in group.rows):
                raise CallbackRefused()
            data = answer_data(mark)
            if data["notice_id"] != group.anchor_outbox_id:
                raise CallbackRefused()
            mark.route_kind, mark.route_target_id = "work_card_hold", data["source_outbox_id"]
            mark.route_basis = {**{key: value for key, value in mark.route_basis.items() if key != "work_answer"}, "source_outbox_id": data["source_outbox_id"]}
            await self.callbacks.consume_group(group, session=writer)
        await self.services.work_callbacks.closed.offer(link, origin, guard=guard)

    async def stage(self, link, origin, row, *, guard):
        return await await_drained(self._stage(link, origin, row, guard=guard))

    async def _stage(self, link, origin, row, *, guard):
        async with self.plans.outbox.sessions() as reader:
            existing = await reader.get(TelegramActRow, row.id)
            mark = (await self.inbox._rows(reader, [origin]))[0]
            captured = InboxRow.from_row(mark)
            data = answer_data(captured)
            parent = await reader.get(TelegramOutboxRow, data["source_outbox_id"])
            source = acknowledged_source(link, parent)
        await self.controls.acts._work_link(link)
        owner = await self.controls.acts.authority.prepare_live_owner(link.owner_user_id)
        if existing is None:
            current, _ = await self.recovery._source(link, data["request_id"])
            if not same_displayed_basis(current, source):
                raise ProductSourceRetired()
        admission = await prepare_attention(self.plans.reader, link)
        composed = _AnswerGuard(guard, admission, link, source, captured, row.id, callbacks=self.callbacks, fresh=existing is None)
        async with self.callbacks.writer(link, composed) as writer:
            if existing is None:
                group = await self.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=composed, session=writer)
                if any("publication_owner" in member.payload or "work_action_id" in member.payload for member in group.rows):
                    raise CallbackRefused()
            else:
                saved = await writer.get(TelegramActRow, row.id, with_for_update=True, populate_existing=True)
                if saved is None:
                    raise CallbackRefused()
                _parent, rows, selected = await self.callbacks._locked(writer, link, row.id)
                if (selected.chat_id, selected.message_id, selected.message_thread_id) != (row.chat_id, row.message_id, row.message_thread_id):
                    raise CallbackRefused()
            request = Respond(
                operation_id=row.id,
                expected_request_revision=data["seen"]["request_revision"],
                expected_assignment_revision=data["seen"]["assignment_revision"],
                text=data["text"],
                disposition=data["purpose"] if data.get("purpose") in {"cannot_provide", "wrong_recipient"} else "supplied",
            )
            result = await self.controls.acts._stage_work_act(link, kind="respond", request_id=data["request_id"], work_id=data["work_id"], request=request, owner=owner, session=writer)
            mark = (await self.inbox._rows(writer, [origin]))[0]
            if existing is None:
                mark.route_kind, mark.route_target_id = "work_input", data["request_id"]
                mark.operation_ids = [row.id]
                mark.route_basis = {
                    **mark.route_basis,
                    "work_input": {"version": 3, "work_id": data["work_id"], "request_id": data["request_id"], "purpose": "answer" if source.request["purpose"] == "information" else "comment", "text": data["text"], "seen": data["seen"]},
                    "work_action_id": row.id,
                }
                for member in group.rows:
                    control = await writer.get(TelegramCallbackRow, member.id)
                    control.payload = {**control.payload, "work_action_id": result.id}
                if data.get("prompt_id") is not None:
                    prompt = await writer.get(TelegramCallbackRow, data["prompt_id"])
                    prompt.consumed_at, prompt.closed_reason = self.callbacks.clock(), "answered"
                    prompt.payload = {**prompt.payload, "work_action_id": result.id}
            composed.captured = (
                replace(
                    captured,
                    route_kind="work_input",
                    route_target_id=data["request_id"],
                    operation_ids=[row.id],
                    route_basis={
                        **captured.route_basis,
                        "work_input": {
                            "version": 3,
                            "work_id": data["work_id"],
                            "request_id": data["request_id"],
                            "purpose": "answer" if source.request["purpose"] == "information" else "comment",
                            "text": data["text"],
                            "seen": data["seen"],
                        },
                        "work_action_id": row.id,
                    },
                )
                if existing is None
                else captured
            )
            composed.action = result
            return result


class _AnswerGuard:
    def __init__(self, base, admission, link, source, captured, identity, *, callbacks, fresh):
        self.base, self.admission, self.link, self.source = base, admission, link, source
        self.captured, self.identity, self.fresh, self.action = captured, identity, fresh, None
        self.callbacks = callbacks

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        held, _ = await self.admission(session=writer, projection=_AnswerProjection(self))
        if "agents:write" not in held:
            raise AgentDenied("The answer is unavailable")
        self.base.current()
        return current


class _AnswerProjection(_RecoveryProjection):
    def __init__(self, guard):
        super().__init__(guard.source, allowed_act_id=guard.identity)
        self.guard = guard

    def extend(self, query):
        g, mark, parent, notice, controls, act = self.guard, TelegramInboundMarkRow, TelegramOutboxRow, aliased(TelegramOutboxRow), TelegramCallbackRow, TelegramActRow
        data = g.captured.route_basis["work_answer"]
        chosen = aliased(TelegramCallbackRow)
        choice_group = select(chosen.group_id).where(chosen.id == g.identity).scalar_subquery()
        facts = callback_facts(g.callbacks.sessions, or_(controls.group_id == data["group_id"], controls.group_id == choice_group))
        return (
            super()
            .extend(query)
            .add_columns(mark, parent, notice, facts, act)
            .join(mark, and_(mark.link_id == g.captured.link_id, mark.chat_id == g.captured.chat_id, mark.message_id == g.captured.message_id))
            .join(parent, parent.id == data["source_outbox_id"])
            .join(notice, notice.id == data["notice_id"])
            .outerjoin(act, act.id == g.identity)
        )

    def check(self, rows):
        g, expected = self.guard, self.guard.captured
        if g.fresh:
            super().check(rows)
        mark, parent, notice, _, act = rows[0][11:16]
        data = answer_data(expected)
        if g.fresh and (mark.route_complete or mark.state == "done"):
            raise CallbackRefused()
        controls = _RequestProjection.objects(rows[0][14])
        original = [row for row in controls if row.group_id == data["group_id"]]
        selected = next((row for row in controls if row.id == g.identity), None)
        if selected is None or selected.kind != ("voice_save_answer" if data["version"] == 2 else "answer_yes") or selected.target_kind != "request" or selected.target_id != data["request_id"]:
            raise CallbackRefused()
        choices = [row for row in controls if row.group_id == selected.group_id]
        answer_source_group(parent, original, data)
        _validate_rows(notice, choices)
        if parent.payload["presentation_version"] != data["presentation_version"] or not same_displayed_basis(acknowledged_source(g.link, parent), g.source):
            raise CallbackRefused()
        if any("publication_owner" in row.payload for row in choices) or any(row.payload.get("work_action_id") not in {None, g.identity} for row in choices):
            raise CallbackRefused()
        if any(row.payload.get("work_action_id") != g.identity for row in choices) and act is not None:
            raise CallbackRefused()
        if g.fresh:
            owns_prompt = data.get("prompt_id") is not None and g.action is not None and all(row.consumed_at is not None and row.closed_reason == "answered" and row.payload.get("work_action_id") == g.identity for row in original)
            if not owns_prompt and any(row.consumed_at is not None or "work_action_id" in row.payload for row in original):
                raise ProductSourceRetired()
            try:
                for row in [] if owns_prompt else original:
                    g.callbacks._usable(row)
            except CallbackRefused:
                raise ProductSourceRetired() from None
            for row in choices:
                g.callbacks._usable(row)
            answer_source_input(parent, mark, data)
        basis = notice.payload.get("product_basis", {})
        if (
            notice.state != "sent"
            or basis.get("origins") != [[mark.link_id, mark.message_id, mark.chat_id]]
            or basis.get("finish_route") is not False
            or not basis.get("parts")
            or not same_body(basis["parts"], answer_parts(data, mark.message_id))
        ):
            raise CallbackRefused()
        if any(type(part.get("protect_content")) is not bool for part in notice.parts):
            raise CallbackRefused()
        acknowledged_parts(notice, notice_recipes(notice, data, mark.message_id))
        if any(row.message_id != notice.parts[-1]["message_ids"][0] for row in choices):
            raise CallbackRefused()
        if not same_input(mark, expected):
            raise CallbackRefused()
        if mark.route_kind not in {"work_answer", "work_input"} or mark.message_thread_id != notice.message_thread_id or mark.message_thread_id != parent.message_thread_id or not answer_input(mark, data):
            raise CallbackRefused()
        if (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id) != (g.link.id, g.link.owner_user_id, g.link.binding_generation, g.link.chat_id):
            raise CallbackRefused()
        if not same_body(data["seen"], g.source.seen) or (data["request_id"], data["work_id"]) != (g.source.request["id"], g.source.work["id"]):
            raise CallbackRefused()
        cleared = data["version"] == 2 and not g.fresh and act is not None and act.state == "done" and "transcript" not in selected.payload and all(row.consumed_at is not None and row.closed_reason == "work-done" for row in choices)
        if not same_body(notice.payload.get("product_keyboard_basis"), answer_keyboard(data, mark.message_id, cleared=cleared)):
            raise CallbackRefused()
        if "work_action_id" in mark.route_basis and (act is None or mark.route_basis["work_action_id"] != g.identity or mark.operation_ids != [g.identity]):
            raise CallbackRefused()
        if g.action is not None and (act is None or any(getattr(act, key) != getattr(g.action, key) for key in _ACT_SCOPE) or not same_body(act.body, g.action.body)):
            raise CallbackRefused()
