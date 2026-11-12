"""Explicit recovery choices for retained replies to closed Work presentations."""

from html import escape
from uuid import uuid4

from app.telegram_bots.callbacks import CallbackRefused, Prompts
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_request_recovery import WorkRequestRecovery
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def closed_parts(mark, *, available):
    parts = []
    if "voice_note" in mark.route_basis:
        parts.extend({"kind": "text", "source_ref": {"key": "voice.transcript_part", "values": {"text": visible_text(chunk)}}, "reply_to_message_id": mark.message_id} for chunk in split_html(escape(mark.sanitized_input["text"])))
    refusal = "prompt.unavailable" if not available else "voice.answer_too_long" if mark.route_basis.get("work_voice_refusal") == "too_long" else "prompt.closed"
    parts.append({"kind": "text", "source_ref": {"key": refusal, "values": {}}, "reply_to_message_id": mark.message_id})
    return parts


def validate_closed_voice(parent, mark):
    if "voice_note" not in mark.route_basis:
        return
    from app.telegram_bots.work_actions import same_body
    from app.telegram_bots.work_outcome_delivery import acknowledged_parts

    basis = parent.payload.get("product_basis", {})
    parts = basis.get("parts", [])
    if not parts or parent.state != "sent" or not same_body(parts, closed_parts(mark, available=parts[-1].get("source_ref", {}).get("key") != "prompt.unavailable")):
        raise CallbackRefused()
    if len(parts) != len(parent.parts) or any(type(part.get("protect_content")) is not bool for part in parent.parts):
        raise CallbackRefused()
    recipes = [{**part, "protect_content": parent.parts[index]["protect_content"], **({"stored_keyboard": True} if index == len(parts) - 1 else {})} for index, part in enumerate(parts)]
    acknowledged_parts(parent, recipes)


class WorkClosedInputs:
    def __init__(self, services, controls):
        self.services, self.controls = services, controls
        self.plans, self.callbacks = controls.plans, services.callbacks
        self.inbox, self.prompts = services.outbox.inbox, Prompts(services.callbacks)
        self.recovery = WorkRequestRecovery(self.plans)

    async def offer(self, link, origin, *, guard):
        async def perform():
            async with self.callbacks.writer(link, guard) as writer:
                mark = (await self.inbox._rows(writer, [origin]))[0]
                self.inbox._current(link, mark)
                if mark.route_kind != "work_card_hold" or mark.route_complete:
                    return None
                if mark.route_basis.get("work_closed") is not None:
                    return mark.route_basis["work_closed"]["notice_id"]
                source_id = mark.route_basis["source_outbox_id"]
                parent = await writer.get(TelegramOutboxRow, source_id)
                if parent is None:
                    request_id = work_id = prompt_id = None
                else:
                    if (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.message_thread_id) != (link.id, link.owner_user_id, link.binding_generation, mark.chat_id, mark.message_thread_id):
                        raise CallbackRefused()
                    request_id, work_id = parent.request_id, parent.work_id
                    prompt_id = parent.payload.get("work_request_basis", {}).get("prompt_id")
            available = False
            if request_id is not None:
                try:
                    current, _settings = await self.recovery._source(link, request_id)
                    available = current.work["id"] == work_id
                except (AgentDenied, ProductSourceRetired):
                    pass
            async with self.callbacks.writer(link, guard) as writer:
                mark = (await self.inbox._rows(writer, [origin]))[0]
                self.inbox._current(link, mark)
                if mark.route_kind != "work_card_hold" or mark.route_complete or mark.route_basis.get("source_outbox_id") != source_id:
                    raise CallbackRefused()
                if mark.route_basis.get("work_closed") is not None:
                    return mark.route_basis["work_closed"]["notice_id"]
                prompt = await writer.get(TelegramCallbackRow, prompt_id) if prompt_id else None
                if prompt is None:
                    # A detached, already-closed recovery context is not a new
                    # typing prompt and must not replace another open prompt.
                    prompt_id = uuid4().hex
                    prompt = TelegramCallbackRow(
                        id=prompt_id,
                        group_id=uuid4().hex,
                        link_id=link.id,
                        chat_id=mark.chat_id,
                        binding_generation=link.binding_generation,
                        message_id=None,
                        message_thread_id=mark.message_thread_id,
                        kind="prompt",
                        target_kind=None,
                        target_id=None,
                        payload={"purpose": "retired_presentation", "anchor_outbox_id": source_id, "work_request_id": request_id},
                        created_at=self.callbacks.clock(),
                        consumed_at=self.callbacks.clock(),
                        closed_reason="answered",
                    )
                    writer.add(prompt)
                    await writer.flush()
                elif (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id, prompt.kind) != (link.id, mark.chat_id, link.binding_generation, mark.message_thread_id, "prompt"):
                    raise CallbackRefused()
                choices = [("prompt_send_new", "label.send_new"), ("prompt_discard", "label.discard")]
                if available:
                    choices.insert(0, ("prompt_reopen", "label.reopen_answer"))
                payload = {"prompt_id": prompt_id, "message_id": mark.message_id}
                keyboard = [[{"kind": kind, "label": label, "target_kind": "request" if request_id else None, "target_id": request_id, "payload": payload}] for kind, label in choices]
                notice_id = await self.services.outbox.plan_product_keyboard(
                    link,
                    source_id="work-closed:" + source_id + "." + str(mark.message_id),
                    parts=closed_parts(mark, available=available),
                    keyboard=keyboard,
                    callbacks=self.callbacks,
                    guard=guard,
                    inbox_rows=[InboxRow.from_row(mark)],
                    finish_route=False,
                    message_thread_id=mark.message_thread_id,
                    session=writer,
                )
                mark = await writer.get(TelegramInboundMarkRow, (link.id, mark.message_id, mark.chat_id), populate_existing=True)
                mark.route_basis = {
                    **mark.route_basis,
                    "work_closed": {"version": 1, "source_outbox_id": source_id, "request_id": request_id, "work_id": work_id, "prompt_id": prompt_id, "notice_id": notice_id},
                }
                await writer.flush()
                return notice_id

        return await await_drained(perform())

    async def unavailable(self, link, row, *, guard):
        """Replace the actual Reopen choice after a known unavailable target."""
        async with self.callbacks.writer(link, guard) as writer:
            group = await self.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            selected = group.selected
            mark = await writer.get(TelegramInboundMarkRow, (link.id, selected.payload.get("message_id"), link.chat_id), with_for_update=True, populate_existing=True)
            if mark is None:
                raise CallbackRefused()
            self.inbox._current(link, mark)
            closed = mark.route_basis.get("work_closed", {})
            if (
                selected.kind != "prompt_reopen"
                or mark.route_kind != "work_card_hold"
                or mark.route_complete
                or mark.message_thread_id != selected.message_thread_id
                or closed.get("notice_id") != group.anchor_outbox_id
                or closed.get("prompt_id") != selected.payload.get("prompt_id")
                or any("publication_owner" in member.payload for member in group.rows)
            ):
                raise CallbackRefused()
            await self.callbacks.consume_group(group, session=writer)
            payload = {"prompt_id": closed["prompt_id"], "message_id": mark.message_id}
            notice_id = await self.services.outbox.plan_product_keyboard(
                link,
                source_id="work-closed-unavailable:" + selected.id,
                parts=closed_parts(mark, available=False) if "voice_note" in mark.route_basis else [{"kind": "edit", "source_ref": {"key": "prompt.unavailable", "values": {}}, "target_message_id": selected.message_id}],
                keyboard=[[{"kind": kind, "label": label, "target_kind": "request", "target_id": closed["request_id"], "payload": payload}] for kind, label in (("prompt_send_new", "label.send_new"), ("prompt_discard", "label.discard"))],
                callbacks=self.callbacks,
                guard=guard,
                inbox_rows=[InboxRow.from_row(mark)],
                finish_route=False,
                message_thread_id=mark.message_thread_id,
                session=writer,
            )
            mark = await writer.get(TelegramInboundMarkRow, (link.id, mark.message_id, mark.chat_id), populate_existing=True)
            mark.route_basis = {**mark.route_basis, "work_closed": {**closed, "notice_id": notice_id}}
            await writer.flush()
            return notice_id

    async def execute_callback(self, channel, row, *, update_id):
        from copy import deepcopy

        if row.kind not in {"prompt_reopen", "prompt_send_new", "prompt_discard"}:
            return False

        async def perform():
            link = deepcopy(channel.link)
            guard = self.services.commands._guard(channel, link)
            async with self.callbacks.writer(link, guard) as writer:
                mark = await writer.get(TelegramInboundMarkRow, (link.id, row.payload.get("message_id"), link.chat_id))
                if mark is None or "work_closed" not in mark.route_basis:
                    return False
                self.inbox._current(link, mark)
                parent = await writer.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id"))
                if parent is None:
                    raise CallbackRefused()
                validate_closed_voice(parent, mark)
                origin = InboxRow.from_row(mark)
            if row.kind == "prompt_reopen":
                from app.telegram_bots.work_reopen import WorkReopen

                await WorkReopen(self).open(channel, link, row, origin, guard=guard)
                return True
            stage = None
            async with self.callbacks.writer(link, guard) as writer:
                group = await self.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                selected = group.selected
                mark = await writer.get(TelegramInboundMarkRow, (link.id, selected.payload.get("message_id"), link.chat_id), with_for_update=True, populate_existing=True)
                if mark is None:
                    raise CallbackRefused()
                self.inbox._current(link, mark)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
                validate_closed_voice(parent, mark)
                closed = mark.route_basis.get("work_closed", {})
                if (
                    mark.message_thread_id != selected.message_thread_id
                    or closed.get("notice_id") != group.anchor_outbox_id
                    or closed.get("prompt_id") != selected.payload.get("prompt_id")
                    or mark.route_kind not in {"work_card_hold", "turn"}
                    or mark.route_complete
                    and "publication_owner" not in selected.payload
                ):
                    raise CallbackRefused()
                publishing = [member for member in group.rows if "publication_owner" in member.payload]
                if publishing and (selected.kind != "prompt_send_new" or not any(member.id == selected.id for member in publishing)):
                    raise CallbackRefused()
                if "voice_note" in mark.route_basis:
                    await self.services.acts.finish_voice_note(
                        link,
                        message_id=mark.message_id,
                        duration=mark.route_basis["voice_note"]["original"]["voice"]["duration"],
                        used_as="message" if selected.kind == "prompt_send_new" else "discarded",
                        session=writer,
                    )
                if selected.kind == "prompt_send_new":
                    if not publishing:
                        if mark.route_kind != "work_card_hold":
                            raise CallbackRefused()
                        # This explicit choice, in the same writer as its stable
                        # callback publication owner, changes the input route.
                        mark.route_kind, mark.route_target_id = "turn", link.agent_ref
                        mark.route_basis = {**mark.route_basis, "input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}
                        await writer.flush()
                    stage = await self.inbox.stage_prompt_message(
                        link, callback_id=selected.id, update_id=update_id, chat_id=selected.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, callbacks=self.callbacks, guard=guard, session=writer
                    )
                    await self.prompts.close(link, link.chat_id, selected.message_thread_id, reason="navigation", guard=guard, session=writer)
                elif selected.kind == "prompt_discard":
                    await self.callbacks.consume_group(group, session=writer)
                    prompt = await writer.get(TelegramCallbackRow, closed["prompt_id"], with_for_update=True, populate_existing=True)
                    if prompt is not None:
                        if (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id, prompt.kind) != (link.id, mark.chat_id, link.binding_generation, mark.message_thread_id, "prompt"):
                            raise CallbackRefused()
                        prompt.consumed_at, prompt.closed_reason = self.callbacks.clock(), "discarded"
                    await self.services.outbox.plan_product_keyboard(
                        link,
                        source_id="work-closed-discard:" + selected.id,
                        parts=[{"kind": "edit", "source_ref": {"key": "prompt.discarded", "values": {}}, "target_message_id": selected.message_id}],
                        keyboard=[],
                        callbacks=self.callbacks,
                        guard=guard,
                        inbox_rows=[InboxRow.from_row(mark)],
                        finish_route=True,
                        message_thread_id=mark.message_thread_id,
                        session=writer,
                    )
                else:
                    raise CallbackRefused("stale.not_waiting")
            if stage is not None:
                guard.current()
                await self.services.turns.accept_staged(channel, stage)
            return True

        return await await_drained(perform())
