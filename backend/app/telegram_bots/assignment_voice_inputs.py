"""Freeze assignment voice prompts before transcription and reuse private drafts."""

from dataclasses import replace

from sqlalchemy import select

from app.telegram_bots.assignment_typing import shown_prompt
from app.telegram_bots.callbacks import CallbackRefused, Prompts, _aware
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_view_origins import anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

PURPOSES = frozenset({"assign_words", "assign_done", "assign_edit"})


def voice_basis(mark, *, owner="assignment"):
    value = thaw(mark.route_basis.get("work_voice"))
    if not isinstance(value, dict) or value.get("owner") != owner:
        return None
    if (
        set(value) != {"version", "owner", "held", "phase", "source_outbox_id", "prompt_id", "signature", "original"}
        or type(value["version"]) is not int
        or value["version"] != 3
        or value["held"] is not True
        or value["phase"] not in {"captured", "ready", "closed"}
    ):
        raise ProductSourceRetired()
    return value


def check_voice_prompt(link, mark, prompt, parent, *, ready=False, owner="assignment", purposes=PURPOSES):
    value = voice_basis(mark, owner=owner)
    if value is None or value["phase"] != ("ready" if ready else "captured") or prompt is None or parent is None:
        raise ProductSourceRetired()
    if (
        (parent.id, prompt.id) != (value["source_outbox_id"], value["prompt_id"])
        or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.message_thread_id) != (link.id, link.owner_user_id, link.binding_generation, mark.chat_id, mark.message_thread_id)
        or (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != (link.id, mark.chat_id, link.binding_generation, mark.message_thread_id)
        or prompt.payload.get("purpose") not in purposes
        or anchor_signature(_item(parent)) != value["signature"]
        or ready
        and not same_body(mark.route_basis.get("voice_note", {}).get("original"), value["original"])
        or not ready
        and not same_body(mark.sanitized_input, value["original"])
    ):
        raise ProductSourceRetired()
    acknowledged, messages = shown_prompt(_item(parent), prompt, accepted_at=mark.accepted_at)
    quoted = value["original"].get("reply_to_message_id")
    if quoted is not None and (type(quoted) is not int or quoted not in messages) or quoted is None and (_aware(mark.accepted_at) < acknowledged or mark.message_id <= prompt.message_id):
        raise ProductSourceRetired()
    return acknowledged, messages


class AssignmentVoiceInputs:
    owner = "assignment"
    purposes = PURPOSES
    intent_key = "assignment_prompt_intent"

    def _basis(self, mark):
        return voice_basis(mark, owner=self.owner)

    def _check(self, link, mark, prompt, parent, *, ready=False):
        return check_voice_prompt(link, mark, prompt, parent, ready=ready, owner=self.owner, purposes=self.purposes)

    def __init__(self, inputs, *, guard_factory=None):
        self.inputs = inputs
        self.outbox, self.callbacks = inputs.outbox, inputs.callbacks
        self.guard_factory = guard_factory

    async def quoted_sources(self, link, message_id, topic):
        from app.telegram_bots.assignment_prompt_owners import quoted_assignment_sources

        return await quoted_assignment_sources(self.outbox, link, message_id, topic, intents=(self.intent_key,))

    async def capture(self, channel, link, origin, *, forwarded, forward_origin, guard):
        if forwarded or link.agent_kind != "instance":
            return None
        quote = origin.sanitized_input.get("reply_to_message_id")

        async def perform():
            from app.telegram_bots.assignment_drafts import origin_facts

            sources = await self.quoted_sources(link, quote, origin.message_thread_id) if quote is not None else []
            async with self.callbacks.writer(link, guard) as writer:
                mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
                self.outbox.inbox._current(link, mark)
                if not same_body(origin_facts(InboxRow.from_row(mark)), origin_facts(origin)):
                    raise ProductSourceRetired()
                if mark.route_kind is not None:
                    return InboxRow.from_row(mark) if self._basis(mark) is not None else None
                parent = None
                if sources:
                    parent = await writer.get(TelegramOutboxRow, sources[0])
                    intent = parent.payload[self.intent_key] if parent is not None else {}
                    prompt = await writer.get(TelegramCallbackRow, intent["prompt_id"]) if type(intent.get("prompt_id")) is str else None
                elif quote is None:
                    selected = await Prompts(self.callbacks).current(link, mark.chat_id, mark.message_thread_id, guard=guard, session=writer)
                    prompt = await writer.get(TelegramCallbackRow, selected.id) if selected is not None else None
                else:
                    candidates = list(
                        await writer.scalars(
                            select(TelegramCallbackRow).where(
                                TelegramCallbackRow.link_id == link.id,
                                TelegramCallbackRow.chat_id == mark.chat_id,
                                TelegramCallbackRow.binding_generation == link.binding_generation,
                                TelegramCallbackRow.message_thread_id == mark.message_thread_id,
                                TelegramCallbackRow.kind == "prompt",
                                TelegramCallbackRow.message_id == quote,
                            )
                        )
                    )
                    prompt = candidates[0] if len(candidates) == 1 else None
                if parent is None and prompt is not None:
                    parent = await writer.get(TelegramOutboxRow, prompt.payload.get("anchor_outbox_id"))
                intent = parent.payload.get(self.intent_key, {}) if parent is not None else {}
                if not sources and intent.get("purpose") not in self.purposes and (prompt is None or prompt.payload.get("purpose") not in self.purposes):
                    return None
                source_id = sources[0] if sources else prompt.payload["anchor_outbox_id"]
                prompt_id = intent.get("prompt_id") or prompt.id
                value = {
                    "version": 3,
                    "owner": self.owner,
                    # Older transcript consumers recognize this as a held Work
                    # input and cannot turn it into an ordinary instruction.
                    "held": True,
                    "phase": "captured" if len(sources) <= 1 else "closed",
                    "source_outbox_id": source_id,
                    "prompt_id": prompt_id,
                    "signature": anchor_signature(_item(parent)) if parent is not None else None,
                    "original": thaw(mark.sanitized_input),
                }
                mark.route_basis = {**mark.route_basis, "work_voice": value}
                try:
                    self._check(link, mark, prompt, parent)
                    self.callbacks._usable(prompt)
                except (CallbackRefused, ProductSourceRetired, KeyError, TypeError, ValueError):
                    value = {**value, "phase": "closed"}
                mark.route_basis = {**mark.route_basis, "work_voice": value}
                captured = InboxRow.from_row(mark)
                routed = await self.outbox.inbox.route(captured, kind="voice", target_id="working", basis={"forwarded": False, "origin": forward_origin, "work_voice": value}, operation_ids=[], session=writer)
                if parent is not None and (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.message_thread_id) == (
                    link.id,
                    link.owner_user_id,
                    link.binding_generation,
                    mark.chat_id,
                    mark.message_thread_id,
                ):
                    await self.outbox.inbox.register_delivery([routed], outbox_id=parent.id, primary=False, finish=False, session=writer)
                return routed

        return await await_drained(perform())

    async def complete(self, link, origin, *, guard, session, resume=False):
        mark = (await self.outbox.inbox._rows(session, [origin]))[0]
        value = self._basis(mark)
        if value is None or mark.route_kind != "turn" or mark.operation_ids or mark.route_complete:
            raise ProductSourceRetired()
        prompt = await session.get(TelegramCallbackRow, value["prompt_id"])
        parent = await session.get(TelegramOutboxRow, value["source_outbox_id"])
        phase = "closed"
        if value["phase"] == "captured":
            try:
                probe = InboxRow.from_row(mark)
                # The original descriptor was retained by complete_voice in
                # the same writer; the transcript is now the accepted text.
                if not same_body(mark.route_basis.get("voice_note", {}).get("original"), value["original"]):
                    raise ProductSourceRetired()
                before = replace(probe, sanitized_input=value["original"])
                self._check(link, before, prompt, parent)
                self.callbacks._usable(prompt)
                # Commit known speech before the guarded current-prompt read.
                # _ready and private admission both recheck that selection;
                # a temporary failure there must retain this transcript.
                phase = "ready"
            except (CallbackRefused, ProductSourceRetired, KeyError, TypeError, ValueError):
                pass
        mark.route_kind, mark.route_target_id = "work_card_hold", value["source_outbox_id"]
        mark.route_basis = {**mark.route_basis, "work_voice": {**value, "phase": phase}, "source_outbox_id": value["source_outbox_id"]}
        await session.flush()
        captured = InboxRow.from_row(mark)
        return captured

    async def resume(self, channel, origin, *, recovery_guard=None):
        return await await_drained(self._resume(channel, origin, recovery_guard=recovery_guard))

    async def _ready(self, link, origin, guard):
        value = self._basis(origin)
        async with self.callbacks.writer(link, guard) as writer:
            mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
            self.outbox.inbox._current(link, mark)
            current = self._basis(mark)
            same = same_body(current, value) or value["phase"] == "ready" and same_body(current, {**value, "phase": "closed"})
            if not same or not same_body(mark.sanitized_input, origin.sanitized_input):
                raise ProductSourceRetired()
            # An admitted draft owns its consumed prompt. A stale ready snapshot
            # may observe that route but cannot retire or reconstruct it.
            if mark.route_kind == "work_card_hold" and not mark.route_complete and current["phase"] == "ready":
                prompt = await writer.get(TelegramCallbackRow, current["prompt_id"])
                parent = await writer.get(TelegramOutboxRow, current["source_outbox_id"])
                valid = True
                try:
                    self._check(link, mark, prompt, parent, ready=True)
                    self.callbacks._usable(prompt)
                except (CallbackRefused, ProductSourceRetired, KeyError, TypeError, ValueError):
                    valid = False
                if valid:
                    # Guarded selection failures retain custody for recovery;
                    # only its actual result proves that this prompt retired.
                    selected = await Prompts(self.callbacks).current(link, mark.chat_id, mark.message_thread_id, guard=guard, session=writer)
                    valid = selected is not None and selected.id == prompt.id
                if not valid:
                    mark.route_basis = {**mark.route_basis, "work_voice": {**current, "phase": "closed"}}
                await writer.flush()
            return InboxRow.from_row(mark)

    async def _resume(self, channel, origin, *, recovery_guard=None):
        value = self._basis(origin)
        if value is None:
            return False
        link = recovery_guard.captured_link if recovery_guard is not None else channel.link
        guard = recovery_guard or (self.guard_factory(channel, link) if self.guard_factory is not None else channel._services.commands._guard(channel, link))
        origin = await self._ready(link, origin, guard)
        if origin.route_complete:
            return True
        value = self._basis(origin)
        if value["phase"] != "ready" or len(origin.sanitized_input["text"]) > self.outbox.config.assign.max_chars:
            from app.telegram_bots.voice import _echo

            key = "prompt.closed" if value["phase"] != "ready" else "assign.too_long"
            async with self.callbacks.writer(link, guard) as writer:
                mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
                self.outbox.inbox._current(link, mark)
                if not same_body(self._basis(mark), value) or not same_body(mark.sanitized_input, origin.sanitized_input):
                    raise ProductSourceRetired()
                heard = {"kind": "text", "reply_to_message_id": origin.message_id, "source_ref": {"key": "voice.heard", "values": {"text": _echo(origin.sanitized_input["text"])}}}
                # Older captures may already own a separate echo. Reconcile its
                # actual ACK before planning the notice; never repeat that echo.
                legacy = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == f"notice:voice-heard:{origin.chat_id}:{origin.message_id}"))
                if legacy is not None:
                    await self.outbox.plan_product(link, source_id=f"voice-heard:{origin.chat_id}:{origin.message_id}", parts=[heard], inbox_rows=[origin], finish_route=False, session=writer)
                    if legacy.state != "sent":
                        return True
                    from app.telegram_bots.work_outcome_delivery import acknowledged_parts

                    if (
                        legacy.message_thread_id != origin.message_thread_id
                        or legacy.class_ != "reply"
                        or len(legacy.parts) != 1
                        or type(legacy.parts[0].get("protect_content")) is not bool
                        or type(legacy.message_id) is not int
                        or legacy.parts[0].get("message_ids") != [legacy.message_id]
                    ):
                        raise ProductSourceRetired()
                    acknowledged_parts(legacy, [{**heard, "protect_content": legacy.parts[0]["protect_content"]}])
                notice = {"kind": "text", "reply_to_message_id": origin.message_id, "source_ref": {"key": key, "values": {"max": self.outbox.config.assign.max_chars} if key == "assign.too_long" else {}}}
                await self.outbox.plan_product(
                    link,
                    source_id=f"assignment-voice-closed:{origin.chat_id}:{origin.message_id}",
                    parts=[notice] if legacy is not None else [heard, notice],
                    inbox_rows=[origin],
                    finish_route=True,
                    session=writer,
                )
            return True
        await self.inputs.answer(link, origin, guard=guard)
        return True
