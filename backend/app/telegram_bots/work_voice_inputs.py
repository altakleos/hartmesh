"""Capture an actual voice reply before transcription; offer consent afterward."""

from datetime import datetime

from pydantic import ValidationError
from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused, Prompts, _aware, _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_input_capture import _TEXT
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_typing import PURPOSES, validate_shown_prompt
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


class WorkVoiceInputs:
    def __init__(self, inputs):
        self.inputs = inputs
        self.answers = inputs.untied

    async def prepare_complete(self, link, origin):
        """Read optional execution eligibility before the transcript writer."""
        data = origin.route_basis.get("work_voice")
        if not getattr(self.answers.plans.reader, "composites", False) or type(data) is not dict or data.get("held") or data.get("purpose") != "answer":
            return False
        try:
            source, _settings = await self.answers.recovery._source(link, data["request_id"])
            return source.request.get("can_resume") is True
        except Exception:
            # The factual answer keeps its existing source owner. An optional
            # execution control must not turn a read failure into permission.
            return False

    async def capture(self, channel, link, origin, *, forwarded, forward_origin, guard):
        """Freeze the source of a real reply without inventing a text Update."""

        async def perform():
            inbox, callbacks = self.answers.inbox, self.answers.callbacks
            quoted = origin.sanitized_input.get("reply_to_message_id")
            source_id = None
            if not forwarded and link.agent_kind == "instance" and quoted is not None:
                source_id = await self.inputs.cards.find(link, quoted, topic=origin.message_thread_id)
            human = getattr(self.answers.services, "human_inputs", None)
            ordinary_prompt = human is not None and not forwarded and quoted is None and await human.answers_prompt(channel, link, origin)
            async with callbacks.writer(link, guard) as writer:
                mark = (await inbox._rows(writer, [origin]))[0]
                inbox._current(link, mark)
                basis = {"forwarded": forwarded, "origin": forward_origin}
                if not forwarded and link.agent_kind == "instance" and quoted is None:
                    current = await Prompts(callbacks).current(link, mark.chat_id, mark.message_thread_id, guard=guard, session=writer)
                    if current is not None and current.payload.get("purpose") in PURPOSES.values() and Prompts.is_visible(current) and _aware(current.created_at) <= _aware(mark.accepted_at):
                        source_id = current.payload["anchor_outbox_id"]
                    elif source_id is None and not ordinary_prompt:
                        found = await self.answers.locator.find(link, origin, guard=guard, session=writer, voice=True)
                        source_id = found.outbox_id if found else None
                if source_id is not None:
                    parent = await writer.get(TelegramOutboxRow, source_id)
                    try:
                        basis["work_voice"] = await self._context(writer, link, mark, parent, source_id, quoted)
                    except (CallbackRefused, ProductSourceRetired, PermanentDeliveryError, KeyError, TypeError, ValueError, AttributeError):
                        # Recognition already selected Work ownership. Retain
                        # that fact even if its source vanished before capture.
                        basis["work_voice"] = {"version": 1, "source_outbox_id": source_id, "original": mark.sanitized_input, "held": True}
                    if parent is not None and (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.message_thread_id) == (
                        link.id,
                        link.owner_user_id,
                        link.binding_generation,
                        mark.chat_id,
                        mark.message_thread_id,
                    ):
                        await inbox.register_delivery([origin], outbox_id=source_id, primary=False, finish=False, session=writer)
                return await inbox.route(InboxRow.from_row(mark), kind="voice", target_id="working", basis=basis, operation_ids=[], session=writer)

        return await await_drained(perform())

    async def _context(self, writer, link, mark, parent, source_id, quoted):
        prompt = None
        source = acknowledged_source(link, parent)
        source_basis = parent.payload.get("work_request_basis", {})
        if source_basis.get("interaction", {}).get("view") == "prompt":
            prompt = await writer.get(TelegramCallbackRow, source_basis["prompt_id"])
            validate_shown_prompt(_item(parent), prompt)
        parts = [part for part in parent.parts if part["presentation_version"] == parent.payload["presentation_version"]]
        if quoted is not None and quoted not in [value for part in parts for value in part["message_ids"]]:
            raise CallbackRefused()
        for part in parts:
            dispatched = datetime.fromisoformat(part["dispatched_at"])
            if dispatched.utcoffset() is None or dispatched > _aware(mark.accepted_at):
                raise CallbackRefused()
        groups = {prompt.group_id} if prompt else {part.get("callback_group_id") for part in parts} - {None}
        if len(groups) != 1:
            raise CallbackRefused()
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups))))
        if prompt is None:
            _validate_rows(parent, controls)
        return {
            "version": 2,
            "source_outbox_id": source_id,
            "presentation_version": parent.payload["presentation_version"],
            "group_id": next(iter(groups)),
            "request_id": source.request["id"],
            "work_id": source.work["id"],
            "seen": source.seen,
            "original": mark.sanitized_input,
            "prompt_id": prompt.id if prompt else None,
            "purpose": prompt.payload["purpose"] if prompt else "answer" if source.request["purpose"] == "information" else "work_comment",
        }

    async def complete(self, link, origin, *, guard, session, resume=False):
        """The caller atomically stores the transcript and its consent preview."""
        data = origin.route_basis.get("work_voice")
        if data is None:
            return False
        mark = (await self.answers.inbox._rows(session, [origin]))[0]
        self.answers.inbox._current(link, mark)
        if mark.route_kind != "turn" or mark.operation_ids or mark.route_complete:
            raise CallbackRefused()
        if data.get("held") is True:
            mark.route_kind, mark.route_target_id = "work_card_hold", data["source_outbox_id"]
            mark.route_basis = {**mark.route_basis, "source_outbox_id": data["source_outbox_id"]}
            await session.flush()
            return InboxRow.from_row(mark)
        try:
            _TEXT.validate_python(mark.sanitized_input["text"])
        except ValidationError:
            mark.route_kind, mark.route_target_id = "work_card_hold", data["source_outbox_id"]
            mark.route_basis = {**mark.route_basis, "source_outbox_id": data["source_outbox_id"], "work_voice_refusal": "too_long"}
            await session.flush()
            return InboxRow.from_row(mark)
        data = {**data, "text": mark.sanitized_input["text"]}
        controls = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == data["group_id"])))
        try:
            if not controls or any("work_action_id" in control.payload for control in controls):
                raise CallbackRefused()
            for control in controls:
                self.answers.callbacks._usable(control)
        except CallbackRefused:
            mark.route_kind, mark.route_target_id = "work_card_hold", data["source_outbox_id"]
            mark.route_basis = {**mark.route_basis, "source_outbox_id": data["source_outbox_id"]}
            await session.flush()
            return InboxRow.from_row(mark)
        if data["purpose"] in {"decide_own", "decide_note", "request_changes"}:
            # Transfer this actual transcript to the existing prompt-owned
            # confirmation path. It retains its specific intent and never
            # dispatches a Respond leaf or an ordinary conversation turn.
            mark.route_kind = mark.route_target_id = None
            await session.flush()
            return await self.inputs.captures.capture(link, InboxRow.from_row(mark), prompt_id=data["prompt_id"], guard=guard, session=session)
        settings = await session.get(TelegramPersonSettingsRow, link.owner_user_id)
        if settings is None:
            raise CallbackRefused()
        data["locale"] = settings.locale
        if resume is True and data["purpose"] == "answer":
            data["resume"] = True
        mark.route_kind, mark.route_target_id = "work_answer", data["request_id"]
        mark.route_basis = {**mark.route_basis, "work_answer": data}
        await session.flush()
        await self.answers.publish_choice(link, mark, data, guard=guard, writer=session)
        return True
