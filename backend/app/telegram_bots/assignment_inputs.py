"""Accepted assignment typing retains one prompt and one extraction identity."""

from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_drafts import AssignmentDraft, DraftProjection, draft_identity, origin_facts, snapshot_draft
from app.telegram_bots.assignment_prompt_sources import AssignmentPromptProjection, require_publication
from app.telegram_bots.assignment_requests import AssignmentDraftRequests
from app.telegram_bots.assignment_typing import shown_prompt
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_input_capture import accepted_text
from app.telegram_bots.work_news_delivery import frozen_news
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class AssignmentInputs:
    def __init__(self, prompts, cards, *, requests=None):
        self.prompts, self.cards, self.acts = prompts, cards, cards.reader.acts
        self.outbox, self.callbacks = prompts.outbox, prompts.callbacks
        self.requests = requests or AssignmentDraftRequests(self.acts)

    async def answer(self, link, inbox, *, guard):
        return await await_drained(self._answer(link, inbox, guard=guard))

    async def _answer(self, link, inbox, *, guard):
        async with self.callbacks.writer(link, guard) as writer:
            mark = (await self.outbox.inbox._rows(writer, [inbox]))[0]
            self.outbox.inbox._current(link, mark)
            incoming = InboxRow.from_row(mark)
            identity = draft_identity(link, incoming)
            if mark.route_kind in {"assignment_draft", "assignment_criteria"}:
                matches = list(
                    await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == link.id, TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.payload["assignment_input"]["input_id"].as_string() == identity))
                )
                if len(matches) != 1:
                    raise CallbackRefused()
                prompt = matches[0]
            elif mark.route_kind is not None:
                return None
            else:
                quote = mark.sanitized_input.get("reply_to_message_id")
                if quote is not None:
                    matches = list(
                        await writer.scalars(
                            select(TelegramCallbackRow).where(
                                TelegramCallbackRow.link_id == link.id,
                                TelegramCallbackRow.chat_id == mark.chat_id,
                                TelegramCallbackRow.binding_generation == mark.binding_generation,
                                TelegramCallbackRow.message_thread_id == mark.message_thread_id,
                                TelegramCallbackRow.kind == "prompt",
                                TelegramCallbackRow.message_id == quote,
                            )
                        )
                    )
                    prompt = matches[0] if len(matches) == 1 else None
                else:
                    found = await self.prompts.prompts.current(link, mark.chat_id, mark.message_thread_id, guard=guard, session=writer)
                    prompt = await writer.get(TelegramCallbackRow, found.id) if found else None
            if prompt is None or prompt.payload.get("purpose") not in {"assign_words", "assign_done", "assign_edit"}:
                return None
            parent = await writer.get(TelegramOutboxRow, prompt.payload.get("anchor_outbox_id"), populate_existing=True)
            if parent is None:
                raise CallbackRefused()
            item = _item(parent)
            _basis, source, _part = frozen_news(SimpleNamespace(item=item, link=link, part=None))
            acknowledged, messages = shown_prompt(item, prompt, accepted_at=mark.accepted_at)
            text = accepted_text(mark, prompt, acknowledged, message_ids=messages)
            if len(text) > self.outbox.config.assign.max_chars:
                raise ValueError("assign-too-long")
            prompt_id = prompt.id
            criteria = prompt.payload["purpose"] == "assign_done"
            editing = prompt.payload["purpose"] == "assign_edit"
            draft_id = prompt.payload["draft_id"] if criteria else identity
            card = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:assignment:" + draft_id))
            existing_card = card.id if card is not None else None
            if existing_card is not None:
                root = await writer.get(TelegramActRow, draft_id, populate_existing=True)
                if root is None or root.body.get("version") != 1 or card.payload.get("assignment_intent") != {"version": 1, "draft_id": draft_id}:
                    raise CallbackRefused()
                snapshot_draft(link, root)
                if not same_body(root.body.get("criteria_origin" if criteria else "origin"), origin_facts(incoming)) or prompt.payload.get("assignment_input") != {
                    "version": 1,
                    "input": origin_facts(incoming),
                    "input_id": identity,
                    "draft_id": draft_id,
                }:
                    raise CallbackRefused()
        current = await prepare_attention(self.prompts.reader, link)
        clock = await self.prompts.reader.prepare_clock(link, item)
        witness = {"version": 1, "input": origin_facts(incoming), "input_id": identity, "draft_id": draft_id}

        async def admission(writer):
            await guard.check(writer)
            parent = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
            prompt = await writer.get(TelegramCallbackRow, prompt_id, with_for_update=True, populate_existing=True)
            mark = await writer.get(TelegramInboundMarkRow, (incoming.link_id, incoming.message_id, incoming.chat_id), with_for_update=True, populate_existing=True)
            if parent is None or _item(parent) != item or prompt is None or mark is None or mark.state == "interrupted" or not same_body(origin_facts(InboxRow.from_row(mark)), witness["input"]):
                raise ProductSourceRetired()
            acknowledged, messages = shown_prompt(_item(parent), prompt, accepted_at=mark.accepted_at)
            route = "assignment_criteria" if criteria else "assignment_draft"
            if accepted_text(mark, prompt, acknowledged, message_ids=messages) != text or mark.route_kind not in {None, route} or mark.route_kind is not None and mark.route_target_id != draft_id:
                raise CallbackRefused()
            captured = prompt.payload.get("assignment_input")
            if captured is None:
                if mark.route_kind is not None:
                    raise CallbackRefused()
                if criteria:
                    target = await writer.get(TelegramActRow, draft_id, populate_existing=True)
                    if target is None or target.body.get("version") != 2:
                        raise CallbackRefused()
                self.callbacks._usable(prompt)
                latest = await self.prompts.prompts.current(link, mark.chat_id, mark.message_thread_id, guard=guard, session=writer)
                if latest is None or latest.id != prompt_id:
                    raise CallbackRefused()
                prompt.payload = {**prompt.payload, "assignment_input": witness}
                prompt.consumed_at, prompt.closed_reason = self.outbox.clock(), "answered"
                await writer.flush()
            elif not same_body(captured, witness) or prompt.consumed_at is None or prompt.closed_reason != "answered":
                raise CallbackRefused()
            root = await writer.get(TelegramActRow, draft_id, populate_existing=True) if criteria else None
            if editing:
                from app.telegram_bots.assignment_editing import admit_edit

                await admit_edit(writer, link, source, draft_id, witness["input"], current, self.acts, observing=existing_card is not None)
            elif root is not None and root.body.get("version") == 1:
                expected = DraftAssignment(objective=source.work["root"]["body"]["request"]["words"], success_criteria=text).model_dump(mode="json")
                snapshot_draft(link, root)
                body = {"version": 1, "draft": expected, "original": expected, "source": "fallback", "due_note": False, "origin": source.work["intent"]["origin"], "criteria_origin": witness["input"]}
                if not same_body(root.body, body) or not existing_card and (root.state != "draft" or root.operation_ids or root.continuation is not None):
                    raise ProductSourceRetired()
                await current(session=writer, projection=_CompletedCriteria(link, _snapshot(root), item, source.work["intent"]))
            else:
                await current(session=writer, projection=AssignmentPromptProjection(self.prompts.reader, link, _item(parent), source, clock=clock, allow_answered=True))
            guard.current()

        if existing_card is not None:
            async with self.callbacks.writer(link, guard) as writer:
                await admission(writer)
            return await self.cards.observe(link, draft_id)
        if criteria:
            async with self.outbox.sessions() as reader:
                root = await reader.get(TelegramActRow, draft_id)
                complete = root is not None and root.body.get("version") == 1
            if complete:
                async with self.callbacks.writer(link, guard) as writer:
                    await admission(writer)
                return await self.cards.plan(link, draft_id, guard=guard)
            value = await self.requests.criteria(link, draft_id, incoming, text=text, admission=admission)
        else:
            previous = DraftAssignment.model_validate(source.work["root"]["body"]["draft"]) if editing else None
            value = await self.requests.request(link, incoming, words=text, previous=previous, source="edit" if editing else "command", admission=admission)
        if isinstance(value, AssignmentDraft):
            if editing:
                await self.cards.observe(link, source.work["intent"]["draft_id"])
            return await self.cards.plan(link, value.id, guard=guard)
        if value.phase == "awaiting_done":
            return await self.prompts.plan(link, incoming, purpose="assign_done", draft_id=value.id, guard=guard)
        return value


class _CompletedCriteria(DraftProjection):
    def __init__(self, link, root, item, intent):
        super().__init__(link, root.body["origin"], saved=root)
        self.item, self.intent = item, intent

    def check_with_permissions(self, rows, held):
        from app.telegram_bots.inbox import _basis

        super().check_with_permissions(rows, held)
        original, criterion = rows[0][self.column + 1], rows[0][self.criteria_column]
        require_publication(original, self.item, self.intent)
        if (
            original.state == "interrupted"
            or original.route_kind != "assignment_draft"
            or original.route_target_id != self.saved.id
            or not same_body(_basis(original), {"draft_id": self.saved.id})
            or criterion.state == "interrupted"
            or criterion.route_kind != "assignment_criteria"
            or criterion.route_target_id != self.saved.id
            or not same_body(_basis(criterion), {"draft_id": self.saved.id})
        ):
            raise ProductSourceRetired()
