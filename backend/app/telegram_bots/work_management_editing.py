"""Saved-work corrections own one prompt, extraction and immutable replacement."""

import asyncio
import logging
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select

from app.telegram_bots.assign import DraftAssignment, draft_assignment
from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.assignment_typing import shown_prompt
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, Prompts
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired, _item, keyboard_cleanup_row
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw
from app.telegram_bots.work_edit_drafts import edit_signature, snapshot_edit
from app.telegram_bots.work_input_capture import accepted_text
from app.telegram_bots.work_management_edit_records import read_input
from app.telegram_bots.work_management_edit_sources import ManagementEditFrame
from app.telegram_bots.work_management_origins import displayed_management, origin_for
from app.telegram_bots.work_management_prompt_sources import WorkManagementPromptProjection, WorkManagementPromptReader, editor_from, prompt_intent
from app.telegram_bots.work_news_delivery import WorkNewsPlans, frozen_news
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.runtime.keyed_lock import AsyncKeyedLockTable
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ManagementEditInput:
    id: str
    phase: str


class ManagementPromptPlans(WorkNewsPlans):
    def __init__(self, **kwargs):
        super().__init__(**kwargs, projection=WorkManagementPromptProjection)
        self.prompts = Prompts(self.callbacks)

    async def stage_metadata(self, writer, link, row, source):
        intent = source.work["intent"]
        prompt = await writer.get(TelegramCallbackRow, intent["prompt_id"], with_for_update=True, populate_existing=True)
        if prompt is None:
            if row.payload["presentation_version"]:
                raise ProductSourceRetired()
            await self.prompts.open(link, "work_edit", {"draft_id": intent["draft_id"]}, chat_id=link.chat_id, message_thread_id=row.message_thread_id, message_id=None, prompt_id=intent["prompt_id"], session=writer, defer_visibility=True)
        await self.prompts.attach(link, intent["prompt_id"], outbox_id=row.id, guard=None, session=writer)


class WorkManagementEdits:
    def __init__(self, cards):
        self.cards, self.acts = cards, cards.acts
        self.outbox, self.callbacks = cards.outbox, cards.callbacks
        self.prompts = ManagementPromptPlans(reader=WorkManagementPromptReader(self.acts), outbox=self.outbox, callbacks=self.callbacks)
        self._locks, self._tasks, self._active, self._stopping = AsyncKeyedLockTable(), set(), set(), False

    async def stop(self):
        self._stopping = True
        if self._tasks:
            await await_drained(asyncio.gather(*tuple(self._tasks), return_exceptions=True))

    def current(self):
        if self._stopping:
            raise ProductSourceRetired()

    async def answer(self, link, incoming, *, guard):
        self.current()
        task = asyncio.create_task(self._answer(link, incoming, guard=guard))
        self._tasks.add(task)
        try:
            return await await_drained(task)
        finally:
            self._tasks.discard(task)

    async def _answer(self, link, incoming, *, guard):
        identity = digest(["management-edit", origin_facts(incoming)])[:32]
        async with self._locks.hold(identity):
            self.current()
            replay = None
            async with self.callbacks.writer(link, guard) as writer:
                mark = (await self.outbox.inbox._rows(writer, [incoming]))[0]
                self.outbox.inbox._current(link, mark)
                if mark.route_kind is None:
                    quote = mark.sanitized_input.get("reply_to_message_id")
                    if quote is not None:
                        found = list(
                            await writer.scalars(
                                select(TelegramCallbackRow).where(
                                    TelegramCallbackRow.link_id == link.id,
                                    TelegramCallbackRow.chat_id == mark.chat_id,
                                    TelegramCallbackRow.binding_generation == mark.binding_generation,
                                    TelegramCallbackRow.kind == "prompt",
                                    TelegramCallbackRow.message_thread_id == mark.message_thread_id,
                                    TelegramCallbackRow.message_id == quote,
                                )
                            )
                        )
                        prompt = found[0] if len(found) == 1 else None
                    else:
                        found = await self.prompts.prompts.current(link, mark.chat_id, mark.message_thread_id, guard=guard, session=writer)
                        prompt = await writer.get(TelegramCallbackRow, found.id) if found else None
                    if prompt is None or prompt.payload.get("purpose") != "work_edit":
                        return None
                    prompt_parent = await writer.get(TelegramOutboxRow, prompt.payload.get("anchor_outbox_id"))
                    if prompt_parent is None:
                        raise CallbackRefused()
                    item = _item(prompt_parent)
                    intent = prompt_intent(item)
                    acknowledged, messages = shown_prompt(item, prompt, accepted_at=mark.accepted_at)
                    words = accepted_text(mark, prompt, acknowledged, message_ids=messages)
                    if len(words) > self.outbox.config.assign.max_chars:
                        raise ValueError("assign-too-long")
                    self.callbacks._usable(prompt)
                    latest = await self.prompts.prompts.current(link, mark.chat_id, mark.message_thread_id, guard=guard, session=writer)
                    if latest is None or latest.id != prompt.id:
                        raise CallbackRefused()
                    basis = {"version": 1, "input": origin_facts(InboxRow.from_row(mark)), "prompt_outbox_id": item.id, "intent": intent, "words": words, "draft_id": identity}
                    await self.outbox.inbox.route(InboxRow.from_row(mark), kind="management_edit", target_id=identity, basis={"management_edit": basis}, operation_ids=[], session=writer)
                    prompt.payload = {**prompt.payload, "management_input": {"version": 1, "input_id": identity, "phase": "captured", "claim_id": None}}
                    prompt.consumed_at, prompt.closed_reason = self.outbox.clock(), "answered"
                    await writer.flush()
                    await self.outbox.inbox.register_delivery([incoming], outbox_id=item.id, primary=False, finish=False, session=writer)
                    await self.outbox.inbox.register_delivery([incoming], outbox_id=intent["parent_id"], primary=False, finish=False, session=writer)
                elif mark.route_kind == "management_edit":
                    basis = thaw(mark.route_basis.get("management_edit"))
                    if not isinstance(basis, dict) or basis.get("draft_id") != identity or mark.route_target_id != identity or not same_body(basis.get("input"), origin_facts(InboxRow.from_row(mark))) or mark.operation_ids:
                        raise ProductSourceRetired()
                    prompt_parent = await writer.get(TelegramOutboxRow, basis["prompt_outbox_id"])
                    prompt = await writer.get(TelegramCallbackRow, basis["intent"]["prompt_id"])
                    if prompt_parent is None or prompt is None:
                        raise ProductSourceRetired()
                    item, intent = _item(prompt_parent), basis["intent"]
                    acknowledged, messages = shown_prompt(item, prompt, accepted_at=mark.accepted_at)
                    if not same_body(prompt_intent(item), intent) or accepted_text(mark, prompt, acknowledged, message_ids=messages) != basis["words"]:
                        raise ProductSourceRetired()
                else:
                    return None
                value = read_input(prompt.payload.get("management_input"))
                if type(value) is not dict or set(value) != {"version", "input_id", "phase", "claim_id"} or value["version"] != 1 or value["input_id"] != identity or prompt.consumed_at is None or prompt.closed_reason != "answered":
                    raise ProductSourceRetired()
                if value["phase"] in {"completed", "failed"}:
                    replay = value["phase"]
                elif value["phase"] != "captured":
                    return ManagementEditInput(identity, value["phase"])
                _frozen, source, _part = frozen_news(SimpleNamespace(item=item, link=link, part=None), acknowledged=True)
                guard.current()
            if replay is not None:
                await self._finish(link, incoming, intent["parent_id"], guard=guard)
                return intent["parent_id"] if replay == "completed" else ManagementEditInput(identity, "failed")
            detail = await self.cards.reader.detail(link, source.work_id)
            prepared = await prepare_attention(self.cards.reader, link)
            frame = ManagementEditFrame(
                self.cards.reader, link, intent["parent_id"], editor_from(intent), detail, opening_intent=source.work["opening"], topic=incoming.message_thread_id, history=source.work["history"], dedupe_key=source.work["owner_key"]
            )
            claim = uuid4().hex

            async def admission(writer, phase):
                self.current()
                await guard.check(writer)
                current = await writer.get(TelegramInboundMarkRow, (incoming.link_id, incoming.message_id, incoming.chat_id), with_for_update=True, populate_existing=True)
                prompt = await writer.get(TelegramCallbackRow, intent["prompt_id"], with_for_update=True, populate_existing=True)
                if (
                    current is None
                    or prompt is None
                    or current.state == "interrupted"
                    or current.route_kind != "management_edit"
                    or current.route_target_id != identity
                    or not same_body(current.route_basis.get("management_edit"), basis)
                    or not same_body(origin_facts(InboxRow.from_row(current)), basis["input"])
                    or current.operation_ids
                ):
                    raise ProductSourceRetired()
                marker = prompt.payload.get("management_input")
                if not same_body(marker, {"version": 1, "input_id": identity, "phase": phase, "claim_id": None if phase == "captured" else claim}) or prompt.consumed_at is None or prompt.closed_reason != "answered":
                    raise ProductSourceRetired()
                await prepared(session=writer, projection=frame)
                guard.current()
                return prompt

            async with self.callbacks.writer(link, guard) as writer:
                prompt = await admission(writer, "captured")
                prompt.payload = {**prompt.payload, "management_input": {"version": 1, "input_id": identity, "phase": "extracting", "claim_id": claim}}
                await writer.flush()
                guard.current()
            self._active.add(identity)
            try:

                async def before_dispatch():
                    async with self.callbacks.writer(link, guard) as writer:
                        await admission(writer, "extracting")

                settings = await self.cards.settings.get(link.owner_user_id)
                if settings is None:
                    raise ProductSourceRetired()
                previous = DraftAssignment.model_validate(source.work["root"]["body"]["draft"])
                proposal = await draft_assignment(basis["words"], agent_name=source.values["name"], settings=settings, config=self.outbox.config, previous=previous, now=self.outbox.clock(), before_dispatch=before_dispatch)
                if proposal is None:
                    async with self.callbacks.writer(link, guard) as writer:
                        prompt = await admission(writer, "extracting")
                        prompt.payload = {**prompt.payload, "management_input": {"version": 1, "input_id": identity, "phase": "failed", "claim_id": claim}}
                        await writer.flush()
                        await prepared(session=writer, projection=frame)
                        guard.current()
                    await self._finish(link, incoming, intent["parent_id"], guard=guard)
                    return ManagementEditInput(identity, "failed")
                async with self.callbacks.writer(link, guard) as writer:
                    prompt = await admission(writer, "extracting")
                    # An omitted deadline leaves the saved assignment's date
                    # intact; explicit deadline words may clear or replace it.
                    if proposal.due_at is None and proposal.due_text is None and previous.due_at is not None:
                        proposal = proposal.model_copy(update={"due_at": previous.due_at})
                    original = await writer.get(TelegramActRow, intent["draft_id"], with_for_update=True, populate_existing=True)
                    snapshot_edit(link, original)
                    witness = {"version": 1, "editor": editor_from(intent), "input": basis["input"], "prompt_outbox_id": item.id, "draft_id": identity}
                    body = {**thaw(original.body), "draft": proposal.model_dump(mode="json")}
                    if await writer.get(TelegramActRow, identity) is not None:
                        raise ProductSourceRetired()
                    fresh = TelegramActRow(id=identity, **{key: getattr(original, key) for key in _ACT_SCOPE}, body=body, state="draft", operation_ids=[], attempts=0, created_at=original.created_at, updated_at=self.outbox.clock())
                    writer.add(fresh)
                    await writer.flush()
                    fresh.continuation = {"edit_draft_source": {"version": 1, "origin_kind": "internal", "signature": edit_signature(fresh, "internal"), "link_id": link.id}}
                    original.state, original.reason, original.result, original.updated_at = "refused", "replaced", witness, self.outbox.clock()
                    parent = await writer.get(TelegramOutboxRow, intent["parent_id"], with_for_update=True, populate_existing=True)
                    old_intent = parent.payload["work_management_intent"]
                    parent.payload = {**parent.payload, "work_management_intent": {**old_intent, "draft_id": identity, "act_id": None}, "work_management_replacements": [*parent.payload.get("work_management_replacements", []), witness]}
                    prompt.payload = {**prompt.payload, "management_input": {"version": 1, "input_id": identity, "phase": "completed", "claim_id": claim}}
                    await writer.flush()
                    snapshot_edit(link, fresh)
                    await prepared(
                        session=writer,
                        projection=ManagementEditFrame(
                            self.cards.reader,
                            link,
                            intent["parent_id"],
                            editor_from(intent),
                            detail,
                            opening_intent=source.work["opening"],
                            topic=incoming.message_thread_id,
                            history=source.work["history"],
                            dedupe_key=source.work["owner_key"],
                            replacement=witness,
                        ),
                    )
                    guard.current()
                await self._finish(link, incoming, intent["parent_id"], guard=guard)
                return intent["parent_id"]
            finally:
                self._active.discard(identity)

    async def _finish(self, link, incoming, parent_id, *, guard):
        """Recover an owed presentation, without another extraction or command."""
        self.current()
        await self.cards.observe(link, parent_id)
        async with self.callbacks.writer(link, guard) as writer:
            mark = (await self.outbox.inbox._rows(writer, [incoming]))[0]
            if mark.state == "interrupted" or mark.route_kind != "management_edit" or not same_body(origin_facts(InboxRow.from_row(mark)), origin_facts(incoming)):
                raise ProductSourceRetired()
            await self.outbox.inbox.register_delivery([incoming], outbox_id=parent_id, primary=True, finish=True, session=writer)
            guard.current()

    async def recover(self, link, *, guard):
        """Retire unknown model claims and repair only their owed publication."""
        self.current()
        async with self.outbox.sessions() as reader:
            pending = [
                InboxRow.from_row(row)
                for row in await reader.scalars(
                    select(TelegramInboundMarkRow)
                    .where(
                        TelegramInboundMarkRow.link_id == link.id,
                        TelegramInboundMarkRow.chat_id == link.chat_id,
                        TelegramInboundMarkRow.binding_generation == link.binding_generation,
                        TelegramInboundMarkRow.route_kind == "management_edit",
                        TelegramInboundMarkRow.state.not_in(("done", "interrupted")),
                    )
                    .order_by(TelegramInboundMarkRow.message_id)
                )
            ]
        recovered = []
        for incoming in pending:
            identity = digest(["management-edit", origin_facts(incoming)])[:32]
            if identity in self._active:
                continue
            try:
                async with self._locks.hold(identity):
                    self.current()
                    basis = thaw(incoming.route_basis.get("management_edit"))
                    if not isinstance(basis, dict) or incoming.route_target_id != identity or basis.get("draft_id") != identity or not same_body(basis.get("input"), origin_facts(incoming)):
                        raise ProductSourceRetired()
                    async with self.outbox.sessions() as reader:
                        prompt_parent = await reader.get(TelegramOutboxRow, basis["prompt_outbox_id"])
                        prompt = await reader.get(TelegramCallbackRow, basis["intent"]["prompt_id"])
                        if prompt_parent is None or prompt is None:
                            raise ProductSourceRetired()
                        item, marker = _item(prompt_parent), read_input(prompt.payload.get("management_input"))
                        intent = prompt_intent(item)
                        acknowledged, messages = shown_prompt(item, prompt, accepted_at=incoming.accepted_at)
                        if not same_body(intent, basis["intent"]) or accepted_text(incoming, prompt, acknowledged, message_ids=messages) != basis["words"] or not isinstance(marker, dict) or marker.get("input_id") != identity:
                            raise ProductSourceRetired()
                        marker = dict(marker)
                        _basis, source, _part = frozen_news(SimpleNamespace(item=item, link=link, part=None), acknowledged=True)
                    if marker.get("phase") == "extracting":
                        detail = await self.cards.reader.detail(link, source.work_id)
                        prepared = await prepare_attention(self.cards.reader, link)
                        frame = ManagementEditFrame(
                            self.cards.reader,
                            link,
                            intent["parent_id"],
                            editor_from(intent),
                            detail,
                            opening_intent=source.work["opening"],
                            topic=incoming.message_thread_id,
                            history=source.work["history"],
                            dedupe_key=source.work["owner_key"],
                        )
                        async with self.callbacks.writer(link, guard) as writer:
                            await prepared(session=writer, projection=frame)
                            mark = (await self.outbox.inbox._rows(writer, [incoming]))[0]
                            prompt = await writer.get(TelegramCallbackRow, intent["prompt_id"], with_for_update=True, populate_existing=True)
                            if (
                                mark.state == "interrupted"
                                or not same_body(mark.route_basis.get("management_edit"), basis)
                                or not same_body(origin_facts(InboxRow.from_row(mark)), basis["input"])
                                or prompt is None
                                or not same_body(prompt.payload.get("management_input"), marker)
                            ):
                                raise ProductSourceRetired()
                            prompt.payload = {**prompt.payload, "management_input": {**marker, "phase": "failed"}}
                            await writer.flush()
                            await prepared(session=writer, projection=frame)
                            guard.current()
                        marker["phase"] = "failed"
                    if marker.get("phase") in {"failed", "completed"}:
                        await self._finish(link, incoming, intent["parent_id"], guard=guard)
                        recovered.append(ManagementEditInput(identity, marker["phase"]))
            except Exception as error:
                logger.debug("Saved edit recovery deferred (%s)", type(error).__name__)
        return tuple(recovered)

    async def open(self, link, callback_id, *, query_id, chat_id, message_id, message_thread_id, guard):
        if type(query_id) is not str or not 1 <= len(query_id) <= 256:
            raise CallbackRefused()

        async def perform():
            await prepare_publication_guard(guard)
            async with self.outbox.sessions() as reader:
                control = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id")) if control else None
                if (
                    control is None
                    or parent is None
                    or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, source = displayed_management(link, parent, control)
                if control.kind != "assign_edit" or source["key"] != "management.edit":
                    raise CallbackRefused()
                origin = origin_for(original, control)
                prompt_id = digest(["management-editor", parent.id, origin, query_id])[:32]
                intent = {"version": 1, "purpose": "work_edit", "prompt_id": prompt_id, "draft_id": control.target_id, "parent_id": parent.id, "origin": origin}
                item = _item(parent)
            editor = editor_from(intent)
            detail = await self.cards.reader.detail(link, item.work_id)
            prepared = await prepare_attention(self.cards.reader, link)
            frame = ManagementEditFrame(
                self.cards.reader, link, item.id, editor, detail, opening_intent=source["work"]["intent"], topic=item.message_thread_id, history=item.payload.get("work_management_replacements", []), dedupe_key=item.dedupe_key, opening=True
            )
            async with self.callbacks.writer(link, guard) as writer:
                await prepared(session=writer, projection=frame)
                row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:management-prompt:" + prompt_id).with_for_update())
                if row is None:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                    parent = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
                    now, identity = self.outbox.clock(), uuid4().hex
                    cleanup = keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=now, dedupe_key="notice:management-editor:" + prompt_id)
                    cleanup.payload = {**cleanup.payload, "work_request_parent_id": identity}
                    writer.add(cleanup)
                    row = TelegramOutboxRow(
                        id=identity,
                        link_id=link.id,
                        owner_user_id=link.owner_user_id,
                        binding_generation=link.binding_generation,
                        chat_id=link.chat_id,
                        instance_id=link.agent_ref,
                        work_id=item.work_id,
                        dedupe_key="notice:management-prompt:" + prompt_id,
                        class_="reply",
                        payload={"presentation_version": 0, "management_prompt_intent": intent, "management_prompt_cleanup_id": cleanup.id},
                        parts=[],
                        state="pending",
                        silent=True,
                        message_thread_id=message_thread_id,
                        not_before=now,
                        created_at=now,
                    )
                    writer.add(row)
                    parent.payload = {**parent.payload, "work_management_editor": editor}
                    await self.callbacks.consume_group(group, reason="editing", session=writer)
                    await writer.flush()
                    await prepared(session=writer, projection=frame)
                    self.callbacks._unexpired(control)
                elif not same_body(row.payload.get("management_prompt_intent"), intent):
                    raise ProductSourceRetired()
                guard.current()
                identity = row.id
            await self.prompts.materialize(link, identity)
            return identity

        return await await_drained(perform())
