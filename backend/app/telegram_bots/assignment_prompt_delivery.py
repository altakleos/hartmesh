"""Assignment typing plans and deferred prompt metadata commit together."""

from uuid import uuid4

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_controls import consent_origin, displayed_assignment
from app.telegram_bots.assignment_drafts import DraftProjection, origin_facts
from app.telegram_bots.assignment_prompt_sources import AssignmentPromptProjection
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, Prompts, _visibility
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_news_delivery import WorkNewsPlans
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class AssignmentPromptPlans(WorkNewsPlans):
    def __init__(self, **kwargs):
        super().__init__(**kwargs, projection=AssignmentPromptProjection)
        self.prompts = Prompts(self.callbacks)

    async def edit(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard, query_id=None):
        if query_id is not None and (type(query_id) is not str or not 1 <= len(query_id) <= 256):
            raise ValueError("invalid-callback-query")

        async def perform():
            from app.telegram_bots.inbox import InboxRow

            async with self.callbacks.writer(link, guard) as writer:
                control = await writer.get(TelegramCallbackRow, callback_id)
                parent = await writer.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id")) if control else None
                if (
                    control is None
                    or parent is None
                    or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, source = displayed_assignment(link, parent, control)
                if control.kind != "assign_edit":
                    raise CallbackRefused()
                origin = source["work"]["root"]["body"]["origin"]
                mark = await writer.get(TelegramInboundMarkRow, (origin["link_id"], origin["message_id"], origin["chat_id"]))
                if mark is None:
                    raise CallbackRefused()
                inbox = InboxRow.from_row(mark)
                identity, edit_origin = control.target_id, consent_origin(original, control)
            query_key = digest([link.id, link.binding_generation, callback_id, query_id]) if query_id is not None else None
            return await self.plan(link, inbox, purpose="assign_edit", draft_id=identity, edit_origin=edit_origin, query_key=query_key, guard=guard)

        return await await_drained(perform())

    async def plan(self, link, inbox, *, purpose, draft_id=None, edit_origin=None, query_key=None, guard):
        if purpose not in {"assign_words", "assign_done", "assign_edit"} or (draft_id is None) != (purpose == "assign_words") or (edit_origin is not None) != (purpose == "assign_edit"):
            raise ValueError("invalid-assignment-prompt")

        async def perform():
            origin = origin_facts(inbox)
            prompt_id = digest(["assignment-prompt", origin, purpose, draft_id, *([edit_origin, query_key] if edit_origin is not None else [])])[:32]
            intent = {"version": 1, "purpose": purpose, "prompt_id": prompt_id, "origin": origin, "draft_id": draft_id, "edit_origin": edit_origin, "query_key": query_key}
            current = await prepare_attention(self.reader, link)
            facts = DraftProjection(link, origin, inbox=inbox)
            if purpose == "assign_edit":
                from app.telegram_bots.assignment_editing import EditFrame

                async with self.outbox.sessions() as reader:
                    root = await reader.get(TelegramActRow, draft_id)
                    if root is None:
                        raise ProductSourceRetired()
                    facts = EditFrame(link, _snapshot(root), edit_origin, clock=self.reader.clock, config=self.outbox.config)
            async with self.callbacks.writer(link, guard) as writer:
                await current(session=writer, projection=facts)
                from sqlalchemy import select

                row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:assignment-prompt:" + prompt_id).with_for_update())
                if row is None:
                    now = self.outbox.clock()
                    row = TelegramOutboxRow(
                        id=uuid4().hex,
                        link_id=link.id,
                        owner_user_id=link.owner_user_id,
                        binding_generation=link.binding_generation,
                        chat_id=link.chat_id,
                        instance_id=link.agent_ref,
                        dedupe_key="notice:assignment-prompt:" + prompt_id,
                        class_="reply",
                        payload={"presentation_version": 0, "assignment_prompt_intent": intent},
                        parts=[],
                        state="pending",
                        silent=True,
                        message_thread_id=inbox.message_thread_id,
                        not_before=now,
                        created_at=now,
                    )
                    writer.add(row)
                    await writer.flush()
                    if purpose == "assign_words":
                        await self.outbox.inbox.route(inbox, kind="assignment_prompt", target_id=prompt_id, basis={"prompt_id": prompt_id}, operation_ids=[], session=writer)
                    if purpose != "assign_edit":
                        await self.outbox.inbox.register_delivery([inbox], outbox_id=row.id, primary=inbox.outbox_id is None, finish=purpose == "assign_words", session=writer)
                elif not same_body(row.payload.get("assignment_prompt_intent"), intent):
                    raise ProductSourceRetired()
                await current(session=writer, projection=facts)
                identity = row.id
            await self.materialize(link, identity, guard=guard)
            return identity

        return await await_drained(perform())

    async def stage_metadata(self, writer, link, row, source):
        intent = source.work["intent"]
        prompt = await writer.get(TelegramCallbackRow, intent["prompt_id"], with_for_update=True, populate_existing=True)
        version = row.payload["presentation_version"]
        if prompt is None:
            if version:
                raise CallbackRefused()
            await self.prompts.open(
                link, intent["purpose"], {"draft_id": intent["draft_id"]}, chat_id=link.chat_id, message_thread_id=row.message_thread_id, message_id=None, prompt_id=intent["prompt_id"], session=writer, defer_visibility=True
            )
        else:
            visibility = _visibility(prompt.payload.get("prompt_visibility"))
            old_version = prompt.payload.get("presentation_version")
            old = [part for part in row.parts if part["presentation_version"] == old_version]
            if (
                (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != (link.id, link.chat_id, link.binding_generation, row.message_thread_id)
                or prompt.kind != "prompt"
                or prompt.payload.get("purpose") != intent["purpose"]
                or prompt.payload.get("draft_id") != intent["draft_id"]
                or prompt.payload.get("anchor_outbox_id") != row.id
                or type(old_version) is not int
                or not 0 <= old_version < version
                or not old
                or old[-1]["part_id"] != visibility["part_id"]
                or old[-1]["state"] != "retired"
                or visibility["state"] != "pending"
                or prompt.consumed_at is not None
                or prompt.message_id is not None
                or prompt.expires_at is not None
                or "assignment_input" in prompt.payload
                or "prompt_successor" in prompt.payload
            ):
                raise CallbackRefused()
            prompt.payload = {**prompt.payload, "presentation_version": version, "prompt_visibility": {**visibility, "part_id": row.parts[-1]["part_id"]}}
            await writer.flush()
        # The caller owns the prepared/current binding and final source fences.
        await self.prompts.attach(link, intent["prompt_id"], outbox_id=row.id, guard=None, session=writer)
