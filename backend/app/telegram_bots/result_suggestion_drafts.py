"""A real result press owns one private drafting attempt, never a Work action."""

from app.telegram_bots.assignment_drafts import AssignmentDraft, draft_identity
from app.telegram_bots.assignment_origins import CallbackAssignmentInput
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.result_suggestion_source import SuggestionAdmissionProjection, SuggestionOriginProjection, acknowledged_result, fresh_result_projection, result_signature
from app.telegram_bots.work_outcome_proof import event_facts
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class ResultSuggestionDrafts:
    def __init__(self, chat):
        self.chat, self.results = chat, chat.runtime.results
        self.callbacks, self.outbox = chat.host.callbacks, chat.host.outbox

    async def open(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            scope = dict(chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard)
            async with self.callbacks.writer(link, guard) as writer:
                group = await self.callbacks.lock_current(link, callback_id, **scope, session=writer)
                control = group.selected
                if control.kind != "assign_from_suggestion" or control.target_kind != "work":
                    raise ProductSourceRetired()
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
                item, source = acknowledged_result(link, parent)
                suggestion = source.work.get("suggestion")
                if not suggestion or (control.target_id, control.payload.get("work_id"), control.payload.get("event_id")) != (item.work_id, item.work_id, suggestion["id"]):
                    raise ProductSourceRetired()
                event = await writer.get(AgentWorkEventRow, suggestion["id"])
                origin = dict(
                    version=1,
                    kind="result_suggestion",
                    link_id=link.id,
                    chat_id=chat_id,
                    binding_generation=link.binding_generation,
                    message_id=message_id,
                    message_thread_id=message_thread_id,
                    callback_id=callback_id,
                    anchor_outbox_id=item.id,
                    presentation_version=item.payload["presentation_version"],
                    anchor_signature=result_signature(item),
                    work_id=item.work_id,
                    event_id=suggestion["id"],
                    event_signature=digest(event_facts(event)),
                    words=suggestion["statement"],
                )
                incoming = CallbackAssignmentInput(origin)
                identity = draft_identity(link, incoming)
                existing = await writer.get(TelegramActRow, identity)
                marker = control.payload.get("suggestion_draft_id")
                if (marker is None) != (existing is None) or marker is not None and marker != identity:
                    raise ProductSourceRetired()
            current = await prepare_attention(self.results.reader, link)
            facts = SuggestionOriginProjection(link, origin)
            fresh = fresh_result_projection(self.results.reader, link, item, source)
            final = SuggestionAdmissionProjection(fresh, facts) if existing is None else facts

            async def admission(writer):
                await self.callbacks.lock_current(link, callback_id, **scope, session=writer)
                await current(session=writer, projection=final)
                root = await writer.get(TelegramActRow, identity)
                selected = await writer.get(TelegramCallbackRow, callback_id)
                marker = selected.payload.get("suggestion_draft_id")
                if (marker is None) != (root is None) or marker is not None and marker != identity:
                    raise ProductSourceRetired()
                await guard.check(writer)
                await current(session=writer, projection=final)

            async def record_origin(writer, root):
                if root.id != identity or root.body["request"]["phase"] != "new":
                    raise ProductSourceRetired()
                await self.callbacks.lock_current(link, callback_id, **scope, session=writer)
                await current(session=writer, projection=facts)
                selected = await writer.get(TelegramCallbackRow, callback_id)
                if selected.payload.get("suggestion_draft_id") is not None:
                    raise ProductSourceRetired()
                selected.payload = {**selected.payload, "suggestion_draft_id": identity}
                await writer.flush()

            if existing is not None and existing.body.get("version") == 1:
                async with self.callbacks.writer(link, guard) as writer:
                    await admission(writer)
                return await self.chat.assignments.observe(link, identity, guard=guard)
            limit = self.outbox.config.assign.max_chars
            if len(origin["words"]) > limit:
                async with self.callbacks.writer(link, guard) as writer:
                    await admission(writer)
                    notice = await self.outbox.plan_product(
                        link,
                        source_id="suggestion-limit:" + digest([callback_id, limit]),
                        parts=[{"kind": "text", "reply_to_message_id": message_id, "source_ref": {"key": "assign.suggestion_too_long", "values": {"max": limit}}}],
                        message_thread_id=message_thread_id,
                        session=writer,
                    )
                    await admission(writer)
                    return notice
            if existing is not None:
                await self.chat.assignment_inputs.requests.reconcile_request(link, identity, admission=admission)
            value = await self.chat.assignment_inputs.requests.request(link, incoming, words=origin["words"], source="suggestion", admission=admission, on_create=record_origin)
            if isinstance(value, AssignmentDraft):
                return await self.chat.assignments.plan(link, value.id, guard=guard)
            if value.phase == "awaiting_done":
                return await self.chat.assignment_prompts.plan(link, incoming, purpose="assign_done", draft_id=value.id, guard=guard)
            return value

        return await await_drained(perform())
