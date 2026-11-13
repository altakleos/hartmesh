"""Assignment typing prompts retain their exact input and optional draft source."""

from dataclasses import asdict
from datetime import timedelta

from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_drafts import DraftProjection, origin_facts, snapshot_draft
from app.telegram_bots.assignment_requests import RequestProjection, request_snapshot
from app.telegram_bots.assignment_sources import private_facts, private_snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.inbox import InboxRow, _basis
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_sources import WorkViewReader
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained

PURPOSES = {"assign_words": "assign.prompt", "assign_done": "assign.ask_done", "assign_edit": "assign.edit_prompt"}


def require_publication(mark, item, intent):
    if item.id not in mark.delivery_ids:
        raise ProductSourceRetired()
    if intent["purpose"] == "assign_words" and (mark.route_kind != "assignment_prompt" or mark.route_target_id != intent["prompt_id"] or not same_body(_basis(mark), {"prompt_id": intent["prompt_id"]})):
        raise ProductSourceRetired()


def prompt_intent(item):
    value = thaw(item.payload.get("assignment_prompt_intent"))
    if type(value) is not dict or set(value) != {"version", "purpose", "prompt_id", "origin", "draft_id", "edit_origin", "query_key"} or type(value["version"]) is not int or value["version"] != 1 or value["purpose"] not in PURPOSES:
        raise ProductSourceRetired()
    if type(value["prompt_id"]) is not str or len(value["prompt_id"]) != 32 or (value["draft_id"] is None) != (value["purpose"] == "assign_words") or (value["edit_origin"] is not None) != (value["purpose"] == "assign_edit"):
        raise ProductSourceRetired()
    if value["query_key"] is not None and (value["purpose"] != "assign_edit" or type(value["query_key"]) is not str or len(value["query_key"]) != 64 or any(ch not in "0123456789abcdef" for ch in value["query_key"])):
        raise ProductSourceRetired()
    return value


class AssignmentPromptReader(WorkViewReader):
    def __init__(self, acts):
        super().__init__(work=acts.work, authority=acts.authority, links=acts.links, clock=acts.outbox.clock)
        self.acts, self.config = acts, acts.outbox.config

    async def read(self, link, item):
        async def perform():
            intent = prompt_intent(item)
            async with self.links.session_factory() as reader:
                raw = await reader.get(TelegramInboundMarkRow, (intent["origin"]["link_id"], intent["origin"]["message_id"], intent["origin"]["chat_id"]))
                if raw is None:
                    raise ProductSourceRetired()
                inbox = InboxRow.from_row(raw)
                root = await reader.get(TelegramActRow, intent["draft_id"]) if intent["draft_id"] else None
                if intent["draft_id"] and root is None:
                    raise ProductSourceRetired()
                saved = _snapshot(root) if root else None
            projection = AssignmentPromptProjection(self, link, item, inbox=inbox, root=saved, clock=await self.prepare_clock(link, item))
            current = await prepare_attention(self, link)
            await current(projection=projection)
            return projection.source

        return await await_drained(perform())


class AssignmentPromptProjection:
    def __init__(self, reader, link, item, source=None, *, inbox=None, root=None, clock=None, allow_answered=False):
        self.reader, self.link, self.item, self.expected, self.clock = reader, link, item, source, clock
        self.intent = prompt_intent(item)
        self.root = private_snapshot(source.work["root"]) if source is not None and source.work["root"] else root
        self.inbox = inbox
        self.allow_answered = allow_answered
        self.edit = self.fresh = None
        if self.intent["purpose"] == "assign_edit":
            from app.telegram_bots.assignment_editing import EditProof
            from app.telegram_bots.assignment_lineage import FreshDraft

            self.edit = EditProof(link, self.intent["edit_origin"], clock=reader.clock, config=reader.config)
            self.fresh = FreshDraft(self.root.id, self.intent["edit_origin"]["anchor_outbox_id"])
        self.facts = RequestProjection(link, self.root) if self.intent["purpose"] == "assign_done" else DraftProjection(link, self.intent["origin"], inbox=inbox, saved=self.root)
        self.source = None

    def extend(self, query):
        result = self.facts.extend(query)
        if self.edit is not None:
            result = self.fresh.extend(self.edit.extend(result))
        self.delivery_column = len(result.column_descriptions)
        delivery, settings, prompt = aliased(TelegramOutboxRow), aliased(TelegramPersonSettingsRow), aliased(TelegramCallbackRow)
        return result.add_columns(delivery, settings, prompt).join(delivery, delivery.id == self.item.id).join(settings, settings.owner_user_id == self.link.owner_user_id).outerjoin(prompt, prompt.id == self.intent["prompt_id"])

    def check(self, rows):
        raise ProductSourceRetired()

    def check_with_permissions(self, rows, held):
        self.facts.check_with_permissions(rows, held)
        delivery, settings = rows[0][self.delivery_column : self.delivery_column + 2]
        mark = rows[0][self.facts.column + 1]
        prompt = rows[0][self.delivery_column + 2]
        root = rows[0][self.facts.column + 2] if self.root is not None else None
        if (
            _item(delivery) != self.item
            or (delivery.link_id, delivery.owner_user_id, delivery.chat_id, delivery.binding_generation, delivery.instance_id, delivery.work_id, delivery.request_id, delivery.event_id, delivery.class_)
            != (self.link.id, self.link.owner_user_id, self.link.chat_id, self.link.binding_generation, self.link.agent_ref, None, None, None, "reply")
            or delivery.message_thread_id != mark.message_thread_id
            or delivery.dedupe_key != "notice:assignment-prompt:" + self.intent["prompt_id"]
            or settings.timezone != self.clock.settings.timezone
        ):
            raise ProductSourceRetired()
        if mark.state == "interrupted" or not same_body(origin_facts(InboxRow.from_row(mark)), self.intent["origin"]):
            raise ProductSourceRetired()
        if self.edit is not None:
            self.edit.check(rows, root, mark)
            self.fresh.check(rows, rows[0][self.edit.column + 1])
        else:
            require_publication(mark, self.item, self.intent)
        if delivery.parts:
            from app.telegram_bots.assignment_typing import validate_prompt

            validate_prompt(_item(delivery), prompt, attached=True)
            if (prompt.consumed_at is not None or "prompt_successor" in prompt.payload) and not (self.allow_answered and prompt.consumed_at is not None and prompt.closed_reason == "answered" and prompt.payload.get("assignment_input")):
                raise ProductSourceRetired()
        if root is not None:
            value = request_snapshot(self.link, root) if self.intent["purpose"] == "assign_done" else snapshot_draft(self.link, root)
            if root.operation_ids or root.continuation is not None or root.stop_dispatch is not None or root.cancel_requested_at is not None:
                raise ProductSourceRetired()
            if self.intent["purpose"] == "assign_done" and value.phase != "awaiting_done":
                raise ProductSourceRetired()
            if self.reader.clock() >= value.created_at + timedelta(hours=self.reader.config.callbacks.draft_ttl_hours):
                raise ProductSourceRetired()
        data = {"intent": self.intent, "root": private_facts(root) if root is not None else None}
        instance, permissions, policy = rows[0][1:4]
        self.source = WorkNewsRead(
            None, "assignment.prompt", {"name": instance.name}, instance.custody != "personal" and policy == "protected", data, digest([self.link.id, self.link.binding_generation, self.intent]), digest([data, instance.name, permissions])
        )
        if self.expected is not None and not same_body(asdict(self.source), asdict(self.expected)):
            raise ProductSourceRetired()
