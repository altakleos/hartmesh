"""Adopt one verified ordinary-chat proposal into the original primary reply."""

import asyncio
from dataclasses import asdict
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_drafts import AssignmentDrafts, DraftProjection, origin_facts, snapshot_draft
from app.telegram_bots.assignment_proposal_records import original_news, validate_proposal_reply_basis
from app.telegram_bots.assignment_proposal_source import check_qualified_proposal
from app.telegram_bots.assignment_sources import AssignmentProjection, private_source
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired, _item, _safe_json
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_news_delivery import recipes
from app.telegram_bots.work_result_navigation import NavigationOrigin
from app.telegram_bots.work_stop_news import NewsClock
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class _DraftFacts(DraftProjection):
    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        instance = rows[0][1]
        self.instance = SimpleNamespace(name=instance.name, custody=instance.custody)
        self.held = frozenset(held)
        self.policy = "allowed" if instance.custody == "personal" or rows[0][3] is None else rows[0][3]


class AssignmentProposalPlans:
    def __init__(self, assignments, *, source):
        self.assignments, self.source = assignments, source
        self.outbox, self.callbacks = assignments.outbox, assignments.callbacks
        self.drafts = AssignmentDrafts(assignments.reader.acts)

    async def plan_terminal(self, record, link, *, guard):
        return await await_drained(self._plan(record, link, guard=guard))

    async def _plan(self, record, link, *, guard):
        qualified = await self.source.read_proposal(record, link, guard=guard)
        if qualified is None:
            return None
        record = qualified.record
        source_basis = check_qualified_proposal(qualified, record)
        await prepare_publication_guard(guard)
        async with self.outbox.sessions() as reader:
            values = []
            for origin in record.origins:
                row = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
                if row is None:
                    raise ProductSourceRetired()
                values.append(InboxRow.from_row(row))
        first = min(values, key=lambda row: (row.accepted_at, row.update_id, row.message_id))
        facts = _DraftFacts(link, origin_facts(first), inbox=first)
        draft_id, install = await self.drafts.prepare_create(link, first, draft=qualified.draft, source="tool", projection=facts)
        settings = await self.assignments.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        clock = NewsClock(settings, await asyncio.to_thread(ZoneInfo, settings.timezone), self.outbox.clock)
        navigation = NavigationOrigin()
        current = await prepare_attention(self.assignments.reader, link)
        check_qualified_proposal(qualified, record)
        await prepare_publication_guard(qualified._guard)
        async with self.callbacks.writer(link, guard) as writer:
            await qualified._guard.check(writer)
            fresh, _marks = await self.outbox.inbox.lock_publication(record, session=writer)
            if fresh != record:
                raise ProductSourceRetired()
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id).with_for_update().execution_options(populate_existing=True))
            root = await writer.get(TelegramActRow, draft_id, with_for_update=True, populate_existing=True)
            if existing is not None:
                proof = validate_proposal_reply_basis(_item(existing))
                if (
                    root is None
                    or proof.record != record
                    or not same_body(existing.payload["reply_basis"]["outcome"]["proposal_basis"], source_basis)
                    or not same_body(root.body, original_news(_item(existing), link)["source"]["work"]["root"]["body"])
                ):
                    raise ProductSourceRetired()
                snapshot_draft(link, root)
                await current(session=writer, projection=DraftProjection(link, root.body["origin"], saved=_snapshot(root)))
                check_qualified_proposal(qualified, record)
                await qualified._guard.check(writer)
                return existing.id
            if root is not None or await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == draft_id).limit(1)) is not None:
                # An orphaned or foreign owner is unknown, never a fresh plan.
                raise ProductSourceRetired()
            saved = await install(writer)
            root = await writer.get(TelegramActRow, saved.id, populate_existing=True)
            source = private_source(link, root, instance=facts.instance, permissions=facts.permissions, policy=facts.policy, held=facts.held, clock=clock, config=self.outbox.config)
            basis = {"version": 1, "source": asdict(source), "locale": settings.locale, "concealed": False, "public_base_url": navigation.base}
            parts, keyboard = recipes(source, basis, "", link)
            inputs = await self.outbox.inbox.snapshot_publication_inputs(record, session=writer)
            outcome = {"kind": "assignment_proposal", "proposal_basis": source_basis}
            reply = {"version": 9, "publication": source_basis["publication"], "outcome": outcome, "presentation": None, "parts": parts, "finish_route": True, "inputs": inputs, "draft_id": draft_id}
            identity, created = await self.outbox._add(
                link,
                dedupe_key="reply:" + record.turn_id,
                class_="reply",
                payload=_safe_json(
                    {"source": outcome, "reply_basis": reply, "assignment_intent": {"version": 1, "draft_id": draft_id}, "work_news_basis": {**basis, "parts": parts, "keyboard": keyboard}, "product_keyboard_basis": keyboard, "parts": parts}
                ),
                session=writer,
                message_thread_id=record.scope.message_thread_id,
                instance_id=link.agent_ref,
            )
            if not created:
                raise ProductSourceRetired()
            parent = await writer.get(TelegramOutboxRow, identity, populate_existing=True)
            if keyboard:
                group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=record.scope.message_thread_id, session=writer)
                parent.parts = [*parent.parts[:-1], {**parent.parts[-1], "callback_group_id": group.group_id}]
                for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                    control.payload = {**control.payload, "presentation_pending": True}
            await writer.flush()
            validate_proposal_reply_basis(_item(parent))
            await self.outbox.inbox.seal_publication(record, outbox_id=identity, assistant_message_id=None, session=writer)
            await writer.flush()
            await current(session=writer, projection=AssignmentProjection(self.assignments.reader, link, _item(parent), source, clock=clock))
            navigation.check(basis)
            check_qualified_proposal(qualified, record)
            await qualified._guard.check(writer)
            return identity
