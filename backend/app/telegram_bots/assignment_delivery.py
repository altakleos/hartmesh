"""One source-owned reply evolves with its private assignment draft."""

from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4

from app.telegram_bots.assignment_drafts import snapshot_draft
from app.telegram_bots.assignment_proposal_records import assignment_parent
from app.telegram_bots.assignment_sources import AssignmentProjection
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_news_delivery import WorkNewsPlans, frozen_news
from app.telegram_bots.work_news_recovery import WorkNewsRecovery
from app.telegram_bots.work_result_navigation import NavigationOrigin
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class AssignmentPlans(WorkNewsPlans):
    def __init__(self, **kwargs):
        super().__init__(**kwargs, projection=AssignmentProjection)

    async def cancel(self, link, callback_id, **kwargs):
        from app.telegram_bots.assignment_recovery import AssignmentRecovery

        return await AssignmentRecovery(self).operate(link, callback_id, cancel=True, **kwargs)

    async def retry(self, link, callback_id, **kwargs):
        from app.telegram_bots.assignment_recovery import AssignmentRecovery

        return await AssignmentRecovery(self).operate(link, callback_id, cancel=False, **kwargs)

    async def observe(self, link, draft_id):
        async def perform():
            async with self.outbox.sessions() as reader:
                root = await reader.get(TelegramActRow, draft_id)
                snapshot_draft(link, root)
                operations = list(root.operation_ids)
                row = await assignment_parent(reader, link, draft_id)
                if row is None:
                    raise ProductSourceRetired()
                item = _item(row)
                if any(getattr(row, key) is not None for key in ("claim_owner", "claim_id", "claimed_at")):
                    return row.id
            if len(operations) > 1:
                raise ProductSourceRetired()
            before = await self.reader.read(link, item)
            if operations:
                from app.telegram_bots.assignment_receipts import prepare_receipt_guard

                admission = await prepare_receipt_guard(self.reader, link, before)
                try:
                    await self.reader.acts.reconcile_delegation(operations[0], admission=admission)
                except Exception:
                    # An unavailable receipt is no proof of completion or absence.
                    pass
            async with self.outbox.sessions() as reader:
                row = await assignment_parent(reader, link, draft_id)
                if row is None:
                    raise ProductSourceRetired()
                item = _item(row)
                if any(getattr(row, key) is not None for key in ("claim_owner", "claim_id", "claimed_at")):
                    return row.id
            source = await self.reader.read(link, item)
            if not item.parts:
                await self.materialize(link, item.id)
                return item.id
            basis, previous, _ = frozen_news(SimpleNamespace(link=link, item=item, part=None))
            settings = await self.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            origin = NavigationOrigin()
            if same_body(asdict(source), asdict(previous)) and basis["locale"] == settings.locale and basis["public_base_url"] == origin.base:
                return item.id
            current = await prepare_attention(self.reader, link)
            clock = await self.reader.prepare_clock(link, item)
            async with self.outbox._writer(link.id) as (writer, _binding):
                row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
                if row is None or _item(row) != item or any(getattr(row, key) is not None for key in ("claim_owner", "claim_id", "claimed_at")):
                    return item.id
                await current(session=writer, projection=self.projection(self.reader, link, item, source, clock=clock))
                await WorkNewsRecovery(self)._replace(writer, link, row, source, settings, origin, unclaimed=True, direct_edit=True)
                await current(session=writer, projection=self.projection(self.reader, link, _item(row), source, clock=clock))
                origin.check(row.payload["work_news_basis"])
                return row.id

        return await await_drained(perform())

    async def plan(self, link, draft_id, *, guard):
        async def perform():
            key = "notice:assignment:" + draft_id
            async with self.callbacks.writer(link, guard) as writer:
                root = await writer.get(TelegramActRow, draft_id, with_for_update=True, populate_existing=True)
                snapshot_draft(link, root)
                if root.operation_ids:
                    raise ProductSourceRetired()
                row = await assignment_parent(writer, link, draft_id)
                if row is None:
                    now = self.outbox.clock()
                    row = TelegramOutboxRow(
                        id=uuid4().hex,
                        link_id=link.id,
                        owner_user_id=link.owner_user_id,
                        binding_generation=link.binding_generation,
                        chat_id=link.chat_id,
                        instance_id=link.agent_ref,
                        dedupe_key=key,
                        class_="reply",
                        payload={"presentation_version": 0, "assignment_intent": {"version": 1, "draft_id": draft_id}},
                        parts=[],
                        state="pending",
                        silent=True,
                        message_thread_id=root.body["origin"]["message_thread_id"],
                        not_before=now,
                        created_at=now,
                    )
                    writer.add(row)
                    await writer.flush()
                if root.body["source"] != "tool":
                    from app.telegram_bots.inbox import InboxRow

                    for index, origin in enumerate([root.body["origin"], *([root.body["criteria_origin"]] if "criteria_origin" in root.body else [])]):
                        mark = await writer.get(TelegramInboundMarkRow, (origin["link_id"], origin["message_id"], origin["chat_id"]), populate_existing=True)
                        if mark is None:
                            raise ProductSourceRetired()
                        inbox = InboxRow.from_row(mark)
                        if index == 0:
                            from app.telegram_bots.assignment_drafts import draft_route_basis

                            inbox = await self.outbox.inbox.route(inbox, kind="assignment_draft", target_id=draft_id, basis=draft_route_basis(inbox, draft_id), operation_ids=[], session=writer)
                        await self.outbox.inbox.register_delivery([inbox], outbox_id=row.id, primary=mark.outbox_id is None, finish=True, session=writer)
                identity = row.id
            await self.materialize(link, identity)
            return identity

        return await await_drained(perform())
