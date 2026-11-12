"""One response owner for a complete collected album; retries stay per actual file."""

from copy import deepcopy

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.file_outcomes import FileOutcomes
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.work_actions import thaw
from app.telegram_bots.work_album_inputs import album_context
from app.telegram_bots.work_file_albums import album_manifest
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow
from deerflow.utils.file_io import await_drained


class WorkAlbumAnswers:
    def __init__(self, files):
        self.files, self.services, self.cards, self.inbox = files, files.services, files.cards, files.inbox

    async def submit(self, channel, origin, *, recover=False):
        async def perform():
            link = deepcopy(channel.link)
            guard = self.services.commands._guard(channel, link)
            async with self.services.callbacks.writer(link, guard) as writer:
                mark = (await self.inbox._rows(writer, [origin]))[0]
                self.inbox._current(link, mark)
                if "work_album" in mark.route_basis:
                    root_id = album_context(mark)["root_message_id"]
                else:
                    manifest = album_manifest(mark, link)
                    if manifest["state"] != "ready":
                        return True
                    root_id = manifest["root_message_id"]
                root = await writer.get(TelegramInboundMarkRow, (link.id, root_id, link.chat_id), with_for_update=True, populate_existing=True)
                if root is None:
                    raise CallbackRefused()
                if root.route_complete:
                    return True
                captured = InboxRow.from_row(root)
                source = root.route_basis["source_outbox_id"]
            captured = await self.cards.capture(link, captured, source_outbox_id=source, guard=guard)
            context = album_context(captured)
            if "work_action_id" not in captured.route_basis:
                async with self.services.callbacks.writer(link, guard) as writer:
                    row = TelegramInboundMarkRow
                    members = list(await writer.scalars(select(row).where(row.link_id == link.id, row.chat_id == link.chat_id, row.message_id.in_(context["member_ids"]))))
                    indexed = {mark.message_id: InboxRow.from_row(mark) for mark in members}
                complete = True
                for identity in context["member_ids"]:
                    member = indexed[identity]
                    file = thaw(member.sanitized_input["files"][0])
                    if recover:
                        receipt = await self.services.file_puts.reconcile_input(link, member, file, origins=None, authority=self.services.authority, current=lambda: channel._callback_current(link))
                    else:
                        receipt = await channel._own_instance_file(link, member, file)
                    if receipt.state != "done":
                        complete = False
                        await FileOutcomes(self.services).plan(channel, link, member, file)
                if not complete:
                    return True
            staged = await self.cards.stage(link, captured, guard=guard)
            if staged.attempts == 0:
                await self.files.inputs.callbacks.acts.check(staged.id, expected={"id": staged.id, "kind": staged.kind, "body": staged.body, "attempts": staged.attempts}, attempt_current=lambda: channel._callback_current(link))
            else:
                await self.files.inputs.callbacks.acts.reconcile_receipt(staged.id)
            await self.files.inputs.callbacks._observe(link, staged.id, guard=guard)
            return True

        return await await_drained(perform())
