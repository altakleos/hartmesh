"""Back and Discard retire only their actual saved-work preview."""

from sqlalchemy.orm import aliased

from app.telegram_bots.assignment_sources import private_snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_edit_drafts import snapshot_edit
from app.telegram_bots.work_management_origins import displayed_management, origin_for
from app.telegram_bots.work_view_sources import detail_read
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def navigation_for(item):
    value = item.payload.get("work_management_navigation")
    if value is not None and (type(value) is not dict or set(value) != {"version", "kind", "origin"} or type(value["version"]) is not int or value["version"] != 1 or value["kind"] not in {"back", "discard"}):
        raise ProductSourceRetired()
    return value


class ManagementNavigationProof:
    def __init__(self, link, item):
        self.link, self.item, self.value = link, item, navigation_for(item)

    def extend(self, query):
        if self.value is None:
            return query
        self.column = len(query.column_descriptions)
        control, member = aliased(TelegramCallbackRow), aliased(TelegramCallbackRow)
        return query.add_columns(control, member).join(control, control.id == self.value["origin"]["callback_id"]).outerjoin(member, member.group_id == control.group_id)

    def check(self, rows, parent, root):
        if self.value is None:
            return
        try:
            control = rows[0][self.column]
            members = list({row[self.column + 1].id: row[self.column + 1] for row in rows if row[self.column + 1] is not None}.values())
            original, source = displayed_management(self.link, parent, control)
            _validate_rows(original, members)
            discard = self.value["kind"] == "discard"
            if (
                origin_for(original, control) != self.value["origin"]
                or control.kind != ("assign_discard" if discard else "back")
                or any(member.consumed_at is None or member.closed_reason != ("discarded" if discard else "back") for member in members)
                or (root is not None) != discard
                or source["work_id"] != parent.work_id
            ):
                raise ValueError()
            if discard and (root.state != "refused" or root.reason != "discarded" or not same_body(root.result, self.value) or not same_body(root.body, source["work"]["root"]["body"])):
                raise ValueError()
        except (ValueError, TypeError, KeyError, AttributeError, CallbackRefused):
            raise ProductSourceRetired() from None


class WorkManagementNavigation:
    def __init__(self, plans):
        self.plans, self.outbox, self.callbacks = plans, plans.outbox, plans.callbacks

    async def operate(self, link, callback_id, *, discard, chat_id, message_id, message_thread_id, guard):
        async def perform():
            from app.telegram_bots.work_management_sources import WorkManagementProjection, management_intent

            async with self.outbox.sessions() as reader:
                control = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id")) if control else None
                if (
                    control is None
                    or parent is None
                    or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, _source = displayed_management(link, parent, control)
                if control.kind != ("assign_discard" if discard else "back"):
                    raise CallbackRefused()
                item = _item(parent)
                intent = management_intent(item)
                witness = {"version": 1, "kind": "discard" if discard else "back", "origin": origin_for(original, control)}
            source = await self.plans.reader.read(link, item)
            act = private_snapshot(source.work["act"]) if source.work["act"] is not None else None
            if act is not None and not (discard and act.state == "refused" and act.reason == "unchanged" and act.attempts == 0):
                raise CallbackRefused()
            clock = await self.plans.reader.prepare_clock(link, item)
            current = await prepare_attention(self.plans.reader, link)
            async with self.callbacks.writer(link, guard) as writer:
                parent = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
                if parent is None or _item(parent) != item:
                    raise CallbackRefused()
                saved = navigation_for(_item(parent))
                await current(session=writer, projection=WorkManagementProjection(self.plans.reader, link, item, source, clock=clock))
                if saved is not None:
                    if not same_body(saved, witness):
                        raise CallbackRefused()
                else:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                    original, _source = displayed_management(link, parent, group.selected)
                    if origin_for(original, group.selected) != witness["origin"]:
                        raise CallbackRefused()
                    if discard:
                        root = await writer.get(TelegramActRow, intent["draft_id"], with_for_update=True, populate_existing=True)
                        snapshot_edit(link, root)
                        if root.state != "draft" or list(root.operation_ids) != ([act.id] if act else []):
                            raise CallbackRefused()
                        root.state, root.reason, root.result, root.updated_at = "refused", "discarded", witness, self.outbox.clock()
                    parent.payload = {**parent.payload, "work_management_navigation": witness}
                    await self.callbacks.consume_group(group, reason="discarded" if discard else "back", session=writer)
                    await writer.flush()
                    projection = WorkManagementProjection(self.plans.reader, link, _item(parent), detail=detail_read(source.work["detail"]), act=act, clock=clock)
                    await current(session=writer, projection=projection)
                    self.callbacks._unexpired(control)
                    guard.current()
            await self.plans.observe(link, item.id, guard=guard)
            return item.id

        return await await_drained(perform())
