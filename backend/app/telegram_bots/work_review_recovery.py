"""Keep unsaved notes when their original review closes or is replaced."""

from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_control_admission import command
from app.telegram_bots.work_input_capture import WorkInputCaptures
from app.telegram_bots.work_input_previews import WorkInputPreviews, _InputGuard, _InputProjection, lock_input_origin
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from deerflow.agent_instances.human_input import HumanInput
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


class WorkReviewRecovery:
    def __init__(self, plans, *, controls=None):
        self.plans, self.controls = plans, controls
        self.captures = WorkInputCaptures(plans.outbox.inbox, plans.callbacks)
        self.previews = WorkInputPreviews(plans, self.captures)

    async def from_callback(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            async with self.plans.callbacks.writer(link, guard) as writer:
                group = await self.plans.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                basis = parent.payload.get("work_request_basis", {})
                if basis.get("version") != 4 or basis.get("interaction", {}).get("view") != "changes" or group.selected.kind not in {"changes_confirm", "changes_open"}:
                    return False
                shown = acknowledged_source(link, parent)
                conclusive = None
                if any("work_action_id" in row.payload for row in group.rows):
                    conclusive = await self._conclusive_confirmation(writer, link, group, shown)
                await lock_input_origin(writer, link, parent, shown, draft_ttl_hours=self.plans.callbacks.config.callbacks.draft_ttl_hours)
                identity = basis["input"]
                origin = InboxRow.from_row(await writer.get(TelegramInboundMarkRow, (identity["link_id"], identity["message_id"], identity["chat_id"]), populate_existing=True))
            return await self.recover(link, origin, guard=guard, conclusive_act=conclusive)

        return await await_drained(perform())

    async def _conclusive_confirmation(self, writer, link, group, shown):
        identity = group.selected.payload.get("work_action_id")
        if self.controls is None or not isinstance(identity, str) or any(row.payload.get("work_action_id") != identity for row in group.rows):
            raise CallbackRefused()
        original = next((row for row in group.rows if row.id == identity and row.kind == "changes_confirm"), None)
        saved = await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True)
        if original is None or saved is None:
            raise CallbackRefused()
        kind, request = command(self.controls.acts, original, shown)
        if (
            saved.state != "conflict"
            or (saved.link_id, saved.owner_user_id, saved.bot_id, saved.telegram_user_id, saved.binding_generation, saved.chat_id, saved.instance_id, saved.work_id, saved.request_id, saved.kind)
            != (link.id, link.owner_user_id, link.bot_id, link.telegram_user_id, link.binding_generation, link.chat_id, link.agent_ref, shown.work["id"], shown.request["id"], kind)
            or not same_body(saved.body, request.model_dump(mode="json", exclude_unset=True))
        ):
            raise CallbackRefused()
        return _snapshot(saved)

    async def recover(self, link, origin, *, guard, conclusive_act=None):
        async def perform():
            async with self.plans.callbacks.writer(link, guard) as writer:
                mark = (await self.captures.inbox._rows(writer, [origin]))[0]
                self.captures.inbox._current(link, mark)
                if origin.link_id != link.id or mark.route_kind != "work_input" or mark.route_basis["work_input"]["purpose"] != "request_changes":
                    return False
                if mark.route_complete:
                    return True
                captured = await self.captures.capture(link, origin, prompt_id=mark.route_basis["work_input"]["prompt_id"], guard=guard, session=writer)
            data = captured.route_basis["work_input"]
            source = await self.plans.reader.read_request(link, data["request_id"])
            if source.request["state"] != "closed":
                return False
            replacement = await self._replacement(link, source)
            if replacement is not None:
                await self.previews.open(link, captured, guard=guard, request_id=replacement, conclusive_act=conclusive_act)
                return True
            admission = await prepare_attention(self.plans.reader, link)
            composed = _ClosedGuard(guard, admission, source, captured, conclusive_act=conclusive_act)
            async with self.plans.callbacks.writer(link, composed) as writer:
                await self.previews._retire_previous(writer, link, {"operation_id": captured.operation_ids[0]})
                key = "stale.withdrawn" if source.request.get("closed_reason") == "withdrawn" else "stale.closed"
                await self.plans.outbox.plan_product_keyboard(
                    link,
                    source_id="work-review-retired:" + captured.operation_ids[0],
                    parts=[{"kind": "text", "source_ref": {"key": key, "values": {}}, "reply_to_message_id": captured.message_id}],
                    keyboard=[],
                    callbacks=self.plans.callbacks,
                    guard=composed,
                    inbox_rows=[captured],
                    finish_route=False,
                    session=writer,
                )
                await self.captures.inbox.finish_route(captured, session=writer)
                composed.complete = True
            return True

        return await await_drained(perform())

    async def _replacement(self, link, source):
        if not source.request.get("can_recover_management"):
            return None
        human, offset, matches = HumanInput(self.plans.reader.work), 0, []
        while True:
            page = await human.list(actor=PrincipalRef("human", link.owner_user_id), view="pending", instance_id=link.agent_ref, work_id=source.work["id"], limit=100, offset=offset)
            matches.extend(row["id"] for row in page["requests"] if row["purpose"] == "review" and row["id"] != source.request["id"] and row["can_manage"])
            if not page["has_more"]:
                break
            if not page["requests"]:
                raise CallbackRefused()
            offset += len(page["requests"])
        if len(matches) > 1:
            raise CallbackRefused()
        return matches[0] if matches else None


class _ClosedGuard(_InputGuard):
    complete = False

    @property
    def captured_link(self):
        return self.base.captured_link

    def current(self):
        return self.base.current()

    async def check(self, writer):
        current = await self.base.check(writer)
        projection = _ClosedProjection(self.source, self.captured, self.complete, conclusive_act=self.conclusive_act)
        held, _target = await self.admission(session=writer, projection=projection)
        if "agents:write" in held and projection.replacement:
            raise DeliveryRetry(timedelta(seconds=5))
        self.base.current()
        return current


class _ClosedProjection(_InputProjection):
    def __init__(self, source, captured, complete, *, conclusive_act=None):
        super().__init__(source, captured, conclusive_act=conclusive_act)
        self.complete = complete
        self.replacement = False

    def extend(self, query):
        candidate = aliased(HumanInputRequestRow)
        return (
            super()
            .extend(query)
            .add_columns(candidate)
            .outerjoin(
                candidate,
                and_(candidate.work_id == AgentWorkRow.id, candidate.state == "pending", candidate.purpose == "review", candidate.recipient_id == TelegramBotLinkRow.owner_user_id),
            )
        )

    def check(self, rows):
        work, request, unresolved = rows[0][8:11]
        if work.revision != self.source.seen["work_revision"] or request.revision != self.source.seen["revision"]:
            raise DeliveryRetry(timedelta(seconds=5))
        if unresolved or request.state != "closed" or request.closed_reason != self.source.request.get("closed_reason"):
            raise ProductSourceRetired()
        self._check_mark(rows, complete=self.complete)
        self.replacement = rows[0][2] & 6 == 6 and any(row[-1] is not None and HumanInput._source_current(row[-1], work) for row in rows)
