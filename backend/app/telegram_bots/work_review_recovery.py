"""Keep unsubmitted notes when their original review closes or is replaced."""

from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
from app.telegram_bots.work_input_capture import WorkInputCaptures
from app.telegram_bots.work_input_previews import WorkInputPreviews, _InputGuard, _InputProjection, lock_input_origin
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from deerflow.agent_instances.human_input import HumanInput
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


class WorkReviewRecovery:
    def __init__(self, plans):
        self.plans = plans
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
                if any("work_action_id" in row.payload for row in group.rows):
                    # An admitted confirmation belongs to its receipt/conflict
                    # owner, including when its journal has been collected.
                    raise CallbackRefused()
                shown = acknowledged_source(link, parent)
                await lock_input_origin(writer, link, parent, shown, draft_ttl_hours=self.plans.callbacks.config.callbacks.draft_ttl_hours)
                identity = basis["input"]
                origin = InboxRow.from_row(await writer.get(TelegramInboundMarkRow, (identity["link_id"], identity["message_id"], identity["chat_id"]), populate_existing=True))
            return await self.recover(link, origin, guard=guard)

        return await await_drained(perform())

    async def recover(self, link, origin, *, guard):
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
                await self.previews.open(link, captured, guard=guard, request_id=replacement)
                return True
            admission = await prepare_attention(self.plans.reader, link)
            composed = _ClosedGuard(guard, admission, source, captured)
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
        projection = _ClosedProjection(self.source, self.captured, self.complete)
        held, _target = await self.admission(session=writer, projection=projection)
        if "agents:write" in held and projection.replacement:
            raise DeliveryRetry(timedelta(seconds=5))
        self.base.current()
        return current


class _ClosedProjection(_InputProjection):
    def __init__(self, source, captured, complete):
        super().__init__(source, captured)
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
        self.replacement = rows[0][2] & 6 == 6 and any(row[13] is not None and HumanInput._source_current(row[13], work) for row in rows)
