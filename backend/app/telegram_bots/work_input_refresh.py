"""Refresh a changed input-owned confirmation without replacing its typed text."""

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_control_admission import _ActionGuard, _ActionProjection, command
from app.telegram_bots.work_input_capture import WorkInputCaptures
from app.telegram_bots.work_input_previews import WorkInputPreviews, _InputProjection, input_state, lock_input_origin
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_request_source import same_displayed_basis
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkInputRefresh:
    def __init__(self, controls):
        self.controls, self.plans = controls, controls.plans
        self.previews = WorkInputPreviews(self.plans, WorkInputCaptures(self.plans.outbox.inbox, self.plans.callbacks))

    async def refresh(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            async with self.plans.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.kind not in {"decide_confirm", "changes_confirm", "changes_open", "back", "decide_note"}:
                    raise CallbackRefused()
                request_id = callback.target_id
                kind = {"decide_confirm": "decide", "changes_confirm": "changes_requested", "changes_open": "changes_requested"}.get(callback.kind)
            source, settings = await self.previews.recovery._source(link, request_id)
            admission = await prepare_attention(self.plans.reader, link)
            composed = _RefreshGuard(guard, admission, source, callback_id, kind)
            async with self.plans.callbacks.writer(link, composed) as writer:
                group = await self.plans.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                basis = parent.payload.get("work_request_basis", {})
                if basis.get("version") != 4:
                    raise CallbackRefused()
                shown = acknowledged_source(link, parent)
                saved = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
                if saved is None:
                    if any("work_action_id" in row.payload for row in group.rows):
                        raise CallbackRefused()
                else:
                    expected_kind, request = command(self.controls.acts, group.selected, shown)
                    if (
                        (saved.link_id, saved.owner_user_id, saved.binding_generation, saved.chat_id, saved.instance_id, saved.work_id, saved.request_id, saved.kind)
                        != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref, source.work["id"], request_id, expected_kind)
                        or not (saved.state == "conflict" or saved.state == "pending" and saved.reason == "attempt-unresolved" and saved.cancel_requested_at is None)
                        or not same_body(saved.body, request.model_dump(mode="json", exclude_unset=True))
                        or any(row.payload.get("work_action_id") != saved.id for row in group.rows)
                    ):
                        raise CallbackRefused()
                if same_displayed_basis(shown, source):
                    return None
                if saved is not None and saved.state == "pending":
                    saved.state, saved.reason = "conflict", "changed"
                    saved.cancel_requested_at = saved.updated_at = self.plans.outbox.clock()
                    await writer.flush()
                # Only the conclusive old refusal may be fenced above. Unknown
                # or completed operations still block this input proof.
                await lock_input_origin(writer, link, parent, shown, draft_ttl_hours=self.plans.callbacks.config.callbacks.draft_ttl_hours)
                identity = basis["input"]
                mark = await writer.get(TelegramInboundMarkRow, (identity["link_id"], identity["message_id"], identity["chat_id"]), populate_existing=True)
                captured = InboxRow.from_row(mark)
                composed.captured = captured
                state = input_state(captured.route_basis["work_input"], source)
                return await self.previews._publish(writer, link, captured, source, settings, state, guard=composed)

        return await await_drained(perform())


class _RefreshGuard(_ActionGuard):
    captured = None

    async def check(self, writer):
        if self.captured is None:
            return await super().check(writer)
        current = await self.base.check(writer)
        # The exact known refusal is allowed only until it is fenced. After
        # publication, source, pause and original unblocked input share the
        # final snapshot after the outer guard's last await.
        held, _target = await self.admission(session=writer, projection=_RefreshProjection(self.source, self.captured, self.kind))
        if "agents:write" not in held:
            raise AgentDenied("The preview is unavailable")
        self.base.current()
        return current


class _RefreshProjection(_InputProjection):
    def __init__(self, source, captured, kind):
        super().__init__(source, captured)
        self.kind = kind

    def check(self, rows):
        super().check(rows)
        _ActionProjection(self.source, allowed_act_id=None, kind=self.kind).check(rows)
