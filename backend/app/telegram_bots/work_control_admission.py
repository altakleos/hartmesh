"""Atomically reserve a Work button choice and its immutable service operation."""

import json

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _plain
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_interactions import interaction_state
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryProjection
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def command(acts, callback, source):
    payload = _plain(callback.payload)
    if callback.target_kind != "request" or callback.target_id != source.request["id"] or payload.get("request_id") != source.request["id"] or payload.get("work_id") != source.work["id"]:
        raise CallbackRefused()
    if json.dumps(payload.get("seen"), sort_keys=True) != json.dumps(source.seen, sort_keys=True):
        raise AgentConflict("Review the current request and all responses")
    seen = payload["seen"]
    if callback.kind == "hi_choice":
        if source.request["purpose"] != "information" or not source.request["can_respond"] or payload.get("choice") not in source.request["choices"]:
            raise CallbackRefused()
        return "respond", Respond(operation_id=callback.id, expected_request_revision=seen["request_revision"], expected_assignment_revision=seen["assignment_revision"], choice=payload["choice"])
    state = interaction_state(payload.get("interaction"), source)
    if callback.kind == "decide_confirm" and state["view"] == "decision":
        return "decide", acts._decision_command(operation_id=callback.id, request_id=source.request["id"], decision=state["decision"], note=state["note"], seen=seen)
    if callback.kind == "accept_confirm" and state["view"] == "accept":
        return "accept", acts._acceptance_command(operation_id=callback.id, request_id=source.request["id"], outcome_id=seen["outcome_id"], evidence_revision=seen["evidence_revision"], seen=seen)
    raise CallbackRefused()


class WorkControlAdmissions:
    def __init__(self, plans, acts):
        self.plans, self.acts = plans, acts
        self.recovery = WorkRequestRecovery(plans)
        self.callbacks, self.outbox = plans.callbacks, plans.outbox
        if acts.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind"):
            raise ValueError("work-controls-require-shared-storage")

    async def stage(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        """Durably admit one acknowledged choice; the receipt worker owns execution."""

        async def perform():
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                existing = await reader.get(TelegramActRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.target_kind != "request":
                    raise CallbackRefused()
                request_id = callback.target_id
            await self.acts._work_link(link)
            owner = await self.acts.authority.prepare_live_owner(link.owner_user_id)
            if existing is None:
                source, _settings = await self.recovery._source(link, request_id)
                admission = await prepare_attention(self.plans.reader, link)
                kind, _request = command(self.acts, callback, source)
                composed = _ActionGuard(guard, admission, source, callback_id, kind)
            else:
                source, composed = None, guard
            async with self.callbacks.writer(link, composed) as writer:
                if existing is None:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                    parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                    captured = acknowledged_source(link, parent)
                    if captured != source:
                        raise AgentConflict("Review the current request and all responses")
                    rows = [await writer.get(TelegramCallbackRow, row.id) for row in group.rows]
                    selected = next(row for row in rows if row.id == callback_id)
                else:
                    # A replay may inspect an old acknowledged basis only while
                    # its original journal still exists. Retention must never
                    # turn this weaker path into a fresh operation admission.
                    saved = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
                    if saved is None:
                        raise CallbackRefused()
                    parent, rows, selected = await self.callbacks._locked(writer, link, callback_id)
                    captured = acknowledged_source(link, parent)
                if (selected.chat_id, selected.message_id, selected.message_thread_id) != (chat_id, message_id, message_thread_id):
                    raise CallbackRefused()
                if any(row.payload.get("work_action_id") not in {None, callback_id} for row in rows):
                    raise CallbackRefused(None)
                kind, request = command(self.acts, selected, captured)
                result = await self.acts._stage_work_act(link, kind=kind, request_id=request_id, work_id=captured.work["id"], request=request, owner=owner, session=writer)
                for row in rows:
                    row.payload = {**row.payload, "work_action_id": result.id}
                return result

        return await await_drained(perform())


class _ActionGuard:
    def __init__(self, base, admission, source, identity, kind):
        if not callable(getattr(base, "current", None)):
            raise CallbackRefused()
        self.base, self.admission, self.source, self.identity, self.kind = base, admission, source, identity, kind

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        held, _target = await self.admission(session=writer, projection=_ActionProjection(self.source, allowed_act_id=self.identity, kind=self.kind))
        if "agents:write" not in held:
            raise AgentDenied("The action is unavailable")
        self.base.current()
        return current


class _ActionProjection(_RecoveryProjection):
    def __init__(self, source, *, allowed_act_id, kind):
        super().__init__(source, allowed_act_id=allowed_act_id)
        self.kind = kind

    def check(self, rows):
        super().check(rows)
        if self.kind in {"decide", "accept"} and rows[0][0].acts_paused_since is not None:
            raise CallbackRefused("link.limited")
