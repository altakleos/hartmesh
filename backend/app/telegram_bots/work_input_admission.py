"""Admit captured Work replies and changes against their original shown basis."""

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_control_admission import _ActionGuard
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_request_source import same_displayed_basis
from deerflow.agent_instances.contract import AgentConflict
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def command(acts, captured):
    data = captured.route_basis["work_input"]
    identity, seen = captured.operation_ids[0], data["seen"]
    if data["purpose"] in {"answer", "cannot_provide", "wrong_recipient"}:
        return "respond", Respond(
            operation_id=identity,
            expected_request_revision=seen["request_revision"],
            expected_assignment_revision=seen["assignment_revision"],
            disposition="supplied" if data["purpose"] == "answer" else data["purpose"],
            text=data["text"],
        )
    if data["purpose"] == "request_changes":
        return "changes_requested", acts._changes_command(operation_id=identity, request_id=data["request_id"], note=data["text"], seen=seen)
    # Own decisions and notes still need an acknowledged explicit confirmation.
    raise CallbackRefused()


class WorkInputAdmissions:
    def __init__(self, controls, captures):
        self.controls, self.captures = controls, captures
        if captures.inbox.session_factory.kw.get("bind") is not controls.outbox.sessions.kw.get("bind"):
            raise ValueError("work-input-requires-shared-storage")

    async def stage(self, link, origin, *, guard):
        async def perform():
            if origin.link_id != link.id:
                raise CallbackRefused()
            async with self.controls.outbox.sessions() as reader:
                row = (await self.captures.inbox._rows(reader, [origin]))[0]
                if row.route_kind != "work_input":
                    raise CallbackRefused()
                prompt_id = row.route_basis["work_input"]["prompt_id"]
                original = InboxRow.from_row(row)
            captured = await self.captures.capture(link, original, prompt_id=prompt_id, guard=guard)
            kind, request = command(self.controls.acts, captured)
            data, identity = captured.route_basis["work_input"], captured.operation_ids[0]
            await self.controls.acts._work_link(link)
            owner = await self.controls.acts.authority.prepare_live_owner(link.owner_user_id)
            async with self.controls.outbox.sessions() as reader:
                existing = await reader.get(TelegramActRow, identity)
                parent = await reader.get(TelegramOutboxRow, data["source_outbox_id"])
                shown = acknowledged_source(link, parent)
            if "work_action_id" in captured.route_basis and (captured.route_basis["work_action_id"] != identity or existing is None):
                raise CallbackRefused()
            if existing is None:
                current, _settings = await self.controls.recovery._source(link, data["request_id"])
                if not same_displayed_basis(shown, current):
                    raise AgentConflict("Review the current request and all responses")
                admission = await prepare_attention(self.controls.plans.reader, link)
                composed = _ActionGuard(guard, admission, current, identity, kind)
            else:
                composed = guard
            async with self.controls.callbacks.writer(link, composed) as writer:
                if existing is not None and await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True) is None:
                    raise CallbackRefused()
                latest = await self.captures.capture(link, original, prompt_id=prompt_id, guard=composed, session=writer)
                if latest.operation_ids != captured.operation_ids or not same_body(latest.route_basis, captured.route_basis):
                    raise CallbackRefused()
                if "work_action_id" in latest.route_basis and (latest.route_basis["work_action_id"] != identity or await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True) is None):
                    raise CallbackRefused()
                result = await self.controls.acts._stage_work_act(
                    link,
                    kind=kind,
                    request_id=data["request_id"],
                    work_id=data["work_id"],
                    request=request,
                    owner=owner,
                    session=writer,
                )
                mark = await writer.get(TelegramInboundMarkRow, (captured.link_id, captured.message_id, captured.chat_id), populate_existing=True)
                mark.route_basis = {**mark.route_basis, "work_action_id": identity}
                return result

        return await await_drained(perform())
