"""Typed Stop admits immutable targets through the existing Work journal."""

from datetime import timedelta
from uuid import uuid4

from sqlalchemy import select

from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired, _item, keyboard_cleanup_row
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_command_stop_choices import CommandStopChoicePlans, CommandStopChoiceReader, choice_intent
from app.telegram_bots.work_command_stop_origins import CommandStopProof, command_basis, operation_id, selected_targets
from app.telegram_bots.work_news_delivery import WorkNewsSources
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_origins import anchor_signature
from app.telegram_bots.work_views import work_buttons
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkCommandStops:
    def __init__(self, chat):
        self.chat, self.host = chat, chat.host
        self.outbox, self.callbacks = self.host.outbox, self.host.callbacks
        self.choices = CommandStopChoicePlans(reader=CommandStopChoiceReader(work=chat.views.reader.work, authority=self.host.authority, links=self.host.links, clock=self.outbox.clock), outbox=self.outbox, callbacks=self.callbacks)
        self.host.work_command_stop_sources = WorkNewsSources(self.choices)

    async def recover(self, link, origin, *, guard):
        await guard.prepare()
        async with self.callbacks.writer(link, guard) as writer:
            mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
            self.outbox.inbox._current(link, mark)
            if not same_body(origin_facts(InboxRow.from_row(mark)), origin_facts(origin)):
                raise ProductSourceRetired()
            basis = command_basis(mark)
            incoming = InboxRow.from_row(mark)
        if basis["selection"] is None:
            from app.telegram_bots.work_command_stop_lifetime import expire_unselected

            if await expire_unselected(self, link, incoming, guard=guard):
                return True
            await self.open_choices(link, incoming, basis, guard=guard)
        else:
            await self.run(link, incoming, basis, guard=guard)
        return True

    async def command(self, channel, link, origin, *, original, guard):
        async def perform():
            nonlocal origin
            await guard.prepare()
            if origin.route_kind == "command_stop":
                basis = command_basis(origin)
            elif origin.route_kind is not None:
                return False
            else:
                if original is not None:
                    return False
                try:
                    snapshot = await self.chat.views.reader.collect(link, as_of=self.outbox.clock())
                except AgentDenied:
                    return False
                targets = []
                for record in snapshot["work"]:
                    if (record.get("attempt") or {}).get("status") not in {"starting", "running"}:
                        continue
                    detail = await self.chat.views.reader.detail(link, record["id"])
                    buttons = work_buttons(detail.record, owner_id=link.owner_user_id, permissions=detail.permissions, held=detail.held, request=detail.request)
                    for line in buttons:
                        targets.extend({"work_id": detail.record["id"], "attempt_id": button["target_id"]} for button in line if button.get("kind") == "stop_attempt")
                if not targets:
                    return False
                selection = {"kind": "command", "choice": "attempt:" + targets[0]["attempt_id"]} if len(targets) == 1 else None
                basis = {"version": 1, "input": origin_facts(origin), "owner_user_id": link.owner_user_id, "instance_id": link.agent_ref, "answer": None, "targets": targets, "selection": selection}
                async with self.callbacks.writer(link, guard) as writer:
                    mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
                    self.outbox.inbox._current(link, mark)
                    if mark.route_kind is not None or not same_body(origin_facts(InboxRow.from_row(mark)), basis["input"]):
                        raise ProductSourceRetired()
                    origin = await self.outbox.inbox.route(
                        origin,
                        kind="command_stop",
                        target_id=digest(["command-stop", basis["input"]])[:32],
                        basis={"stop_command": basis},
                        operation_ids=[operation_id(basis["input"], targets[0])] if selection is not None else [],
                        session=writer,
                    )
            if basis["selection"] is None:
                from app.telegram_bots.work_command_stop_lifetime import expire_unselected

                if await expire_unselected(self, link, origin, guard=guard):
                    return True
                await self.open_choices(link, origin, basis, guard=guard)
                return True
            await self.run(link, origin, basis, guard=guard)
            return True

        return await await_drained(perform())

    async def open_choices(self, link, origin, basis, *, guard):
        key = "notice:command-stop:" + digest(basis["input"])
        async with self.callbacks.writer(link, guard) as writer:
            mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
            if not same_body(command_basis(mark), basis):
                raise ProductSourceRetired()
            row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
            if row is None:
                now = self.outbox.clock()
                row = TelegramOutboxRow(
                    id=uuid4().hex,
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    binding_generation=link.binding_generation,
                    chat_id=link.chat_id,
                    instance_id=link.agent_ref,
                    message_thread_id=origin.message_thread_id,
                    class_="reply",
                    dedupe_key=key,
                    payload={"presentation_version": 0, "command_stop_intent": {"version": 1, "message_id": origin.message_id, "chat_id": origin.chat_id, "signature": digest(basis)}},
                    parts=[],
                    state="pending",
                    silent=True,
                    created_at=now,
                    not_before=now,
                )
                writer.add(row)
                await writer.flush()
            await self.outbox.inbox.register_delivery([origin], outbox_id=row.id, finish=False, session=writer)
            identity = row.id
        await self.choices.materialize(link, identity, guard=guard)
        return identity

    async def run(self, link, origin, basis, *, guard):
        failure = None
        for target in selected_targets(basis):
            try:
                await self.run_target(link, origin, basis, target, guard=guard)
            except Exception as exc:
                # Each original target owns an independent promise. A failed
                # read or observation cannot prevent another target settling.
                guard.current()
                failure = failure or exc
        if failure is not None:
            raise failure

    async def select(self, channel, control, *, link, guard):
        from types import SimpleNamespace

        from app.telegram_bots.work_news_delivery import frozen_news

        await guard.prepare()
        async with self.callbacks.writer(link, guard) as writer:
            row = await writer.get(TelegramCallbackRow, control.id, populate_existing=True)
            parent = await writer.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id"), populate_existing=True) if row is not None else None
            if row is None or parent is None:
                raise CallbackRefused()
            item = _item(parent)
            intent = choice_intent(item)
            mark = await writer.get(TelegramInboundMarkRow, (link.id, intent["message_id"], link.chat_id), with_for_update=True, populate_existing=True)
            if mark is None:
                raise CallbackRefused()
            basis = command_basis(mark)
            if basis["selection"] is not None:
                saved = basis["selection"]
                if saved["kind"] != "callback" or saved["origin"]["callback_id"] != row.id or saved["choice"] != row.payload.get("choice"):
                    raise CallbackRefused(None)
            else:
                group = await self.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                _frozen, source, _part = frozen_news(SimpleNamespace(link=link, item=item, part=None), acknowledged=True)
                members = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.selected.group_id).execution_options(populate_existing=True)))
                _validate_rows(item, members)
                if source.key != "command.stop" or not same_body(source.work["basis"], basis) or (row.kind, row.target_kind, row.target_id) != ("stop_which", "link", link.id):
                    raise CallbackRefused()
                selection = {
                    "kind": "callback",
                    "choice": row.payload["choice"],
                    "origin": {"callback_id": row.id, "anchor_outbox_id": parent.id, "presentation_version": parent.payload["presentation_version"], "signature": anchor_signature(item)},
                }
                basis = {**basis, "selection": selection}
                mark.route_basis = {**mark.route_basis, "stop_command": basis}
                mark.operation_ids = [operation_id(basis["input"], target) for target in selected_targets(basis)]
                command_basis(mark)
                await self.callbacks.consume_group(group, reason="stop-chosen", session=writer)
                cleanup = keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=self.outbox.clock())
                writer.add(cleanup)
                await writer.flush()
                await self.outbox.inbox.register_delivery([InboxRow.from_row(mark)], outbox_id=cleanup.id, finish=False, session=writer)
            incoming = InboxRow.from_row(mark)
            guard.current()
        await self.run(link, incoming, basis, guard=guard)

    async def run_target(self, link, origin, basis, target, *, guard):
        identity = operation_id(basis["input"], target)
        marker = {"version": 1, "basis": basis, "target": target}

        async def admission(writer, act):
            await guard.check(writer)
            mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
            if not same_body(command_basis(mark), basis):
                raise ProductSourceRetired()
            if act is not None:
                row = await writer.get(TelegramActRow, identity, populate_existing=True)
                if row.continuation not in (None, {}) and not same_body(row.continuation, {"command_stop_source": marker}):
                    raise ProductSourceRetired()
                row.continuation = {"command_stop_source": marker}
                await writer.flush()
                proof = CommandStopProof(link, row)
                await proof.read(writer)
                row.first_attempt_at = proof.accepted_at
                if self.outbox.clock() >= proof.accepted_at + timedelta(hours=1):
                    row.state, row.reason = "refused", "stop-consent-expired"
                    if row.stop_dispatch is not None:
                        row.stop_dispatch = {**row.stop_dispatch, "state": "ended"}
                await writer.flush()
            guard.current()

        act = await self.host.acts.stage_stop(link, operation_id=identity, **target, admission=admission)
        act = await self.host.acts._work_record(act.id)
        async with self.outbox.sessions() as reader:
            await CommandStopProof(link, act).read(reader)

        def current():
            try:
                guard.current()
                return True
            except Exception:
                return False

        if act.state == "pending" and act.attempts == 0:
            await self.host.acts.check(act.id, expected={"id": act.id, "kind": act.kind, "body": act.body, "attempts": act.attempts}, attempt_current=current)
        guard.current()
        await self.chat.executions.observe(link, identity, guard=guard)
