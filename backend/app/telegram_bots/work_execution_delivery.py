"""One durable reply follows each source-qualified chat execution intent."""

from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired, _item, keyboard_cleanup_row
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_execution_sources import WorkExecutionProjection, execution_origin_rows, qualify_execution_origin, require_execution_scope
from app.telegram_bots.work_news_delivery import WorkNewsPlans, frozen_news
from app.telegram_bots.work_news_recovery import WorkNewsRecovery
from app.telegram_bots.work_result_navigation import NavigationOrigin
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def require_shown_act(saved, shown, link):
    if (
        saved is None
        or saved.id != shown["id"]
        or shown["kind"] != "activate"
        or any(getattr(saved, key) != shown[key] for key in _ACT_SCOPE)
        or not same_body(saved.body, shown["body"])
        or (saved.link_id, saved.owner_user_id, saved.bot_id, saved.telegram_user_id, saved.binding_generation, saved.chat_id, saved.instance_id)
        != (link.id, link.owner_user_id, link.bot_id, link.telegram_user_id, link.binding_generation, link.chat_id, link.agent_ref)
    ):
        raise CallbackRefused()


class WorkExecutionPlans(WorkNewsPlans):
    def __init__(self, *, acts, product="HartMesh", **kwargs):
        super().__init__(**kwargs, projection=WorkExecutionProjection)
        self.acts = acts
        self.reader.product = product
        self.reader.acts = acts
        if acts.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind"):
            raise ValueError("work-execution-requires-shared-storage")

    async def observe(self, link, act_id, *, guard=None):
        async def perform():
            if guard is not None:
                from app.telegram_bots.links import prepare_publication_guard

                await prepare_publication_guard(guard)
            # Receipt observation may settle the journal, but never calls check(),
            # commands, launching or physical cancellation.
            captured = await self.acts._work_record(act_id)
            require_execution_scope(link, captured)
            try:
                act = await self.acts.reconcile_receipt(act_id)
            except Exception:
                # Receipt failure proves neither completion nor absence. The
                # final source still qualifies this exact saved journal.
                act = await self.acts._work_record(act_id)
            require_execution_scope(link, act)
            key = "notice:work-execution:" + act_id
            async with self.outbox._writer(link.id) as (writer, _binding):
                if guard is not None:
                    await guard.check(writer)
                journal = await writer.get(TelegramActRow, act_id, with_for_update=True, populate_existing=True)
                if journal is None or journal.state != act.state or journal.attempts != act.attempts:
                    raise ProductSourceRetired()
                row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
                if row is None:
                    if act.state == "pending" or act.kind == "activate" and act.state == "done":
                        return None
                    now = self.outbox.clock()
                    callback, parent, controls, primary = await execution_origin_rows(writer, journal)
                    if parent is None or callback.message_thread_id != parent.message_thread_id:
                        raise ProductSourceRetired()
                    origin = qualify_execution_origin(link, _snapshot(journal), callback, parent, controls, activation_parent=primary)
                    row = TelegramOutboxRow(
                        id=uuid4().hex,
                        link_id=link.id,
                        owner_user_id=link.owner_user_id,
                        binding_generation=link.binding_generation,
                        chat_id=link.chat_id,
                        instance_id=link.agent_ref,
                        work_id=act.work_id,
                        dedupe_key=key,
                        class_="reply",
                        payload={"presentation_version": 0, "work_execution_intent": {"version": 1, "act_id": act_id, "origin": origin}},
                        parts=[],
                        state="pending",
                        silent=True,
                        message_thread_id=parent.message_thread_id,
                        not_before=now,
                        created_at=now,
                    )
                    writer.add(row)
                    await writer.flush()
                identity = row.id
                if guard is not None:
                    await guard.check(writer)
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, identity)
                item = _item(row)
                claimed = any(getattr(row, key) is not None for key in ("claim_owner", "claim_id", "claimed_at"))
            if claimed:
                return identity
            current = await self.reader.inspect(link, item)
            source = current.source
            if source is None and current.act.state != "done":
                # A pending recovery is no proof of completion. Keep any fully
                # acknowledged Cancel/Retry while its outcome is still unknown.
                return identity
            if not item.parts and source is not None:
                await self.materialize(link, identity, guard=guard)
                return identity
            basis, prior = None, None
            if item.parts:
                basis, prior, _ = frozen_news(SimpleNamespace(link=link, item=item, part=None))
            settings = await self.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            origin = NavigationOrigin()
            if source is not None and prior is not None and same_body(asdict(source), asdict(prior)) and settings.locale == basis["locale"] and origin.base == basis["public_base_url"]:
                return identity
            admission = await prepare_attention(self.reader, link)
            async with self.outbox._writer(link.id) as (writer, _binding):
                if guard is not None:
                    await guard.check(writer)
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                if _item(row) != item or any(getattr(row, key) is not None for key in ("claim_owner", "claim_id", "claimed_at")):
                    return identity
                await admission(session=writer, projection=current)
                if source is None:
                    # Never discard an in-flight SDK claim. Once its ACK is
                    # retained, old controls may close behind the status root.
                    groups = {part.get("callback_group_id") for part in row.parts} - {None}
                    controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups))))
                    cleaned = set()
                    for control in controls:
                        if control.consumed_at is None:
                            control.consumed_at, control.closed_reason = self.outbox.clock(), "completed"
                        if control.message_id is not None and control.group_id not in cleaned:
                            cleaned.add(control.group_id)
                            writer.add(keyboard_cleanup_row(control, owner_user_id=link.owner_user_id, now=self.outbox.clock()))
                    if row.state == "pending":
                        row.state, row.skip_reason, row.closed_at = "skipped", "superseded", self.outbox.clock()
                else:
                    await WorkNewsRecovery(self)._replace(writer, link, row, source, settings, origin, direct_edit=True, unclaimed=True)
                    await admission(session=writer, projection=current.with_item(_item(row)))
                    origin.check(row.payload["work_news_basis"])
                if guard is not None:
                    await guard.check(writer)
            return identity

        return await await_drained(perform())

    async def cancel(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            await prepare_publication_guard(guard)
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id")) if callback is not None else None
                if (
                    callback is None
                    or parent is None
                    or (callback.link_id, callback.chat_id, callback.binding_generation, callback.message_id, callback.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original = acknowledged_view(link, parent, callback.payload.get("presentation_version"))
                source = original.payload["work_news_basis"]["source"]
                if (
                    source["key"] != "execution.activate"
                    or not source["work"]["notice"]["cancel"]
                    or callback.kind != "cancel_wait"
                    or callback.target_kind != "act"
                    or callback.target_id != source["work"]["act"]["id"]
                    or callback.payload.get("act_id") != callback.target_id
                ):
                    raise CallbackRefused()
                act_id, signature = callback.target_id, anchor_signature(original)
                saved = await reader.get(TelegramActRow, act_id)
                require_shown_act(saved, source["work"]["act"], link)
                replay = callback.payload.get("work_action_id") == act_id
                if replay and saved.cancel_requested_at is None and saved.state != "done":
                    raise CallbackRefused()
            if not replay:

                async def admission(writer, act):
                    await guard.check(writer)
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                    row = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                    shown = acknowledged_view(link, row, group.presentation_version)
                    if anchor_signature(shown) != signature:
                        raise CallbackRefused()
                    target = await writer.get(TelegramActRow, act_id, with_for_update=True, populate_existing=True)
                    require_shown_act(target, source["work"]["act"], link)
                    rows = [await writer.get(TelegramCallbackRow, control.id, populate_existing=True) for control in group.rows]
                    _validate_rows(shown, rows)
                    if any(control.payload.get("work_action_id") is not None for control in rows):
                        raise CallbackRefused()
                    if act is not None:
                        for control in rows:
                            control.payload = {**control.payload, "work_action_id": act_id}
                        await writer.flush()
                    guard.current()

                try:
                    result = await self.acts.cancel_activation(act_id, admission=admission)
                except Exception:
                    result = await self.acts._work_record(act_id)
                    require_shown_act(result, source["work"]["act"], link)
                    if result.cancel_requested_at is None:
                        raise
            else:
                async with self.callbacks.writer(link, guard) as writer:
                    selected = await writer.get(TelegramCallbackRow, callback_id, with_for_update=True, populate_existing=True)
                    row = await writer.get(TelegramOutboxRow, selected.payload.get("anchor_outbox_id"), with_for_update=True, populate_existing=True) if selected else None
                    if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.message_thread_id, row.instance_id) != (
                        link.id,
                        link.owner_user_id,
                        link.binding_generation,
                        chat_id,
                        message_thread_id,
                        link.agent_ref,
                    ):
                        raise CallbackRefused()
                    controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == selected.group_id).with_for_update().execution_options(populate_existing=True)))
                    original = acknowledged_view(link, row, selected.payload.get("presentation_version"))
                    target = await writer.get(TelegramActRow, act_id, with_for_update=True, populate_existing=True)
                    require_shown_act(target, source["work"]["act"], link)
                    _validate_rows(original, controls)
                    if anchor_signature(original) != signature or any(control.payload.get("work_action_id") != act_id for control in controls) or target.cancel_requested_at is None and target.state != "done":
                        raise CallbackRefused()
                result = await self.acts.reconcile_receipt(act_id)
            await self.observe(link, act_id, guard=guard)
            return result

        return await await_drained(perform())

    async def retry(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id")) if callback else None
                if callback is None or parent is None:
                    raise CallbackRefused()
                original = acknowledged_view(link, parent, callback.payload.get("presentation_version"))
                source = original.payload["work_news_basis"]["source"]
                if source["key"] not in {"execution.activate", "execution.stop"} or not source["work"]["notice"]["retry"]:
                    raise CallbackRefused()
                expected = {**source["work"]["act"], "effective": source["work"]["effective"]}
                signature = anchor_signature(original)
            async with self.callbacks.writer(link, guard) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                row = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                original = acknowledged_view(link, row, group.presentation_version)
                saved = await writer.get(TelegramActRow, expected["id"], with_for_update=True, populate_existing=True)
                if (
                    anchor_signature(original) != signature
                    or group.selected.kind != "act_check"
                    or group.selected.target_kind != "act"
                    or group.selected.target_id != expected["id"]
                    or group.selected.payload.get("act_id") != expected["id"]
                    or saved is None
                    or any(getattr(saved, key) != expected[key] for key in _ACT_SCOPE)
                    or not same_body(saved.body, expected["body"])
                    or (saved.link_id, saved.owner_user_id, saved.binding_generation, saved.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.agent_ref)
                ):
                    raise CallbackRefused()

            def current():
                try:
                    guard.current()
                    return True
                except Exception:
                    return False

            result = await self.acts.check(expected["id"], expected=expected, attempt_current=current)
            await self.observe(link, result.id, guard=guard)
            return result

        return await await_drained(perform())
