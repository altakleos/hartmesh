"""Re-materialize current Work previews without repeating their business actions."""

from copy import deepcopy
from dataclasses import asdict
from datetime import timedelta

from sqlalchemy import and_, select, true

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, ProductSourceRetired, _safe_json, keyboard_cleanup_row
from app.telegram_bots.work_request_source import require_show
from app.telegram_bots.work_requests import present_request
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkRequestRecovery:
    family_versions = {1, 2}
    basis_version = 2

    def __init__(self, plans):
        self.plans = plans
        self.outbox, self.reader, self.callbacks = plans.outbox, plans.reader, plans.callbacks

    async def _source(self, link, request_id, *, intent=None):
        source = await self.reader.read_request(link, request_id)
        request = source.request
        if request["state"] != "pending" or request["needs_routing"] or request["recipient_id"] != link.owner_user_id or not (request["can_respond"] if request["purpose"] == "information" else request["can_manage"]):
            raise ProductSourceRetired()
        settings = await self.plans.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        return source, settings

    async def refresh(self, captured):
        return await await_drained(self._refresh(captured))

    async def _refresh(self, captured):
        source, settings = await self._source(captured.link, captured.item.payload["work_request_basis"]["source"]["request"]["id"], intent=captured.item.payload.get("work_event_intent"))
        admission = await prepare_attention(self.reader, captured.link)
        async with self.outbox._writer(captured.link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
            if current != captured:
                raise DeliveryClaimRetired()
            await self._admit(admission, writer, source)
            await self._replace(writer, captured.link, row, source, settings)
            await self._admit(admission, writer, source)

    async def refresh_anchor(self, link, outbox_id, *, expected_version, guard):
        """Refresh one acknowledged current anchor for a source-aware controller."""

        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, outbox_id)
                if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id) or "work_request_basis" not in row.payload:
                    raise CallbackRefused()
                if row.state != "sent" or row.payload["presentation_version"] != expected_version:
                    return None
                request_id = row.request_id
            source, settings = await self._source(link, request_id, intent=row.payload.get("work_event_intent"))
            admission = await prepare_attention(self.reader, link)
            composed = _RecoveryGuard(self, guard, admission, source)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True, populate_existing=True)
                if row is None or row.state != "sent" or row.payload["presentation_version"] != expected_version:
                    return None
                await self._replace(writer, link, row, source, settings)
                return outbox_id

        return await await_drained(perform())

    async def reveal(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        """Only a current acknowledged Show control can open protected content."""

        async def perform():
            # The discovery read supplies no authority. The writer locks and
            # validates the complete original group after current source reads.
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.kind != "reveal" or callback.target_kind != "outbox":
                    raise CallbackRefused()
                parent = await reader.get(TelegramOutboxRow, callback.target_id)
                if parent is None or "work_request_basis" not in parent.payload or parent.link_id != link.id:
                    raise CallbackRefused()
                identity, request_id = parent.id, parent.request_id
            source, settings = await self._source(link, request_id, intent=parent.payload.get("work_event_intent"))
            admission = await prepare_attention(self.reader, link)
            composed = _RecoveryGuard(self, guard, admission, source)
            async with self.callbacks.writer(link, composed) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
                if group.anchor_outbox_id != identity or group.selected.kind != "reveal" or group.selected.payload.get("outbox_id") != identity or row.state != "sent" or not row.payload["work_request_basis"]["concealed"]:
                    raise CallbackRefused()
                await self.callbacks.consume_group(group, reason="revealed", session=writer)
                await self._replace(writer, link, row, source, settings, reveal=callback_id)
                return identity

        return await await_drained(perform())

    async def _admit(self, admission, writer, source):
        held, _target = await admission(session=writer, projection=_RecoveryProjection(source))
        if "agents:write" not in held:
            raise AgentDenied("The request is unavailable")

    def _presentation(self, source, settings, link, row, concealed):
        return present_request(source, locale=settings.locale, concealed=concealed, outbox_id=row.id)

    def _snapshot(self, source):
        return asdict(source)

    def _extras(self, source):
        return {}

    def _owns_basis(self, basis):
        return "result_files" not in basis

    def _separate_reveal(self, reveal, fresh):
        return reveal is not None and len(fresh) > 1

    async def _replace(self, writer, link, row, source, settings, *, reveal=None, force_new=False, choice=None):
        if "navigation_origin" in row.payload.get("work_request_basis", {}):
            # A reusable digest gesture has its own authority and physical
            # anchor. Ordinary notification repair cannot discard that proof.
            raise ProductSourceRetired()
        if not self._owns_basis(row.payload.get("work_request_basis", {})):
            # Result receipts and file ownership require their dedicated
            # recovery transition; ordinary text refresh would erase them.
            raise ProductSourceRetired()
        if (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id) or row.payload.get("work_request_basis", {}).get(
            "version"
        ) not in self.family_versions:
            raise ProductSourceRetired()
        if (row.instance_id, row.work_id, row.request_id) != (source.request["instance_id"], source.work["id"], source.request["id"]):
            raise ProductSourceRetired()
        work = await writer.get(AgentWorkRow, source.work["id"], populate_existing=True)
        request = await writer.get(HumanInputRequestRow, source.request["id"], populate_existing=True)
        if work is None or request is None or work.revision != source.seen["work_revision"] or request.revision != source.seen["revision"]:
            raise DeliveryRetry(timedelta(seconds=5))
        # A new preview must not manufacture a second identity while the old
        # operation could still commit. Recovery of that receipt owns progress.
        unresolved = await writer.scalar(select(TelegramActRow.id).where(TelegramActRow.owner_user_id == link.owner_user_id, TelegramActRow.request_id == request.id, TelegramActRow.state.in_({"pending", "waiting", "uncertain"})).limit(1))
        if unresolved is not None:
            raise DeliveryRetry(timedelta(seconds=5))
        choice_origin = choice or row.payload["work_request_basis"].get("choice_origin")
        if choice_origin is not None and self.basis_version != 6:
            raise ProductSourceRetired()
        if choice_origin is not None and choice is None:
            from app.telegram_bots.work_routing_origin import require_choice

            proof = await writer.get(TelegramCallbackRow, choice_origin, with_for_update=True, populate_existing=True)
            require_choice(proof, row)
        reply_origin = None if choice_origin is not None else reveal or row.payload["work_request_basis"].get("reply_origin") or row.payload["work_request_basis"].get("reveal")
        if reply_origin is not None:
            proof = await writer.get(TelegramCallbackRow, reply_origin, with_for_update=True, populate_existing=True)
            require_show(proof, row)
        version = row.payload["presentation_version"] + 1
        old = deepcopy(row.parts)
        old_groups = {part.get("callback_group_id") for part in old if part["presentation_version"] == version - 1} - {None}
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(old_groups)).order_by(TelegramCallbackRow.id).with_for_update()))
        try:
            _validate_rows(row, controls, allow_pending=True)
        except CallbackRefused:
            raise ProductSourceRetired() from None
        for control in controls:
            own = next((part for part in old if part.get("callback_group_id") == control.group_id and part["presentation_version"] == version - 1), None)
            if own is None or control.message_id is not None and (own["state"] != "sent" or own["message_ids"] != [control.message_id]):
                raise ProductSourceRetired()
        for control in controls:
            if control.consumed_at is None:
                control.consumed_at, control.closed_reason = self.outbox.clock(), "routing-refresh" if choice is not None else "superseded"
        target = next((part for part in old if part["presentation_version"] == version - 1 and part["state"] == "sent" and part["message_ids"]), None)
        for part in old:
            if part["state"] not in {"sent", "unavailable", "retired"}:
                part["state"] = "retired"
        concealed = source.protected and reply_origin is None and choice_origin is None
        presentation = self._presentation(source, settings, link, row, concealed)
        keyboard = [list(line) for line in presentation.keyboard]
        recipes = []
        for index, supplied in enumerate(presentation.parts):
            recipe = {**supplied, "source_ref": {"key": "work.request.presentation", "values": {"request_id": request.id, "part": index}}}
            if index == len(presentation.parts) - 1 and keyboard:
                recipe["stored_keyboard"] = True
            recipes.append(recipe)
        snapshot = self._snapshot(source)
        snapshot["responses"] = list(snapshot["responses"])
        basis = {
            "version": self.basis_version,
            **self._extras(source),
            "source": snapshot,
            "locale": settings.locale,
            "concealed": concealed,
            "parts": recipes,
            "keyboard": keyboard,
            "reveal": reveal,
            "cleanup_id": None,
            "reply_origin": reply_origin,
        }
        if choice_origin is not None:
            basis["choice_origin"] = choice_origin
        fresh = self.outbox._parts(recipes, version)
        # Telegram cannot add protection to an existing unprotected message.
        # A replacement receives a new anchor only after its complete ACK.
        multipart_reveal = self._separate_reveal(reveal, fresh)
        if target is not None and not force_new and not multipart_reveal and (not source.protected or target.get("protect_content")):
            fresh[0].update(kind="edit", target_message_id=target["message_ids"][0], target_protect_content=bool(target.get("protect_content")))
        cleanup_groups = set()
        target_id = fresh[0].get("target_message_id")
        for control in controls:
            if control.message_id is not None and control.message_id != target_id and control.group_id not in cleanup_groups:
                cleanup_groups.add(control.group_id)
                cleanup = keyboard_cleanup_row(control, owner_user_id=link.owner_user_id, now=self.outbox.clock(), dedupe_key=f"notice:work-preview-cleanup:{row.id}:{version}:{control.group_id}")
                cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id}
                writer.add(cleanup)
                if multipart_reveal and control.id == reveal:
                    basis["cleanup_id"] = cleanup.id
        row.class_ = "reply" if reply_origin is not None or choice_origin is not None else "question"
        row.payload = _safe_json({**row.payload, "presentation_version": version, "work_request_basis": basis, "product_keyboard_basis": keyboard})
        row.parts = old + fresh
        row.state, row.not_before = "pending", self.outbox.clock()
        row.claim_owner = row.claim_id = row.claimed_at = None
        row.sent_at = row.closed_at = row.failed_reason = row.degraded_at = None
        if keyboard:
            group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=version, chat_id=link.chat_id, message_id=None, message_thread_id=row.message_thread_id, session=writer)
            fresh[-1]["callback_group_id"] = group.group_id
            row.parts = old + fresh
            # Relocation/translation does not renew an action's age limit.
            if controls:
                created = min(control.created_at for control in controls)
                for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                    control.created_at = created
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_({part.get("callback_group_id") for part in fresh} - {None}))):
            control.payload = {**control.payload, "presentation_pending": True}
        await writer.flush()


class _RecoveryGuard:
    def __init__(self, host, base, admission, source):
        if not callable(getattr(base, "current", None)):
            raise CallbackRefused()
        self.host, self.base, self.admission, self.source = host, base, admission, source

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        # This must follow the base guard's last account/provider await, also
        # when callbacks.writer performs its exit check after the mutation.
        await self.host._admit(self.admission, writer, self.source)
        self.base.current()
        return current


class _RecoveryProjection:
    """Current source, pending actions and privacy in the final account read."""

    def __init__(self, source, *, allowed_act_id=None):
        self.source, self.allowed_act_id = source, allowed_act_id

    def extend(self, query):
        work, request, bot = AgentWorkRow, HumanInputRequestRow, TelegramBotLinkRow
        unresolved = (
            select(TelegramActRow.id)
            .where(
                TelegramActRow.owner_user_id == bot.owner_user_id,
                TelegramActRow.request_id == request.id,
                TelegramActRow.state.in_({"pending", "waiting", "uncertain"}),
                TelegramActRow.id != self.allowed_act_id if self.allowed_act_id is not None else true(),
            )
            .correlate(bot, request)
            .exists()
        )
        return (
            query.add_columns(work, request, unresolved)
            .join(work, and_(work.id == self.source.work["id"], work.instance_id == bot.agent_ref))
            .join(request, and_(request.id == self.source.request["id"], request.work_id == work.id, request.instance_id == bot.agent_ref))
        )

    def check(self, rows):
        bot, target, permissions, policy = rows[0][:4]
        work, request, unresolved = rows[0][8:11]
        if self.source.request["purpose"] != "information" and permissions & 6 != 6:
            raise AgentDenied("The request is unavailable")
        if work.revision != self.source.seen["work_revision"] or request.revision != self.source.seen["revision"] or unresolved or (target.custody == "company" and policy == "protected") != self.source.protected:
            raise DeliveryRetry(timedelta(seconds=5))
        if request.recipient_id != bot.owner_user_id or request.state != "pending":
            raise ProductSourceRetired()
