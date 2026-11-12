"""Refresh and reveal the complete current routing presentation without effects."""

import asyncio
from dataclasses import asdict, dataclass
from datetime import timedelta

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.config import load_config
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
from app.telegram_bots.work_actions import request_for
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryGuard, _RecoveryProjection
from app.telegram_bots.work_routing import present_routing
from app.telegram_bots.work_routing_authority import RoutingCandidates
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class _RoutingSource:
    value: object
    link: object
    public_base_url: str | None

    def __getattr__(self, key):
        return getattr(self.value.source, key)

    @property
    def routing(self):
        return {"recipients": list(self.value.recipients), "after_wrong": self.value.after_wrong, "public_base_url": self.public_base_url}


class WorkRoutingRecovery(WorkRequestRecovery):
    family_versions = {5, 6}
    basis_version = 6

    async def refresh_choice(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        """Replace shown choices without retrying a refused or unknown command."""

        async def perform():
            from app.telegram_bots.attention_authority import prepare_attention

            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.kind != "route_to" or callback.target_kind != "request":
                    raise CallbackRefused()
                request_id = callback.target_id
            source, settings = await self._source(link, request_id)
            admission = await prepare_attention(self.reader, link)
            composed = _RecoveryGuard(self, guard, admission, source)
            async with self.callbacks.writer(link, composed) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, with_for_update=True, populate_existing=True)
                shown = acknowledged_source(link, parent)
                if parent.payload["work_request_basis"]["version"] not in self.family_versions or (shown.request["id"], shown.work["id"]) != (request_id, source.work["id"]):
                    raise CallbackRefused()
                saved = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
                if saved is None:
                    if any("work_action_id" in row.payload for row in group.rows):
                        raise CallbackRefused()
                else:
                    if (
                        (saved.link_id, saved.owner_user_id, saved.binding_generation, saved.instance_id, saved.work_id, saved.request_id, saved.kind)
                        != (link.id, link.owner_user_id, link.binding_generation, link.agent_ref, source.work["id"], request_id, "route")
                        or saved.state != "refused"
                        or saved.reason != "recipient-ineligible"
                        or any(row.payload.get("work_action_id") != saved.id for row in group.rows)
                    ):
                        raise CallbackRefused()
                    body = request_for("route", saved.body, identity=saved.id)
                    if body.recipient_id != group.selected.payload["recipient_id"] or body.expected_request_revision != group.selected.payload["request_revision"]:
                        raise CallbackRefused()
                await self._replace(writer, link, parent, source, settings, choice=callback_id)
                from app.telegram_bots.work_outcome_transfer import transfer_outcome

                await transfer_outcome(writer, link, callback_id, parent.id, outbox=self.outbox)
                return parent.id

        return await await_drained(perform())

    async def _source(self, link, request_id):
        from deerflow.config.app_config import get_app_config

        value = await self.reader.read_routing(link, request_id)
        if value.source.request["state"] == "closed" or not (value.source.request["needs_routing"] or value.after_wrong):
            raise ProductSourceRetired()
        settings = await self.plans.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        base = load_config(await asyncio.to_thread(get_app_config)).public_base_url
        return _RoutingSource(value, link, base), settings

    async def repair(self, link, outbox_id, *, guard):
        """Replace a source-held failed delivery; never reclaim an active sender."""

        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, outbox_id)
                if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                    raise CallbackRefused()
                if row.state != "failed" or row.failed_reason != "work-routing-refresh-required" or row.claim_id is not None:
                    return None
                version, request_id = row.payload["presentation_version"], row.request_id
            source, settings = await self._source(link, request_id)
            from app.telegram_bots.attention_authority import prepare_attention

            admission = await prepare_attention(self.reader, link)
            composed = _RecoveryGuard(self, guard, admission, source)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True, populate_existing=True)
                if row is None or row.state != "failed" or row.failed_reason != "work-routing-refresh-required" or row.claim_id is not None or row.payload["presentation_version"] != version:
                    return None
                await self._replace(writer, link, row, source, settings, force_new=True)
                return outbox_id

        return await await_drained(perform())

    def _presentation(self, source, settings, link, row, concealed):
        return present_routing(source.value, locale=settings.locale, link=link, public_base_url=source.public_base_url, concealed=concealed, outbox_id=row.id)

    def _snapshot(self, source):
        return asdict(source.value.source)

    def _extras(self, source):
        return {"routing": source.routing}

    async def _admit(self, admission, writer, source):
        held, _target = await admission(session=writer, projection=_RoutingRecoveryProjection(self.outbox.sessions, source))
        if "agents:write" not in held:
            raise AgentDenied("The request is unavailable")


class _RoutingRecoveryProjection(_RecoveryProjection):
    def __init__(self, sessions, source):
        super().__init__(source)
        self.candidates = RoutingCandidates(sessions, source.link, source.value.source, source.routing)

    def extend(self, query):
        return self.candidates.extend(super().extend(query))

    def check(self, rows):
        _bot, target, permissions, policy = rows[0][:4]
        work, request, unresolved = rows[0][8:11]
        if permissions & 6 != 6:
            raise AgentDenied("The request is unavailable")
        if work.revision != self.source.seen["work_revision"] or request.revision != self.source.seen["revision"] or unresolved or (target.custody == "company" and policy == "protected") != self.source.protected:
            raise DeliveryRetry(timedelta(seconds=5))
        if request.state == "closed":
            raise ProductSourceRetired()
        self.candidates.check(rows)
