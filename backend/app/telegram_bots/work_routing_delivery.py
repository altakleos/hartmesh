"""Dormant source-owned recipient selection for Work notifications."""

import asyncio
from dataclasses import asdict

from sqlalchemy import select

from app.telegram_bots.attention_authority import _SCOPE
from app.telegram_bots.config import load_config
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _safe_json
from app.telegram_bots.work_request_delivery import WorkRequestPlans
from app.telegram_bots.work_request_source import WorkRequestSources, _RequestProjection, frozen, same_displayed_basis
from app.telegram_bots.work_routing import present_routing, routing_basis
from app.telegram_bots.work_routing_authority import RoutingCandidates
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


class WorkRoutingPlans(WorkRequestPlans):
    async def _plan(self, link, *, request_id, dedupe_key):
        from deerflow.config.app_config import get_app_config

        value = await self.reader.read_routing(link, request_id)
        public_base_url = load_config(await asyncio.to_thread(get_app_config)).public_base_url
        source = value.source
        if source.request["state"] == "closed" or not (source.request["needs_routing"] or value.after_wrong):
            raise AgentConflict("The request no longer needs routing")
        settings = await self.settings.get(link.owner_user_id)
        if settings is None:
            raise AgentDenied("The owner settings are unavailable")
        async with self.outbox._writer(link.id) as (writer, binding):
            if binding is None or any(getattr(binding, key) != getattr(link, key) for key in _SCOPE) or binding.state != "connected" or binding.health != "ok":
                raise AgentDenied("The bot binding is no longer current")
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe_key).with_for_update())
            if existing is not None:
                if existing.binding_generation != link.binding_generation or existing.request_id != request_id or existing.payload.get("work_request_basis", {}).get("version") not in {5, 6}:
                    raise ValueError("work-routing-source-conflict")
                return existing.id
            work = await writer.get(AgentWorkRow, source.work["id"])
            request = await writer.get(HumanInputRequestRow, request_id)
            if work is None or request is None or work.revision != source.seen["work_revision"] or request.revision != source.seen["revision"] or request.state == "closed":
                raise AgentConflict("The request changed during preparation")
            identity, _ = await self.outbox._add(
                link,
                dedupe_key=dedupe_key,
                class_="question",
                payload={"parts": [{"kind": "text", "source_ref": {"key": "concealed.question", "values": {}}}]},
                instance_id=link.agent_ref,
                work_id=source.work["id"],
                request_id=request_id,
                session=writer,
            )
            row = await writer.get(TelegramOutboxRow, identity)
            return await self.freeze_routing(writer, link, row, value, settings, public_base_url)

    async def freeze_routing(self, writer, link, row, value, settings, public_base_url, *, navigation_origin=None):
        identity, source = row.id, value.source
        request_id = source.request["id"]
        shown = present_routing(value, locale=settings.locale, link=link, public_base_url=public_base_url, concealed=source.protected and navigation_origin is None, outbox_id=identity)
        controls = [list(line) for line in shown.keyboard]
        parts = [
            {**part, "source_ref": {"key": "work.request.presentation", "values": {"request_id": request_id, "part": index}}, **({"stored_keyboard": True} if controls and index == len(shown.parts) - 1 else {})}
            for index, part in enumerate(shown.parts)
        ]
        source_data = asdict(source)
        source_data["responses"] = list(source_data["responses"])
        basis = {
            "version": 5,
            "source": source_data,
            "routing": routing_basis(value, public_base_url),
            "locale": settings.locale,
            "concealed": source.protected,
            "parts": parts,
            "keyboard": controls,
        }
        if navigation_origin is not None:
            from app.telegram_bots.work_navigation_origin import validate_origin

            basis.update(navigation_origin=validate_origin(navigation_origin), concealed=False)
        row = await writer.get(TelegramOutboxRow, identity)
        row.payload = _safe_json({**row.payload, "work_request_basis": basis, "product_keyboard_basis": controls})
        saved = self.outbox._parts(parts, 0)
        if controls:
            group = await self.callbacks.write_group(link, controls, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
            saved[-1]["callback_group_id"] = group.group_id
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                control.payload = {**control.payload, "presentation_pending": True}
        row.parts = saved
        await writer.flush()
        return identity


class _RoutingProjection(_RequestProjection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.candidates = RoutingCandidates(self.host.outbox.sessions, self.captured.link, self.source, self.basis["routing"])

    def extend(self, query):
        return self.candidates.extend(super().extend(query))

    def check(self, rows):
        super().check(rows)
        self.candidates.check(rows)


class WorkRoutingSources(WorkRequestSources):
    projection_type = _RoutingProjection

    async def _prepare(self, captured):
        from deerflow.config.app_config import peek_loaded_app_config

        basis, source, part = frozen(captured)
        if basis["version"] not in {5, 6}:
            raise ProductSourceRetired()
        current = await self.reader.read_routing(captured.link, source.request["id"], response_id=basis["routing"].get("response_id"))
        if current.source.request["state"] == "closed" or not (current.source.request["needs_routing"] or current.after_wrong or current.response_id is not None):
            raise ProductSourceRetired()
        from app.telegram_bots.work_event_source import require_event_source, unsolicited_event

        if unsolicited_event(captured.item, basis):
            require_event_source(captured.item, current.source)
        config = peek_loaded_app_config()
        if config is None:
            raise ProductSourceRetired()
        settings = await self.settings.get(captured.link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        routing = routing_basis(current, load_config(config).public_base_url)
        changed = not same_displayed_basis(current.source, source) or settings.locale != basis["locale"] or basis["routing"] != routing
        if changed:
            if self.recovery is not None:
                if "navigation_origin" in basis:
                    refresh = getattr(self.recovery, "refresh_navigation", None)
                    if refresh is None:
                        raise ProductSourceRetired()
                    await refresh(captured)
                else:
                    await self.recovery.refresh(captured)
                raise DeliveryClaimRetired()
            raise ProductSourceRetired()
        return basis, current.source, part
