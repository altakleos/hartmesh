"""Atomic saved request previews; SDK delivery requires its source integration.

No worker or chat entry point installs this planner yet. Its source family stays
closed at the raw sender until current-recipient and reveal handlers are ready.
"""

from dataclasses import asdict

from sqlalchemy import select

from app.telegram_bots.attention_authority import _SCOPE
from app.telegram_bots.producer import _safe_json
from app.telegram_bots.settings import SettingsRepository
from app.telegram_bots.work_requests import present_request
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkRequestPlans:
    def __init__(self, *, outbox, reader, callbacks):
        self.outbox, self.reader, self.callbacks = outbox, reader, callbacks
        engine = outbox.sessions.kw.get("bind")
        if reader.links.session_factory.kw.get("bind") is not engine or callbacks.sessions.kw.get("bind") is not engine:
            raise ValueError("request-plans-require-shared-storage")
        self.settings = SettingsRepository(outbox.sessions, outbox.config)

    async def plan(self, link, *, request_id, dedupe_key):
        return await await_drained(self._plan(link, request_id=request_id, dedupe_key=dedupe_key))

    async def _plan(self, link, *, request_id, dedupe_key):
        source = await self.reader.read_request(link, request_id)
        request = source.request
        if request["state"] != "pending" or request["needs_routing"] or request["recipient_id"] != link.owner_user_id:
            raise AgentConflict("The request no longer needs this notification")
        if not (request["can_respond"] if request["purpose"] == "information" else request["can_manage"]):
            raise AgentDenied("The request is unavailable")
        settings = await self.settings.get(link.owner_user_id)
        if settings is None:
            raise AgentDenied("The owner settings are unavailable")
        async with self.outbox._writer(link.id) as (writer, binding):
            if binding is None or any(getattr(binding, key) != getattr(link, key) for key in _SCOPE) or binding.state != "connected" or binding.health != "ok":
                raise AgentDenied("The bot binding is no longer current")
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe_key).with_for_update())
            if existing is not None:
                basis = existing.payload.get("work_request_basis", {})
                if existing.binding_generation != link.binding_generation or existing.request_id != request_id or basis.get("version") != 1:
                    raise ValueError("work-request-source-conflict")
                return existing.id
            # These are read fences for a saved presentation, not permission
            # to dispatch. The eventual sender must re-read source authority.
            work = await writer.get(AgentWorkRow, source.work["id"])
            current = await writer.get(HumanInputRequestRow, request_id)
            if work is None or current is None or work.revision != source.seen["work_revision"] or current.revision != source.seen["revision"] or current.state != "pending":
                raise AgentConflict("The request changed during preparation")
            # Allocate the outbox identity in this transaction before freezing
            # its reveal control. No placeholder can commit without its plan.
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
            await self._freeze(writer, link, row, source, settings)
            return identity

    async def _freeze(self, writer, link, row, source, settings, *, pending=False):
        """Borrow an already-qualified writer and preserve its delivery identity."""
        identity, request_id = row.id, source.request["id"]
        presentation = present_request(source, locale=settings.locale, concealed=source.protected, outbox_id=identity)
        controls = [list(line) for line in presentation.keyboard]
        parts = []
        for index, supplied in enumerate(presentation.parts):
            part = {**supplied, "source_ref": {"key": "work.request.presentation", "values": {"request_id": request_id, "part": index}}}
            if controls and index == len(presentation.parts) - 1:
                part["stored_keyboard"] = True
            parts.append(part)
        snapshot = asdict(source)
        snapshot["responses"] = list(snapshot["responses"])
        basis = {"version": 1, "source": snapshot, "locale": settings.locale, "concealed": source.protected, "parts": parts, "keyboard": controls}
        row = await writer.get(TelegramOutboxRow, identity)
        row.payload = _safe_json({**row.payload, "work_request_basis": basis, "product_keyboard_basis": controls})
        saved_parts = self.outbox._parts(parts, 0)
        if controls:
            group = await self.callbacks.write_group(link, controls, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
            saved_parts[-1]["callback_group_id"] = group.group_id
            if pending:
                for callback in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                    callback.payload = {**callback.payload, "presentation_pending": True}
        row.parts = saved_parts
        await writer.flush()
        return identity
