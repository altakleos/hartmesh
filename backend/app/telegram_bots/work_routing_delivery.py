"""Dormant source-owned recipient selection for Work notifications."""

import asyncio
from dataclasses import asdict
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import func, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import _SCOPE
from app.telegram_bots.authority import AccessOff, _PreparedLiveOwner
from app.telegram_bots.config import load_config
from app.telegram_bots.producer import ProductSourceRetired, _safe_json
from app.telegram_bots.work_request_delivery import WorkRequestPlans
from app.telegram_bots.work_request_source import WorkRequestSources, _RequestProjection, frozen, same_displayed_basis
from app.telegram_bots.work_routing import present_routing
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow


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
                if existing.binding_generation != link.binding_generation or existing.request_id != request_id or existing.payload.get("work_request_basis", {}).get("version") != 5:
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
            shown = present_routing(value, locale=settings.locale, link=link, public_base_url=public_base_url, concealed=source.protected, outbox_id=identity)
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
                "routing": {"recipients": list(value.recipients), "after_wrong": value.after_wrong, "public_base_url": public_base_url},
                "locale": settings.locale,
                "concealed": source.protected,
                "parts": parts,
                "keyboard": controls,
            }
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
        from deerflow.config.app_config import peek_loaded_app_config

        super().__init__(*args, **kwargs)
        self.config = peek_loaded_app_config()
        if self.config is None:
            raise ProductSourceRetired()
        self.sign_on_only = self.config.auth.local.enabled is False
        self.public_base_url = load_config(self.config).public_base_url

    def _mode_current(self):
        from deerflow.config.app_config import peek_loaded_app_config

        if (
            peek_loaded_app_config() is not self.config
            or (self.config.auth.local.enabled is False) != self.sign_on_only
            or load_config(self.config).public_base_url != self.public_base_url
            or self.basis["routing"]["public_base_url"] != self.public_base_url
        ):
            raise ProductSourceRetired()

    def extend(self, query):
        # Compact correlated facts keep each complete delivery out of the
        # candidate/account-disable cross product. Password hashes never enter it.
        query = super().extend(query)
        user, grant, disabled = aliased(UserRow), aliased(AgentInstanceGrantRow), aliased(DisabledIdentityRow)
        fields = {column.name: getattr(user, column.name) for column in UserRow.__table__.columns if column.name != "password_hash"}
        fields.update(permissions=grant.permissions, disabled_issuer=disabled.issuer, disabled_subject=disabled.subject, disabled_at=disabled.disabled_at)
        args = [value for pair in fields.items() for value in pair]
        sqlite = self.host.outbox.sessions.kw["bind"].dialect.name == "sqlite"
        aggregate = func.json_group_array(func.json_object(*args)) if sqlite else func.json_agg(func.json_build_object(*args))
        people = (
            select(aggregate)
            .select_from(grant)
            .join(user, user.id == grant.user_id)
            .outerjoin(disabled, or_(disabled.subject == user.id, disabled.subject == user.oauth_id))
            .where(grant.instance_id == TelegramBotLinkRow.agent_ref)
            .correlate(TelegramBotLinkRow)
            .scalar_subquery()
        )
        return query.add_columns(people)

    def check(self, rows):
        from app.gateway.deps import bind_user_repo

        super().check(rows)
        self._mode_current()
        required = 3 if self.source.request["purpose"] == "information" else 6
        latest_wrong = max((row for row in self.source.responses if row["disposition"] == "wrong_recipient"), key=lambda row: (row["created_at"], row["id"]), default=None)
        excluded = latest_wrong["actor_id"] if latest_wrong else None
        users, facts = {}, {}
        for user in self.objects(rows[0][-1]):
            users[user.id] = user
            if user.disabled_issuer is not None:
                facts.setdefault(user.id, []).append(
                    SimpleNamespace(issuer=user.disabled_issuer, subject=user.disabled_subject, disabled_at=datetime.fromisoformat(user.disabled_at) if isinstance(user.disabled_at, str) else user.disabled_at)
                )
        repository = bind_user_repo(self.host.outbox.sessions)
        recipients, eligible = [], set()
        for user in users.values():
            if user.permissions & required != required:
                continue
            user.password_hash = None
            for key in ("last_sign_in_at", "released_at"):
                if isinstance(getattr(user, key), str):
                    setattr(user, key, datetime.fromisoformat(getattr(user, key)))
            owner = _PreparedLiveOwner(user.id, "", self.sign_on_only, repository, self.host.outbox.sessions.kw["bind"].sync_engine)
            try:
                owner.check_snapshot(user, facts.get(user.id, []))
            except AccessOff:
                continue
            eligible.add(user.id)
            if user.id != excluded:
                recipients.append({"user_id": user.id, "name": user.email, "permissions": user.permissions})
        recipients.sort(key=lambda row: (row["user_id"] != self.captured.link.owner_user_id, row["name"].casefold(), row["user_id"]))
        if not self.basis["routing"]["after_wrong"] and self.source.request["recipient_id"] in eligible:
            raise ProductSourceRetired()
        if recipients != self.basis["routing"]["recipients"]:
            raise ProductSourceRetired()
        self._mode_current()


class WorkRoutingSources(WorkRequestSources):
    projection_type = _RoutingProjection

    async def _prepare(self, captured):
        from deerflow.config.app_config import peek_loaded_app_config

        basis, source, part = frozen(captured)
        if basis["version"] != 5:
            raise ProductSourceRetired()
        current = await self.reader.read_routing(captured.link, source.request["id"])
        if current.source.request["state"] == "closed" or not (current.source.request["needs_routing"] or current.after_wrong):
            raise ProductSourceRetired()
        config = peek_loaded_app_config()
        if config is None or not same_displayed_basis(current.source, source) or basis["routing"] != {"recipients": list(current.recipients), "after_wrong": current.after_wrong, "public_base_url": load_config(config).public_base_url}:
            raise ProductSourceRetired()
        return basis, current.source, part
