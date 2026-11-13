"""One durable notice through another eligible bot when credentials stop working."""

from datetime import UTC, datetime
from types import SimpleNamespace

from sqlalchemy import and_, select, text
from sqlalchemy.orm import aliased

from app.telegram_bots.links import BotLink
from app.telegram_bots.pages import page_address
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryEligibility, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_result_navigation import NavigationOrigin
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def owns(item):
    return "rejected_link_id" in item.payload or any(part.get("source_ref", {}).get("key") == "link.token_rejected_other" for part in item.parts)


_SOURCE_SCOPE = ("id", "owner_user_id", "agent_kind", "agent_ref", "bot_id", "bot_name", "telegram_user_id", "chat_id", "binding_generation", "connection_id", "token_revision")
_PART_METADATA = {"part_id", "ordinal", "presentation_version", "state", "message_ids", "sent_at", "dispatched_at", "plain_fallback", "url_button_fallback", "invalid_token_verification_pending", "callback_group_id"}


def recipe(basis, rejected):
    if (
        type(basis) is not dict
        or set(basis) != {"version", "scope", "values", "public_base_url"}
        or type(basis["version"]) is not int
        or basis["version"] != 1
        or not same_body(basis["scope"], {key: getattr(rejected, key) for key in _SOURCE_SCOPE})
        or type(basis["values"]) is not dict
        or set(basis["values"]) != {"name", "product"}
        or any(type(value) is not str or not value for value in basis["values"].values())
        or basis["public_base_url"] is not None
        and type(basis["public_base_url"]) is not str
    ):
        raise DeliveryClaimRetired()
    part = {"kind": "text", "source_ref": {"key": "link.token_rejected_other", "values": basis["values"]}, "protect_content": False}
    address = page_address("web.telegram", rejected, public_base_url=basis["public_base_url"])
    if address is not None:
        part["url_button"] = {"page": "web.telegram", "url": address}
    return part


def require_source(link, item, rejected):
    if (
        rejected is None
        or rejected.id == link.id
        or rejected.health != "token_rejected"
        or rejected.state != "connected"
        or (rejected.owner_user_id, rejected.telegram_user_id, rejected.chat_id) != (link.owner_user_id, link.telegram_user_id, link.chat_id)
        or item.payload.get("rejected_link_id") != rejected.id
        or item.dedupe_key != f"rejected:{rejected.id}:{rejected.token_revision}"
        or item.class_ != "service"
        or item.message_thread_id is not None
        or len(item.parts) != 1
        or item.parts[0].get("source_ref", {}).get("key") != "link.token_rejected_other"
    ):
        raise DeliveryClaimRetired()
    expected = recipe(item.payload.get("rejected_basis"), rejected)
    actual = item.parts[0]
    if (
        not same_body({key: value for key, value in actual.items() if key not in _PART_METADATA}, expected)
        or actual.get("callback_group_id") is not None
        or (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id)
        or set(item.payload) - {"rejected_link_id", "rejected_basis", "presentation_version", "notification_reserved_at"}
        or type(item.payload.get("presentation_version", 0)) is not int
        or item.payload.get("presentation_version", 0) != 0
    ):
        raise DeliveryClaimRetired()


class RejectedProjection:
    def __init__(self, link, item):
        self.link, self.item = link, item
        self.origin = NavigationOrigin()

    def extend(self, query):
        self.column = len(query.column_descriptions)
        rejected = aliased(TelegramBotLinkRow)
        return query.add_columns(rejected).outerjoin(rejected, rejected.id == self.item.payload.get("rejected_link_id"))

    def check(self, rows):
        require_source(self.link, self.item, rows[0][self.column])
        self.origin.check(self.item.payload["rejected_basis"])


class _PlanProjection(RejectedProjection):
    def extend(self, query):
        query = super().extend(query)
        self.delivery_column = None
        if getattr(self.item, "id", None) is not None:
            delivery = aliased(TelegramOutboxRow)
            self.delivery_column = len(query.column_descriptions)
            query = query.add_columns(delivery).join(delivery, delivery.id == self.item.id)
        if self.link.agent_kind != "instance":
            return query
        self.target_column = len(query.column_descriptions)
        instance, grant, policy = aliased(AgentInstanceRow), aliased(AgentInstanceGrantRow), aliased(TelegramInstancePolicyRow)
        return (
            query.add_columns(instance, grant.permissions, policy.policy)
            .outerjoin(instance, instance.id == self.link.agent_ref)
            .outerjoin(grant, and_(grant.instance_id == instance.id, grant.user_id == self.link.owner_user_id))
            .outerjoin(policy, policy.instance_id == instance.id)
        )

    def check(self, rows):
        super().check(rows)
        if self.delivery_column is not None:
            delivery = rows[0][self.delivery_column]
            if not _same_item(delivery, self.item):
                raise DeliveryClaimRetired()
            require_source(self.link, _item(delivery), rows[0][self.column])
        if self.link.agent_kind == "instance":
            instance, permission, policy = rows[0][self.target_column : self.target_column + 3]
            policy = "allowed" if instance is not None and instance.custody == "personal" or policy is None else policy
            if instance is None or instance.status != "active" or type(permission) is not int or not permission & 1 or policy != "allowed":
                raise DeliveryClaimRetired()


async def eligibility(host, link, item):
    if not owns(item):
        return None
    async with host.links.session_factory() as reader:
        rejected = await reader.get(TelegramBotLinkRow, item.payload.get("rejected_link_id"))
    try:
        require_source(link, item, rejected)
    except DeliveryClaimRetired:
        return DeliveryEligibility.skip("superseded")
    if await host.instance_policy(link) == "protected":
        return DeliveryEligibility.skip("superseded")
    return None


class RejectedNotices:
    def __init__(self, services, *, available=lambda: True):
        self.services, self.available = services, available

    async def plan(self, rejected):
        return await await_drained(self._plan(rejected))

    async def _plan(self, rejected):
        if not self.available() or rejected.state != "connected" or rejected.health != "token_rejected" or rejected.telegram_user_id is None:
            return None
        host = self.services
        key = f"rejected:{rejected.id}:{rejected.token_revision}"
        async with host.links.session_factory() as reader:
            rows = list(
                await reader.scalars(
                    select(TelegramBotLinkRow).where(
                        TelegramBotLinkRow.owner_user_id == rejected.owner_user_id,
                        TelegramBotLinkRow.telegram_user_id == rejected.telegram_user_id,
                        TelegramBotLinkRow.state == "connected",
                        TelegramBotLinkRow.health == "ok",
                        TelegramBotLinkRow.id != rejected.id,
                    )
                )
            )
        earliest = datetime.min.replace(tzinfo=UTC)
        candidates = [BotLink.from_row(row) for row in rows]
        candidates.sort(key=lambda row: (row.agent_kind != "lead_agent", -(row.last_update_at or earliest).timestamp(), row.id))
        origin = NavigationOrigin()
        values = await host.prepare_ingress_values(rejected)
        basis = {"version": 1, "scope": {key: getattr(rejected, key) for key in _SOURCE_SCOPE}, "values": {"name": values["name"], "product": values["product"]}, "public_base_url": origin.base}
        origin.check(basis)
        part = recipe(basis, rejected)
        for candidate in candidates:
            if not self.available():
                return None
            probe = SimpleNamespace(
                link_id=candidate.id,
                owner_user_id=candidate.owner_user_id,
                binding_generation=candidate.binding_generation,
                chat_id=candidate.chat_id,
                message_thread_id=None,
                class_="service",
                payload={"rejected_link_id": rejected.id, "rejected_basis": basis},
                parts=[part],
                dedupe_key=key,
            )
            verdict = await host.authority.delivery(candidate, probe)
            if verdict.decision != "allow" or verdict.protect_content:
                continue
            owner = await host.prepare_live_owner(candidate.owner_user_id)
            guard = host.links.publication_guard(candidate, check_live_owner=owner, check_attempt_current=self.available)
            async with host.outbox.sessions() as writer, writer.begin():
                if writer.get_bind().dialect.name == "sqlite":
                    await writer.execute(text("BEGIN IMMEDIATE"))
                await writer.scalar(select(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == candidate.owner_user_id).with_for_update())
                await writer.execute(select(TelegramBotLinkRow.id).where(TelegramBotLinkRow.owner_user_id == candidate.owner_user_id).order_by(TelegramBotLinkRow.id).with_for_update())
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.owner_user_id == candidate.owner_user_id, TelegramOutboxRow.dedupe_key == key))
                if existing is not None:
                    recipient = await writer.get(TelegramBotLinkRow, existing.link_id)
                    source = await writer.get(TelegramBotLinkRow, rejected.id)
                    if recipient is None:
                        raise DeliveryClaimRetired()
                    require_source(BotLink.from_row(recipient), _item(existing), source)
                    origin.check(existing.payload["rejected_basis"])
                    return existing.id
                await guard.check_projection(writer, _PlanProjection(candidate, probe))
                identity, _ = await host.outbox._add(candidate, dedupe_key=key, class_="service", payload={"rejected_link_id": rejected.id, "rejected_basis": basis, "parts": [part]}, session=writer)
                item = _item(await writer.get(TelegramOutboxRow, identity))
                await writer.flush()
                await guard.check_projection(writer, _PlanProjection(candidate, item))
                return identity
        return None
