"""One final SQL snapshot of notification preferences and send authority."""

import asyncio
from datetime import timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_, select

from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.settings import _snapshot
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained


async def authorize_notification(host, captured, *, silent, now):
    """Prepare external readers first; check mutable SQL facts together last."""
    from app.telegram_bots.authority import AccessOff
    from app.telegram_bots.producer import DeliveryClaimRetired

    async def read():
        owner = await host.authority.prepare_live_owner(captured.link.owner_user_id)
        preferences = await host.settings.get(captured.link.owner_user_id)
        if preferences is None:
            raise DeliveryClaimRetired()
        zone = await asyncio.to_thread(ZoneInfo, preferences.timezone)
        bot, delivery, settings, connection = TelegramBotLinkRow, TelegramOutboxRow, TelegramPersonSettingsRow, ChannelConnectionRow
        query = (
            select(bot, delivery, settings, connection, UserRow, DisabledIdentityRow)
            .join(delivery, and_(delivery.link_id == bot.id, delivery.id == captured.item.id))
            .join(settings, settings.owner_user_id == bot.owner_user_id)
            .join(connection, connection.id == bot.connection_id)
            .join(UserRow, UserRow.id == bot.owner_user_id)
            .outerjoin(DisabledIdentityRow, or_(DisabledIdentityRow.subject == UserRow.id, DisabledIdentityRow.subject == UserRow.oauth_id))
            .where(bot.id == captured.link.id)
            .execution_options(populate_existing=True)
        )
        agents = None
        if captured.link.agent_kind == "instance":
            from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
            from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

            agents = getattr(getattr(host.authority.app, "state", None), "agent_instances", None)
            if agents is None or agents.directory.session_factory.kw.get("bind") is not host.links.session_factory.kw.get("bind"):
                raise DeliveryClaimRetired()
            query = (
                query.add_columns(AgentInstanceRow, AgentInstanceGrantRow.permissions, TelegramInstancePolicyRow.policy)
                .join(AgentInstanceRow, AgentInstanceRow.id == bot.agent_ref)
                .join(AgentInstanceGrantRow, and_(AgentInstanceGrantRow.instance_id == AgentInstanceRow.id, AgentInstanceGrantRow.user_id == bot.owner_user_id))
                .outerjoin(TelegramInstancePolicyRow, TelegramInstancePolicyRow.instance_id == AgentInstanceRow.id)
            )
        from app.telegram_bots.rejected_notices import RejectedProjection, owns

        rejected = RejectedProjection(captured.link, captured.item) if owns(captured.item) else None
        if rejected is not None:
            query = rejected.extend(query)
        async with host.links.session_factory() as reader:
            facts = (await reader.execute(query)).all()
        if not facts:
            raise DeliveryClaimRetired()
        current, row, saved_settings, bound, user, _disabled = facts[0][:6]
        owner.check_snapshot(user, [fact[5] for fact in facts])
        latest = host.outbox._sending(
            current,
            row,
            link_id=captured.link.id,
            part_id=captured.part["part_id"],
            claim_owner=captured.claim_owner,
            claim_id=captured.claim_id,
            token_revision=captured.token_revision,
        )
        scope = ("id", "owner_user_id", "agent_kind", "agent_ref", "bot_id", "token_revision", "binding_generation", "connection_id", "telegram_user_id", "chat_id")
        if latest.part != captured.part or latest.item != captured.item or any(getattr(latest.link, field) != getattr(captured.link, field) for field in scope):
            raise DeliveryClaimRetired()
        if (bound.owner_user_id, bound.provider, bound.status, bound.external_account_id, bound.workspace_id, bound.bot_user_id) != (
            current.owner_user_id,
            "telegram",
            "connected",
            str(current.telegram_user_id),
            str(current.bot_id),
            str(current.bot_id),
        ):
            raise DeliveryClaimRetired()
        if agents is not None:
            from app.telegram_bots.policy import POLICIES, pending_discard

            instance, permissions, policy = facts[0][6:9]
            policy = "allowed" if instance.custody == "personal" or policy is None else policy
            needed = 1 if row.class_ == "service" else 3  # Use; Work content also requires Inspect.
            if (
                instance.custody not in {"personal", "company"}
                or instance.status != "active"
                or type(permissions) is not int
                or not 1 <= permissions <= 7
                or permissions & needed != needed
                or policy not in POLICIES
                or policy == "off"
                or policy == "protected"
                and not captured.part.get("protect_content")
                or pending_discard(current) is not None
                or getattr(getattr(host.authority.app, "state", None), "agent_instances", None) is not agents
            ):
                raise DeliveryClaimRetired()
        if rejected is not None:
            rejected.check(facts)
            if agents is not None and policy == "protected":
                raise DeliveryClaimRetired()
        actual = _snapshot(saved_settings)
        if actual.timezone != preferences.timezone or preference_skip(latest.item, actual, now):
            raise DeliveryClaimRetired()
        # Use the prepared zone object: this final projection performs no I/O.
        local = now.astimezone(zone)
        minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
        quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
        recent = current.last_update_at is not None and now - aware(current.last_update_at) <= timedelta(seconds=host.config.outbox.recent_act_seconds)
        actual_silent = bool(row.silent or captured.part.get("silent") or quiet and not recent)
        if actual_silent != bool(silent):
            raise DeliveryClaimRetired()

    try:
        await await_drained(read())
    except AccessOff:
        raise DeliveryClaimRetired() from None
