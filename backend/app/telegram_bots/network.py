"""Manager admission for actual instance network requests; no policy grants here."""

from sqlalchemy import and_, or_, select

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.policy import POLICIES, pending_discard
from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramInstancePolicyRow, TelegramOutboxRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow


def is_network_card(item, part=None):
    """Called after closed reply-basis validation; file parts own no card authority."""
    basis = item.payload.get("reply_basis", {})
    if basis.get("version") not in {5, 7}:
        return False
    from app.telegram_bots.producer import _FINAL_PARTS

    if part is None:
        version = item.payload.get("presentation_version", 0)
        part = next((value for value in item.parts if value.get("presentation_version") == version and value.get("state") not in _FINAL_PARTS), None)
    selected = [part] if part is not None else []
    if not any(value.get("source_ref", {}).get("key") == "human_input.presentation" for value in selected):
        return False
    return any(button.get("kind") == "network" for row in basis["keyboard"] for button in row)


async def check_network_manager(host, link, writer, owner, *, captured=None, notice_manager=None):
    """Borrow the UI writer; its final joined snapshot precedes commit/effect."""
    agents = getattr(getattr(host.authority.app, "state", None), "agent_instances", None)
    engine = host.links.session_factory.kw.get("bind")
    if agents is None or agents.directory.session_factory.kw.get("bind") is not engine or writer.bind is not engine or not writer.in_transaction():
        raise CallbackRefused()
    bot, instance, grant, policy = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, TelegramInstancePolicyRow
    statement = (
        select(bot, instance.status, instance.custody, grant.permissions, policy.policy, UserRow, DisabledIdentityRow)
        .join(instance, instance.id == bot.agent_ref)
        .outerjoin(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
        .outerjoin(policy, policy.instance_id == instance.id)
        .join(UserRow, UserRow.id == bot.owner_user_id)
        .outerjoin(DisabledIdentityRow, or_(DisabledIdentityRow.subject == UserRow.id, DisabledIdentityRow.subject == UserRow.oauth_id))
        .where(bot.id == link.id)
        .execution_options(populate_existing=True)
    )
    if captured is not None:
        statement = statement.add_columns(TelegramOutboxRow).join(TelegramOutboxRow, and_(TelegramOutboxRow.id == captured.item.id, TelegramOutboxRow.link_id == bot.id))
        if notice_manager is not None:
            from sqlalchemy.orm import aliased

            from app.telegram_bots.network_notices import validate_notice

            if type(notice_manager) is not bool:
                raise CallbackRefused()
            validate_notice(captured.item, captured.part)
            primary = aliased(TelegramOutboxRow)
            statement = statement.add_columns(primary).outerjoin(primary, primary.id == captured.item.payload["reply_basis"]["parent_id"])
        else:
            version = captured.item.payload.get("presentation_version", 0)
            groups = {part["callback_group_id"] for part in captured.item.parts if part.get("presentation_version") == version and part.get("source_ref", {}).get("key") == "human_input.presentation" and part.get("callback_group_id")}
            if len(groups) != 1:
                raise CallbackRefused()
            statement = statement.add_columns(TelegramCallbackRow).outerjoin(TelegramCallbackRow, TelegramCallbackRow.group_id == next(iter(groups)))
    rows = (await writer.execute(statement)).all()
    if not rows:
        raise CallbackRefused()
    current, status, custody, permissions, value, user, _ = rows[0][:7]
    owner.check_snapshot(user, [row[6] for row in rows])
    value = "allowed" if custody == "personal" or value is None else value
    fields = ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "token_revision", "binding_generation", "connection_id", "telegram_user_id", "chat_id")
    if (
        any(getattr(current, key) != getattr(link, key) for key in fields)
        or current.state != "connected"
        or current.health != "ok"
        or status != "active"
        or custody not in {"personal", "company"}
        or type(permissions) is not int
        or not permissions & int(AgentPermission.USE)
        or value not in POLICIES
        or value == "off"
        or pending_discard(current) is not None
        or getattr(host.authority.app.state, "agent_instances", None) is not agents
    ):
        raise CallbackRefused()
    manager = bool(permissions & int(AgentPermission.MANAGE))
    if notice_manager is not None and manager is not notice_manager:
        raise CallbackRefused()
    if notice_manager is None and not manager:
        raise CallbackRefused("net.blocked_ask")
    if captured is not None:
        from app.telegram_bots.callbacks import _validate_rows
        from app.telegram_bots.producer import Outbox, _item

        parent = rows[0][7]
        fresh = Outbox._sending(current, parent, link_id=link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
        if fresh != captured:
            raise CallbackRefused()
        if notice_manager is not None:
            from app.telegram_bots.network_notices import parent_ready

            if rows[0][8] is None or not parent_ready(captured.item, _item(rows[0][8]), state=rows[0][8].state):
                raise CallbackRefused()
        else:
            controls = {row[8].id: row[8] for row in rows if row[8] is not None}
            _validate_rows(parent, list(controls.values()))
            for control in controls.values():
                host.callbacks._usable(control)
    return value


async def authorize_network_card(host, captured, *, _notice_manager=None):
    """Final SDK decision shares one snapshot for grants, account, claim and UI."""
    from app.telegram_bots.authority import AccessOff, AuthorityUnavailable, CeilingRefused, require
    from app.telegram_bots.producer import DeliveryClaimRetired
    from deerflow.utils.file_io import await_drained

    async def perform():
        try:
            await require(captured.link.owner_user_id, "agents:read")
            owner = await host.prepare_live_owner(captured.link.owner_user_id)
            async with host.links.session_factory() as reader, reader.begin():
                policy = await check_network_manager(host, captured.link, reader, owner, captured=captured, notice_manager=_notice_manager)
                if policy == "protected" and not captured.part.get("protect_content"):
                    raise DeliveryClaimRetired()
        except (AccessOff, AuthorityUnavailable, CeilingRefused, ValueError, PermissionError):
            raise DeliveryClaimRetired() from None

    return await await_drained(perform())


async def authorize_network_notice(host, captured, *, manager):
    if type(manager) is not bool:
        from app.telegram_bots.producer import DeliveryClaimRetired

        raise DeliveryClaimRetired()
    return await authorize_network_card(host, captured, _notice_manager=manager)
