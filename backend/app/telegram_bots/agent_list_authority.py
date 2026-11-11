"""One joined owner/binding/target snapshot before list publication or delivery."""

import re
from copy import deepcopy

from sqlalchemy import and_, false, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow
from deerflow.persistence.user.access import DisabledIdentityRow, RoleLimitRow, issuer_key
from deerflow.persistence.user.model import UserRow


def validate_basis(basis):
    if type(basis) is not dict or set(basis) != {"version", "owner_user_id", "custody", "targets", "open_bots", "administrator"} or type(basis["version"]) is not int or basis["version"] != 1:
        raise CallbackRefused()
    if type(basis["owner_user_id"]) is not str or not basis["owner_user_id"] or basis["custody"] not in {"personal", "company"} or type(basis["administrator"]) is not bool:
        raise CallbackRefused()
    if type(basis["targets"]) is not list or len(basis["targets"]) > 20 or type(basis["open_bots"]) is not dict:
        raise CallbackRefused()
    seen = set()
    for target in basis["targets"]:
        if type(target) is not dict or set(target) != {"id", "name", "custody", "status", "generation", "definition_revision", "permissions", "owner_id"}:
            raise CallbackRefused()
        if (
            type(target["id"]) is not str
            or not re.fullmatch(r"[0-9a-f]{32}", target["id"])
            or target["id"] in seen
            or type(target["name"]) is not str
            or not target["name"]
            or len(target["name"]) > 128
            or target["custody"] != basis["custody"]
            or target["status"] not in {"active", "suspended", "archived"}
            or type(target["generation"]) is not int
            or target["generation"] < 1
            or type(target["definition_revision"]) is not str
            or not re.fullmatch(r"[0-9a-f]{64}", target["definition_revision"])
            or type(target["permissions"]) is not int
            or target["permissions"] not in {1, 3}
            or target["owner_id"] is not None
            and type(target["owner_id"]) is not str
        ):
            raise CallbackRefused()
        seen.add(target["id"])
    if set(basis["open_bots"]) - seen:
        raise CallbackRefused()
    for bot in basis["open_bots"].values():
        if (
            type(bot) is not dict
            or set(bot) != {"id", "bot_id", "bot_username"}
            or type(bot["id"]) is not str
            or type(bot["bot_id"]) is not int
            or type(bot["bot_username"]) is not str
            or not re.fullmatch(r"[A-Za-z0-9_]{5,32}", bot["bot_username"])
        ):
            raise CallbackRefused()
    return basis


def source_basis(source):
    targets = {entry.id for entry in source.snapshot.entries}
    bots = {
        bot.agent_ref: {"id": bot.id, "bot_id": bot.bot_id, "bot_username": bot.bot_username}
        for bot in source.bots
        if bot.owner_user_id == source.snapshot.owner_user_id and bot.agent_kind == "instance" and bot.agent_ref in targets and type(bot.bot_username) is str and re.fullmatch(r"[A-Za-z0-9_]{5,32}", bot.bot_username)
    }
    return validate_basis({"version": 1, "owner_user_id": source.snapshot.owner_user_id, "custody": source.snapshot.custody, "targets": deepcopy(list(source.facts)), "open_bots": bots, "administrator": source.administrator})


async def check_targets(host, link, writer, owner, *, basis, captured=None):
    from app.telegram_bots.agent_list_source import _fact
    from app.telegram_bots.authority import AccessOff, AuthorityUnavailable

    validate_basis(basis)
    agents = getattr(host.authority.app.state, "agent_instances", None)
    engine = host.links.session_factory.kw.get("bind")
    if agents is None or agents.directory.session_factory.kw.get("bind") is not engine:
        raise AuthorityUnavailable()
    if writer.bind is not engine or not writer.in_transaction() or link.agent_kind != "lead_agent" or link.agent_ref != "lead_agent" or link.owner_user_id != basis["owner_user_id"]:
        raise CallbackRefused()
    targets = {target["id"]: target for target in basis["targets"]}
    bot, instance, grant, owned = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, aliased(TelegramBotLinkRow)
    query = (
        select(bot, UserRow, DisabledIdentityRow, RoleLimitRow, instance, grant.permissions, owned)
        .join(UserRow, UserRow.id == bot.owner_user_id)
        .outerjoin(DisabledIdentityRow, or_(DisabledIdentityRow.subject == UserRow.id, DisabledIdentityRow.subject == UserRow.oauth_id))
        .outerjoin(RoleLimitRow, RoleLimitRow.subject == UserRow.oauth_id)
        .outerjoin(instance, instance.id.in_(targets) if targets else false())
        .outerjoin(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
        .outerjoin(owned, and_(owned.agent_kind == "instance", owned.agent_ref == instance.id, owned.owner_user_id == bot.owner_user_id))
        .where(bot.id == link.id)
        .execution_options(populate_existing=True)
    )
    if captured is not None:
        query = query.add_columns(TelegramOutboxRow).join(TelegramOutboxRow, and_(TelegramOutboxRow.id == captured.item.id, TelegramOutboxRow.link_id == bot.id))
    rows = (await writer.execute(query)).all()
    if not rows:
        raise CallbackRefused()
    current, user = rows[0][:2]
    try:
        owner.check_snapshot(user, [row[2] for row in rows])
    except AccessOff:
        raise CallbackRefused() from None
    limited = any(row[3] is not None and row[3].role != "admin" and (not user.oauth_issuer or row[3].issuer == issuer_key(user.oauth_issuer)) for row in rows) if user.oauth_id and user.oauth_provider else False
    administrator = user.system_role == "admin" and not limited
    scope = ("id", "owner_user_id", "agent_kind", "agent_ref", "bot_id", "token_revision", "binding_generation", "connection_id", "telegram_user_id", "chat_id")
    if (
        any(getattr(current, name) != getattr(link, name) for name in scope)
        or current.state != "connected"
        or current.health != "ok"
        or administrator != basis["administrator"]
        or getattr(host.authority.app.state, "agent_instances", None) is not agents
    ):
        raise CallbackRefused()
    found = set()
    for row in rows:
        target, permissions, own_bot = row[4:7]
        if target is None:
            continue
        expected = targets.get(target.id)
        if expected is None or type(permissions) is not int or permissions & expected["permissions"] != expected["permissions"] or _fact(target, required=expected["permissions"]) != expected:
            raise CallbackRefused()
        if target.id in basis["open_bots"] and (own_bot is None or {key: getattr(own_bot, key) for key in ("id", "bot_id", "bot_username")} != basis["open_bots"][target.id]):
            raise CallbackRefused()
        found.add(target.id)
    if found != set(targets):
        raise CallbackRefused()
    if captured is not None:
        from app.telegram_bots.producer import Outbox

        fresh = Outbox._sending(current, rows[0][7], link_id=link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
        if fresh != captured or captured.item.payload.get("agent_list_basis") != basis:
            raise CallbackRefused()
    return True


class AgentListGuard:
    def __init__(self, host, base, basis):
        self.host, self.base = host, base
        self.basis = deepcopy(validate_basis(basis))
        self.captured_link = base.captured_link
        self.owner = None

    async def prepare(self):
        from app.telegram_bots.authority import require

        await self.base.prepare()
        await require(self.captured_link.owner_user_id, "agents:read")
        self.owner = await self.host.prepare_live_owner(self.captured_link.owner_user_id)
        self.base.current()

    async def check(self, writer):
        current = await self.base.check(writer)
        if self.owner is None:
            raise CallbackRefused()
        await check_targets(self.host, current, writer, self.owner, basis=self.basis)
        self.base.current()
        return current


async def authorize_list(host, captured):
    from app.telegram_bots.authority import AccessOff, CeilingRefused, require
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.utils.file_io import await_drained

    async def read():
        try:
            basis = validate_basis(captured.item.payload.get("agent_list_basis"))
            if captured.part.get("source_ref", {}).get("key") != "agents.presentation":
                raise CallbackRefused()
            await require(captured.link.owner_user_id, "agents:read")
            owner = await host.prepare_live_owner(captured.link.owner_user_id)
            async with host.links.session_factory() as reader, reader.begin():
                await check_targets(host, captured.link, reader, owner, basis=basis, captured=captured)
        except (AccessOff, CeilingRefused, CallbackRefused):
            raise ProductSourceRetired() from None

    return await await_drained(read())
