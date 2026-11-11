"""List publication authority over actual owner, binding and instance SQL rows."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_agent_list_source import lists as lists
from test_telegram_instance_presentation import presentation as presentation

from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow
from deerflow.persistence.user.access import DisabledIdentityRow

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("change", [None, "use", "deleted", "name", "open_bot", "owner", "binding"])
async def test_one_final_snapshot_rejects_lost_list_authority(lists, change):
    from app.telegram_bots.agent_list_authority import check_targets, source_basis
    from app.telegram_bots.agent_list_source import read_list
    from app.telegram_bots.callbacks import CallbackRefused

    e = lists
    result = await read_list(e.services, e.link, custody="company", before_read=e.list_current)
    basis = source_basis(result)
    owner = await e.services.prepare_live_owner(e.link.owner_user_id)
    async with e.db.sessions.begin() as writer:
        if change == "use":
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        elif change in {"deleted", "name"}:
            await writer.execute(update(AgentInstanceRow).values(**({"status": "deleted"} if change == "deleted" else {"name": "Changed name"})))
        elif change == "open_bot":
            await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.instance_link.id).values(bot_username="other_bot"))
        elif change == "owner":
            writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=datetime.now(UTC)))
        elif change == "binding":
            await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(binding_generation=e.link.binding_generation + 1))
    async with e.db.sessions.begin() as reader:
        if change is None:
            assert await check_targets(e.services, e.link, reader, owner, basis=basis)
        else:
            with pytest.raises(CallbackRefused):
                await check_targets(e.services, e.link, reader, owner, basis=basis)


async def test_role_limit_retires_captured_administrator_navigation(lists):
    from app.telegram_bots.agent_list_authority import check_targets, source_basis
    from app.telegram_bots.agent_list_source import read_list
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.persistence.user.access import RoleLimitRow
    from deerflow.persistence.user.model import UserRow

    e = lists
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(system_role="admin", oauth_provider="fixture", oauth_id="subject", oauth_issuer="https://id.example"))
    result = await read_list(e.services, e.link, custody="company", before_read=e.list_current)
    assert result.administrator
    basis = source_basis(result)
    owner = await e.services.prepare_live_owner(e.link.owner_user_id)
    async with e.db.sessions.begin() as writer:
        writer.add(RoleLimitRow(issuer="https://id.example", subject="subject", role="user", limited_at=datetime.now(UTC)))
    async with e.db.sessions.begin() as reader:
        with pytest.raises(CallbackRefused):
            await check_targets(e.services, e.link, reader, owner, basis=basis)
