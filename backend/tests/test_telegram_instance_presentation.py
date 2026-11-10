"""Instance presentation uses actual grants and adopted definition evidence."""

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_instance import instance_environment
from test_telegram_threads import close

from deerflow.agent_instances.contract import DefinitionSnapshot
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.spaces.contract import PrincipalRef

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def presentation(db, monkeypatch):
    e = await instance_environment(db, monkeypatch)
    e.db = db
    e.live.config.ui.roles = []
    e.services = e.channel._services
    e.services.authority = e.live.authority
    e.services._app_config = e.live.config
    e.services._app = e.http_app
    from app.telegram_bots.supervisor import TelegramSupervisor

    async def values(link):
        return await TelegramSupervisor.prepare_ingress_values(e.services, link)

    e.services.prepare_ingress_values = values
    e.services.prepare_live_owner = e.live.authority.prepare_live_owner
    e.services.product_name = "HartMesh"
    from app.telegram_bots.callbacks import CallbackRepository, Prompts

    e.callbacks = e.services.callbacks = CallbackRepository(db.sessions, config=db.config)
    e.prompts = e.services.prompts = Prompts(e.callbacks)
    e.services.channels = lambda: (e.channel,)
    e.services.channel = lambda identity: e.channel if identity == e.link.id else None
    try:
        yield e
    finally:
        await close(e)


async def definition(e, name, permissions=7):
    snapshot = DefinitionSnapshot.capture(owner_id=e.link.owner_user_id, config={"name": name, "description": "Private definition text"}, soul="Private adopted soul")
    async with e.db.sessions.begin() as writer:
        writer.add(AgentDefinitionRevisionRow(revision=snapshot.revision, owner_id=snapshot.owner_id, config=snapshot.config, soul=snapshot.soul))
        await writer.flush()
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(definition_revision=snapshot.revision))
        await writer.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=permissions))


@pytest.mark.parametrize("name,permissions,found", [("analyst-1234abcd", 7, True), ("analyst-1234abcd", 1, False), ("analyst", 7, False), ("removed-1234abcd", 7, False)])
async def test_role_requires_inspect_valid_definition_suffix_and_current_catalog(presentation, monkeypatch, name, permissions, found):
    from app.telegram_bots.roles import role_of
    from deerflow.config.ui_config import RoleConfig

    e = presentation
    e.live.config.ui.roles = [RoleConfig(id="analyst", name="Reporting analyst", description="Explains reports", soul="Public role guidance")]
    await definition(e, name, permissions)
    result = await role_of(e.http_app, PrincipalRef("human", e.link.owner_user_id), e.link.agent_ref)
    assert (result is not None) is found
    if found:
        assert result["name"] == "Reporting analyst" and result["description"] == "Explains reports"
        assert "Private" not in str(result)


@pytest.mark.parametrize("custody,other_inspector,shared", [("personal", False, False), ("personal", True, True), ("company", False, False), ("company", True, True)])
async def test_privacy_uses_current_readers_for_both_kinds(presentation, custody, other_inspector, shared):
    e = presentation
    await definition(e, "custom", 7)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(custody=custody, owner_id=e.link.owner_user_id if custody == "personal" else None))
        if other_inspector:
            writer.add(AgentInstanceGrantRow(instance_id=e.link.agent_ref, user_id="another-reader", permissions=2))
    from app.telegram_bots.presentation import instance_values

    values = await instance_values(e.live.authority, e.link, product="HartMesh")
    assert values["custody"] == custody and values["read_by_others"] is shared
    assert values["role"] is None and values["role_description"] is None
    assert values["name"] == "Fixture employee"


async def test_help_privacy_and_pages_use_current_instance_grants(presentation):
    from app.telegram_bots.commands import CallbackCommands

    e = presentation
    await definition(e, "custom", 7)
    e.live.config.model_extra["telegram_bots"] = {"enabled": True, "public_base_url": "https://app.example"}
    commands = CallbackCommands(e.services)
    help_ = await commands._presentation("help", e.link)
    assert help_["key"] == "help.agent" and help_["values"]["custody"] == "company"
    privacy = await commands._presentation("privacy", e.link)
    assert privacy["values"]["read_by_others"] is False
    pages = await commands._web_keyboard(e.link)
    assert any("tab=files" in button["url"] for row in pages for button in row)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=1))
    pages = await commands._web_keyboard(e.link)
    assert not any("tab=files" in button["url"] or "tab=memory" in button["url"] or "tab=access" in button["url"] for row in pages for button in row)


@pytest.mark.parametrize("custody", ["personal", "company"])
async def test_instance_profile_names_kind_and_registers_current_menu(presentation, custody):
    from test_telegram_profile import script, sent

    from app.telegram_bots.profile import ProfileSync

    e = presentation
    await definition(e, "custom", 7)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(custody=custody, owner_id=e.link.owner_user_id if custody == "personal" else None))
    # The actual SDK request recorder belongs to the channel created by the fixture.
    e.request = e.channel.bot.request
    script(e)
    assert await ProfileSync(e.services).push(e.link)
    assert [item["command"] for item in sent(e, "setMyCommands")["commands"]] == ["new", "stop", "settings", "help"]
    kind = "AI assistant" if custody == "personal" else "AI employee"
    assert kind in sent(e, "setMyShortDescription")["short_description"]
    assert kind in sent(e, "setMyDescription")["description"]
    assert "Private definition" not in str(e.request.calls)


@pytest.mark.parametrize("custody", ["personal", "company"])
async def test_connect_records_instance_hello_and_silent_privacy(presentation, custody):
    from _telegram_repository_test_support import code_of, rows
    from test_telegram_bot_handlers import message

    from app.telegram_bots.strangers import StrangerLedger
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = presentation
    await definition(e, "custom", 7)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(custody=custody, owner_id=e.link.owner_user_id if custody == "personal" else None))
    e.services.strangers = StrangerLedger()
    e.link = await e.db.links.unbind(e.link, actor_id=e.link.owner_user_id)
    e.channel.link = e.link
    code = code_of(await e.db.links.mint_code(e.link))
    assert await e.channel.handle_update(message(e, 1800, content="/start " + code))
    planned = [row for row in await rows(e.db, TelegramOutboxRow) if row.binding_generation == e.channel.link.binding_generation and row.state != "skipped"]
    assert len(planned) == 1
    first, privacy = planned[0].parts
    assert first["source_ref"]["key"] == ("hello.assistant" if custody == "personal" else "hello.employee")
    assert first["source_ref"]["values"]["read_by_others"] is False
    assert privacy["source_ref"]["key"] == "hello.privacy" and privacy["silent"] is True


async def test_instance_unknown_slash_never_looks_up_a_skill(presentation, monkeypatch):
    from test_telegram_bot_handlers import message

    from app.telegram_bots import adapter

    e = presentation

    def forbidden(*args):
        raise AssertionError("An instance command must never resolve a custom skill")

    monkeypatch.setattr(adapter, "slash_skill_outcome", forbidden)
    assert await e.channel.handle_update(message(e, 1801, content="/fictional_skill something"))
    from _telegram_repository_test_support import rows

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    assert any(part.get("source_ref", {}).get("key") == "cmd.unknown" for row in await rows(e.db, TelegramOutboxRow) for part in row.parts)


@pytest.mark.parametrize("in_limiter", [False, True])
async def test_saved_role_presentation_requires_inspect_at_delivery(presentation, in_limiter):
    from _telegram_repository_test_support import rows
    from test_telegram_bot_handlers import message

    from app.telegram_bots.commands import CallbackCommands
    from app.telegram_bots.producer import OutboxItem
    from deerflow.config.ui_config import RoleConfig
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = presentation
    await e.db.links.end_pending(e.link.id, skip_reason="superseded")
    e.live.config.ui.roles = [RoleConfig(id="analyst", name="Reporting analyst", description="Explains reports", soul="Public role guidance")]
    await definition(e, "analyst-1234abcd", 7)
    e.services.commands = CallbackCommands(e.services)
    e.services.callbacks = e.callbacks
    e.services.prompts = e.prompts
    assert await e.channel.handle_update(message(e, 1802, content="/help"))
    row = next(row for row in await rows(e.db, TelegramOutboxRow) if row.parts[0].get("source_ref", {}).get("key") == "help.agent")
    item = OutboxItem(**{name: getattr(row, name) for name in OutboxItem.__dataclass_fields__})
    assert item.parts[0]["source_ref"]["values"]["role"] == "Reporting analyst"
    assert (await e.live.authority.delivery(e.link, item, item.parts[0])).decision == "allow"

    async def withdraw():
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=1))

    if in_limiter:
        import asyncio

        from app.telegram_bots.producer import OutboxSender

        e.services.eligibility = e.live.authority.delivery
        limiter = e.channel.bot.rate_limiter

        async def changed(delay):
            await withdraw()
            limiter._short_hold_until = 0

        limiter._sleep = changed
        limiter._short_hold_until = limiter._time() + 1

        async def raw(*, link_id, **kwargs):
            return await e.channel.send_part(**kwargs)

        sender = OutboxSender(e.services.outbox, run_id="role-limiter-fixture", deliver_part=raw, eligibility=e.live.authority.delivery)
        try:
            assert await sender.tick() == 1
            await asyncio.wait_for(sender.drain(), 3)
            assert [call.endpoint for call in e.channel.bot.request.calls] == ["getMe"]
        finally:
            await sender.stop()
    else:
        await withdraw()
    assert (await e.live.authority.delivery(e.link, item, item.parts[0])).decision != "allow"


async def test_colleague_assistant_hello_names_owner_and_always_discloses_readers(presentation):
    from uuid import uuid4

    from app.telegram_bots.presentation import instance_values
    from app.telegram_bots.render import render_plain
    from deerflow.persistence.user.model import UserRow

    e = presentation
    await definition(e, "custom", 7)
    owner = str(uuid4())
    async with e.db.sessions.begin() as writer:
        writer.add(UserRow(id=owner, email="colleague@example.com", system_role="user"))
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(custody="personal", owner_id=owner))
    values = await instance_values(e.live.authority, e.link, product="HartMesh")
    assert values["owner"] == "colleague@example.com" and values["read_by_others"] is True
    text = render_plain("hello.assistant", "en", email="reader@example.test", **values)
    assert "colleague@example.com's AI assistant" in text and "People who can see all" in text


async def test_invalid_adopted_definition_omits_role_without_blocking_fixed_commands(presentation):
    from app.telegram_bots.commands import CallbackCommands

    e = presentation
    await definition(e, "analyst-1234abcd", 7)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentDefinitionRevisionRow).values(config={"name": "corrupt"}))
    commands = CallbackCommands(e.services)
    help_ = await commands._presentation("help", e.link)
    assert help_["values"]["role"] is None and help_["values"]["role_description"] is None
    assert (await commands._presentation("privacy", e.link))["key"] == "privacy.full"
    assert (await commands._presentation("settings", e.link))["key"] == "settings.state_basic"
