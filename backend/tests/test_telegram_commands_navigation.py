"""Actual SQL commands/actions with controlled host target and no transport/model.

The pinned adapter, SDK and real target composition are qualified separately.
"""

import asyncio
import importlib
from datetime import timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select, update
from telegram import Chat, Message, Update, User
from test_telegram_callbacks import NOW
from test_telegram_callbacks import callbacks_environment as callbacks_environment  # noqa: F401
from test_telegram_publication_custody import publication_environment as publication_environment  # noqa: F401

from app.telegram_bots.acts import Acts
from app.telegram_bots.callbacks import CallbackRefused, Prompts
from app.telegram_bots.threads import ThreadCoordinator
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest_asyncio.fixture
async def command_environment(callbacks_environment, monkeypatch):
    from app.telegram_bots import pages

    e = callbacks_environment
    current, preparations = [True], []

    async def prepare_values(link):
        assert link.id == e.link.id
        preparations.append(True)
        return {"name": "Controlled assistant", "agent": "Controlled assistant", "product": "HartMesh"}

    services = SimpleNamespace(
        links=e.links,
        outbox=e.outbox,
        settings=e.db.settings,
        callbacks=e.callbacks,
        prompts=Prompts(e.callbacks),
        acts=Acts(e.sessions, settings=e.db.settings, callbacks=e.callbacks, outbox=e.outbox),
        product_name="HartMesh",
        prepare_live_owner=e.live.authority.prepare_live_owner,
        prepare_ingress_values=prepare_values,
    )
    channel = SimpleNamespace(link=e.link, _admission=lambda update: SimpleNamespace(link=e.link), _callback_current=lambda link: current[0] and link == e.link, _source=lambda update, purpose: f"{purpose}:{update.update_id}")
    monkeypatch.setattr(pages, "get_app_config", lambda: SimpleNamespace(model_extra={"telegram_bots": {"public_base_url": "https://product.example"}}))
    channel.application = SimpleNamespace(running=True)
    remote, creates = {}, []

    class ControlledThreadClient:
        endpoint_id = e.endpoint_id

        async def create_thread(self, owner, *, thread_id, metadata, before_dispatch):
            await before_dispatch()
            creates.append(thread_id)
            remote[thread_id] = {"thread_id": thread_id, "owner_user_id": owner, "agent_instance": None, "metadata": metadata}
            return remote[thread_id]

        async def get_thread(self, owner, thread_id, *, before_dispatch):
            await before_dispatch()
            return remote[thread_id]

    threads = ThreadCoordinator(links=e.links, authority=e.live.authority, client=ControlledThreadClient(), channel=lambda _identity: channel, send_allowed=lambda: current[0], outbox=e.outbox, product_name="HartMesh")

    async def controlled_target(*_args):
        return None

    # The actual controller/SQL are used; target loading and remote Gateway
    # proof remain controlled collaborators in this orchestration test.
    monkeypatch.setattr(threads, "_target", controlled_target)
    services.threads = threads
    commands = importlib.import_module("app.telegram_bots.commands").CallbackCommands(services)

    def incoming(word, identity=1801, topic=None):
        return Update(identity, message=Message(message_id=identity, date=NOW, chat=Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Human", False), text="/" + word, message_thread_id=topic))

    try:
        yield SimpleNamespace(**vars(e), services=services, channel=channel, current=current, commands=commands, incoming=incoming, preparations=preparations, creates=creates)
    finally:
        await threads.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("word,key", [("help", "help.custom_agent"), ("privacy", "privacy.full"), ("settings", "settings.state_basic"), ("web", "web.list")])
async def test_commands_save_exact_canonical_route_and_one_required_plan(command_environment, word, key):
    e = command_environment
    update = e.incoming(word)
    await e.commands.handle(e.channel, update, word)
    await e.commands.handle(e.channel, update, word)
    async with e.sessions() as session:
        origin = await session.get(TelegramInboundMarkRow, (e.link.id, update.message.message_id, e.link.chat_id))
        rows = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:command:1801")))
        assert len(rows) == 1
        assert origin.route_kind == "command" and origin.route_target_id == word and origin.route_complete
        assert origin.route_basis["key"] == key
        assert rows[0].payload["product_keyboard_basis"] == origin.route_basis["keyboard"]
        assert origin.delivery_ids == [rows[0].id]
        assert rows[0].parts[0]["source_ref"] == {"key": key, "values": origin.route_basis["values"]}
        if word == "help":
            assert origin.route_basis["values"]["voice_enabled"] is False
        if word == "settings":
            assert origin.route_basis["values"] == {"language": "English"}


@pytest.mark.asyncio
async def test_web_list_callback_repeats_per_query_but_same_delivery_is_idempotent(command_environment):
    e = command_environment
    await e.commands.handle(e.channel, e.incoming("help"), "help")
    async with e.sessions() as session:
        rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "web_list")))
        identity, group, anchor = rows[0].id, rows[0].group_id, rows[0].payload["anchor_outbox_id"]
    assert await e.callbacks.adopt(e.link, group, 501, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=anchor, presentation_version=0, check_current=lambda: True)
    row = await e.callbacks.get(identity)
    for query in ("one", "one", "two"):
        await e.commands.execute(e.channel, row, query_id=query)
    async with e.sessions() as session:
        rows = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["product_basis"]["parts"].is_not(None))))
        web = [row for row in rows if row.parts[0]["source_ref"]["key"] == "web.list"]
        assert len(web) == 2
    assert (await e.callbacks.get(identity)).consumed_at is None


@pytest.mark.asyncio
async def test_settings_leaf_uses_saved_choice_and_actual_atomic_act(command_environment):
    e = command_environment
    async with e.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).values(profile_pending=False))
    await e.commands.handle(e.channel, e.incoming("settings"), "settings")
    async with e.sessions() as session:
        row = next(row for row in await session.scalars(select(TelegramCallbackRow)) if row.payload.get("value") == "zh-CN")
        identity, group, anchor = row.id, row.group_id, row.payload["anchor_outbox_id"]
    assert await e.callbacks.adopt(e.link, group, 501, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=anchor, presentation_version=0, check_current=lambda: True)
    await e.commands.execute(e.channel, await e.callbacks.get(identity), query_id="settings-query")
    assert (await e.db.settings.get(e.link.owner_user_id)).locale == "zh-CN"
    assert (await e.db.links.get(e.link.id)).profile_pending is True, "Every public text of the person's bots follows the language"
    async with e.sessions() as session:
        act = await session.get(TelegramActRow, identity)
        assert act.state == "done" and act.body == {"field": "locale", "old": "en-US", "new": "zh-CN"}


@pytest.mark.asyncio
async def test_non_general_new_does_not_reserve_or_reset_general_conversation(command_environment):
    e = command_environment

    async def forbidden(*_args, **_kwargs):
        pytest.fail("A topic command must not reserve General")

    e.services.threads = SimpleNamespace(reserve_new_conversation=forbidden, new_conversation=forbidden)
    await e.commands.handle(e.channel, e.incoming("new", topic=99), "new")
    async with e.sessions() as session:
        row = await session.get(TelegramInboundMarkRow, (e.link.id, 1801, e.link.chat_id))
        assert row.route_complete and row.route_basis["key"] == "tap.failed"
        assert "thread_preparation" not in row.route_basis


async def new_choice(e, kind):
    await e.commands.handle(e.channel, e.incoming("new"), "new")
    async with e.sessions() as session:
        row = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind))
        identity, group, anchor = row.id, row.group_id, row.payload["anchor_outbox_id"]
        mark = await session.get(TelegramInboundMarkRow, (e.link.id, 1801, e.link.chat_id))
        assert not mark.route_complete and mark.route_basis["keyboard_template"] == "new.confirm_plain"
        assert row.payload["thread_id"] == mark.route_basis["thread_preparation"]["creation_id"]
    assert e.creates == [], "Offering a conversation must not make a Gateway creating call"
    assert await e.callbacks.adopt(e.link, group, 501, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=anchor, presentation_version=0, check_current=lambda: True)
    return await e.callbacks.get(identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,key,creates", [("new_keep", "new.kept", 0), ("new_confirm", "new.done", 1)])
async def test_new_choice_uses_original_command_and_finishes_only_after_choice(command_environment, kind, key, creates):
    e = command_environment
    selected = await new_choice(e, kind)
    await e.commands.execute(e.channel, selected, query_id="new-choice")
    assert len(e.creates) == creates
    async with e.sessions() as session:
        mark = await session.get(TelegramInboundMarkRow, (e.link.id, 1801, e.link.chat_id))
        assert mark.route_complete and mark.route_basis["callback_choice"]["callback_id"] == selected.id
        result = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:choice:" + selected.id))
        assert result.parts[0]["source_ref"]["key"] == key and result.parts[0]["target_message_id"] == 501
        assert result.payload["product_basis"]["origins"] == [[e.link.id, 1801, e.link.chat_id]]
    assert (await e.callbacks.get(selected.id)).consumed_at is not None


@pytest.mark.asyncio
async def test_lost_thread_completion_result_reuses_selected_bookmark_without_second_creation(command_environment, monkeypatch):
    e = command_environment
    selected = await new_choice(e, "new_confirm")
    original, faults = e.services.threads.new_conversation, [True]

    async def lost(*args, **kwargs):
        result = await original(*args, **kwargs)
        if faults and faults.pop():
            raise RuntimeError("controlled result loss after mapping commit")
        return result

    monkeypatch.setattr(e.services.threads, "new_conversation", lost)
    with pytest.raises(RuntimeError, match="controlled result loss"):
        await e.commands.execute(e.channel, selected, query_id="first")
    assert len(e.creates) == 1 and (await e.callbacks.get(selected.id)).consumed_at is None
    await e.commands.execute(e.channel, selected, query_id="second")
    assert len(e.creates) == 1 and (await e.callbacks.get(selected.id)).consumed_at is not None


async def saved_action(e, *, kind, target_kind=None, target_id=None, payload=None):
    identity = await e.outbox.plan_product_keyboard(
        e.link,
        source_id="saved-action",
        parts=[{"kind": "text", "source_ref": {"key": "tap.failed", "values": {}}}],
        keyboard=[[{"label": "label.close", "kind": kind, "target_kind": target_kind, "target_id": target_id, "payload": payload or {}}]],
        callbacks=e.callbacks,
        guard=e.guard_factory(),
    )
    async with e.sessions() as session:
        row = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity))
        callback_id, group = row.id, row.group_id
    assert await e.callbacks.adopt(e.link, group, 501, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=identity, presentation_version=0, check_current=lambda: True)
    return await e.callbacks.get(callback_id)


@pytest.mark.asyncio
async def test_dismiss_consumes_saved_group_and_edits_only_its_actual_anchor(command_environment):
    e = command_environment
    selected = await saved_action(e, kind="dismiss")
    await e.commands.execute(e.channel, selected, query_id="dismiss-query")
    async with e.sessions() as session:
        result = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:callback:" + selected.id))
        assert result.parts[0]["kind"] == "edit" and result.parts[0]["target_message_id"] == 501
        assert result.parts[0]["source_ref"] == {"key": "tap.failed", "values": {}}
        assert result.payload["product_keyboard_basis"] == []
    assert (await e.callbacks.get(selected.id)).closed_reason == "done"


@pytest.mark.asyncio
async def test_delivery_retry_only_requeues_its_saved_current_target_without_consuming_action(command_environment):
    e = command_environment
    target = await e.outbox.plan_product(e.link, source_id="retry-target", parts=[{"kind": "text", "source_ref": {"key": "tap.failed", "values": {}}}])
    async with e.sessions.begin() as session:
        item = await session.get(TelegramOutboxRow, target)
        item.state, item.failed_reason, item.closed_at = "failed", "network", NOW
    selected = await saved_action(e, kind="delivery_retry", target_kind="outbox", target_id=target, payload={"outbox_id": target})
    await e.commands.execute(e.channel, selected, query_id="retry-query")
    async with e.sessions() as session:
        item = await session.get(TelegramOutboxRow, target)
        assert item.state == "pending" and item.failed_reason is None and item.closed_at is None
    assert (await e.callbacks.get(selected.id)).consumed_at is None and e.creates == []


@pytest.mark.asyncio
async def test_future_action_is_fixed_failure_without_consuming_or_creating(command_environment):
    e = command_environment
    selected = await saved_action(e, kind="voice_ok")
    await e.commands.execute(e.channel, selected, query_id="future-query")
    async with e.sessions() as session:
        results = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:callback:"))))
        assert len(results) == 1
        result = results[0]
        assert result.parts[0]["source_ref"] == {"key": "tap.failed", "values": {}}
        assert result.payload["product_keyboard_basis"] == []
    assert (await e.callbacks.get(selected.id)).consumed_at is None and e.creates == []


@pytest.mark.asyncio
async def test_original_command_retirement_during_target_preparation_admits_no_inbox(command_environment, monkeypatch):
    e = command_environment
    started, release = asyncio.Event(), asyncio.Event()

    async def held(_link):
        started.set()
        await release.wait()
        return {"product": "HartMesh", "agent": "Controlled assistant"}

    monkeypatch.setattr(e.services, "prepare_ingress_values", held)
    async with e.sessions() as session:
        prior = set(await session.scalars(select(TelegramOutboxRow.id)))
    task = asyncio.create_task(e.commands.handle(e.channel, e.incoming("settings"), "settings"))
    try:
        await asyncio.wait_for(started.wait(), 3)
        e.current[0] = False
        release.set()
        with pytest.raises(CallbackRefused):
            await task
        async with e.sessions() as session:
            assert await session.get(TelegramInboundMarkRow, (e.link.id, 1801, e.link.chat_id)) is None
            assert set(await session.scalars(select(TelegramOutboxRow.id))) == prior
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_command_navigation_closes_only_its_topic_prompt(command_environment):
    e = command_environment
    local = await e.services.prompts.open(e.link, "controlled-future", {}, chat_id=e.link.chat_id, message_thread_id=None, message_id=801)
    other = await e.services.prompts.open(e.link, "controlled-future", {}, chat_id=e.link.chat_id, message_thread_id=99, message_id=802)
    await e.commands.handle(e.channel, e.incoming("help"), "help")
    assert (await e.callbacks.get(local)).closed_reason == "command"
    assert (await e.callbacks.get(other)).consumed_at is None


@pytest.mark.asyncio
async def test_retired_web_callback_refuses_before_fresh_navigation_preparation(command_environment, monkeypatch):
    e = command_environment
    selected = await saved_action(e, kind="web_list", target_kind="link", target_id=e.link.id)
    e.current[0] = False

    async def forbidden(_link):
        pytest.fail("A retired callback must not start navigation preparation")

    monkeypatch.setattr(e.commands, "_web_keyboard", forbidden)
    with pytest.raises(CallbackRefused):
        await e.commands.execute(e.channel, selected, query_id="retired-web")


@pytest.mark.asyncio
async def test_retirement_after_real_route_commit_does_not_close_prompt_or_start_next_leaf(command_environment, monkeypatch):
    e = command_environment
    prompt = await e.services.prompts.open(e.link, "controlled-future", {}, chat_id=e.link.chat_id, message_thread_id=None, message_id=801)
    committed, release, closes = asyncio.Event(), asyncio.Event(), []
    route, close = e.outbox.inbox.route, e.services.prompts.close

    async def held_route(*args, **kwargs):
        result = await route(*args, **kwargs)
        committed.set()
        await release.wait()
        return result

    async def observed_close(*args, **kwargs):
        closes.append(True)
        return await close(*args, **kwargs)

    monkeypatch.setattr(e.outbox.inbox, "route", held_route)
    monkeypatch.setattr(e.services.prompts, "close", observed_close)
    task = asyncio.create_task(e.commands.handle(e.channel, e.incoming("help"), "help"))
    try:
        await asyncio.wait_for(committed.wait(), 3)
        e.current[0] = False
        release.set()
        with pytest.raises(CallbackRefused):
            await task
        assert closes == []
        assert (await e.callbacks.get(prompt)).consumed_at is None
        async with e.sessions() as session:
            mark = await session.get(TelegramInboundMarkRow, (e.link.id, 1801, e.link.chat_id))
            assert mark.route_kind == "command" and not mark.route_complete
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("valid", [False, True])
async def test_authenticated_web_navigation_clears_only_its_scope_guard_without_consuming_keyboard(command_environment, valid):
    e = command_environment
    selected = await saved_action(e, kind="web_list", target_kind="link", target_id=e.link.id)
    local = await e.services.prompts.open(e.link, "controlled-future", {}, chat_id=e.link.chat_id, message_thread_id=None, message_id=801)
    other = await e.services.prompts.open(e.link, "controlled-future", {}, chat_id=e.link.chat_id, message_thread_id=99, message_id=802)
    e.clock[0] += timedelta(seconds=601)
    assert (await e.services.prompts.current(e.link, e.link.chat_id, None)).closed_reason == "expired"
    assert (await e.services.prompts.current(e.link, e.link.chat_id, 99)).closed_reason == "expired"
    if not valid:
        async with e.sessions.begin() as session:
            actual = await session.get(TelegramCallbackRow, selected.id)
            actual.consumed_at, actual.closed_reason = NOW, "done"
        with pytest.raises(CallbackRefused):
            await e.commands.execute(e.channel, selected, query_id="closed-navigation")
        assert (await e.callbacks.get(local)).closed_reason == "expired"
    else:
        await e.commands.execute(e.channel, selected, query_id="navigation")
        assert await e.services.prompts.current(e.link, e.link.chat_id, None) is None
        assert (await e.callbacks.get(local)).closed_reason == "navigation"
        assert (await e.callbacks.get(selected.id)).consumed_at is None
    assert (await e.callbacks.get(other)).closed_reason == "expired"
