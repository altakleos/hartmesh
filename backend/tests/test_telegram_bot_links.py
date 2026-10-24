"""Real SQL capacity, borrowed writer, cleanup and replacement boundaries."""

import asyncio
from dataclasses import FrozenInstanceError, fields
from datetime import UTC, datetime, timedelta

import pytest
from _telegram_repository_test_support import add_link, bind_link, code_of, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import event, update

from app.telegram_bots.links import AgentLinked, BotLink, BotLinked, BotLinkRepository, LimitDeployment, LimitPerson, TooManyCodes
from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelConversationRow, ChannelCredentialRow, ChannelOAuthStateRow
from deerflow.persistence.telegram_bots.model import (
    TelegramAccountRow,
    TelegramAssistantAddRow,
    TelegramBotLinkEventRow,
    TelegramBotLinkRow,
    TelegramCallbackRow,
    TelegramDeploymentRow,
    TelegramOutboxRow,
    TelegramPendingFileRow,
    TelegramPersonSettingsRow,
    TelegramWorkTopicRow,
)

pytestmark = pytest.mark.asyncio


async def test_bot_link_projection_excludes_token_and_is_frozen(db):
    link = await add_link(db)
    assert isinstance(link, BotLink)
    assert {f.name for f in fields(BotLink)} == set(TelegramBotLinkRow.__table__.columns.keys()) - {"encrypted_token"}
    assert "wrapped-fixture" not in repr(link)
    assert await db.links.token(link.id) == ("wrapped-fixture-9000000001", 1)
    assert link.created_at.tzinfo is not None
    with pytest.raises(FrozenInstanceError):
        link.id = "changed"
    assert await db.links.get_by_bot(link.bot_id) == link
    assert await db.links.list_for_agent(link.agent_kind, link.agent_ref) == [link]


async def test_bot_id_unique_per_deployment(db):
    first = await add_link(db)
    for owner, own in [(first.owner_user_id, True), ("owner-b", False)]:
        with pytest.raises(BotLinked) as exc:
            await add_link(db, owner=owner, target="other")
        assert exc.value.own is own
        assert not own and exc.value.agent_name is None or own
    assert len(await db.links.list_all()) == 1


async def test_one_link_per_person_per_agent(db):
    first = await add_link(db, target="same-agent")
    with pytest.raises(AgentLinked) as exc:
        await add_link(db, bot=first.bot_id + 1, target="same-agent")
    assert exc.value.bot_id == first.bot_id
    await add_link(db, owner="owner-b", bot=first.bot_id + 2, target="same-agent")
    assert len(await db.links.list_all()) == 2


async def test_limits_per_person_and_deployment_include_every_state(db):
    db.config.max_bots, db.config.max_bots_per_person = 4, 3
    for index, state in enumerate(["connected", "held", "blocked"]):
        row = await add_link(db, bot=9_000_000_001 + index)
        await db.links.update(row.id, state=state)
    with pytest.raises(LimitPerson):
        await add_link(db, bot=9_000_000_004)
    await add_link(db, owner="owner-b", bot=9_000_000_004)
    with pytest.raises(LimitDeployment):
        await add_link(db, owner="owner-c", bot=9_000_000_005)


async def test_deployment_capacity_serializes_distinct_owners(db):
    db.config.max_bots = 1
    start = asyncio.Event()

    async def compete(index):
        await start.wait()
        owner = f"owner-{index}"
        try:
            async with db.links.capacity_writer(owner, timezone="UTC", locale="en-US") as session:
                await db.links.add(owner_user_id=owner, agent_kind="lead_agent", agent_ref="lead_agent", bot_id=9_000_000_010 + index, bot_username=f"fixture_{index}_bot", encrypted_token="wrapped", actor_id=owner, session=session)
            return "added"
        except LimitDeployment:
            return "full"

    tasks = [asyncio.create_task(compete(i)) for i in range(4)]
    start.set()
    outcomes = await asyncio.gather(*tasks)
    assert outcomes.count("added") == 1 and outcomes.count("full") == 3
    assert len(await rows(db, TelegramBotLinkRow)) == len(await rows(db, TelegramPersonSettingsRow)) == 1
    assert len(await rows(db, TelegramDeploymentRow)) == 1


@pytest.mark.parametrize("same_bot", [True, False])
async def test_two_adds_give_one_typed_uniqueness_refusal(db, same_bot):
    await db.settings.ensure("owner", timezone="UTC", locale="en-US")
    start = asyncio.Event()

    async def compete(index):
        await start.wait()
        try:
            return await db.links.add(
                owner_user_id="owner",
                agent_kind="custom_agent",
                agent_ref=f"target-{index}" if same_bot else "same",
                bot_id=9_000_000_010 + (0 if same_bot else index),
                bot_username="fixture_bot",
                encrypted_token="wrapped",
                actor_id="owner",
            )
        except (BotLinked, AgentLinked) as exc:
            return exc

    tasks = [asyncio.create_task(compete(i)) for i in range(2)]
    start.set()
    outcomes = await asyncio.gather(*tasks)
    assert sum(isinstance(x, BotLink) for x in outcomes) == 1
    assert sum(isinstance(x, BotLinked if same_bot else AgentLinked) for x in outcomes) == 1


async def test_first_owner_add_and_code_roll_back_together_without_nested_sessions(db):
    calls, commits = 0, 0
    original = db.sessions

    def factory():
        nonlocal calls
        calls += 1
        return original()

    @event.listens_for(db.engine.sync_engine, "commit")
    def committed(connection):
        nonlocal commits
        commits += 1

    links = BotLinkRepository(factory, db.connections, db.config)
    with pytest.raises(RuntimeError, match="fixture-rollback"):
        async with links.capacity_writer("first-owner", timezone="Europe/London", locale="zh-CN") as session:
            link = await links.add(owner_user_id="first-owner", agent_kind="lead_agent", agent_ref="lead_agent", bot_id=9_000_000_001, bot_username="fixture_bot", encrypted_token="wrapped", actor_id="first-owner", session=session)
            await links.mint_code(link, session=session)
            assert calls == 1 and commits == 0
            raise RuntimeError("fixture-rollback")
    assert calls == 1 and commits == 0
    for cls in [TelegramPersonSettingsRow, TelegramBotLinkRow, ChannelOAuthStateRow, TelegramBotLinkEventRow, TelegramDeploymentRow]:
        assert not await rows(db, cls)


async def test_capacity_writer_preserves_existing_settings_and_copy_state(db):
    settings = await db.settings.ensure("owner", timezone="Asia/Shanghai", locale="zh-CN")
    now = datetime.now(UTC)
    async with db.sessions.begin() as session:
        session.add(TelegramDeploymentRow(id=1, host_id="kept-host", polling_state="host_changed", adopted_at=now, updated_at=now))
    with pytest.raises(RuntimeError):
        async with db.links.capacity_writer("owner", timezone="bad-ignored", locale="bad-ignored"):
            raise RuntimeError("fixture")
    assert await db.settings.get("owner") == settings
    async with db.links.capacity_writer("owner"):
        pass
    deployment = (await rows(db, TelegramDeploymentRow))[0]
    assert (deployment.host_id, deployment.polling_state) == ("kept-host", "host_changed")


async def test_failed_first_code_admission_leaves_no_partial_settings_or_link(db):
    now = datetime.now(UTC)
    async with db.sessions.begin() as session:
        session.add_all([ChannelOAuthStateRow(state_hash=f"hash-{i}", owner_user_id="first", provider="telegram", expires_at=now + timedelta(hours=1)) for i in range(5)])
    with pytest.raises(TooManyCodes):
        async with db.links.capacity_writer("first", timezone="UTC", locale="en-US") as session:
            link = await db.links.add(owner_user_id="first", agent_kind="lead_agent", agent_ref="lead_agent", bot_id=9_000_000_001, bot_username="fixture_bot", encrypted_token="wrapped", actor_id="first", session=session)
            await db.links.mint_code(link, session=session)
    assert not await rows(db, TelegramPersonSettingsRow)
    assert not await rows(db, TelegramBotLinkRow)
    assert not await rows(db, TelegramBotLinkEventRow)
    assert len(await rows(db, ChannelOAuthStateRow)) == 5


async def test_missing_settings_with_existing_links_is_not_repaired(db):
    link = await add_link(db)
    from sqlalchemy import delete

    async with db.sessions.begin() as session:
        await session.execute(delete(TelegramPersonSettingsRow))
    with pytest.raises(RuntimeError, match="settings"):
        async with db.links.capacity_writer(link.owner_user_id, timezone="UTC", locale="en-US"):
            pass
    assert not await rows(db, TelegramPersonSettingsRow)


async def test_empty_owner_removal_creates_no_settings(db):
    assert await db.links.remove_all_for_owner("missing", actor_id="host") == []
    assert await db.links.disconnect_all("missing", actor_id="host") == 0
    assert not await rows(db, TelegramPersonSettingsRow)


async def test_codes_waiting_and_remove_end_only_current_links_code(db):
    one = await add_link(db)
    two = await add_link(db, bot=one.bot_id + 1)
    other = await add_link(db, owner="owner-b", bot=one.bot_id + 2)
    first = await db.links.mint_code(one)
    again = await db.links.mint_code(one)
    await db.links.mint_code(two)
    await db.links.mint_code(other)
    assert code_of(first) != code_of(again)
    assert await db.links.codes_waiting(one.owner_user_id) == 2
    assert await db.links.codes_waiting(one.owner_user_id, except_link_id=one.id) == 1
    async with db.sessions.begin() as session:
        session.add(ChannelOAuthStateRow(state_hash="expired", owner_user_id=one.owner_user_id, provider="telegram", expires_at=datetime.now(UTC) - timedelta(seconds=1)))
        session.add(ChannelOAuthStateRow(state_hash="other-provider", owner_user_id=one.owner_user_id, provider="slack", expires_at=datetime.now(UTC) + timedelta(hours=1)))
    assert await db.links.codes_waiting(one.owner_user_id) == 2
    await db.links.remove(one.id, actor_id=one.owner_user_id)
    assert await db.links.codes_waiting(one.owner_user_id) == 1
    assert await db.links.codes_waiting(other.owner_user_id) == 1
    assert not any(row.metadata_json.get("telegram_bot_link_id") == one.id and row.consumed_at is None for row in await rows(db, ChannelOAuthStateRow))


async def test_unbind_ends_callbacks_deliveries_and_old_generation_metadata(db):
    one = await bind_link(db, await add_link(db))
    two = await bind_link(db, await add_link(db, bot=one.bot_id + 1), message=102, update=202)
    now = datetime.now(UTC)
    async with db.sessions.begin() as session:
        for link in [one, two]:
            session.add(TelegramCallbackRow(id=("a" if link.id == one.id else "b") * 32, group_id="g" * 32, link_id=link.id, chat_id=link.chat_id, binding_generation=link.binding_generation, kind="fixture", created_at=now))
            session.add(
                TelegramOutboxRow(
                    id=("c" if link.id == one.id else "d") * 32,
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    dedupe_key="fixture",
                    class_="reply",
                    binding_generation=link.binding_generation,
                    state="sending",
                    not_before=now,
                    claim_owner="old-worker",
                    claim_id="old-claim",
                    created_at=now,
                )
            )
            session.add(
                TelegramPendingFileRow(
                    link_id=link.id, message_id=1, chat_id=link.chat_id, binding_generation=link.binding_generation, file_id="file", file_unique_id="unique", filename="f.txt", mime_type="text/plain", size=1, is_image=False, created_at=now
                )
            )
            session.add(TelegramWorkTopicRow(link_id=link.id, work_id="w" * 32, chat_id=link.chat_id, binding_generation=link.binding_generation, creation_state="ready", name="topic", created_at=now))
    changed = await db.links.unbind(one, actor_id=one.owner_user_id)
    assert changed.state == "waiting_connect" and changed.bind_mode == "code"
    assert changed.binding_generation == one.binding_generation + 1
    assert (changed.connection_id, changed.telegram_user_id, changed.chat_id, changed.connected_name) == (None, None, None, None)
    callbacks = {row.link_id: row for row in await rows(db, TelegramCallbackRow)}
    assert callbacks[one.id].consumed_at is not None and callbacks[two.id].consumed_at is None
    deliveries = {row.link_id: row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "fixture"}
    assert deliveries[one.id].state == "skipped" and deliveries[one.id].skip_reason == "link-not-connected"
    assert deliveries[one.id].claim_owner is None and deliveries[one.id].claim_id is None
    assert deliveries[two.id].state == "sending"
    assert [row.link_id for row in await rows(db, TelegramPendingFileRow)] == [two.id]
    assert [row.link_id for row in await rows(db, TelegramWorkTopicRow)] == [two.id]


async def test_policy_stop_preserves_callbacks_and_current_metadata(db):
    link = await bind_link(db, await add_link(db))
    now = datetime.now(UTC)
    async with db.sessions.begin() as session:
        session.add(TelegramCallbackRow(id="a" * 32, group_id="g" * 32, link_id=link.id, chat_id=link.chat_id, binding_generation=link.binding_generation, kind="fixture", created_at=now))
        session.add(
            TelegramPendingFileRow(
                link_id=link.id, message_id=1, chat_id=link.chat_id, binding_generation=link.binding_generation, file_id="f", file_unique_id="u", filename="f", mime_type="text/plain", size=1, is_image=False, created_at=now
            )
        )
    await db.links.end_pending(link.id, skip_reason="policy-off", consume_callbacks=False)
    assert (await rows(db, TelegramCallbackRow))[0].consumed_at is None
    assert len(await rows(db, TelegramPendingFileRow)) == 1


async def test_account_row_is_deleted_when_its_own_last_binding_ends(db):
    one = await bind_link(db, await add_link(db), account=7_000_000_001)
    two = await bind_link(db, await add_link(db, bot=one.bot_id + 1), account=7_000_000_002)
    three = await bind_link(db, await add_link(db, bot=one.bot_id + 2), account=7_000_000_001)
    await db.links.unbind(one, actor_id=one.owner_user_id)
    assert len(await rows(db, TelegramAccountRow)) == 2
    await db.links.unbind(three, actor_id=three.owner_user_id)
    accounts = await rows(db, TelegramAccountRow)
    assert [(row.telegram_user_id, row.owner_user_id) for row in accounts] == [(two.telegram_user_id, two.owner_user_id)]
    await bind_link(db, await add_link(db, owner="owner-b", bot=one.bot_id + 3), account=7_000_000_001)


async def test_remove_deletes_conversations_and_preserves_manager_child_token(db):
    manager = await bind_link(db, await add_link(db))
    child = await add_link(db, bot=manager.bot_id + 1, origin="managed", manager_link_id=manager.id, creator_telegram_user_id=manager.telegram_user_id, bind_mode="creator")
    async with db.sessions.begin() as session:
        session.add(
            ChannelConversationRow(id="a" * 32, connection_id=manager.connection_id, owner_user_id=manager.owner_user_id, provider="telegram", external_conversation_id=str(manager.chat_id), external_topic_id="", thread_id="old-thread")
        )
    await db.links.remove(manager.id, actor_id=manager.owner_user_id)
    assert not await rows(db, ChannelConversationRow)
    assert (await db.links.get(child.id)).manager_link_id is None
    assert (await db.links.token(child.id))[0] == f"wrapped-fixture-{child.bot_id}"


async def test_health_precedence_keeps_pause_when_conflict_ends(db):
    link = await add_link(db)
    assert (await db.links.set_health(link.id, "unreachable", holds=True)).health == "unreachable"
    conflicted = await db.links.set_health(link.id, "conflict", holds=True)
    assert conflicted.health == "conflict" and conflicted.acts_paused_since is not None
    assert (await db.links.set_health(link.id, "token_rejected", holds=True)).health == "token_rejected"
    assert (await db.links.set_health(link.id, "token_rejected", holds=False)).health == "conflict"
    assert (await db.links.set_health(link.id, "conflict", holds=False)).health == "unreachable"
    ok = await db.links.set_health(link.id, "unreachable", holds=False)
    assert ok.health == "ok" and ok.acts_paused_since == conflicted.acts_paused_since


async def test_link_events_never_carry_a_token(db):
    link = await add_link(db)
    token = str(123456789) + ":" + "secret" * 6
    with pytest.raises(ValueError):
        await db.links.event(link, "retry", actor_id=link.owner_user_id, detail={"token": token})
    with pytest.raises(ValueError):
        await db.links.event(link, "remove", actor_id=link.owner_user_id, detail={"agent_name": token})
    await db.links.event(link, "retry", actor_id=link.owner_user_id)
    assert all(token not in repr(row.detail) and "secret" not in repr(row.detail) for row in await rows(db, TelegramBotLinkEventRow))


async def test_removed_notices_are_owner_scoped_and_seen_once(db):
    link = await add_link(db)
    await db.links.remove(link.id, actor_id="host", action="agent_removed", detail={"agent_kind": link.agent_kind, "agent_ref": link.agent_ref, "agent_name": "Fixture", "bot_username": link.bot_username})
    notices = await db.links.removed_for_owner(link.owner_user_id)
    assert len(notices) == 1
    identity = notices[0]["id"]
    assert await db.links.removed_for_owner("other") == []
    assert not await db.links.mark_seen("other", identity)
    assert await db.links.mark_seen(link.owner_user_id, identity)
    assert not await db.links.mark_seen(link.owner_user_id, identity)
    assert not await db.links.removed_for_owner(link.owner_user_id)


async def test_repeated_disconnect_all_does_not_revoke_again_or_emit_more_events(db):
    link = await bind_link(db, await add_link(db))
    assert await db.links.disconnect_all(link.owner_user_id, actor_id=link.owner_user_id) == 1
    after = await db.links.get(link.id)
    events = len(await rows(db, TelegramBotLinkEventRow))
    assert await db.links.disconnect_all(link.owner_user_id, actor_id=link.owner_user_id) == 0
    assert await db.links.get(link.id) == after
    assert len(await rows(db, TelegramBotLinkEventRow)) == events


async def test_remove_cannot_delete_a_foreign_connections_credentials_or_conversation(db):
    link = await add_link(db)
    async with db.sessions.begin() as session:
        session.add(ChannelConnectionRow(id="f" * 32, owner_user_id="other-owner", provider="telegram", external_account_id="foreign-account", workspace_id="foreign-bot", status="connected"))
        await session.flush()
        session.add(ChannelCredentialRow(connection_id="f" * 32, encrypted_access_token="foreign-opaque"))
        session.add(ChannelConversationRow(id="e" * 32, connection_id="f" * 32, owner_user_id="other-owner", provider="telegram", external_conversation_id="foreign-account", external_topic_id="", thread_id="foreign-thread"))
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(connection_id="f" * 32))
    await db.links.remove(link.id, actor_id=link.owner_user_id)
    assert (await rows(db, ChannelConnectionRow))[0].status == "connected"
    assert (await rows(db, ChannelCredentialRow))[0].encrypted_access_token == "foreign-opaque"
    assert (await rows(db, ChannelConversationRow))[0].thread_id == "foreign-thread"


async def test_conflict_transition_metadata_is_atomic_and_omission_preserves_it(db):
    link = await add_link(db)
    since = datetime.now(UTC)
    until = since + timedelta(hours=24)
    transition = {"conflict_since": since, "probe_until": until, "webhook_host": "hooks.example.com"}
    first = await db.links.set_health(link.id, "conflict", holds=True, transition=transition)
    assert (first.conflict_since, first.probe_until, first.webhook_host) == (since, until, "hooks.example.com")
    again = await db.links.set_health(link.id, "conflict", holds=True)
    assert (again.conflict_since, again.probe_until, again.acts_paused_since) == (since, until, first.acts_paused_since)
    await db.links.set_health(link.id, "secret_unreadable", holds=True)
    current = await db.links.set_health(link.id, "secret_unreadable", holds=False)
    assert current.health == "conflict" and current.conflict_since == since and current.probe_until == until
    cleared_host = await db.links.set_health(link.id, "conflict", holds=True, transition={**transition, "webhook_host": None})
    assert cleared_host.webhook_host is None and cleared_host.conflict_since == since
    ended = await db.links.set_health(link.id, "conflict", holds=False)
    assert (ended.conflict_since, ended.probe_until, ended.webhook_host) == (None, None, None)
    assert ended.acts_paused_since == first.acts_paused_since


async def test_failed_health_commit_does_not_install_an_uncommitted_cause(db):
    from sqlalchemy.orm import Session

    link = await add_link(db)
    await db.links.set_health(link.id, "unreachable", holds=True)

    def refuse(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-health-commit")

    event.listen(Session, "before_commit", refuse)
    try:
        with pytest.raises(RuntimeError, match="fixture-health-commit"):
            await db.links.set_health(link.id, "secret_unreadable", holds=True)
    finally:
        event.remove(Session, "before_commit", refuse)
    assert (await db.links.get(link.id)).health == "unreachable"
    assert (await db.links.set_health(link.id, "unreachable", holds=False)).health == "ok"


async def test_generic_link_updates_cannot_rewrite_binding_authority_or_fences(db):
    link = await add_link(db)
    for key, value in {
        "owner_user_id": "other",
        "binding_generation": 0,
        "connection_id": "other-connection",
        "telegram_user_id": 7_000_000_002,
        "chat_id": 7_000_000_002,
        "connected_name": "other",
        "connect_expires_at": datetime.now(UTC),
        "connect_conflict_epoch": 0,
        "work_baseline_generation": 1,
        "creator_telegram_user_id": 7_000_000_002,
        "manager_link_id": "other-manager",
        "bind_mode": "creator",
        "origin": "managed",
        "health": "ok",
    }.items():
        with pytest.raises(ValueError):
            await db.links.update(link.id, **{key: value})
    assert await db.links.get(link.id) == link


async def test_remove_all_retains_preferences_and_history_while_cleaning_only_owner(db):
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    one = await bind_link(db, await add_link(db))
    await add_link(db, bot=one.bot_id + 1)
    other = await bind_link(db, await add_link(db, owner="other", bot=one.bot_id + 2), account=7_000_000_002)
    preference = await db.settings.get(one.owner_user_id)
    now = datetime.now(UTC)
    async with db.sessions.begin() as session:
        session.add(TelegramAssistantAddRow(id="b" * 32, owner_user_id=one.owner_user_id, link_id=one.id, state="draft", created_at=now, updated_at=now))
        session.add(
            TelegramActRow(
                id="a" * 32, link_id=one.id, bot_id=one.bot_id, owner_user_id=one.owner_user_id, binding_generation=one.binding_generation, telegram_user_id=one.telegram_user_id, kind="fixture", state="done", created_at=now, updated_at=now
            )
        )
    removed = await db.links.remove_all_for_owner(one.owner_user_id, actor_id="host", detail={"by": "operator"})
    assert len(removed) == 2
    assert await db.links.list_for_owner(one.owner_user_id) == []
    assert await db.settings.get(one.owner_user_id) == preference
    assert await db.links.get(other.id) == other
    assert [(row.telegram_user_id, row.owner_user_id) for row in await rows(db, TelegramAccountRow)] == [(other.telegram_user_id, other.owner_user_id)]
    action = (await rows(db, TelegramActRow))[0]
    assert action.state == "done" and action.link_id is None
    add = (await rows(db, TelegramAssistantAddRow))[0]
    assert add.state == "abandoned" and add.link_id is None
    events = [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "remove"]
    assert len(events) == 2 and all(row.actor_id == "host" and row.detail == {"by": "operator"} for row in events)


@pytest.mark.parametrize("invalid", ["naive", "reversed", "url", "token", "unknown"])
async def test_invalid_conflict_transition_is_refused_without_changes(db, invalid):
    link = await add_link(db)
    since = datetime.now(UTC)
    transition = {"conflict_since": since, "probe_until": since + timedelta(hours=24), "webhook_host": None}
    if invalid == "naive":
        transition["probe_until"] = since.replace(tzinfo=None)
    elif invalid == "reversed":
        transition["probe_until"] = since - timedelta(seconds=1)
    elif invalid == "url":
        transition["webhook_host"] = "https://hooks.example.com/path"
    elif invalid == "token":
        transition["webhook_host"] = str(123456789) + ":" + "secret" * 6
    else:
        transition["raw_update"] = {}
    with pytest.raises(ValueError):
        await db.links.set_health(link.id, "conflict", holds=True, transition=transition)
    assert await db.links.get(link.id) == link
