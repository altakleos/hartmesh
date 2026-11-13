"""Instance lifecycle over the real managed supervisor, SQL and offline SDK."""

import asyncio

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from test_telegram_bot_adapter import message as sent_message
from test_telegram_bot_handlers import message
from test_telegram_instance import instance_environment
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build
from test_telegram_threads import close

from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def lifecycle(db, monkeypatch):
    e = await instance_environment(db, monkeypatch)
    async with db.engine.begin() as connection:
        await connection.run_sync(lambda sync: AgentWorkRow.__table__.create(sync, checkfirst=True))
    e.db = db
    await e.channel.stop()
    await db.links.end_pending(e.link.id, skip_reason="superseded")
    e.runtime = build(db, monkeypatch)
    supervisor = e.runtime.supervisor
    supervisor.authority = e.live.authority
    supervisor._app_config = e.live.config
    e.runtime.normal.enqueue_get_me(bot_id=e.link.bot_id)
    held = e.runtime.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    token = str(e.link.bot_id) + ":" + "A" * 35
    await db.links.store_token(e.link.id, await supervisor.encrypt_token(e.link.bot_id, token), actor_id=e.link.owner_user_id)
    await supervisor.check()
    await supervisor.start()
    await asyncio.wait_for(held.started.wait(), 3)
    e.supervisor = supervisor
    e.channel = supervisor.channel(e.link.id)
    e.link = e.channel.link
    try:
        yield e
    finally:
        await supervisor.stop()
        await close(e)


@pytest.mark.parametrize("content", ["Can you answer?", "/settings", "/help"])
@pytest.mark.parametrize("status,key", [("suspended", "agent.paused"), ("archived", "agent.retired")])
async def test_inactive_instance_keeps_polling_and_answers_fixed_status(lifecycle, status, key, content):
    from app.telegram_bots.render import render

    e = lifecycle
    poll = e.supervisor.poll_task(e.link.id)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status=status))
    await e.supervisor.reconcile()
    assert e.supervisor.poll_task(e.link.id) is poll
    assert (await e.db.links.get(e.link.id)).health == "agent_unavailable"
    sent_message(e.runtime.normal)
    assert await e.channel.handle_update(message(e, 2100, content=content))
    await asyncio.gather(*tuple(e.channel._ingress_tasks))
    texts = [call.parameters["text"] for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]
    assert texts == [render(key, "en", name="Fixture employee", product="HartMesh")]
    assert e.runtime.service.bus.inbound_queue.empty()
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status="active"))
    await e.supervisor.reconcile()
    assert (await e.db.links.get(e.link.id)).health == "ok"


async def test_deleted_instance_removes_link_and_records_owner_notice_once(lifecycle):
    e = lifecycle
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status="deleted"))
    sent_message(e.runtime.normal)
    e.runtime.normal.enqueue("setMyDescription", result=True)
    await e.supervisor.reconcile()
    assert await e.db.links.get(e.link.id) is None
    assert e.supervisor.poll_task(e.link.id) is None
    assert len([call for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]) == 1
    assert "Fixture employee was deleted" in next(call.parameters["text"] for call in e.runtime.normal.calls if call.endpoint == "sendMessage")
    removed = await e.db.links.removed_for_owner(e.link.owner_user_id)
    assert len(removed) == 1 and removed[0]["agent_name"] == "Fixture employee" and removed[0]["agent_ref"] == e.link.agent_ref
    assert removed[0]["bot_username"] == e.link.bot_username and removed[0]["reason"] == "agent-deleted"
    await e.supervisor.reconcile()
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramBotLinkEventRow.action).where(TelegramBotLinkEventRow.link_id == e.link.id)))
    assert actions.count("agent_removed") == 1


@pytest.mark.parametrize("uncertainty", ["absent", "read_failure"])
async def test_missing_or_failing_instance_service_never_means_deletion(lifecycle, monkeypatch, uncertainty):
    e = lifecycle
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status="deleted"))
    if uncertainty == "absent":
        e.http_app.state.agent_instances = None
    else:

        async def failed(*args):
            raise RuntimeError("Synthetic service read failure")

        monkeypatch.setattr(e.live.authority, "instance_identity", failed)
    await e.supervisor.reconcile()
    assert await e.db.links.get(e.link.id) is not None
    assert not await e.db.links.removed_for_owner(e.link.owner_user_id)
    assert not [call for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]


async def test_lost_use_keeps_polling_and_replies_at_most_once_an_hour(lifecycle):
    e = lifecycle
    poll = e.supervisor.poll_task(e.link.id)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=2))
    await e.supervisor.reconcile()
    assert e.supervisor.poll_task(e.link.id) is poll
    sent_message(e.runtime.normal)
    for identity in (2101, 2102):
        assert await e.channel.handle_update(message(e, identity, content="Can you answer?"))
        await asyncio.gather(*tuple(e.channel._ingress_tasks))
    texts = [call.parameters["text"] for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]
    assert len(texts) == 1 and "You no longer have access" in texts[0]
    assert e.runtime.service.bus.inbound_queue.empty()
    e.runtime.clock.advance(3600)
    sent_message(e.runtime.normal, identity=502)
    assert await e.channel.handle_update(message(e, 2103, content="Can you answer now?"))
    await asyncio.gather(*tuple(e.channel._ingress_tasks))
    assert len([call for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]) == 2


async def test_reconcile_marks_renamed_profile_owed_and_schedules_owner_push(lifecycle):
    from datetime import UTC, datetime

    e = lifecycle
    await e.db.links.finish_profile(e.link.id, token_revision=e.link.token_revision, version=1, bot_name="Fixture employee", locale="en-US", at=datetime.now(UTC), target={"name": "Fixture employee", "custody": "company"})
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(name="Renamed employee"))
    attempt = e.supervisor.profile_sync._attempt
    attempt.reset_mock()
    observed = asyncio.Event()

    async def pushed(link, parts):
        if link.id == e.link.id:
            observed.set()
        return False

    attempt.side_effect = pushed
    await e.supervisor.reconcile()
    await asyncio.wait_for(observed.wait(), 3)
    assert (await e.db.links.get(e.link.id)).profile_pending is True
    assert any(call.args[0].id == e.link.id for call in attempt.await_args_list)


async def test_deleted_personal_instance_removes_owner_and_colleague_links(lifecycle):
    from datetime import UTC, datetime
    from uuid import uuid4

    from _telegram_bot_test_support import RecordingRequest
    from _telegram_repository_test_support import code_of

    from deerflow.persistence.user.model import UserRow

    e = lifecycle
    colleague = str(uuid4())
    async with e.db.sessions.begin() as writer:
        writer.add(UserRow(id=colleague, email="colleague@example.com", system_role="user"))
        writer.add(AgentInstanceGrantRow(instance_id=e.link.agent_ref, user_id=colleague, permissions=1))
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(custody="personal", owner_id=e.link.owner_user_id))
    await e.db.settings.ensure(colleague, timezone="UTC", locale="en-US")
    other = await e.db.links.add(owner_user_id=colleague, agent_kind="instance", agent_ref=e.link.agent_ref, bot_id=9_000_000_004, bot_username="colleague_fixture_bot", encrypted_token="wrapped-fixture", actor_id=colleague)
    other = await e.db.links.bind(
        other,
        telegram_user_id=7_000_000_004,
        chat_id=7_000_000_004,
        first_name="Colleague",
        username=None,
        code=code_of(await e.db.links.mint_code(other)),
        start_identity={"update_id": 2201, "message_id": 2201, "chat_id": 7_000_000_004},
        hello_parts=[{"source_ref": {"key": "hello.assistant", "values": {"email": "colleague@example.com", "name": "Fixture employee", "product": "HartMesh", "role": None}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
    )
    await e.db.links.end_pending(other.id, skip_reason="superseded")
    normal, updates = RecordingRequest(), RecordingRequest()
    e.supervisor.request_factory = lambda **kw: normal if kw["connection_pool_size"] == 8 else updates
    normal.enqueue_get_me(bot_id=other.bot_id)
    held = updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    token = str(other.bot_id) + ":" + "A" * 35
    other = await e.db.links.store_token(other.id, await e.supervisor.encrypt_token(other.bot_id, token), actor_id=colleague)
    await e.supervisor.add(other, token)
    await asyncio.wait_for(held.started.wait(), 3)
    # Seat release keeps an account tombstone. Its deleted assistant must still
    # be reconciled for every owner, including a colleague's live account.
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(released_at=datetime.now(UTC)))
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status="deleted"))
    for request in (e.runtime.normal, normal):
        sent_message(request)
        request.enqueue("setMyDescription", result=True)
    await e.supervisor.reconcile()
    for link, request in ((e.link, e.runtime.normal), (other, normal)):
        assert await e.db.links.get(link.id) is None
        assert len(await e.db.links.removed_for_owner(link.owner_user_id)) == 1
        assert len([call for call in request.calls if call.endpoint == "sendMessage"]) == 1


@pytest.mark.parametrize("at_refresh", [False, True])
@pytest.mark.parametrize("key", ["access.agent_off", "agent.paused", "agent.retired"])
async def test_lifecycle_notice_is_retired_when_state_recovers_in_sdk_limiter(lifecycle, monkeypatch, key, at_refresh):
    e = lifecycle
    async with e.db.sessions.begin() as writer:
        if key == "access.agent_off":
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        else:
            await writer.execute(update(AgentInstanceRow).values(status="suspended" if key == "agent.paused" else "archived"))
    limiter = e.channel.bot.rate_limiter

    async def recovered(delay):
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=7))
            await writer.execute(update(AgentInstanceRow).values(status="active"))
        limiter._short_hold_until = 0

    if at_refresh:
        refresh, calls = e.channel.refresh_link, 0

        async def refresh_then_recover():
            nonlocal calls
            calls += 1
            result = await refresh()
            if calls == 4:  # Last context refresh inside the actual SDK limiter.
                await recovered(0)
            return result

        monkeypatch.setattr(e.channel, "refresh_link", refresh_then_recover)
    else:
        limiter._sleep = recovered
        limiter._short_hold_until = limiter._time() + 1
    sent_message(e.runtime.normal)
    await asyncio.wait_for(e.supervisor._notice(e.channel, key), 3)
    assert not [call for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]


async def test_active_instance_with_released_account_is_held_and_poller_retired(lifecycle):
    from datetime import UTC, datetime

    from deerflow.persistence.user.model import UserRow

    e = lifecycle
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(released_at=datetime.now(UTC)))
    sent_message(e.runtime.normal)
    await e.supervisor.reconcile()
    assert (await e.db.links.get(e.link.id)).state == "held"
    assert e.supervisor.poll_task(e.link.id) is None
    assert len([call for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]) == 1


async def test_automatic_rename_push_preserves_other_bots_expired_retry_window(lifecycle):
    from datetime import UTC, datetime, timedelta

    from test_telegram_bot_adapter import build, start

    e = lifecycle
    other = await e.db.links.add(
        owner_user_id=e.link.owner_user_id, agent_kind="lead_agent", agent_ref="lead_agent", bot_id=9_000_000_005, bot_username="expired_fixture_bot", encrypted_token="wrapped-fixture", actor_id=e.link.owner_user_id
    )
    second = await build(e.db, connected=False, link=other)
    await start(second)
    e.supervisor._channels[other.id] = second.channel
    sync = e.supervisor.profile_sync
    sync._failing_since[other.id] = datetime.now(UTC) - timedelta(hours=e.supervisor.config.profile.retry_hours + 1)
    await e.db.links.finish_profile(e.link.id, token_revision=e.link.token_revision, version=1, bot_name="Fixture employee", locale="en-US", at=datetime.now(UTC), target={"name": "Fixture employee", "custody": "company"})
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).values(name="Renamed employee"))
    sync._attempt.reset_mock()
    observed = asyncio.Event()

    async def pushed(link, parts):
        if link.id == e.link.id:
            observed.set()
        return False

    sync._attempt.side_effect = pushed
    try:
        await e.supervisor.reconcile()
        await asyncio.wait_for(observed.wait(), 3)
        pushes = [task for task in e.supervisor._background if task.get_name() in {"telegram-profile-owner", "telegram-profile-renamed"}]
        await asyncio.gather(*pushes)
        assert [call.args[0].id for call in sync._attempt.await_args_list] == [e.link.id]
    finally:
        e.supervisor._channels.pop(other.id, None)
        await second.channel.stop()


async def test_lifecycle_grant_read_drains_repeated_cancellation(lifecycle):
    from sqlalchemy import event
    from sqlalchemy.util.concurrency import await_only

    e = lifecycle
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []

    def pause(connection, cursor, statement, parameters, context, many):
        if "FROM agent_instances JOIN agent_instance_grants" in statement:
            entered.set()
            await_only(release.wait())
            completed.append(True)

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", pause)
    task = asyncio.create_task(e.live.authority.instance_lifecycle(e.link))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not completed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert completed == [True]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", pause)
