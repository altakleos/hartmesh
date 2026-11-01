"""Real stored callback prechecks, controlled acknowledgement/effect collaborators."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import event, update
from test_telegram_callbacks import NOW, api
from test_telegram_callbacks import callbacks_environment as callbacks_environment  # noqa: F401
from test_telegram_publication_custody import publication_environment as publication_environment  # noqa: F401

from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow


def query_for(e, identity, **changes):
    values = dict(id="query", data="h1:" + identity, from_user=SimpleNamespace(id=e.link.telegram_user_id), message=SimpleNamespace(chat=SimpleNamespace(id=e.link.chat_id), message_id=902, message_thread_id=None))
    return SimpleNamespace(**(values | changes))


def channel_for(e, calls):
    async def answer(query_id, key=None, *, captured_link, deadline):
        assert captured_link is e.link
        await asyncio.sleep(0)
        assert asyncio.get_running_loop().time() < deadline
        calls.append(("answer", query_id, key))

    return SimpleNamespace(link=e.link, answer_callback_ack=answer)


@pytest.mark.asyncio
async def test_other_person_alert_precedes_metadata_backend_unavailability():
    calls = []
    link = SimpleNamespace(id="link", owner_user_id="owner", bot_id=9, agent_kind="lead_agent", agent_ref="lead_agent", connection_id="connection", telegram_user_id=7, chat_id=7, binding_generation=1, token_revision=1)

    async def unavailable(_link):
        calls.append("metadata")
        raise RuntimeError("controlled unavailable backend")

    async def answer(_identity, key, **_kwargs):
        await asyncio.sleep(0)
        calls.append(key)

    async def execute(*_args):
        calls.append("effect")

    channel = SimpleNamespace(link=link, answer_callback_ack=answer)
    query = SimpleNamespace(id="query", from_user=SimpleNamespace(id=8), message=SimpleNamespace(chat=SimpleNamespace(id=7)))
    await api().CallbackHandler(SimpleNamespace(current_link=unavailable), execute=execute).press(channel, query)
    assert calls == ["stale.other_person"]


@pytest.mark.asyncio
async def test_single_acknowledgement_precedes_effect_and_adopts_actual_group(callbacks_environment):
    e, calls = callbacks_environment, []
    plan = await e.plan()

    async def execute(channel, row):
        calls.append(("effect", row.id, row.message_id))

    await api().CallbackHandler(e.callbacks, execute=execute).press(channel_for(e, calls), query_for(e, plan.group.callback_ids[0]))
    assert calls == [("answer", "query", None), ("effect", plan.group.callback_ids[0], 902)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case,key",
    [
        ("person", "stale.other_person"),
        ("chat", "stale.other_person"),
        ("inline", "stale.other_person"),
        ("missing", "stale.not_waiting"),
        ("message", "stale.not_waiting"),
        ("topic", "stale.not_waiting"),
        ("consumed", "stale.not_waiting"),
        ("expired", "stale.not_waiting"),
        ("health", "stale.not_waiting"),
        ("pending", "stale.not_waiting"),
    ],
)
async def test_seven_prechecks_refuse_once_without_effect(callbacks_environment, case, key):
    e, calls = callbacks_environment, []
    plan = await e.plan(message_id=902)
    query = query_for(e, plan.group.callback_ids[0])
    if case == "person":
        query.from_user.id += 1
    elif case == "chat":
        query.message.chat.id += 1
    elif case == "inline":
        query.message = None
    elif case == "missing":
        query.data = "h1:" + "0" * 32
    elif case == "message":
        query.message.message_id += 1
    elif case == "topic":
        query.message.message_thread_id = 99
    else:
        async with e.sessions.begin() as session:
            if case == "health":
                await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(health="conflict"))
            else:
                row = await session.get(TelegramCallbackRow, plan.group.callback_ids[0])
                if case == "consumed":
                    row.consumed_at = NOW
                elif case == "expired":
                    row.expires_at = NOW - timedelta(seconds=1)
                else:
                    row.payload = {**row.payload, "presentation_pending": True}

    async def execute(*_args):
        calls.append(("effect",))

    await api().CallbackHandler(e.callbacks, execute=execute).press(channel_for(e, calls), query)
    assert calls == [("answer", "query", key)]


def test_pause_table_covers_every_declared_conditional_kind():
    module = api()
    link = SimpleNamespace(agent_kind="lead_agent", can_manage_bots=True)
    paused = {"decide_confirm", "decide_confirm_resume", "accept_confirm", "work_change_save", "work_withdraw_confirm", "add_open", "add_confirm"}
    assert paused.issubset(module.CALLBACK_POLICIES)
    for kind in module.CALLBACK_POLICIES:
        if kind in {"network", "bot_create", "act_check", "prompt_reopen"}:
            continue
        assert module.pause_covers(kind, {}, link) is (kind in paused)
    assert module.pause_covers("network", {"option_id": "allow_temporary"}, link)
    assert not module.pause_covers("network", {"option_id": "deny"}, link)
    assert module.pause_covers("bot_create", {}, link)
    assert not module.pause_covers("bot_create", {}, SimpleNamespace(agent_kind="custom_agent"))
    assert not module.pause_covers("bot_create", {}, SimpleNamespace(agent_kind="lead_agent", can_manage_bots=False))
    assert module.pause_covers("act_check", {}, link, original_act_kind="decide")
    assert not module.pause_covers("act_check", {}, link, original_act_kind="settings")


@pytest.mark.parametrize("age,target,usable", [(timedelta(hours=25), "draft", False), (timedelta(hours=25), "thread", True), (timedelta(days=181), "thread", False)])
def test_a_draft_lapses_sooner_and_every_button_lapses_at_the_age_limit(age, target, usable):
    row = SimpleNamespace(consumed_at=None, created_at=NOW - age, expires_at=None, target_kind=target)
    repo = api().CallbackRepository(None, clock=lambda: NOW)
    if usable:
        repo._usable(row)
        return
    with pytest.raises(api().CallbackRefused) as failure:
        repo._usable(row)
    assert failure.value.key == "stale.not_waiting"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,payload,paused", [("add_open", {}, True), ("network", {"option_id": "allow_temporary"}, True), ("network", {"option_id": "deny"}, False), ("work_start", {}, False), ("changes_open", {}, False)])
async def test_pause_is_checked_before_any_future_leaf_handler(callbacks_environment, kind, payload, paused):
    e, calls = callbacks_environment, []
    plan = await e.plan(keyboard=[[dict(label="label.back", kind=kind, payload=payload)]], message_id=902)
    async with e.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(acts_paused_since=NOW))

    async def execute(*_args):
        calls.append(("effect",))

    await api().CallbackHandler(e.callbacks, execute=execute).press(channel_for(e, calls), query_for(e, plan.group.callback_ids[0]))
    assert calls == ([("answer", "query", "link.limited")] if paused else [("answer", "query", None), ("effect",)])
    assert (await e.callbacks.get(plan.group.callback_ids[0])).consumed_at is None


@pytest.mark.asyncio
async def test_held_precheck_deadline_answers_once_retains_cleanup_and_never_adopts(callbacks_environment, monkeypatch):
    e, calls = callbacks_environment, []
    plan = await e.plan()
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.callbacks.get

    async def held(identity):
        row = await original(identity)
        entered.set()
        await release.wait()
        return row

    monkeypatch.setattr(e.callbacks, "get", held)
    monkeypatch.setattr(api(), "PRESS_TIMEOUT_SECONDS", 0.1)

    async def execute(*_args):
        calls.append(("effect",))

    async def yielding_answer(query_id, key=None, *, captured_link, deadline):
        # The actual adapter has awaited preparation before its raw SDK call.
        await asyncio.sleep(0)
        assert captured_link is e.link and asyncio.get_running_loop().time() < deadline
        calls.append(("answer", query_id, key))

    channel = channel_for(e, calls)
    channel.answer_callback_ack = yielding_answer
    task = asyncio.create_task(api().CallbackHandler(e.callbacks, execute=execute).press(channel, query_for(e, plan.group.callback_ids[0])))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.sleep(0.15)
        assert not task.done(), "The original ingress lifetime must retain precheck cleanup"
        assert calls == [("answer", "query", "tap.failed")], "Nothing is known about the button yet, so nothing is said against it"
    finally:
        release.set()
        await task
    assert (await original(plan.group.callback_ids[0])).message_id is None


@pytest.mark.asyncio
async def test_queued_original_binding_never_adopts_current_channel_or_enters_sql(callbacks_environment):
    e, calls, queries = callbacks_environment, [], []
    plan = await e.plan()
    channel = channel_for(e, calls)
    channel.link = replace(e.link, binding_generation=e.link.binding_generation + 1)

    def dispatched(*_args):
        queries.append(True)

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", dispatched)

    async def execute(*_args):
        calls.append(("effect",))

    try:
        await api().CallbackHandler(e.callbacks, execute=execute).press(channel, query_for(e, plan.group.callback_ids[0]), captured_link=e.link)
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", dispatched)
    assert queries == []
    assert calls == [("answer", "query", "stale.not_waiting")]


@pytest.mark.asyncio
async def test_retired_first_read_does_not_admit_another_metadata_query(callbacks_environment, monkeypatch):
    e, calls, lookups = callbacks_environment, [], []
    plan = await e.plan()
    entered, release = asyncio.Event(), asyncio.Event()
    original_link, original_get = e.callbacks.current_link, e.callbacks.get

    async def held(link):
        result = await original_link(link)
        entered.set()
        await release.wait()
        return result

    async def counted(identity):
        lookups.append(identity)
        return await original_get(identity)

    monkeypatch.setattr(e.callbacks, "current_link", held)
    monkeypatch.setattr(e.callbacks, "get", counted)
    monkeypatch.setattr(api(), "PRESS_TIMEOUT_SECONDS", 0.1)

    async def execute(*_args):
        calls.append(("effect",))

    task = asyncio.create_task(api().CallbackHandler(e.callbacks, execute=execute).press(channel_for(e, calls), query_for(e, plan.group.callback_ids[0])))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.sleep(0.15)
    finally:
        release.set()
        await task
    assert lookups == []
    assert calls == [("answer", "query", "tap.failed")]
