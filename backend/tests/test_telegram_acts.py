"""Settings Acts over real SQL; future Work actions are intentionally absent."""

import asyncio
import importlib
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select, text, update
from sqlalchemy.util import await_only
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_custody import publication_environment as publication_environment

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_settings_metadata_joins_owned_transaction_without_a_separate_commit(db):
    api = importlib.import_module("app.telegram_bots.acts")
    link = await bind_link(db, await add_link(db))
    acts = api.Acts(db.sessions, settings=db.settings, callbacks=None, outbox=None)
    identity = uuid4().hex
    with pytest.raises(RuntimeError, match="rollback owner"):
        async with db.sessions() as session, session.begin():
            if db.dialect == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            row = await acts.write(link, act_id=identity, kind="settings", body={"field": "locale", "old": "en-US", "new": "zh-CN"}, session=session)
            assert row.id == identity and row.state == "pending"
            assert row.owner_user_id == link.owner_user_id
            assert row.operation_ids == () and row.first_attempt_at is None
            raise RuntimeError("rollback owner")
    async with db.sessions() as session:
        assert await session.scalar(select(TelegramActRow).where(TelegramActRow.id == identity)) is None


@pytest_asyncio.fixture
async def settings_act_environment(callbacks_environment):
    e = callbacks_environment
    planned = []

    class RecordingSettingsPlanner:
        """Observe the actual public planner without replacing its SQL/keyboard work."""

        async def plan_product_keyboard(self, link, **kwargs):
            assert kwargs["session"].get_bind() is e.sessions.kw["bind"].sync_engine
            planned.append(kwargs)
            return await e.outbox.plan_product_keyboard(link, **kwargs)

    api = importlib.import_module("app.telegram_bots.acts")
    act = api.Acts(e.sessions, settings=e.db.settings, callbacks=e.callbacks, outbox=RecordingSettingsPlanner())
    plan = await e.plan(message_id=902)
    return SimpleNamespace(**vars(e), acts=act, original_plan=plan, planned=planned, chinese=plan.group.callback_ids[1])


async def apply(e, callback_id=None, **kwargs):
    return await e.acts.apply_settings(e.link, callback_id or e.chinese, chat_id=e.link.chat_id, message_id=902, message_thread_id=None, guard=e.guard_factory(), **kwargs)


@pytest.mark.asyncio
async def test_settings_act_commits_preference_receipt_group_and_edit_together(settings_act_environment):
    e = settings_act_environment
    result = await apply(e)
    assert result.id == e.chinese and result.state == "done"
    assert dict(result.body) == {"field": "locale", "old": "en-US", "new": "zh-CN"}
    assert result.attempts == 1 and result.first_attempt_at is not None
    assert result.result["locale"] == "zh-CN" and len(result.result["outbox_id"]) == 32
    assert (await e.db.settings.get(e.link.owner_user_id)).locale == "zh-CN"
    assert len(e.planned) == 1 and e.planned[0]["source_id"] == "settings:" + e.chinese
    assert e.planned[0]["parts"][0] == {"kind": "edit", "target_message_id": 902, "source_ref": {"key": "settings.state_basic", "values": {"language": "中文"}}}
    assert [button["payload"]["value"] for button in e.planned[0]["keyboard"][0]] == ["en-US", "zh-CN"]
    for identity in e.original_plan.group.callback_ids:
        assert (await e.callbacks.get(identity)).closed_reason == "done"
    with pytest.raises(TypeError):
        result.body["new"] = "forged"


@pytest.mark.asyncio
async def test_settings_replay_returns_original_receipt_without_reverting_later_choice(settings_act_environment):
    e = settings_act_environment
    first = await apply(e)
    await e.db.settings.update(e.link.owner_user_id, locale="en-US")
    second = await apply(e)
    assert second == first and len(e.planned) == 1
    assert (await e.db.settings.get(e.link.owner_user_id)).locale == "en-US"


@pytest.mark.asyncio
async def test_failed_required_edit_rolls_back_setting_act_and_group(settings_act_environment, monkeypatch):
    e = settings_act_environment

    async def fail(*args, **kwargs):
        raise RuntimeError("required edit failed")

    monkeypatch.setattr(e.acts.outbox, "plan_product_keyboard", fail)
    with pytest.raises(RuntimeError, match="required edit failed"):
        await apply(e)
    assert (await e.db.settings.get(e.link.owner_user_id)).locale == "en-US"
    async with e.sessions() as session:
        assert await session.get(TelegramActRow, e.chinese) is None
        assert await session.scalar(select(TelegramCallbackRow.consumed_at).where(TelegramCallbackRow.id == e.chinese)) is None
        rows = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:settings:" + e.chinese)))
        assert rows == []


@pytest.mark.asyncio
async def test_simultaneous_same_press_has_one_first_attempt_and_one_edit(settings_act_environment):
    e = settings_act_environment
    first, second = await asyncio.gather(apply(e), apply(e))
    assert first == second and first.attempts == 1
    assert len(e.planned) == 1
    async with e.sessions() as session:
        assert len(list(await session.scalars(select(TelegramActRow)))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{"field": "notify", "value": "none"}, {"field": "locale", "value": True}, {"field": "locale", "value": "unknown"}, {"field": "locale", "value": "zh-CN", "extra": "unknown"}])
async def test_settings_accepts_only_closed_saved_locale_choice(settings_act_environment, payload):
    e = settings_act_environment
    async with e.sessions.begin() as session:
        row = await session.get(TelegramCallbackRow, e.chinese)
        original = {key: value for key, value in row.payload.items() if key not in {"field", "value"}}
        row.payload = {**original, **payload}
    with pytest.raises(ValueError):
        await apply(e)
    assert not e.planned and (await e.db.settings.get(e.link.owner_user_id)).locale == "en-US"
    async with e.sessions() as session:
        assert await session.get(TelegramActRow, e.chinese) is None


@pytest.mark.asyncio
async def test_last_original_attempt_fence_rolls_back_all_staged_changes(settings_act_environment, monkeypatch):
    e = settings_act_environment
    actual = e.acts.outbox.plan_product_keyboard

    async def withdraw_after_real_plan(*args, **kwargs):
        result = await actual(*args, **kwargs)
        e.attempt_current[0] = False
        return result

    monkeypatch.setattr(e.acts.outbox, "plan_product_keyboard", withdraw_after_real_plan)
    with pytest.raises(ValueError):
        await apply(e)
    assert (await e.db.settings.get(e.link.owner_user_id)).locale == "en-US"
    async with e.sessions() as session:
        assert await session.get(TelegramActRow, e.chinese) is None
        assert await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:settings:" + e.chinese)) is None
    assert (await e.callbacks.get(e.chinese)).consumed_at is None


@pytest.mark.asyncio
async def test_cancelled_started_setting_rolls_back_before_any_new_edit(settings_act_environment, monkeypatch):
    e = settings_act_environment
    entered, release = asyncio.Event(), asyncio.Event()
    actual = e.db.settings.update

    async def hold_after_actual_setting(*args, **kwargs):
        result = await actual(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(e.db.settings, "update", hold_after_actual_setting)
    task = asyncio.create_task(apply(e))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not e.planned
        assert (await e.db.settings.get(e.link.owner_user_id)).locale == "en-US"
        assert (await e.callbacks.get(e.chinese)).consumed_at is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_committed_setting_survives_cancel_and_exact_replay_uses_receipt(settings_act_environment):
    """Actual COMMIT then held SQLAlchemy completion; not a native cursor probe."""
    e = settings_act_environment
    entered, release = asyncio.Event(), asyncio.Event()
    sessions = e.sessions.class_.sync_session_class

    def after_commit(session):
        if any(isinstance(row, TelegramActRow) and row.id == e.chinese for row in session.identity_map.values()):
            entered.set()
            await_only(release.wait())

    event.listen(sessions, "after_commit", after_commit)
    task = asyncio.create_task(apply(e))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        # A separate actual read observes the committed effect before release.
        assert (await e.db.settings.get(e.link.owner_user_id)).locale == "zh-CN"
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        receipt = await apply(e)
        assert receipt.state == "done" and receipt.attempts == 1 and len(e.planned) == 1
        assert not e.acts._operations
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(sessions, "after_commit", after_commit)


@pytest.mark.asyncio
async def test_old_binding_cannot_apply_a_saved_setting(settings_act_environment):
    e = settings_act_environment
    async with e.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(binding_generation=e.link.binding_generation + 1))
    with pytest.raises(ValueError):
        await apply(e)
    assert not e.planned and (await e.db.settings.get(e.link.owner_user_id)).locale == "en-US"


@pytest.mark.asyncio
async def test_settings_works_with_zero_named_ceilings_without_clearing_pause(settings_act_environment, monkeypatch):
    from app.gateway import authz

    e = settings_act_environment
    permissions = []

    async def no_named_permissions(*args, **kwargs):
        permissions.append(kwargs)
        return frozenset()

    monkeypatch.setattr(authz, "resolve_route_permissions", no_named_permissions)
    checker = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    async with e.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(acts_paused_since=datetime.now(UTC)))
    result = await e.acts.apply_settings(e.link, e.chinese, chat_id=e.link.chat_id, message_id=902, message_thread_id=None, guard=e.guard_factory(check_live_owner=checker))
    assert result.state == "done" and permissions
    assert (await e.links.get(e.link.id)).acts_paused_since is not None
