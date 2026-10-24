"""Transaction-safe Telegram preferences and the person's local clock."""

from __future__ import annotations

import asyncio
import dataclasses
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.settings import PersonSettings, SettingsRepository, format_time, in_quiet_hours, local_now, silent_now
from deerflow.persistence.base import Base
from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

NOW = datetime(2026, 10, 16, 18, 0, tzinfo=UTC)


@pytest_asyncio.fixture(scope="module", loop_scope="module", params=["sqlite", "postgres"])
async def database(tmp_path_factory, request):
    admin = None
    schema = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is not configured")
        url = make_url(uri).set(drivername="postgresql+asyncpg")
        ssl = {"ssl": False} if url.query.get("sslmode") == "disable" else {}
        url = url.difference_update_query(["sslmode"])
        admin = create_async_engine(url, connect_args=ssl)
        schema = "telegram_settings_" + uuid4().hex
        async with admin.begin() as connection:
            await connection.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**ssl, "server_settings": {"search_path": schema}})
    else:
        directory = tmp_path_factory.mktemp("telegram-settings")
        engine = create_async_engine(f"sqlite+aiosqlite:///{directory / 'settings.db'}", connect_args={"timeout": 10})
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=[TelegramPersonSettingsRow.__table__]))
        yield engine
    finally:
        await engine.dispose()
        if admin is not None:
            try:
                async with admin.begin() as connection:
                    await connection.execute(DropSchema(schema, cascade=True, if_exists=True))
            finally:
                await admin.dispose()


@pytest_asyncio.fixture(loop_scope="module")
async def repository(database):
    async with database.begin() as connection:
        await connection.execute(sa.delete(TelegramPersonSettingsRow))
    factory = async_sessionmaker(database, expire_on_commit=False)
    repo = SettingsRepository(factory, TelegramBotsConfig())
    yield repo, factory, database
    async with database.begin() as connection:
        await connection.execute(sa.delete(TelegramPersonSettingsRow))


def settings(**columns):
    return PersonSettings(
        **{
            "owner_user_id": "owner-a",
            "timezone": "Europe/London",
            "locale": "en-US",
            "quiet_enabled": True,
            "quiet_start_minute": 1260,
            "quiet_end_minute": 420,
            "notify": "all",
            "summary_enabled": True,
            "summary_minute": 480,
            "voice_agreed_for": None,
            "voice_agreed_at": None,
            "updated_at": NOW,
            **columns,
        }
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_ensure_initializes_configured_defaults_and_returns_frozen_settings(repository):
    _, factory, _ = repository
    config = TelegramBotsConfig(quiet={"default_enabled": False, "default_start": "22:30", "default_end": "06:45"}, summary={"default_enabled": False, "default_time": "09:15"})
    repo = SettingsRepository(factory, config)
    assert await repo.get("owner-a") is None
    actual = await repo.ensure("owner-a", timezone="America/Sao_Paulo", locale="zh-CN")
    assert dataclasses.asdict(actual) | {"updated_at": NOW} == {
        "owner_user_id": "owner-a",
        "timezone": "America/Sao_Paulo",
        "locale": "zh-CN",
        "quiet_enabled": False,
        "quiet_start_minute": 1350,
        "quiet_end_minute": 405,
        "notify": "all",
        "summary_enabled": False,
        "summary_minute": 555,
        "voice_agreed_for": None,
        "voice_agreed_at": None,
        "updated_at": NOW,
    }
    assert actual.updated_at.utcoffset() == timedelta(0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        actual.locale = "en-US"
    assert await repo.get("owner-a") == actual


@pytest.mark.asyncio(loop_scope="module")
async def test_ensure_preserves_every_existing_preference_and_ignores_initial_arguments(repository):
    repo, _, _ = repository
    await repo.ensure("owner-a", timezone="America/New_York", locale="en-US")
    updated = await repo.update("owner-a", notify="questions", quiet_enabled=False, summary_enabled=False, voice_agreed_for="gemini", voice_agreed_at=NOW)
    actual = await repo.ensure("owner-a", timezone="invalid timezone", locale="invalid locale")
    assert actual == updated


@pytest.mark.asyncio(loop_scope="module")
async def test_ensure_borrows_session_without_checkout_or_commit_and_rolls_back_first_settings(repository):
    _, factory, _ = repository

    def no_checkout():
        raise AssertionError("borrowed ensure opened a second session")

    repo = SettingsRepository(no_checkout, TelegramBotsConfig())
    async with factory() as session:
        async with session.begin():
            result = await repo.ensure("owner-a", timezone="UTC", locale="en-US", session=session)
            assert result.owner_user_id == "owner-a" and session.in_transaction()
            await session.rollback()
    assert await SettingsRepository(factory, TelegramBotsConfig()).get("owner-a") is None


@pytest.mark.asyncio(loop_scope="module")
async def test_borrowed_ensure_does_not_commit_other_writes_or_overwrite_existing_on_rollback(repository):
    repo, factory, _ = repository
    original = await repo.ensure("owner-a", timezone="UTC", locale="en-US")
    async with factory() as session:
        async with session.begin():
            await session.execute(sa.update(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == "owner-a").values(notify="none"))
            result = await repo.ensure("owner-a", timezone="Europe/London", locale="zh-CN", session=session)
            assert result.notify == "none"
            await session.rollback()
    assert await repo.get("owner-a") == original


@pytest.mark.asyncio(loop_scope="module")
async def test_concurrent_first_ensure_preserves_the_winning_row(repository):
    repo, factory, engine = repository
    started = asyncio.Event()

    def observe(_connection, _cursor, statement, _parameters, _context, _executemany):
        if "BEGIN IMMEDIATE" in statement.upper() or statement.lstrip().upper().startswith("INSERT INTO TELEGRAM_PERSON_SETTINGS"):
            started.set()

    task = None
    async with factory() as session:
        async with session.begin():
            if engine.dialect.name == "sqlite":
                await session.execute(sa.text("BEGIN IMMEDIATE"))
            winner = await repo.ensure("owner-a", timezone="Europe/London", locale="zh-CN", session=session)
            sa.event.listen(engine.sync_engine, "before_cursor_execute", observe)
            try:
                task = asyncio.create_task(repo.ensure("owner-a", timezone="America/New_York", locale="en-US"))
                await asyncio.wait_for(started.wait(), timeout=10)
                assert not task.done(), "the second writer must wait for the uncommitted winner"
                await session.commit()
                assert await asyncio.wait_for(task, timeout=10) == winner
            finally:
                sa.event.remove(engine.sync_engine, "before_cursor_execute", observe)
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
    assert await repo.get("owner-a") == winner


@pytest.mark.asyncio(loop_scope="module")
async def test_update_keeps_other_owners_and_normalizes_timestamps(repository):
    repo, _, _ = repository
    first = await repo.ensure("owner-a", timezone="UTC", locale="en-US")
    other = await repo.ensure("owner-b", timezone="Asia/Shanghai", locale="zh-CN")
    agreement = datetime(2026, 10, 16, 23, 0, tzinfo=datetime.fromisoformat("2026-01-01T00:00:00+05:00").tzinfo)
    actual = await repo.update("owner-a", timezone="Europe/London", locale="zh-CN", quiet_start_minute=1200, quiet_end_minute=360, notify="none", summary_minute=510, voice_agreed_for="openai", voice_agreed_at=agreement)
    assert actual.owner_user_id == first.owner_user_id and actual.voice_agreed_at == NOW
    assert actual.voice_agreed_at.utcoffset() == timedelta(0)
    assert actual.updated_at >= first.updated_at
    assert await repo.get("owner-b") == other
    cleared = await repo.update("owner-a", voice_agreed_for=None, voice_agreed_at=None)
    assert cleared.voice_agreed_for is None and cleared.voice_agreed_at is None
    assert await repo.get("owner-a") == cleared


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(
    "columns",
    [
        {"unknown": 1},
        {"owner_user_id": "other"},
        {"updated_at": NOW},
        {"timezone": "not/a-zone"},
        {"locale": "fr-FR"},
        {"quiet_enabled": 1},
        {"summary_enabled": "false"},
        {"quiet_start_minute": -1},
        {"quiet_end_minute": 1440},
        {"summary_minute": True},
        {"notify": "all-messages"},
        {"voice_agreed_for": "other"},
        {"voice_agreed_at": NOW.replace(tzinfo=None)},
        {"quiet_start_minute": 420},
    ],
)
async def test_update_rejects_invalid_or_identity_columns_without_changes(repository, columns):
    repo, _, _ = repository
    original = await repo.ensure("owner-a", timezone="UTC", locale="en-US")
    with pytest.raises((ValueError, TypeError)):
        await repo.update("owner-a", **columns)
    assert await repo.get("owner-a") == original


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("timezone,locale", [("", "en-US"), ("not/a-zone", "en-US"), ("UTC", "fr-FR")])
async def test_first_settings_require_valid_timezone_and_locale(repository, timezone, locale):
    repo, _, _ = repository
    with pytest.raises(ValueError):
        await repo.ensure("owner-a", timezone=timezone, locale=locale)
    assert await repo.get("owner-a") is None


@pytest.mark.asyncio(loop_scope="module")
async def test_update_missing_owner_refuses_without_inventing_settings(repository):
    repo, _, _ = repository
    with pytest.raises(ValueError, match="settings-not-found"):
        await repo.update("missing", notify="none")
    assert await repo.get("missing") is None


@pytest.mark.parametrize(
    "moment,expected_hour,expected_offset",
    [
        (datetime(2026, 1, 1, 12, tzinfo=UTC), 7, -5),
        (datetime(2026, 7, 1, 12, tzinfo=UTC), 8, -4),
        (datetime(2026, 3, 8, 6, 59, tzinfo=UTC), 1, -5),
        (datetime(2026, 3, 8, 7, 0, tzinfo=UTC), 3, -4),
    ],
)
def test_local_now_uses_each_calls_timezone_and_dst(moment, expected_hour, expected_offset):
    actual = local_now(settings(timezone="America/New_York"), moment)
    assert actual.hour == expected_hour and actual.utcoffset() == timedelta(hours=expected_offset)


@pytest.mark.parametrize("hour,minute,expected", [(20, 59, False), (21, 0, True), (23, 59, True), (0, 0, True), (6, 59, True), (7, 0, False)])
def test_cross_midnight_quiet_hours_are_start_inclusive_and_end_exclusive(hour, minute, expected):
    assert in_quiet_hours(datetime(2026, 10, 16, hour, minute, tzinfo=UTC), settings(timezone="UTC")) is expected


def test_daytime_quiet_hours_disabled_and_empty_windows():
    value = settings(timezone="UTC", quiet_start_minute=600, quiet_end_minute=720)
    assert in_quiet_hours(NOW.replace(hour=10), value)
    assert not in_quiet_hours(NOW.replace(hour=12), value)
    assert not in_quiet_hours(NOW.replace(hour=10), dataclasses.replace(value, quiet_enabled=False))
    assert not in_quiet_hours(NOW.replace(hour=10), dataclasses.replace(value, quiet_end_minute=600))


def test_quiet_hours_follow_both_repetitions_of_a_dst_hour():
    value = settings(timezone="America/New_York", quiet_start_minute=60, quiet_end_minute=120)
    assert in_quiet_hours(datetime(2026, 11, 1, 5, 30, tzinfo=UTC), value)
    assert in_quiet_hours(datetime(2026, 11, 1, 6, 30, tzinfo=UTC), value)
    assert not in_quiet_hours(datetime(2026, 11, 1, 7, 0, tzinfo=UTC), value)


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_format_time_compares_local_dates_and_uses_deterministic_day_names(locale):
    value = settings(timezone="Asia/Shanghai", locale=locale)
    assert format_time(datetime(2026, 10, 16, 16, 15, tzinfo=UTC), value, NOW) == "00:15"
    assert format_time(datetime(2026, 10, 16, 10, 0, tzinfo=UTC), value, NOW) == "Fri 16 Oct, 18:00"
    assert format_time(datetime(2026, 10, 18, 1, 5, tzinfo=UTC), value, NOW) == "Sun 18 Oct, 09:05"


@pytest.mark.parametrize("seconds,expected", [(None, True), (1801, True), (1800, False), (1799, False), (0, False), (-1, False)])
def test_silent_now_honors_null_old_recent_and_future_activity(seconds, expected):
    moment = NOW.replace(hour=23)
    link = SimpleNamespace(last_update_at=moment - timedelta(seconds=seconds) if seconds is not None else None)
    assert silent_now(link, settings(timezone="UTC"), TelegramBotsConfig(), moment) is expected


def test_silent_now_uses_configuration_and_never_silences_outside_quiet_hours():
    config = TelegramBotsConfig(outbox={"recent_act_seconds": 60})
    moment = NOW.replace(hour=23)
    link = SimpleNamespace(last_update_at=moment - timedelta(seconds=61))
    assert silent_now(link, settings(timezone="UTC"), config, moment)
    assert not silent_now(link, settings(timezone="UTC", quiet_enabled=False), config, moment)
    assert not silent_now(SimpleNamespace(last_update_at=None), settings(timezone="UTC"), config, NOW)


def test_clock_helpers_reject_naive_caller_timestamps():
    value = settings(timezone="UTC")
    with pytest.raises(ValueError):
        local_now(value, NOW.replace(tzinfo=None))
    with pytest.raises(ValueError):
        format_time(NOW.replace(tzinfo=None), value, NOW)
