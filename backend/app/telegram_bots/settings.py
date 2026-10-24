"""Per-person preferences and local clocks for managed Telegram bots."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.telegram_bots.config import TelegramBotsConfig
from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

if TYPE_CHECKING:
    from app.telegram_bots.links import BotLink


@dataclass(frozen=True)
class PersonSettings:
    owner_user_id: str
    timezone: str
    locale: str
    quiet_enabled: bool
    quiet_start_minute: int
    quiet_end_minute: int
    notify: str
    summary_enabled: bool
    summary_minute: int
    voice_agreed_for: str | None
    voice_agreed_at: datetime | None
    updated_at: datetime


def _aware(moment: datetime) -> datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("a timestamp must include a timezone")
    return moment


def _stored_time(moment: datetime | None) -> datetime | None:
    # SQLite's DateTime adapter drops the zone; persisted timestamps are UTC.
    if moment is None:
        return None
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


class _Preferences(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    timezone: str = Field(min_length=1, max_length=64)
    locale: Literal["en-US", "zh-CN"]
    quiet_enabled: bool
    quiet_start_minute: int = Field(ge=0, le=1439)
    quiet_end_minute: int = Field(ge=0, le=1439)
    notify: Literal["all", "questions", "none"]
    summary_enabled: bool
    summary_minute: int = Field(ge=0, le=1439)
    voice_agreed_for: Literal["openai", "gemini"] | None
    voice_agreed_at: datetime | None

    @field_validator("timezone")
    @classmethod
    def valid_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ValueError, ZoneInfoNotFoundError):
            raise ValueError("invalid-timezone") from None
        return value

    @field_validator("voice_agreed_at")
    @classmethod
    def aware_agreement(cls, value: datetime | None) -> datetime | None:
        return _aware(value).astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def distinct_quiet_times(self) -> _Preferences:
        if self.quiet_start_minute == self.quiet_end_minute:
            raise ValueError("quiet hours require two different times")
        return self


def _snapshot(row: TelegramPersonSettingsRow) -> PersonSettings:
    return PersonSettings(
        owner_user_id=row.owner_user_id,
        **{name: getattr(row, name) for name in _Preferences.model_fields if name != "voice_agreed_at"},
        voice_agreed_at=_stored_time(row.voice_agreed_at),
        updated_at=_stored_time(row.updated_at),
    )


def _minute(value: str) -> int:
    hour, minute = value.split(":")
    return int(hour) * 60 + int(minute)


class SettingsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], config: TelegramBotsConfig) -> None:
        self.session_factory = session_factory
        self.config = config

    @asynccontextmanager
    async def _writer(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session, session.begin():
            if session.get_bind().dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            yield session

    async def get(self, owner_user_id: str) -> PersonSettings | None:
        async with self.session_factory() as session:
            row = await session.get(TelegramPersonSettingsRow, owner_user_id)
            return _snapshot(row) if row is not None else None

    async def ensure(self, owner_user_id: str, *, timezone: str, locale: str, session: AsyncSession | None = None) -> PersonSettings:
        if session is None:
            async with self._writer() as writer:
                return await self.ensure(owner_user_id, timezone=timezone, locale=locale, session=writer)
        row = await session.get(TelegramPersonSettingsRow, owner_user_id, populate_existing=True)
        if row is not None:
            return _snapshot(row)
        if not isinstance(owner_user_id, str) or not 1 <= len(owner_user_id) <= 64:
            raise ValueError("invalid-owner")
        preferences = _Preferences(
            timezone=timezone,
            locale=locale,
            quiet_enabled=self.config.quiet.default_enabled,
            quiet_start_minute=_minute(self.config.quiet.default_start),
            quiet_end_minute=_minute(self.config.quiet.default_end),
            notify="all",
            summary_enabled=self.config.summary.default_enabled,
            summary_minute=_minute(self.config.summary.default_time),
            voice_agreed_for=None,
            voice_agreed_at=None,
        )
        dialect = session.get_bind().dialect.name
        if dialect == "sqlite":
            insert = sqlite_insert
        elif dialect == "postgresql":
            insert = postgres_insert
        else:
            raise ValueError("unsupported settings database")
        statement = insert(TelegramPersonSettingsRow).values(owner_user_id=owner_user_id, **preferences.model_dump(), updated_at=datetime.now(UTC)).on_conflict_do_nothing(index_elements=["owner_user_id"])
        await session.execute(statement)
        # A concurrent insert may have won; return its complete preferences.
        row = await session.get(TelegramPersonSettingsRow, owner_user_id, populate_existing=True)
        if row is None:
            raise RuntimeError("settings initialization did not produce a row")
        return _snapshot(row)

    async def update(self, owner_user_id: str, **columns) -> PersonSettings:
        if set(columns) - _Preferences.model_fields.keys():
            raise ValueError("unknown or immutable settings field")
        async with self._writer() as session:
            row = await session.scalar(select(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == owner_user_id).with_for_update())
            if row is None:
                raise ValueError("settings-not-found")
            if not columns:
                return _snapshot(row)
            current = _snapshot(row)
            preferences = _Preferences.model_validate({**{name: getattr(current, name) for name in _Preferences.model_fields}, **columns})
            for name, value in preferences.model_dump().items():
                setattr(row, name, value)
            row.updated_at = datetime.now(UTC)
            await session.flush()
            return _snapshot(row)


def local_now(settings: PersonSettings, now: datetime | None = None) -> datetime:
    return _aware(now if now is not None else datetime.now(UTC)).astimezone(ZoneInfo(settings.timezone))


def in_quiet_hours(now: datetime, settings: PersonSettings) -> bool:
    if not settings.quiet_enabled:
        return False
    local = local_now(settings, now)
    minute = local.hour * 60 + local.minute
    start, end = settings.quiet_start_minute, settings.quiet_end_minute
    if start > end:
        return minute >= start or minute < end
    return start <= minute < end


_WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def format_time(moment: datetime, settings: PersonSettings, now: datetime | None = None) -> str:
    local = local_now(settings, moment)
    clock = f"{local.hour:02}:{local.minute:02}"
    if local.date() == local_now(settings, now).date():
        return clock
    return f"{_WEEKDAYS[local.weekday()]} {local.day} {_MONTHS[local.month - 1]}, {clock}"


def silent_now(link: BotLink, settings: PersonSettings, config: TelegramBotsConfig, now: datetime | None = None) -> bool:
    moment = _aware(now if now is not None else datetime.now(UTC))
    if not in_quiet_hours(moment, settings):
        return False
    return link.last_update_at is None or moment - _aware(link.last_update_at) > timedelta(seconds=config.outbox.recent_act_seconds)
