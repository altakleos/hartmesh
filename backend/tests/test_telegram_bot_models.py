"""ORM behavior and database identity boundaries for Telegram persistence."""

from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateTable

from deerflow.persistence.base import Base
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots import (
    TelegramAccountRow,
    TelegramActRow,
    TelegramAssistantAddRow,
    TelegramBotIntentRow,
    TelegramBotLinkEventRow,
    TelegramBotLinkRow,
    TelegramCallbackRow,
    TelegramDeploymentRow,
    TelegramInboundMarkRow,
    TelegramInstancePolicyRow,
    TelegramOutboxRow,
    TelegramPendingFileRow,
    TelegramPersonSettingsRow,
    TelegramWorkCursorRow,
    TelegramWorkTopicRow,
)

ROWS = (
    TelegramBotLinkRow,
    TelegramBotLinkEventRow,
    TelegramAccountRow,
    TelegramPersonSettingsRow,
    TelegramBotIntentRow,
    TelegramInstancePolicyRow,
    TelegramAssistantAddRow,
    TelegramWorkTopicRow,
    TelegramCallbackRow,
    TelegramActRow,
    TelegramWorkCursorRow,
    TelegramOutboxRow,
    TelegramInboundMarkRow,
    TelegramPendingFileRow,
    TelegramDeploymentRow,
)
NOW = datetime(2026, 10, 24, tzinfo=UTC)


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    path = tmp_path_factory.mktemp("telegram-models") / "models.sqlite"
    value = sa.create_engine(f"sqlite:///{path}")

    @sa.event.listens_for(value, "connect")
    def foreign_keys(connection, record):
        connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(value, tables=[ChannelConnectionRow.__table__, *(row.__table__ for row in ROWS)])
    yield value
    value.dispose()


@pytest.fixture
def session(engine):
    with engine.connect() as connection:
        transaction = connection.begin()
        with Session(bind=connection) as value:
            yield value
        if transaction.is_active:
            transaction.rollback()


def link(identity="a", *, owner="owner-a", bot_id=9_000_000_001, **fields):
    return TelegramBotLinkRow(
        **{
            "id": identity * 32,
            "owner_user_id": owner,
            "agent_kind": "lead_agent",
            "agent_ref": "lead_agent",
            "bot_id": bot_id,
            "bot_username": "fixture_bot",
            "encrypted_token": "wrapped-fixture",
            "state": "waiting_connect",
            "created_at": NOW,
            "updated_at": NOW,
            **fields,
        }
    )


def intent(identity, *, owner="owner-a", **fields):
    return TelegramBotIntentRow(
        **{
            "id": identity * 32,
            "owner_user_id": owner,
            "manager_link_id": "a" * 32,
            "manager_binding_generation": 0,
            "agent_kind": "custom_agent",
            "agent_ref": identity,
            "suggested_username": "fixture_bot",
            "suggested_name": "Fixture",
            "source": "web",
            "expires_at": NOW + timedelta(minutes=1),
            "created_at": NOW,
            **fields,
        }
    )


def test_exports_cover_fifteen_distinct_base_models():
    assert len(ROWS) == len({row.__tablename__ for row in ROWS}) == 15
    assert all(issubclass(row, Base) for row in ROWS)
    assert TelegramOutboxRow.class_.property.columns[0].name == "class"


def test_models_have_only_explicit_python_defaults_and_no_server_defaults():
    defaults = {
        "telegram_bot_links": {
            "token_revision": 1,
            "connect_conflict_epoch": 0,
            "origin": "pasted",
            "can_manage_bots": False,
            "bind_mode": "code",
            "binding_generation": 0,
            "health": "ok",
            "topics_enabled": False,
            "profile_pending": True,
            "profile_version": 0,
            "notices": dict,
        },
        "telegram_bot_link_events": {"detail": dict},
        "telegram_person_settings": {"locale": "en-US", "quiet_enabled": True, "quiet_start_minute": 1260, "quiet_end_minute": 420, "notify": "all", "summary_enabled": True, "summary_minute": 480},
        "telegram_callbacks": {"payload": dict},
        "telegram_acts": {"operation_ids": list, "body": dict, "attempts": 0},
        "telegram_outbox": {"payload": dict, "parts": list, "attempts": 0},
        "telegram_inbound_marks": {"route_basis": dict, "sanitized_input": dict, "grouped_message_ids": list, "operation_ids": list, "delivery_ids": list, "route_complete": False},
        "telegram_deployment": {"polling_state": "running"},
    }
    for row in ROWS:
        expected = defaults.get(row.__tablename__, {})
        for column in row.__table__.columns:
            assert column.server_default is None, (row.__tablename__, column.name)
            assert column.onupdate is None, (row.__tablename__, column.name)
            if column.name not in expected:
                assert column.default is None, (row.__tablename__, column.name)
            elif expected[column.name] in (dict, list):
                assert column.default.is_callable
            else:
                assert type(column.default.arg) is type(expected[column.name])
                assert column.default.arg == expected[column.name]


@pytest.mark.parametrize("row,column", [(TelegramDeploymentRow, "id"), (TelegramAccountRow, "telegram_user_id")])
def test_external_numeric_primary_keys_do_not_generate_postgres_sequences(row, column):
    assert row.__table__.c[column].autoincrement is False
    ddl = str(CreateTable(row.__table__).compile(dialect=postgresql.dialect()))
    assert "SERIAL" not in ddl and "DEFAULT" not in ddl


def test_json_defaults_are_fresh_for_each_inserted_row(session):
    first, second = link(), link("b", owner="owner-b", bot_id=9_000_000_002)
    session.add_all([first, second])
    session.flush()
    assert first.notices == second.notices == {} and first.notices is not second.notices
    assert first.profile_pending is True and first.connect_conflict_epoch == 0
    acts = [
        TelegramActRow(id=key * 32, bot_id=first.bot_id, owner_user_id=first.owner_user_id, binding_generation=0, telegram_user_id=8_000_000_001, kind="future_kind", state="pending", created_at=NOW, updated_at=NOW) for key in ("c", "d")
    ]
    session.add_all(acts)
    session.flush()
    assert acts[0].body == acts[1].body == {} and acts[0].body is not acts[1].body
    assert acts[0].operation_ids == acts[1].operation_ids == [] and acts[0].operation_ids is not acts[1].operation_ids
    acts[0].operation_ids.append("one")
    first.notices["once"] = "now"
    assert acts[1].operation_ids == [] and second.notices == {}


@pytest.mark.parametrize("clear_existing", [False, True])
def test_optional_act_json_persists_real_sql_null(session, clear_existing):
    value = {"reference": "original"} if clear_existing else None
    act = TelegramActRow(
        id="a" * 32,
        bot_id=9_000_000_001,
        owner_user_id="owner-a",
        binding_generation=0,
        telegram_user_id=42,
        kind="stop",
        state="pending",
        continuation=value,
        stop_dispatch=value,
        result=value,
        created_at=NOW,
        updated_at=NOW,
    )
    session.add(act)
    session.flush()
    if clear_existing:
        for name in ("continuation", "stop_dispatch", "result"):
            column = getattr(TelegramActRow, name)
            assert session.scalar(sa.select(column.is_not(None))) is True
            setattr(act, name, None)
        session.flush()
    for name in ("continuation", "stop_dispatch", "result"):
        # Deserialization returns Python None for both SQL NULL and JSON null;
        # check the SQL predicate used by absence/recovery queries instead.
        assert session.scalar(sa.select(getattr(TelegramActRow, name).is_(None))) is True


@pytest.mark.parametrize("identity", ["bot", "owner-agent"])
def test_bot_and_owner_target_uniqueness_is_enforced(session, identity):
    session.add(link())
    session.flush()
    session.add(link("b", owner="owner-b" if identity == "bot" else "owner-a", bot_id=9_000_000_001 if identity == "bot" else 9_000_000_002))
    with pytest.raises(IntegrityError):
        session.flush()


@pytest.mark.parametrize("ended", ["consumed_at", "cancelled_at"])
def test_closed_managed_intents_do_not_block_a_new_intent(session, ended):
    session.add(link())
    session.flush()
    session.add_all([intent("b", **{ended: NOW}), intent("c")])
    session.flush()
    assert session.scalar(sa.select(sa.func.count()).select_from(TelegramBotIntentRow)) == 2


def test_expired_but_unclosed_intent_still_blocks_a_second_open_intent(session):
    session.add(link())
    session.flush()
    session.add(intent("b", expires_at=NOW - timedelta(seconds=1)))
    session.flush()
    session.add(intent("c"))
    with pytest.raises(IntegrityError):
        session.flush()


def test_token_operation_receipts_are_unique_per_owner_and_allow_null_operation_ids(session):
    values = dict(link_id="a" * 32, bot_id=9_000_000_001, actor_id="host", action="token_replace_receipt", created_at=NOW)
    session.add_all(
        [
            TelegramBotLinkEventRow(id="a" * 32, owner_user_id="owner-a", operation_id="operation", **values),
            TelegramBotLinkEventRow(id="b" * 32, owner_user_id="owner-b", operation_id="operation", **values),
            TelegramBotLinkEventRow(id="c" * 32, owner_user_id="owner-a", **values),
            TelegramBotLinkEventRow(id="d" * 32, owner_user_id="owner-a", **values),
        ]
    )
    session.flush()
    session.add(TelegramBotLinkEventRow(id="e" * 32, owner_user_id="owner-a", operation_id="operation", **values))
    with pytest.raises(IntegrityError):
        session.flush()


@pytest.mark.parametrize("field,value", [("locale", "invalid"), ("quiet_start_minute", -1), ("quiet_end_minute", 1440), ("summary_minute", 1440), ("notify", "invalid"), ("voice_agreed_for", "other")])
def test_person_settings_closed_sets_and_clock_ranges_are_enforced(session, field, value):
    session.add(TelegramPersonSettingsRow(**{"owner_user_id": "owner-a", "timezone": "UTC", "updated_at": NOW, field: value}))
    with pytest.raises(IntegrityError):
        session.flush()


def test_person_settings_defaults_and_nullable_voice_consent(session):
    settings = TelegramPersonSettingsRow(owner_user_id="owner-a", timezone="UTC", updated_at=NOW)
    session.add(settings)
    session.flush()
    assert settings.locale == "en-US" and settings.notify == "all"
    assert settings.quiet_start_minute == 1260 and settings.quiet_end_minute == 420 and settings.summary_minute == 480
    assert settings.voice_agreed_for is None and settings.voice_agreed_at is None


def test_singleton_deployment_requires_the_external_id_one(session):
    session.add(TelegramDeploymentRow(id=2, updated_at=NOW))
    with pytest.raises(IntegrityError):
        session.flush()


def test_deleting_channel_binding_preserves_link_and_clears_connection_id(session):
    connection = ChannelConnectionRow(id="binding", owner_user_id="owner-a", provider="telegram")
    session.add(connection)
    session.flush()
    session.add(link(connection_id=connection.id))
    session.flush()
    session.execute(sa.delete(ChannelConnectionRow).where(ChannelConnectionRow.id == connection.id))
    assert session.scalar(sa.select(TelegramBotLinkRow.connection_id)) is None
    assert session.scalar(sa.select(TelegramBotLinkRow.bot_id)) == 9_000_000_001


def test_link_deletion_cascades_live_routing_but_retains_history_and_other_links(session):
    session.add(link())
    session.flush()
    session.add(link("b", bot_id=9_000_000_002, agent_kind="custom_agent", agent_ref="other", manager_link_id="a" * 32))
    session.add_all(
        [
            TelegramBotLinkEventRow(id="e" * 32, link_id="a" * 32, owner_user_id="owner-a", bot_id=9_000_000_001, actor_id="host", action="add", created_at=NOW),
            TelegramActRow(id="c" * 32, link_id="a" * 32, bot_id=9_000_000_001, owner_user_id="owner-a", binding_generation=0, telegram_user_id=42, kind="future_kind", state="done", created_at=NOW, updated_at=NOW),
            TelegramAssistantAddRow(id="d" * 32, link_id="a" * 32, owner_user_id="owner-a", state="draft", created_at=NOW, updated_at=NOW),
            intent("f"),
            TelegramWorkTopicRow(link_id="a" * 32, work_id="work", chat_id=42, binding_generation=0, creation_state="creating", name="Work", created_at=NOW),
            TelegramCallbackRow(id="g" * 32, group_id="group", link_id="a" * 32, chat_id=42, binding_generation=0, kind="future_kind", created_at=NOW),
            TelegramWorkCursorRow(link_id="a" * 32, work_id="work", instance_id="instance", binding_generation=0, revision=1, updated_at=NOW),
            TelegramOutboxRow(id="h" * 32, link_id="a" * 32, owner_user_id="owner-a", dedupe_key="reply", class_="reply", binding_generation=0, state="pending", not_before=NOW, created_at=NOW),
            TelegramInboundMarkRow(link_id="a" * 32, message_id=1, update_id=1, chat_id=42, binding_generation=0, accepted_at=NOW, state="accepted"),
            TelegramPendingFileRow(link_id="a" * 32, message_id=1, chat_id=42, binding_generation=0, file_id="file", file_unique_id="unique", filename="note.txt", mime_type="text/plain", size=1, is_image=False, created_at=NOW),
        ]
    )
    session.flush()
    session.execute(sa.delete(TelegramBotLinkRow).where(TelegramBotLinkRow.id == "a" * 32))
    for row in (TelegramBotIntentRow, TelegramWorkTopicRow, TelegramCallbackRow, TelegramWorkCursorRow, TelegramOutboxRow, TelegramInboundMarkRow, TelegramPendingFileRow):
        assert session.scalar(sa.select(sa.func.count()).select_from(row)) == 0, row.__tablename__
    for row in (TelegramBotLinkRow, TelegramActRow, TelegramAssistantAddRow, TelegramBotLinkEventRow):
        assert session.scalar(sa.select(sa.func.count()).select_from(row)) == 1, row.__tablename__
    assert session.scalar(sa.select(TelegramBotLinkRow.manager_link_id)) is None
    assert session.scalar(sa.select(TelegramActRow.link_id)) is None
    assert session.scalar(sa.select(TelegramAssistantAddRow.link_id)) is None
