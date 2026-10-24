"""Pure imports, prepared voice models and borrowed operator removal seams."""

import os
import subprocess
import sys
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db

from app.telegram_bots import voice
from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.links import BotLinkRepository
from deerflow.config.model_config import ModelConfig
from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow, TelegramOutboxRow, TelegramPersonSettingsRow


def test_repository_import_does_not_load_telegram_runtime():
    program = """
import importlib.abc, sys
class NoRuntime(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'telegram' or fullname.startswith('telegram.') or fullname in ('app.telegram_bots.adapter', 'app.telegram_bots.polling', 'app.telegram_bots.producer', 'app.telegram_bots.supervisor'):
            raise AssertionError('repository import reached ' + fullname)
sys.meta_path.insert(0, NoRuntime())
import app.telegram_bots.links
"""
    completed = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=20, env=os.environ.copy())
    assert completed.returncode == 0, completed.stderr


def test_voice_prepared_models_never_read_managed_configuration(monkeypatch):
    def forbidden():
        raise AssertionError("prepared voice qualification read managed configuration")

    monkeypatch.setattr(voice, "get_app_config", forbidden)
    for variable in ("OPENAI_BASE_URL", "OPENAI_API_BASE", "GOOGLE_GEMINI_BASE_URL", "GOOGLE_GENAI_USE_VERTEXAI"):
        monkeypatch.delenv(variable, raising=False)
    model = ModelConfig(name="prepared", model="fixture", use="langchain_openai:ChatOpenAI", api_key="synthetic-" + uuid4().hex)
    config = TelegramBotsConfig(voice={"provider": "auto"})
    assert voice.resolve_transcriber(config, models=[]) is None
    transcriber = voice.resolve_transcriber(config, models=[model])
    assert isinstance(transcriber, voice.OpenAITranscriber)
    assert transcriber.api_key == model.api_key


@pytest.mark.asyncio
async def test_borrowed_owner_removal_is_atomic_and_keeps_other_owners(db):
    link = await bind_link(db, await add_link(db))
    other = await add_link(db, owner="owner-b", bot=link.bot_id + 1)
    before_events = len(await rows(db, TelegramBotLinkEventRow))
    with pytest.raises(RuntimeError, match="caller refusal"):
        async with db.links.capacity_writer(link.owner_user_id) as writer:
            removed = await db.links.remove_all_for_owner(link.owner_user_id, actor_id="host", detail={"by": "operator"}, session=writer)
            assert [row.id for row in removed] == [link.id]
            raise RuntimeError("caller refusal")
    assert await db.links.get(link.id) is not None
    assert len(await rows(db, TelegramBotLinkEventRow)) == before_events
    async with db.links.capacity_writer(link.owner_user_id) as writer:
        removed = await db.links.remove_all_for_owner(link.owner_user_id, actor_id="host", detail={"by": "operator"}, session=writer)
        assert [row.id for row in removed] == [link.id]
    assert await db.links.get(link.id) is None
    assert await db.links.get(other.id) is not None
    assert len(await rows(db, TelegramOutboxRow)) == 0
    assert {row.owner_user_id for row in await rows(db, TelegramPersonSettingsRow)} == {link.owner_user_id, other.owner_user_id}
    events = await rows(db, TelegramBotLinkEventRow)
    removed_events = [event for event in events if event.action == "remove"]
    assert len(removed_events) == 1
    assert removed_events[0].actor_id == "host" and removed_events[0].detail == {"by": "operator"}


@pytest.mark.asyncio
async def test_borrowed_owner_removal_refuses_foreign_or_noncapacity_sessions(db):
    link = await add_link(db)
    async with db.sessions.begin() as session:
        with pytest.raises(ValueError, match="owning writer"):
            await db.links.remove_all_for_owner(link.owner_user_id, actor_id="host", session=session)
    async with db.links.capacity_writer("owner-b") as writer:
        with pytest.raises(ValueError, match="owning writer"):
            await db.links.remove_all_for_owner(link.owner_user_id, actor_id="host", session=writer)
    other_repository = BotLinkRepository(db.sessions, db.connections, db.config)
    async with db.links.capacity_writer(link.owner_user_id) as writer:
        with pytest.raises(ValueError, match="owning writer"):
            await other_repository.remove_all_for_owner(link.owner_user_id, actor_id="host", session=writer)
    assert await db.links.get(link.id) is not None
