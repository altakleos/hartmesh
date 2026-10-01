"""Download all my data: every piece of one person's own work, in one prepared archive, and nobody else's.

A person can take all of their own work out in one download: every
conversation (as the page shows it), every file of theirs (uploads, outputs
whether presented or not, workspace, their own files and skills), their
memory, their schedules and their custom agents. The archive is built on the
data disk and downloaded, then deleted. It holds no credential and nothing
another person owns, and nothing an administrator can use to take someone
else's.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import io
import json
import logging
import os
import stat
import threading
import tomllib
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi import FastAPI
from fastapi.testclient import TestClient
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from starlette.requests import ClientDisconnect

from app.gateway import account_export, transcript
from app.gateway.artifact_archive import ArtifactArchiveError, copy_file
from app.gateway.auth.models import User
from app.gateway.routers import account_export as account_export_router
from app.gateway.routers import memory as memory_router
from deerflow.config.account_export_config import AccountExportConfig
from deerflow.config.paths import Paths
from deerflow.constants import BROWSER_FRAMES_DIRNAME, MCP_INTERNAL_DIRNAME, TOOL_RESULTS_DIRNAME
from deerflow.persistence.agents import file as file_agent_store
from deerflow.persistence.thread_meta.memory import THREADS_NS, MemoryThreadMetaStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import EditReplayVisibility

# The real sources, kept before a deployment double replaces them on the module.
_REAL_MEMORY_EXPORT_DOCUMENT = account_export.memory_export_document
_REAL_OWNED_AGENT_DOCUMENTS = account_export.owned_agent_documents

PERSON_A = User(id=UUID("11111111-1111-4111-8111-111111111111"), email="ana@example.com", password_hash="x", system_role="user")
PERSON_B = User(id=UUID("22222222-2222-4222-8222-222222222222"), email="ben@example.com", password_hash="x", system_role="user")
ADMIN = User(id=UUID("33333333-3333-4333-8333-333333333333"), email="root@example.com", password_hash="x", system_role="admin")
A_ID = str(PERSON_A.id)
B_ID = str(PERSON_B.id)
B_MARKER = "BEN-PRIVATE-MARKER-7f3a"
SENTINEL = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"
CREATED_AT = "2026-09-20T08:15:00+00:00"
SPILL_DIR = "tool-spill"


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _config(**overrides) -> AccountExportConfig:
    values = {"part_bytes": 2**31, "min_free_bytes": 0, "expires_after_seconds": 3600, "max_concurrent": 2} | overrides
    return AccountExportConfig.model_construct(**values)


def _disk(*, free: int, total: int = 2**40):
    return lambda path: SimpleNamespace(total=total, used=total - free, free=free)


# ── A deployment with two people ─────────────────────────────────────────


class _RawAccessor:
    def __init__(self, checkpointer):
        self._checkpointer = checkpointer

    async def aget(self, config):
        saved = await self._checkpointer.aget_tuple(config)
        if saved is None:
            return SimpleNamespace(values={}, config={})
        return SimpleNamespace(values=saved.checkpoint["channel_values"], config=saved.config)


class _Schedules:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict]] = {}

    async def list_by_user(self, user_id):
        return self.rows.get(user_id, [])


class _Deployment:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config: AccountExportConfig | None = None) -> None:
        self.paths = Paths(tmp_path / "home")
        self.app = SimpleNamespace(state=SimpleNamespace())
        # The real store: ``user_id=None`` would mean every person's threads.
        self.threads = MemoryThreadMetaStore(InMemoryStore())
        self.checkpointer = InMemorySaver()
        self.events = MemoryRunEventStore()
        self.schedules = _Schedules()
        run_manager = AsyncMock()
        run_manager.list_successful_regenerate_sources.return_value = set()
        run_manager.list_edit_replay_visibility.return_value = EditReplayVisibility()
        self.app.state.thread_store = self.threads
        self.app.state.run_event_store = self.events
        self.app.state.run_manager = run_manager
        self.app.state.checkpointer = self.checkpointer
        self.app.state.scheduled_task_repo = self.schedules
        self.memory: dict[tuple[str, str | None], dict] = {}
        self.agents: dict[str, list[dict]] = {}
        self.config = config or _config()
        monkeypatch.setattr(
            transcript, "build_checkpoint_state_accessor", lambda scope, *, thread_id, assistant_id=None, checkpoint_id=None: (_RawAccessor(self.checkpointer), {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}})
        )
        monkeypatch.setattr(account_export, "memory_export_document", self._memory)
        monkeypatch.setattr(account_export, "owned_agent_documents", lambda user_id: self.agents.get(user_id))
        self.service = account_export.AccountExportService(self.app, paths=self.paths, config=lambda: self.config, spill_dir_name=lambda: SPILL_DIR)

    async def _memory(self, user_id: str, *, agent_name: str | None = None) -> dict | None:
        return self.memory.get((user_id, agent_name))

    async def thread(self, person: User, thread_id: str, title: str) -> None:
        await self.threads.create(thread_id, user_id=str(person.id), display_name=title)
        record = await self.threads.get(thread_id, user_id=str(person.id))
        await self.threads._store.aput(THREADS_NS, thread_id, {**record, "created_at": CREATED_AT, "updated_at": CREATED_AT})

    async def conversation(self, person: User, thread_id: str, title: str, turns: list[tuple[str, str]]) -> None:
        await self.thread(person, thread_id, title)
        for index, (kind, text) in enumerate(turns):
            await self.events.put(
                thread_id=thread_id,
                run_id=f"{thread_id}-run",
                event_type="llm.ai.response" if kind == "ai" else "llm.human.input",
                category="message",
                content={"type": kind, "id": f"{thread_id}-{index}", "content": text, "additional_kwargs": {}},
                metadata={"caller": "lead_agent"},
            )
        checkpoint = empty_checkpoint()
        checkpoint["channel_values"] = {"title": title}
        checkpoint["channel_versions"] = {"title": 1}
        await self.checkpointer.aput({"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}, checkpoint, {"step": 1, "source": "loop", "writes": {}, "parents": {}}, {"title": 1})

    def file(self, person: User, thread_id: str, area: str, relative: str, data: bytes) -> Path:
        path = self.paths.thread_dir(thread_id, user_id=str(person.id)) / "user-data" / area / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    def own_file(self, person: User, relative: str, data: bytes) -> Path:
        path = self.paths.user_files_dir(str(person.id)) / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    def skill_file(self, person: User, relative: str, data: bytes) -> Path:
        path = self.paths.user_custom_skills_dir(str(person.id)) / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    async def export(self, person: User) -> account_export.ExportJob:
        job = self.service.start(person)
        await asyncio.wait_for(job.task, 30)
        await self.service.settled()
        return job

    def left(self) -> list[Path]:
        root = self.paths.exports_dir()
        return list(root.iterdir()) if root.exists() else []


def _archive(job: account_export.ExportJob) -> dict[str, bytes]:
    """Every entry of every part, by path, decompressed."""
    entries: dict[str, bytes] = {}
    for part in job.parts:
        with zipfile.ZipFile(part) as archive:
            assert archive.testzip() is None, part.name
            for name in archive.namelist():
                assert name not in entries, f"{name} is in two parts"
                entries[name] = archive.read(name)
    return entries


def _manifest(job: account_export.ExportJob) -> dict:
    return json.loads(_archive(job)["manifest.json"])


def _skipped(job: account_export.ExportJob) -> dict[str, str]:
    return {item["path"]: item["reason"] for item in _manifest(job)["skipped"]}


def _anywhere(entries: dict[str, bytes], text: str) -> list[str]:
    """Every entry whose name or decompressed content holds ``text``."""
    return [name for name, data in entries.items() if text in name or text.encode() in data]


SCHEDULE = {
    "id": "task-1",
    "title": "Weekly numbers",
    "prompt": "Send me the weekly numbers",
    "schedule_type": "cron",
    "schedule_spec": {"cron": "0 9 * * 1"},
    "timezone": "UTC",
    "status": "enabled",
    "context_mode": "fresh_thread_per_run",
    "assistant_id": None,
    "thread_id": None,
    "overlap_policy": "skip",
    "created_at": CREATED_AT,
    "updated_at": CREATED_AT,
    "next_run_at": "2026-09-28T09:00:00+00:00",
    "last_run_at": None,
}


async def _two_people(deployment: _Deployment) -> None:
    await deployment.conversation(PERSON_A, "a-thread-1", "August numbers", [("human", "How did August go?"), ("ai", "<think>check the sheet</think>Revenue was up.")])
    await deployment.conversation(PERSON_A, "a-thread-2", "Hiring plan", [("human", "Draft the hiring plan"), ("ai", "Here is the plan.")])
    deployment.file(PERSON_A, "a-thread-1", "uploads", "august.xlsx", b"ana's spreadsheet")
    deployment.file(PERSON_A, "a-thread-1", "outputs", "report.pdf", b"presented report")
    deployment.file(PERSON_A, "a-thread-1", "outputs", "drafts/unpresented.md", b"never presented")
    deployment.file(PERSON_A, "a-thread-2", "workspace", "scratch/notes.txt", b"workspace notes")
    deployment.own_file(PERSON_A, "Reports/kept.pdf", b"kept in my files")
    deployment.skill_file(PERSON_A, "pricing/SKILL.md", b"---\nname: pricing\n---\nHow Ana prices jobs")
    deployment.memory[(A_ID, None)] = {"version": "1.0", "facts": [{"id": "f1", "content": "Ana runs the Lisbon office"}]}
    deployment.memory[(A_ID, "analyst")] = {"version": "1.0", "facts": [{"id": "f2", "content": "Ana wants totals first"}]}
    # The scheduler's own bookkeeping sits beside the definition in the row.
    deployment.schedules.rows[A_ID] = [{**SCHEDULE, "user_id": A_ID, "lease_owner": "gateway-1", "lease_token": SENTINEL, "last_error": None}]
    deployment.agents[A_ID] = [{"name": "analyst", "description": "Reads the numbers", "soul": "You read spreadsheets."}]
    await deployment.conversation(PERSON_B, "b-thread-1", f"Ben's {B_MARKER}", [("human", f"secret plan {B_MARKER}"), ("ai", "noted")])
    deployment.file(PERSON_B, "b-thread-1", "uploads", f"{B_MARKER}.txt", B_MARKER.encode())
    deployment.own_file(PERSON_B, "mine.txt", B_MARKER.encode())
    deployment.skill_file(PERSON_B, "bens/SKILL.md", B_MARKER.encode())
    deployment.memory[(B_ID, None)] = {"version": "1.0", "facts": [{"id": "b1", "content": B_MARKER}]}


# ── Complete ─────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_the_export_holds_every_conversation_and_every_file_byte_for_byte(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    job = await deployment.export(PERSON_A)

    assert job.state == "ready", job.error
    entries = _archive(job)
    manifest = json.loads(entries["manifest.json"])
    # What ``POST /api/threads/search`` lists for the person, no more and no fewer.
    assert sorted(conversation["id"] for conversation in manifest["conversations"]) == sorted(row["thread_id"] for row in await deployment.threads.search(limit=1000, user_id=A_ID))
    user_dir = deployment.paths.user_dir(A_ID)
    on_disk = {}
    for path in (user_dir / "threads").rglob("*"):
        if path.is_file():
            thread_id, _, *rest = path.relative_to(user_dir / "threads").parts
            on_disk[f"conversations/{thread_id}/files/{'/'.join(rest)}"] = path.read_bytes()
    for prefix, root in (("my-files", deployment.paths.user_files_dir(A_ID)), ("my-skills", deployment.paths.user_custom_skills_dir(A_ID))):
        on_disk |= {f"{prefix}/{path.relative_to(root).as_posix()}": path.read_bytes() for path in root.rglob("*") if path.is_file()}
    assert len(on_disk) == 6
    for entry, data in on_disk.items():
        assert entries[entry] == data, entry
    listed = {item["path"]: item for item in manifest["files"]}
    for entry, data in entries.items():
        if entry == "manifest.json":
            continue
        assert listed[entry]["size"] == len(data) and listed[entry]["sha256"] == hashlib.sha256(data).hexdigest(), entry
    assert set(listed) == set(entries) - {"manifest.json"}
    assert manifest["totals"] == {"conversations": 2, "files": len(listed), "bytes": sum(item["size"] for item in listed.values()), "skipped": 0}
    assert job.progress.files_done == job.progress.files_total == 6


@pytest.mark.anyio
async def test_every_conversation_is_exported_however_many_there_are(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    for index in range(150):
        await deployment.thread(PERSON_A, f"a-thread-{index:03d}", f"Conversation {index}")
    await deployment.thread(PERSON_B, "b-thread-1", "Ben's")

    manifest = _manifest(await deployment.export(PERSON_A))

    assert sorted(conversation["id"] for conversation in manifest["conversations"]) == [f"a-thread-{index:03d}" for index in range(150)]


@pytest.mark.anyio
async def test_each_transcript_is_the_one_the_conversation_s_own_export_writes(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    job = await deployment.export(PERSON_A)

    entries = _archive(job)
    manifest = json.loads(entries["manifest.json"])
    exported_at = datetime.fromisoformat(manifest["exported_at"])
    for conversation in manifest["conversations"]:
        record = await deployment.threads.get(conversation["id"], user_id=A_ID)
        read = await transcript.read_conversation(SimpleNamespace(app=deployment.app), conversation["id"], record, user_id=A_ID)
        assert entries[f"conversations/{conversation['id']}/transcript.md"].decode() == transcript.transcript_markdown(title=read.title, created_at=read.created_at, messages=read.messages, exported_at=exported_at)
        assert entries[f"conversations/{conversation['id']}/transcript.json"].decode() == transcript.transcript_json(title=read.title, thread_id=read.thread_id, created_at=read.created_at, messages=read.messages, exported_at=exported_at)
        assert conversation["message_count"] == 2 and conversation["title"] == read.title
    assert b"check the sheet" not in entries["conversations/a-thread-1/transcript.md"]


@pytest.mark.anyio
async def test_the_manifest_says_who_when_which_release_and_what(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    version = tomllib.loads((Path(account_export.__file__).resolve().parents[2] / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]

    manifest = _manifest(await deployment.export(PERSON_A))

    assert manifest["format"] == "account-export" and manifest["version"] == 1
    assert manifest["person"] == {"id": A_ID, "email": "ana@example.com"}
    assert manifest["release"] == version != "unknown"
    assert datetime.fromisoformat(manifest["exported_at"]).tzinfo is not None
    assert sorted(manifest["conversations"], key=lambda conversation: conversation["id"])[0] == {
        "id": "a-thread-1",
        "title": "August numbers",
        "created_at": CREATED_AT,
        "updated_at": CREATED_AT,
        "message_count": 2,
        "transcripts": ["conversations/a-thread-1/transcript.md", "conversations/a-thread-1/transcript.json"],
    }
    assert manifest["parts"] == 1 and all(item["part"] == 1 for item in manifest["files"])
    assert manifest["skipped"] == [] and manifest["folders_without_conversation"] == []


@pytest.mark.anyio
async def test_message_count_is_the_transcript_s(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi"), ("ai", ""), ("ai", "answer")])

    entries = _archive(await deployment.export(PERSON_A))

    [conversation] = json.loads(entries["manifest.json"])["conversations"]
    assert conversation["message_count"] == len(json.loads(entries["conversations/a-thread-1/transcript.json"])["messages"]) == 2


@pytest.mark.anyio
async def test_memory_schedules_and_agents_come_as_their_definitions(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    entries = _archive(await deployment.export(PERSON_A))

    assert json.loads(entries["memory.json"]) == deployment.memory[(A_ID, None)]
    # The whole definition, and none of the scheduler's bookkeeping.
    assert json.loads(entries["scheduled-tasks.json"]) == [SCHEDULE]
    assert json.loads(entries["agents.json"]) == deployment.agents[A_ID]
    assert json.loads(entries["agents/analyst/memory.json"]) == deployment.memory[(A_ID, "analyst")]


@pytest.mark.anyio
async def test_memory_json_is_what_the_memory_export_route_answers(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])
    documents = {
        None: {"version": "1.0", "lastUpdated": "2026-09-20T00:00:00Z", "facts": [{"id": "f1", "content": "Ana runs the Lisbon office", "category": "context", "confidence": 0.9, "createdAt": "2026-09-20T00:00:00Z", "source": "t"}]},
        "analyst": {"version": "1.0", "lastUpdated": "2026-09-21T00:00:00Z", "facts": [{"id": "f2", "content": "Totals first", "category": "preference", "confidence": 0.8, "createdAt": "2026-09-21T00:00:00Z", "source": "t"}]},
    }
    asked: list[tuple[str, str | None]] = []

    class _Manager:
        supports_agent_scoped_management = True

        def get_memory(self, *, user_id, agent_name=None):
            asked.append((user_id, agent_name))
            return documents[agent_name]

    monkeypatch.setattr(memory_router, "get_memory_manager", lambda: _Manager())
    monkeypatch.setattr(account_export, "memory_export_document", _REAL_MEMORY_EXPORT_DOCUMENT)
    deployment.agents[A_ID] = [{"name": "analyst"}]
    app = make_authed_test_app(user_factory=lambda: PERSON_A)
    app.include_router(memory_router.router)
    with TestClient(app) as client:
        route = client.get("/api/memory/export")
    assert route.status_code == 200

    entries = _archive(await deployment.export(PERSON_A))

    assert json.loads(entries["memory.json"]) == route.json()
    assert json.loads(entries["agents/analyst/memory.json"])["facts"][0]["content"] == "Totals first"
    # The export read the person's own memory, whoever the route's caller was.
    assert asked[1:] == [(A_ID, None), (A_ID, "analyst")]


@pytest.mark.anyio
async def test_a_memory_backend_that_keeps_no_document_gives_none(monkeypatch) -> None:
    class _Minimal:
        def get_memory(self, *, user_id, agent_name=None):
            raise NotImplementedError

    monkeypatch.setattr(memory_router, "get_memory_manager", lambda: _Minimal())

    assert await account_export.memory_export_document(A_ID) is None


@pytest.mark.anyio
async def test_agents_are_left_out_where_the_feature_is_off_and_memory_where_the_backend_keeps_none(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])

    entries = _archive(await deployment.export(PERSON_A))

    assert "agents.json" not in entries and "memory.json" not in entries
    readme = entries["README.md"].decode()
    assert "custom agents are turned off" in readme and "keeps no memory document" in readme


def _agents_on_files(monkeypatch, paths: Paths) -> None:
    monkeypatch.setattr(file_agent_store._ac, "get_paths", lambda: paths)
    monkeypatch.setattr(account_export, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    monkeypatch.setattr(account_export, "get_agent_store", file_agent_store.FileAgentStore)


def _agent(root: Path, name: str, config: str, soul: str | None = None) -> None:
    (root / name).mkdir(parents=True)
    (root / name / "config.yaml").write_text(config, encoding="utf-8")
    if soul is not None:
        (root / name / "SOUL.md").write_text(soul, encoding="utf-8")


def test_the_agents_a_person_made_are_theirs_alone_with_their_soul_and_no_github_setting(tmp_path, monkeypatch, caplog) -> None:
    caplog.set_level(logging.DEBUG)
    paths = Paths(tmp_path)
    _agents_on_files(monkeypatch, paths)
    _agent(paths.user_agents_dir(A_ID), "analyst", f"name: analyst\ndescription: Reads the numbers\ngithub:\n  installation_id: 42\n  bot_login: {SENTINEL}\n", soul="You read spreadsheets.")
    _agent(paths.user_agents_dir(A_ID), "broken", "name: [unclosed\n")
    _agent(paths.agents_dir, "legacy", "name: legacy\n")
    _agent(paths.user_agents_dir(B_ID), "bens", "name: bens\n")

    documents = account_export.owned_agent_documents(A_ID)

    assert [document["name"] for document in documents] == ["analyst"]
    assert documents[0]["soul"] == "You read spreadsheets."
    assert SENTINEL not in json.dumps(documents)
    # The unreadable agent is counted, never named.
    logged = "\n".join(record.getMessage() + (record.exc_text or "") for record in caplog.records)
    assert "broken" not in logged and str(tmp_path) not in logged
    monkeypatch.setattr(account_export, "get_agents_api_config", lambda: SimpleNamespace(enabled=False))
    assert account_export.owned_agent_documents(A_ID) is None


@pytest.mark.anyio
async def test_the_readme_says_what_is_where_and_what_was_left_out(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    deployment.file(PERSON_A, "a-thread-1", "outputs", "bad:name.txt", b"windows cannot hold this name")

    readme = _archive(await deployment.export(PERSON_A))["README.md"].decode()

    assert "2 conversations and 6 of your files" in readme
    # Whose it is, in the words the page's dialog uses.
    assert "It includes nothing from anyone else, and none of your saved passwords, keys or connection settings." in readme
    assert "- August numbers: 2026-09-20, 2 messages, in `conversations/a-thread-1/`" in readme
    assert "- Hiring plan: 2026-09-20, 2 messages, in `conversations/a-thread-2/`" in readme
    assert "conversations/a-thread-1/files/outputs/bad:name.txt: a name some computers cannot hold" in readme
    assert "`memory.json`" in readme and "`scheduled-tasks.json`" in readme and "`agents.json`" in readme
    assert B_MARKER not in readme


@pytest.mark.anyio
async def test_a_title_cannot_write_markup_into_the_readme(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "[click](https://example.com)\n# Heading", [("human", "hi")])

    readme = _archive(await deployment.export(PERSON_A))["README.md"].decode()

    assert "- \\[click\\](https://example.com) \\# Heading: " in readme


@pytest.mark.anyio
async def test_files_of_a_conversation_no_longer_listed_are_exported_and_named(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])
    deployment.file(PERSON_A, "gone-thread", "outputs", "left.txt", b"still mine")

    job = await deployment.export(PERSON_A)

    entries = _archive(job)
    assert entries["conversations/gone-thread/files/outputs/left.txt"] == b"still mine"
    assert _manifest(job)["folders_without_conversation"] == ["gone-thread"]
    assert "`conversations/gone-thread/`" in entries["README.md"].decode()


# ── Only this person's ──────────────────────────────────────────────────


@pytest.mark.anyio
async def test_nothing_of_another_person_is_in_the_archive(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    job = await deployment.export(PERSON_A)

    # Transcripts, memory and the manifest are compressed: look inside each.
    assert _anywhere(_archive(job), B_MARKER) == []
    for part in job.parts:
        assert B_MARKER.encode() not in part.read_bytes(), part.name


@pytest.mark.anyio
async def test_the_archive_holds_no_credential_and_no_process_state(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    _agents_on_files(monkeypatch, deployment.paths)
    monkeypatch.setattr(account_export, "owned_agent_documents", _REAL_OWNED_AGENT_DOCUMENTS)
    _agent(deployment.paths.user_agents_dir(A_ID), "analyst", f"name: analyst\ngithub:\n  installation_id: 42\n  bot_login: {SENTINEL}\n")
    # An MCP server's own temporary files, the tool-result spill (default and
    # configured) and the browser frames sit in the person's directories.
    deployment.file(PERSON_A, "a-thread-1", "workspace", f"{MCP_INTERNAL_DIRNAME}/tmp/token", SENTINEL.encode())
    deployment.file(PERSON_A, "a-thread-1", "outputs", f"{SPILL_DIR}/call-1.txt", SENTINEL.encode())
    deployment.file(PERSON_A, "a-thread-1", "outputs", f"{TOOL_RESULTS_DIRNAME}/call-2.txt", SENTINEL.encode())
    deployment.file(PERSON_A, "a-thread-1", "outputs", f"{BROWSER_FRAMES_DIRNAME}/frame.png", SENTINEL.encode())
    integration = deployment.paths.user_dir(A_ID) / "integrations" / "token.json"
    integration.parent.mkdir(parents=True)
    integration.write_text(SENTINEL, encoding="utf-8")

    job = await deployment.export(PERSON_A)

    assert job.state == "ready", job.error
    entries = _archive(job)
    assert "agents.json" in entries and "conversations/a-thread-1/files/outputs/report.pdf" in entries
    assert _anywhere(entries, SENTINEL) == []
    assert not any(name in entry for entry in entries for name in (MCP_INTERNAL_DIRNAME, SPILL_DIR, TOOL_RESULTS_DIRNAME, BROWSER_FRAMES_DIRNAME))


@pytest.mark.anyio
async def test_links_hard_links_pipes_and_unsafe_names_are_left_out_and_named(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])
    deployment.file(PERSON_A, "a-thread-1", "outputs", "fine.txt", b"fine")
    outputs = deployment.paths.sandbox_outputs_dir("a-thread-1", user_id=A_ID)
    os.symlink(deployment.own_file(PERSON_B, "secret.txt", B_MARKER.encode()), outputs / "link.txt")
    (outputs / "bad:name.txt").write_bytes(b"windows cannot hold this name")
    os.link(deployment.file(PERSON_A, "a-thread-1", "workspace", "one.txt", b"linked"), outputs / "two.txt")
    os.mkfifo(outputs / "pipe")

    job = await deployment.export(PERSON_A)

    entries = _archive(job)
    assert "conversations/a-thread-1/files/outputs/fine.txt" in entries
    assert _skipped(job) == {
        "conversations/a-thread-1/files/outputs/bad:name.txt": "unsafe_name",
        "conversations/a-thread-1/files/outputs/link.txt": "link",
        "conversations/a-thread-1/files/outputs/pipe": "not_a_file",
        "conversations/a-thread-1/files/outputs/two.txt": "hard_link",
        "conversations/a-thread-1/files/workspace/one.txt": "hard_link",
    }
    assert _anywhere(entries, B_MARKER) == []


@pytest.mark.anyio
async def test_two_names_one_case_insensitive_disk_would_merge_keep_one(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])
    deployment.file(PERSON_A, "a-thread-1", "outputs", "Report.txt", b"upper")
    deployment.file(PERSON_A, "a-thread-1", "outputs", "report.txt", b"lower")

    job = await deployment.export(PERSON_A)

    assert len([name for name in _archive(job) if name.lower().endswith("/report.txt")]) == 1
    assert list(_skipped(job).values()) == ["name_collision"]


@pytest.mark.anyio
async def test_a_linked_area_or_user_data_directory_is_not_followed(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    b_user_data = deployment.paths.thread_dir("b-thread-1", user_id=B_ID) / "user-data"
    await deployment.conversation(PERSON_A, "a-thread-3", "Linked", [("human", "hi")])
    a_thread = deployment.paths.thread_dir("a-thread-3", user_id=A_ID)
    a_thread.mkdir(parents=True, exist_ok=True)
    os.symlink(b_user_data, a_thread / "user-data")
    os.symlink(b_user_data / "uploads", deployment.paths.thread_dir("a-thread-2", user_id=A_ID) / "user-data" / "uploads")

    job = await deployment.export(PERSON_A)

    assert _anywhere(_archive(job), B_MARKER) == []
    assert _skipped(job)["conversations/a-thread-2/files/uploads"] == "link"
    assert _skipped(job)["conversations/a-thread-3/files"] == "link"


@pytest.mark.anyio
async def test_a_linked_conversation_folder_is_not_followed_and_is_named(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    threads = deployment.paths.user_dir(A_ID) / "threads"
    os.symlink(deployment.paths.thread_dir("b-thread-1", user_id=B_ID), threads / "b-thread-1")

    job = await deployment.export(PERSON_A)

    assert _anywhere(_archive(job), B_MARKER) == []
    assert _skipped(job) == {"conversations/b-thread-1": "link"}


# ── What changes while it runs ──────────────────────────────────────────


@pytest.mark.anyio
async def test_a_file_that_changes_while_copied_is_left_out_of_the_archive_not_only_the_manifest(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    def _directory_replaced(archive, path, entry, *, components, **kwargs):
        if path.name == "report.pdf":
            # Its directory looks swapped once the bytes were written.
            (directory, device, inode), *rest = reversed(components)
            components = (*reversed(rest), (directory, device, inode + 1))
        return copy_file(archive, path, entry, components=components, **kwargs)

    monkeypatch.setattr(account_export, "copy_file", _directory_replaced)

    job = await deployment.export(PERSON_A)

    assert job.state == "ready", job.error
    entries = _archive(job)
    assert "conversations/a-thread-1/files/outputs/report.pdf" not in entries
    assert entries["conversations/a-thread-1/files/uploads/august.xlsx"] == b"ana's spreadsheet"
    assert _skipped(job) == {"conversations/a-thread-1/files/outputs/report.pdf": "changed"}


@pytest.mark.parametrize(
    ("parts", "unsafe"),
    [
        (["report.pdf"], False),
        (["drafts", "zero\u200dwidth joiner.md"], False),
        (["bell\x07.txt"], True),
        (["right-to-left\u202e.txt"], True),
        (["name."], True),
        (["name "], True),
        (["CON.txt"], True),
        (["a|b.txt"], True),
        (["tools.skill", "SKILL.md"], True),
        ([".browser-frames", "frame.png"], True),
        ([".artifact-edit-123"], True),
        (["..", "escape.txt"], True),
    ],
)
def test_which_names_an_archive_cannot_hold(parts: list[str], unsafe: bool) -> None:
    from app.gateway.artifact_archive import reserved_dir_names, unsafe_entry_parts

    assert unsafe_entry_parts(parts, reserved_dir_names()) is unsafe


def test_a_copy_that_fails_takes_its_entry_back(tmp_path) -> None:
    source = tmp_path / "file.bin"
    source.write_bytes(b"y" * 5000)
    found = os.lstat(source)
    part = tmp_path / "part.zip"
    with zipfile.ZipFile(part, "w", allowZip64=True) as archive:
        archive.writestr("before.txt", b"kept")
        with pytest.raises(ArtifactArchiveError):
            copy_file(archive, source, "changed.bin", identity=(found.st_dev, found.st_ino + 1), components=())
        with pytest.raises(ArtifactArchiveError):
            copy_file(archive, source, "moved.bin", identity=(found.st_dev, found.st_ino), components=((tmp_path, 0, 0),))
        copy_file(archive, source, "after.bin", identity=(found.st_dev, found.st_ino), components=())

    with zipfile.ZipFile(part) as archive:
        assert archive.testzip() is None
        assert archive.namelist() == ["before.txt", "after.bin"]
        assert archive.read("after.bin") == b"y" * 5000
    assert part.stat().st_size < 2 * 5000 + 1000


@pytest.mark.anyio
async def test_a_directory_swapped_for_a_link_after_planning_is_named_changed(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    outputs = deployment.paths.sandbox_outputs_dir("a-thread-1", user_id=A_ID)
    real_plan = deployment.service._plan

    def _plan_then_swap(user_id, cancel):
        plan = real_plan(user_id, cancel)
        moved = outputs.with_name("outputs-moved")
        outputs.rename(moved)
        os.symlink(moved, outputs)
        return plan

    monkeypatch.setattr(deployment.service, "_plan", _plan_then_swap)

    job = await deployment.export(PERSON_A)

    assert "conversations/a-thread-1/files/outputs/report.pdf" not in _archive(job)
    assert _skipped(job)["conversations/a-thread-1/files/outputs/report.pdf"] == "changed"


@pytest.mark.anyio
async def test_a_file_deleted_while_it_runs_is_named_and_the_rest_exports(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    real_plan = deployment.service._plan

    def _plan_then_delete(user_id, cancel):
        plan = real_plan(user_id, cancel)
        deployment.paths.sandbox_outputs_dir("a-thread-1", user_id=A_ID).joinpath("report.pdf").unlink()
        return plan

    monkeypatch.setattr(deployment.service, "_plan", _plan_then_delete)

    job = await deployment.export(PERSON_A)

    assert job.state == "ready"
    assert _skipped(job) == {"conversations/a-thread-1/files/outputs/report.pdf": "vanished"}


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads every directory")
@pytest.mark.anyio
async def test_a_directory_that_cannot_be_read_is_named_and_the_rest_exports(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    locked = deployment.file(PERSON_A, "a-thread-2", "workspace", "locked/inside.txt", b"x").parent
    locked.chmod(0)
    try:
        job = await deployment.export(PERSON_A)
    finally:
        locked.chmod(stat.S_IRWXU)

    assert job.state == "ready", job.error
    assert _skipped(job) == {"conversations/a-thread-2/files/workspace/locked": "unreadable"}
    assert "conversations/a-thread-2/files/workspace/scratch/notes.txt" in _archive(job)


@pytest.mark.anyio
async def test_a_conversation_that_cannot_be_read_is_named_and_the_rest_exports(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    real_read = transcript.read_conversation

    async def _one_fails(scope, thread_id, record, *, user_id):
        if thread_id == "a-thread-2":
            raise RuntimeError("the checkpoint could not be read")
        return await real_read(scope, thread_id, record, user_id=user_id)

    monkeypatch.setattr(transcript, "read_conversation", _one_fails)

    job = await deployment.export(PERSON_A)

    assert job.state == "ready"
    manifest = _manifest(job)
    unread = next(conversation for conversation in manifest["conversations"] if conversation["id"] == "a-thread-2")
    assert unread == {"id": "a-thread-2", "title": "Hiring plan", "created_at": CREATED_AT, "updated_at": CREATED_AT, "message_count": None, "transcripts": []}
    assert _skipped(job) == {"conversations/a-thread-2/transcript": "unreadable"}
    assert "conversations/a-thread-2/files/workspace/scratch/notes.txt" in _archive(job)
    assert "conversations/a-thread-1/transcript.md" in _archive(job)


# ── Size ─────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_a_file_over_the_per_run_archive_limit_exports(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "Big", [("human", "hi")])
    big = deployment.file(PERSON_A, "a-thread-1", "outputs", "big.bin", b"")
    with big.open("r+b") as handle:
        handle.truncate(101 * 1024 * 1024)
        handle.seek(-4, os.SEEK_END)
        handle.write(b"tail")

    job = await deployment.export(PERSON_A)

    assert job.state == "ready", job.error
    with zipfile.ZipFile(job.parts[0]) as archive:
        info = archive.getinfo("conversations/a-thread-1/files/outputs/big.bin")
        assert info.file_size == 101 * 1024 * 1024
        with archive.open(info) as handle:
            handle.seek(info.file_size - 4)
            assert handle.read() == b"tail"


@pytest.mark.anyio
async def test_a_file_past_the_zip64_limit_exports(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])
    deployment.file(PERSON_A, "a-thread-1", "outputs", "big.bin", b"q" * 50_000)
    monkeypatch.setattr(zipfile, "ZIP64_LIMIT", 20_000)

    job = await deployment.export(PERSON_A)

    assert job.state == "ready", job.error
    assert _archive(job)["conversations/a-thread-1/files/outputs/big.bin"] == b"q" * 50_000


@pytest.mark.anyio
async def test_past_the_ceiling_the_export_splits_into_parts_each_file_whole(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch, _config(part_bytes=3000))
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])
    for index in range(5):
        deployment.file(PERSON_A, "a-thread-1", "outputs", f"file-{index}.bin", bytes([index]) * 1000)
    deployment.file(PERSON_A, "a-thread-1", "outputs", "larger-than-a-part.bin", b"z" * 5000)

    job = await deployment.export(PERSON_A)

    assert job.state == "ready" and len(job.parts) > 2
    entries = _archive(job)
    manifest = json.loads(entries["manifest.json"])
    with zipfile.ZipFile(job.parts[-1]) as last:
        assert "manifest.json" in last.namelist()
    for item in manifest["files"]:
        with zipfile.ZipFile(job.parts[item["part"] - 1]) as archive:
            assert hashlib.sha256(archive.read(item["path"])).hexdigest() == item["sha256"]
    assert manifest["parts"] == len(job.parts)
    assert entries["conversations/a-thread-1/files/outputs/larger-than-a-part.bin"] == b"z" * 5000


def test_parts_fill_to_the_ceiling_exactly_and_count_from_zero_again(tmp_path) -> None:
    parts = account_export._Parts(tmp_path, 100)
    assert [parts.add_bytes(name, b"x" * size)["part"] for name, size in [("a", 60), ("b", 40), ("c", 60), ("d", 30), ("e", 20)]] == [1, 1, 2, 2, 3]
    parts.close()


def test_the_manifest_that_overflows_names_the_part_it_opened(tmp_path) -> None:
    parts = account_export._Parts(tmp_path, 100)
    parts.add_bytes("a", b"x" * 90)
    parts.add_last("manifest.json", lambda count: json.dumps({"parts": count}).encode())
    parts.close()

    assert len(parts.paths) == 2
    with zipfile.ZipFile(parts.paths[-1]) as last:
        assert json.loads(last.read("manifest.json")) == {"parts": 2}


@pytest.mark.anyio
async def test_a_nearly_full_disk_refuses_before_writing_anything_and_leaves_nothing(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch, _config(min_free_bytes=2**20))
    await _two_people(deployment)
    monkeypatch.setattr(account_export.shutil, "disk_usage", _disk(free=2**20 + 10))
    opened: list[int] = []
    real_room = account_export._Parts._room
    monkeypatch.setattr(account_export._Parts, "_room", lambda self, size: (opened.append(size), real_room(self, size))[1])

    job = await deployment.export(PERSON_A)

    # Ten bytes above the floor: the export's own size is what does not fit.
    assert job.state == "failed" and job.error == "no_space"
    assert opened == [] and deployment.left() == []


@pytest.mark.anyio
async def test_what_other_exports_still_have_to_write_counts_against_the_free_space(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    other = account_export.ExportJob(user_id=B_ID, directory=tmp_path / "other", started_at=datetime.now(UTC))
    other.progress.bytes_total, other.progress.bytes_done = 10_000, 1_000
    deployment.service._jobs[B_ID] = other
    monkeypatch.setattr(account_export.shutil, "disk_usage", _disk(free=9_000 + 50))

    job = await deployment.export(PERSON_A)

    assert job.error == "no_space"


@pytest.mark.anyio
async def test_a_file_larger_than_the_disk_is_left_out_and_the_rest_exports(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    sparse = deployment.file(PERSON_A, "a-thread-1", "outputs", "sparse.img", b"")
    with sparse.open("r+b") as handle:
        handle.truncate(2**20)
    monkeypatch.setattr(account_export.shutil, "disk_usage", _disk(free=2**19, total=2**19))

    job = await deployment.export(PERSON_A)

    assert job.state == "ready", job.error
    assert _skipped(job) == {"conversations/a-thread-1/files/outputs/sparse.img": "too_large"}
    assert "conversations/a-thread-1/files/outputs/report.pdf" in _archive(job)


@pytest.mark.anyio
async def test_a_disk_that_fills_while_it_runs_stops_the_export_and_leaves_nothing(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch, _config(min_free_bytes=2**20))
    await _two_people(deployment)
    real_add = account_export._Parts.add_file
    copied: list[str] = []

    def _add_then_fill(self, found, cancel):
        copied.append(found.entry)
        monkeypatch.setattr(account_export.shutil, "disk_usage", _disk(free=2**19))
        return real_add(self, found, cancel)

    monkeypatch.setattr(account_export.shutil, "disk_usage", _disk(free=2**30))
    monkeypatch.setattr(account_export._Parts, "add_file", _add_then_fill)

    job = await deployment.export(PERSON_A)

    assert job.state == "failed" and job.error == "no_space" and len(copied) == 1
    assert deployment.left() == []


@pytest.mark.anyio
async def test_a_disk_that_fills_during_a_copy_stops_the_export(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    def _full(archive, path, entry, **kwargs):
        raise ArtifactArchiveError("not available", code="artifact_changed") from OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(account_export, "copy_file", _full)

    job = await deployment.export(PERSON_A)

    assert job.state == "failed" and job.error == "no_space"
    assert deployment.left() == []


# ── One at a time, and gone afterwards ──────────────────────────────────


@pytest.mark.anyio
async def test_asking_again_while_it_builds_returns_the_one_in_progress(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    first = deployment.service.start(PERSON_A)
    second = deployment.service.start(PERSON_A)
    await first.task

    assert second is first
    assert deployment.service.start(PERSON_A) is first


@pytest.mark.anyio
async def test_the_deployment_prepares_only_so_many_at_once(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch, _config(max_concurrent=1))
    await _two_people(deployment)

    first = deployment.service.start(PERSON_A)
    with pytest.raises(account_export.ExportBusy):
        deployment.service.start(PERSON_B)
    await first.task

    other = deployment.service.start(PERSON_B)
    await other.task
    assert other.state == "ready"


def _download(service: account_export.AccountExportService, number: int, *, complete: bool = True) -> None:
    download = service.open_part(A_ID, number)
    assert download is not None
    service.close_part(download, complete=complete)


@pytest.mark.anyio
async def test_a_downloaded_export_can_be_downloaded_again_for_a_while_then_goes(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch, _config(part_bytes=3000, expires_after_seconds=3600))
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])
    for index in range(4):
        deployment.file(PERSON_A, "a-thread-1", "outputs", f"file-{index}.bin", bytes([index]) * 1500)
    job = await deployment.export(PERSON_A)
    parts = list(job.parts)
    assert len(parts) >= 2
    loop = asyncio.get_running_loop()

    _download(deployment.service, 1)

    # A part that went out may not have arrived whole: it stays, and goes out again.
    assert parts[0].exists() and job.state == "ready"
    assert 3590 < job.expiry.when() - loop.time() <= 3600
    _download(deployment.service, 1)
    for number in range(2, len(parts) + 1):
        _download(deployment.service, number)
    assert job.state == "downloaded" and all(part["downloaded"] for part in job.document()["parts"])
    # Once everything went out, it waits only a little longer.
    assert account_export.REDOWNLOAD_SECONDS - 10 < job.expiry.when() - loop.time() <= account_export.REDOWNLOAD_SECONDS
    _download(deployment.service, 1)
    assert all(part.exists() for part in parts)

    deployment.service._expire(job)
    await deployment.service.settled()

    assert deployment.service.status(A_ID) is None
    assert deployment.left() == []


@pytest.mark.anyio
async def test_an_export_nobody_downloads_is_deleted_when_it_expires(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch, _config(expires_after_seconds=600))
    await _two_people(deployment)
    job = await deployment.export(PERSON_A)
    loop = asyncio.get_running_loop()

    assert job.expiry is not None and 590 < job.expiry.when() - loop.time() <= 600
    assert job.document()["expires_at"] == job.expires_at.isoformat()
    deployment.service._expire(job)
    await deployment.service.settled()

    assert deployment.service.status(A_ID) is None
    assert deployment.left() == []


@pytest.mark.anyio
async def test_it_never_expires_while_a_part_downloads_and_waits_again_after(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch, _config(part_bytes=3000, expires_after_seconds=600))
    await deployment.conversation(PERSON_A, "a-thread-1", "One", [("human", "hi")])
    for index in range(4):
        deployment.file(PERSON_A, "a-thread-1", "outputs", f"file-{index}.bin", bytes([index]) * 1500)
    job = await deployment.export(PERSON_A)
    first_deadline = job.expires_at

    download = deployment.service.open_part(A_ID, 1)
    assert job.expiry is None and job.document()["expires_at"] is None
    deployment.service._expire(job)
    assert deployment.service.status(A_ID) is job

    deployment.service.close_part(download, complete=True)
    assert job.expiry is not None and job.expires_at >= first_deadline


@pytest.mark.anyio
async def test_a_download_of_an_export_that_was_replaced_touches_nothing_of_the_new_one(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    first = await deployment.export(PERSON_A)
    download = deployment.service.open_part(A_ID, 1)
    deployment.service.discard(A_ID)
    second = await deployment.export(PERSON_A)

    deployment.service.close_part(download, complete=True)
    await deployment.service.settled()

    assert second is not first and second.sent == set() and second.parts[0].exists()
    # Nor does it bring the old one's clock back.
    assert first.expiry is None or first.expiry.cancelled()


@pytest.mark.anyio
async def test_a_failed_export_s_record_expires(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    monkeypatch.setattr(account_export.shutil, "disk_usage", _disk(free=0))

    job = await deployment.export(PERSON_A)

    assert job.error == "no_space" and job.expiry is not None
    assert job.document()["error"] == {"code": "no_space", "detail": "there is not enough free space to prepare the export; ask your administrator"}
    deployment.service._expire(job)
    assert deployment.service.status(A_ID) is None


@pytest.mark.anyio
async def test_discarding_while_it_builds_stops_it_and_leaves_nothing(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    reached = asyncio.Event()
    release = asyncio.Event()
    real_read = transcript.read_conversation
    reads: list[str] = []

    async def _slow(scope, thread_id, record, *, user_id):
        reads.append(thread_id)
        reached.set()
        await release.wait()
        return await real_read(scope, thread_id, record, user_id=user_id)

    monkeypatch.setattr(transcript, "read_conversation", _slow)
    job = deployment.service.start(PERSON_A)
    await asyncio.wait_for(reached.wait(), 5)
    deployment.service.discard(A_ID)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(job.task, 5)
    await deployment.service.settled()

    assert len(reads) == 1
    assert deployment.service.status(A_ID) is None
    assert deployment.left() == []


@pytest.mark.anyio
async def test_discarding_mid_copy_stops_the_copy_before_its_files_are_removed(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    copying = threading.Event()
    stopped: list[bool] = []

    def _slow_copy(archive, path, entry, *, cancel_event, **kwargs):
        copying.set()
        stopped.append(cancel_event.wait(5))
        return copy_file(archive, path, entry, cancel_event=cancel_event, **kwargs)

    monkeypatch.setattr(account_export, "copy_file", _slow_copy)
    job = deployment.service.start(PERSON_A)
    await asyncio.to_thread(copying.wait, 5)
    deployment.service.discard(A_ID)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(job.task, 5)
    await deployment.service.settled()

    # The worker thread saw the discard and had stopped before the files went.
    assert stopped == [True]
    assert deployment.left() == []


@pytest.mark.anyio
async def test_the_prepared_files_are_private(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    old = os.umask(0)
    try:
        job = await deployment.export(PERSON_A)
    finally:
        os.umask(old)

    assert stat.S_IMODE(job.directory.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(part.stat().st_mode) == 0o600 for part in job.parts)


@pytest.mark.anyio
async def test_a_memory_that_cannot_be_read_is_named_and_the_rest_exports(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    async def _corrupt(user_id, *, agent_name=None):
        raise RuntimeError("the memory document is corrupt")

    monkeypatch.setattr(account_export, "memory_export_document", _corrupt)

    job = await deployment.export(PERSON_A)

    assert job.state == "ready"
    assert _skipped(job) == {"memory.json": "unreadable", "agents/analyst/memory.json": "unreadable"}
    assert "agents.json" in _archive(job)


@pytest.mark.anyio
async def test_a_disk_that_fills_on_a_small_write_is_no_space(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    real_add = account_export._Parts.add_bytes

    def _full_on_readme(self, entry, data):
        if entry == "README.md":
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_add(self, entry, data)

    monkeypatch.setattr(account_export._Parts, "add_bytes", _full_on_readme)

    job = await deployment.export(PERSON_A)

    assert job.state == "failed" and job.error == "no_space"
    assert deployment.left() == []


def test_a_gateway_that_starts_empties_what_an_earlier_one_left(tmp_path, caplog) -> None:
    caplog.set_level(logging.INFO)
    paths = Paths(tmp_path / "home")
    left = paths.exports_dir() / "abandoned"
    left.mkdir(parents=True)
    (left / "part-001.zip").write_bytes(b"left from a crash")

    account_export.AccountExportService(SimpleNamespace(state=SimpleNamespace()), paths=paths, config=AccountExportConfig)

    assert list(paths.exports_dir().iterdir()) == []
    assert "Removed 1 account exports an earlier Gateway left" in caplog.text


@pytest.mark.anyio
async def test_a_gateway_that_stops_removes_every_export(tmp_path, monkeypatch) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    await deployment.export(PERSON_A)
    building = deployment.service.start(PERSON_B)

    await deployment.service.close()

    assert building.task.done() and deployment.service.status(A_ID) is None
    assert deployment.left() == []


# ── What the logs say ───────────────────────────────────────────────────


_NEVER_LOGGED = ("August numbers", "Hiring plan", "august.xlsx", "report.pdf", "unpresented", "notes.txt", "kept.pdf", "pricing", "bad:name", "Revenue", "Lisbon", "a-thread-1")


@pytest.mark.anyio
async def test_the_logs_carry_counts_never_a_title_a_file_name_or_content(tmp_path, monkeypatch, caplog) -> None:
    caplog.set_level(logging.DEBUG)
    deployment = _Deployment(tmp_path, monkeypatch, _config(max_concurrent=1))
    await _two_people(deployment)
    deployment.file(PERSON_A, "a-thread-1", "outputs", "bad:name.txt", b"x")

    await deployment.export(PERSON_A)
    deployment.service.discard(A_ID)
    deployment.service._jobs[B_ID] = account_export.ExportJob(user_id=B_ID, directory=tmp_path / "other", started_at=datetime.now(UTC))
    with pytest.raises(account_export.ExportBusy):
        deployment.service.start(PERSON_A)
    deployment.service._jobs.pop(B_ID)
    monkeypatch.setattr(account_export.shutil, "disk_usage", _disk(free=0))
    await deployment.export(PERSON_A)

    logged = "\n".join(record.getMessage() + (record.exc_text or "") for record in caplog.records)
    for line in ("started: 2 conversations, 6 files", "ready in", "1 left out", "removed (ready)", "refused: busy", "refused while finding files: no_space"):
        assert line in logged, line
    for secret in (*_NEVER_LOGGED, str(deployment.paths.base_dir)):
        assert secret not in logged, secret


@pytest.mark.anyio
async def test_a_failure_logs_no_file_name(tmp_path, monkeypatch, caplog) -> None:
    caplog.set_level(logging.DEBUG)
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)

    def _broken(archive, path, entry, **kwargs):
        raise ValueError(f"cannot read {path}")

    monkeypatch.setattr(account_export, "copy_file", _broken)

    job = await deployment.export(PERSON_A)

    assert job.error == "failed" and deployment.left() == []
    logged = "\n".join(record.getMessage() + (record.exc_text or "") + str(record.exc_info or "") for record in caplog.records)
    assert "failed while copying files: ValueError" in logged
    for secret in (*_NEVER_LOGGED, str(deployment.paths.base_dir)):
        assert secret not in logged, secret


# ── The routes ──────────────────────────────────────────────────────────


def _routes_app(tmp_path, monkeypatch, user: User, *, auth_source: str | None = None):
    if auth_source is None:
        app = make_authed_test_app(user_factory=lambda: user)
    else:
        # Signed in some other way than an interactive session.
        app = FastAPI()

        @app.middleware("http")
        async def _signed_in(request, call_next):
            request.state.user = user
            request.state.auth_source = auth_source
            return await call_next(request)

    deployment = _Deployment(tmp_path, monkeypatch)
    app.state.account_export = deployment.service
    deployment.service._app = app
    for name in ("thread_store", "run_event_store", "run_manager", "checkpointer", "scheduled_task_repo"):
        setattr(app.state, name, getattr(deployment.app.state, name))
    app.include_router(account_export_router.router)
    return app, deployment


async def _expired(deployment: _Deployment, person: User) -> None:
    deployment.service._expire(deployment.service.status(str(person.id)))
    await deployment.service.settled()


def test_a_part_deleted_since_it_was_found_answers_404(tmp_path, monkeypatch) -> None:
    app, deployment = _routes_app(tmp_path, monkeypatch, PERSON_A)

    with TestClient(app) as client:
        client.portal.call(_two_people, deployment)
        client.post("/api/account/export")
        job = _built(client, deployment, PERSON_A)
        job.parts[0].unlink()
        response = client.get("/api/account/export/parts/1")

    assert response.status_code == 404 and job.streams == 0


def _built(client: TestClient, deployment: _Deployment, person: User) -> account_export.ExportJob:
    job = deployment.service.status(str(person.id))
    client.portal.call(lambda: asyncio.wait_for(asyncio.shield(job.task), 10))
    return job


def test_a_person_starts_follows_and_downloads_their_export(tmp_path, monkeypatch) -> None:
    app, deployment = _routes_app(tmp_path, monkeypatch, PERSON_A)
    with TestClient(app) as client:
        client.portal.call(_two_people, deployment)
        started = client.post("/api/account/export")
        assert started.status_code == 202, started.text
        _built(client, deployment, PERSON_A)
        status = client.get("/api/account/export").json()
        assert status["state"] == "ready" and status["parts"] == [{"number": 1, "size": status["parts"][0]["size"], "downloaded": False}]
        assert status["progress"]["conversations_done"] == status["progress"]["conversations_total"] == 2
        assert status["progress"]["files_done"] == status["progress"]["files_total"] == 6 and status["skipped"] == 0
        download = client.get("/api/account/export/parts/1")
        assert download.status_code == 200
        assert download.headers["content-type"] == "application/zip"
        assert download.headers["content-disposition"] == f'attachment; filename="account-export-{datetime.now(UTC):%Y-%m-%d}.zip"'
        assert download.headers["cache-control"] == "no-store"
        assert int(download.headers["content-length"]) == len(download.content) == status["parts"][0]["size"]
        with zipfile.ZipFile(io.BytesIO(download.content)) as archive:
            assert "manifest.json" in archive.namelist() and "README.md" in archive.namelist()
        after = client.get("/api/account/export")
        again = client.get("/api/account/export/parts/1")
        client.portal.call(lambda: _expired(deployment, PERSON_A))
        gone = client.get("/api/account/export")

    assert after.status_code == 200 and after.json()["state"] == "downloaded" and after.json()["parts"][0]["downloaded"] is True
    assert again.status_code == 200 and again.content == download.content
    assert gone.status_code == 404
    assert deployment.left() == []


def test_a_split_download_names_its_part_and_length(tmp_path, monkeypatch) -> None:
    app, deployment = _routes_app(tmp_path, monkeypatch, PERSON_A)
    deployment.config = _config(part_bytes=3000)
    for index in range(4):
        deployment.file(PERSON_A, "a-thread-1", "outputs", f"file-{index}.bin", bytes([index]) * 1500)

    with TestClient(app) as client:
        client.portal.call(deployment.conversation, PERSON_A, "a-thread-1", "One", [("human", "hi")])
        client.post("/api/account/export")
        count = len(_built(client, deployment, PERSON_A).parts)
        response = client.get("/api/account/export/parts/1")

    assert count >= 2
    assert response.headers["content-disposition"].endswith(f'-part-1-of-{count}.zip"')
    assert int(response.headers["content-length"]) == len(response.content)


@pytest.mark.anyio
@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
@pytest.mark.parametrize("drop", ["before_the_first_byte", "sending_a_chunk", "the_client_disconnects"])
async def test_a_dropped_download_keeps_its_part(tmp_path, monkeypatch, spec_version: str, drop: str) -> None:
    deployment = _Deployment(tmp_path, monkeypatch)
    await _two_people(deployment)
    job = await deployment.export(PERSON_A)
    monkeypatch.setattr(account_export_router, "_CHUNK_BYTES", 64)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(account_export=deployment.service)), state=SimpleNamespace(user=PERSON_A))
    response = await account_export_router.download_part(1, request)
    sent: list[str] = []

    async def _receive():
        if drop == "the_client_disconnects":
            return {"type": "http.disconnect"}
        await asyncio.Event().wait()

    async def _send(message):
        sent.append(message["type"])
        if (drop, message["type"]) in (("before_the_first_byte", "http.response.start"), ("sending_a_chunk", "http.response.body")):
            raise OSError("the connection went away")
        if drop == "the_client_disconnects" and message["type"] == "http.response.body":
            await asyncio.sleep(0)

    scope = {"type": "http", "asgi": {"spec_version": spec_version}, "method": "GET", "headers": []}
    with contextlib.suppress(OSError, ClientDisconnect, BaseExceptionGroup):
        await asyncio.wait_for(response(scope, _receive, _send), 5)

    assert job.parts[0].exists() and job.streams == 0
    # It waits for the next download again.
    assert job.expiry is not None and deployment.service.status(A_ID) is job
    assert deployment.service.open_part(A_ID, 1) is not None


def test_starting_again_returns_the_same_export_and_discarding_removes_it(tmp_path, monkeypatch) -> None:
    app, deployment = _routes_app(tmp_path, monkeypatch, PERSON_A)

    with TestClient(app) as client:
        client.portal.call(_two_people, deployment)
        first = client.post("/api/account/export").json()
        second = client.post("/api/account/export").json()
        discarded = client.delete("/api/account/export")
        gone = client.get("/api/account/export")
        client.portal.call(deployment.service.settled)

    assert first["started_at"] == second["started_at"]
    assert discarded.status_code == 204 and gone.status_code == 404
    assert deployment.left() == []


def test_a_busy_gateway_answers_429_with_retry_after(tmp_path, monkeypatch) -> None:
    app, deployment = _routes_app(tmp_path, monkeypatch, PERSON_A)
    deployment.config = _config(max_concurrent=1)
    deployment.service._jobs[B_ID] = account_export.ExportJob(user_id=B_ID, directory=tmp_path / "x", started_at=datetime.now(UTC))

    with TestClient(app) as client:
        response = client.post("/api/account/export")

    assert response.status_code == 429
    assert response.headers["retry-after"] == "60" and response.json()["code"] == "busy"


@pytest.mark.parametrize("auth_source", ["pat", "internal", "auth_disabled"])
def test_only_an_interactive_session_can_start_or_download_one(tmp_path, monkeypatch, auth_source: str) -> None:
    app, deployment = _routes_app(tmp_path, monkeypatch, ADMIN, auth_source=auth_source)

    with TestClient(app) as client:
        responses = [client.post("/api/account/export"), client.get("/api/account/export"), client.get("/api/account/export/parts/1"), client.delete("/api/account/export")]

    assert [response.status_code for response in responses] == [403, 403, 403, 403]
    assert deployment.left() == []


def test_a_person_turned_off_cannot_start_one(tmp_path, monkeypatch) -> None:
    refused = PERSON_A.model_copy(update={"disabled_at": datetime.now(UTC)})
    app, deployment = _routes_app(tmp_path, monkeypatch, refused)

    with TestClient(app) as client:
        response = client.post("/api/account/export")

    assert response.status_code == 401
    assert deployment.service.status(A_ID) is None


def test_no_route_names_a_person_and_no_token_can_reach_one() -> None:
    from app.gateway.auth.pat import is_pat_allowed_route

    for route in account_export_router.router.routes:
        assert not any(name in route.path for name in ("user", "owner", "person")), route.path
        for method in route.methods:
            assert not is_pat_allowed_route(method, route.path.replace("{number}", "1")), route.path


def test_the_administrator_cannot_download_someone_else_s_export(tmp_path, monkeypatch) -> None:
    app, deployment = _routes_app(tmp_path, monkeypatch, ADMIN)

    with TestClient(app) as client:
        client.portal.call(_two_people, deployment)
        client.portal.call(deployment.export, PERSON_A)
        status = client.get("/api/account/export")
        part = client.get("/api/account/export/parts/1")
        discarded = client.delete("/api/account/export")

    job = deployment.service.status(A_ID)
    assert status.status_code == 404 and part.status_code == 404 and discarded.status_code == 204
    assert job.state == "ready" and job.parts[0].exists()


def test_where_more_than_one_gateway_process_serves_it_is_unavailable(tmp_path, monkeypatch) -> None:
    app, _ = _routes_app(tmp_path, monkeypatch, PERSON_A)
    app.state.account_export = None

    with TestClient(app) as client:
        response = client.post("/api/account/export")

    assert response.status_code == 503
    assert account_export.runs_in_this_process(multi_gateway=True) is False
    monkeypatch.setenv("GATEWAY_WORKERS", "2")
    assert account_export.runs_in_this_process(multi_gateway=False) is False
    monkeypatch.setenv("GATEWAY_WORKERS", "1")
    assert account_export.runs_in_this_process(multi_gateway=False) is True
