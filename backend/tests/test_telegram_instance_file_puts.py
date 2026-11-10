"""File journals and bounded retries over real Telegram custody and Home writes."""

import asyncio
import hashlib
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Chat, Document, Message, Update, User
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_publication import publication as publication

from app.channels.inbound_leases import InboundFileLease
from deerflow.agent_instances.agent_files import AgentFileRefusal
from deerflow.agent_instances.contract import ResidentFilesBusy
from deerflow.persistence.telegram_bots.model import TelegramActRow

pytestmark = pytest.mark.asyncio


async def admitted(e, *, name="input.txt", size=4, text="", message_id=911, topic=None, album=None):
    file = dict(type="file", file_id="input-file", file_unique_id="input-unique", filename=name, mime_type="text/plain", size=size, message_id=message_id, telegram_bot_id=e.link.bot_id)
    incoming = Update(
        2000 + message_id,
        message=Message(
            message_id,
            datetime.now(UTC),
            Chat(e.link.chat_id, "private"),
            from_user=User(e.link.telegram_user_id, "Person", False),
            caption=text or None,
            message_thread_id=topic,
            media_group_id=album,
            document=Document(file["file_id"], file["file_unique_id"], file_name=name, mime_type=file["mime_type"], file_size=size),
        ),
    )
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "media", "text": text, "files": [file], **({"media_group_id": album} if album else {})})
    row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 2, "kind": "chat_input", "forwarded": False, "origin": None}}, operation_ids=[])
    return row, file


async def runner(e, monkeypatch, *, name="input.txt", size=4, download_result=None, text=""):
    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.file_puts import FilePuts

    origin, file = await admitted(e, name=name, size=size, text=text)
    service = FilePuts(CallbackRepository(e.db.sessions), e.inbox, config=e.db.config.files)
    clock, sleeps, hints, releases, calls = [0.0], [], [], [], []

    async def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    async def progress(key):
        hints.append(key)

    async def download(descriptor, check):
        await check()
        calls.append(descriptor)
        async with e.db.sessions() as reader:
            actual = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
            assert actual.state == "pending" and actual.operation_ids
        if download_result is not None:
            return download_result

        async def release():
            releases.append(True)

        return InboundFileLease(b"data", release)

    monkeypatch.setattr(service, "_time", lambda: clock[0])
    monkeypatch.setattr(service, "_sleep", sleep)
    args = dict(authority=e.live.authority, current=lambda: e.active[0], download=download, progress=progress)
    return service, origin, file, args, sleeps, hints, releases, calls


async def test_file_journal_precedes_download_and_receipt_replays_without_bytes(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    first = await service.put(e.link, origin, file, **args)
    second = await service.put(e.link, origin, file, **args)
    assert first == second and first.state == "done" and first.attempts == 1
    assert first.id == hashlib.sha256(f"{e.link.id}:input-unique:911".encode()).hexdigest()[:32]
    assert first.body == dict(name="input.txt", size=4, sha256=hashlib.sha256(b"data").hexdigest(), path="uploads/input.txt", file_id="input-file", file_unique_id="input-unique", message_id=911)
    assert first.result == dict(path="uploads/input.txt", size=4, sha256=hashlib.sha256(b"data").hexdigest(), deduplicated=False)
    assert len(calls) == len(releases) == 1 and sleeps == [] and hints == ["file.saving"]
    assert e.bus.inbound_queue.empty()
    assert "data" not in str(dict(first.continuation))


@pytest.mark.parametrize(
    "reason,cause,expected_calls,state",
    [("file-write-failed", None, 41, "uncertain"), ("storage-busy", "workflow", 2, "refused"), ("storage-busy", "lifecycle", 1, "refused"), ("files-full", None, 1, "refused"), ("invalid-name", None, 2, "refused")],
)
async def test_only_three_closed_refusals_repeat(file_admission, monkeypatch, reason, cause, expected_calls, state):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    operations, paths = [], []

    async def refused(**kwargs):
        operations.append(kwargs["operation_id"])
        paths.append(kwargs["path"])
        raise ResidentFilesBusy("Employee", cause=cause) if cause else AgentFileRefusal(reason)

    monkeypatch.setattr(e.file_service.agent_files, "apply", refused)
    result = await service.put(e.link, origin, file, **args)
    assert result.state == state and result.reason == reason and result.attempts == expected_calls
    assert len(operations) == expected_calls and len(calls) == len(releases) == 1
    if reason == "invalid-name":
        assert paths == ["uploads/input.txt", "uploads/telegram-911.txt"]
        assert operations == [result.id, hashlib.sha256((result.id + ":rename").encode()).hexdigest()[:32]]
    else:
        assert set(operations) == {result.id}
    assert sleeps == ([3] * (expected_calls - 1) if reason != "invalid-name" else [])
    assert ("file.waiting" in hints) == (reason == "file-write-failed")


async def test_transient_write_recovers_under_same_id_to_actual_home(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    original, operations = e.file_service.agent_files.apply, []

    async def transient(**kwargs):
        operations.append(kwargs["operation_id"])
        if len(operations) == 1:
            raise AgentFileRefusal("file-write-failed")
        return await original(**kwargs)

    monkeypatch.setattr(e.file_service.agent_files, "apply", transient)
    result = await service.put(e.link, origin, file, **args)
    assert result.state == "done" and result.attempts == 2 and operations == [result.id] * 2 and sleeps == [3]
    assert len(releases) == 1


@pytest.mark.parametrize("failure,state,reason", [("file.too_large_in", "refused", "file-too-large"), ("file.not_saved", "uncertain", "download-failed")])
async def test_download_failures_store_no_bytes_and_never_write(file_admission, monkeypatch, failure, state, reason):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch, size=None, download_result=failure)

    async def forbidden(**kwargs):
        raise AssertionError("A refused download cannot write")

    monkeypatch.setattr(e.file_service.agent_files, "apply", forbidden)
    result = await service.put(e.link, origin, file, **args)
    assert (result.state, result.reason) == (state, reason) and result.result is None
    assert result.body["path"] is None and result.body["sha256"] is None
    assert len(calls) == 1 and not sleeps and not releases


async def test_forged_descriptor_never_journals_or_downloads(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    for replacement in ({"file_id": "forged"}, {"size": True}, {"filename": "another.txt"}):
        with pytest.raises(ValueError):
            await service.put(e.link, origin, {**file, **replacement}, **args)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "file_put")))


async def test_uncertain_restart_never_restarts_a_write(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    saved = await service.put(e.link, origin, file, **args)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, saved.id)
        row.state, row.result = "pending", None
        row.body = {**row.body, "path": None, "sha256": None}

    async def forbidden(*args, **kwargs):
        raise AssertionError("An interrupted file call needs an explicit press")

    args["download"] = forbidden
    result = await service.put(e.link, origin, file, **args)
    assert result.state == "uncertain" and result.reason == "interrupted"


async def test_repeated_cancellation_drains_native_write_and_lease(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    entered, finish = asyncio.Event(), asyncio.Event()
    original = e.file_service.agent_files.apply

    async def held(**kwargs):
        entered.set()
        await finish.wait()
        assert not releases and not kwargs["content"].closed
        return await original(**kwargs)

    monkeypatch.setattr(e.file_service.agent_files, "apply", held)
    task = asyncio.create_task(service.put(e.link, origin, file, **args))
    async with asyncio.timeout(10):
        await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and not releases
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert releases == [True]
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert row.state == "done"


async def retry_button(e, service, act, *, identity="file-retry", message_id=1911):
    from app.telegram_bots.instance_file_authority import prepare_file_write
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    access = await prepare_file_write(e.live.authority, e.link, current=lambda: e.active[0])
    outbox = e.channel._services.outbox
    keyboard = [[dict(label="label.send_again", kind="file_retry", target_kind="act", target_id=act.id, payload={"act_id": act.id})]]
    parent = await outbox.plan_product_keyboard(
        e.link, source_id=identity, parts=[{"kind": "text", "source_ref": {"key": "file.not_saved", "values": {"file": act.body["name"]}}}], keyboard=keyboard, callbacks=service.callbacks, guard=access.guard
    )
    async with e.db.sessions() as reader:
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent))
        callback_id, group_id = selected.id, selected.group_id
    assert await service.callbacks.adopt(e.link, group_id, message_id, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: e.active[0])
    return callback_id


async def test_explicit_press_gets_child_id_and_replay_never_downloads_again(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    original, operations = e.file_service.agent_files.apply, []

    async def lifecycle(**kwargs):
        operations.append(kwargs["operation_id"])
        raise ResidentFilesBusy("Employee", cause="lifecycle")

    monkeypatch.setattr(e.file_service.agent_files, "apply", lifecycle)
    failed = await service.put(e.link, origin, file, **args)
    callback = await retry_button(e, service, failed)

    async def applied(**kwargs):
        operations.append(kwargs["operation_id"])
        return await original(**kwargs)

    monkeypatch.setattr(e.file_service.agent_files, "apply", applied)
    saved = await service.retry(e.link, failed.id, callback_id=callback, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None, **args)
    replay = await service.retry(e.link, failed.id, callback_id=callback, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None, **args)
    assert saved == replay and saved.id == failed.id and saved.state == "done"
    assert operations == [saved.id, hashlib.sha256((saved.id + ":retry:2").encode()).hexdigest()[:32]]
    assert len(calls) == len(releases) == 2 and not sleeps
    assert (await service.callbacks.get(callback)).consumed_at is not None


@pytest.mark.parametrize("changed", ["target", "message", "used"])
async def test_explicit_retry_requires_actual_current_button(file_admission, monkeypatch, changed):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, download_result="file.not_saved")
    failed = await service.put(e.link, origin, file, **args)
    callback = await retry_button(e, service, failed)
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    async with e.db.sessions.begin() as writer:
        button = await writer.get(TelegramCallbackRow, callback)
        if changed == "target":
            button.target_id = "f" * 32
        elif changed == "used":
            button.consumed_at = datetime.now(UTC)
    with pytest.raises(ValueError):
        await service.retry(e.link, failed.id, callback_id=callback, chat_id=e.link.chat_id, message_id=1912 if changed == "message" else 1911, message_thread_id=None, **args)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramActRow, failed.id)
        assert row.operation_ids == [failed.id]


async def test_explicit_retry_after_uncertain_commit_deduplicates_actual_file(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    original = e.file_service.agent_files.apply

    async def lost_response(**kwargs):
        await original(**kwargs)
        raise RuntimeError("response was lost after the actual write")

    monkeypatch.setattr(e.file_service.agent_files, "apply", lost_response)
    with pytest.raises(RuntimeError):
        await service.put(e.link, origin, file, **args)
    assert releases == [True]
    uncertain = await service.put(e.link, origin, file, **args)
    assert uncertain.state == "uncertain" and len(calls) == 1
    callback = await retry_button(e, service, uncertain)
    monkeypatch.setattr(e.file_service.agent_files, "apply", original)
    saved = await service.retry(e.link, uncertain.id, callback_id=callback, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None, **args)
    assert saved.result["deduplicated"] is True and saved.body["path"] == "uploads/input.txt"
    assert len(calls) == len(releases) == 2
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    async with e.db.sessions() as reader:
        cleanup = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["kind"].as_string() == "keyboard_cleanup"))
        assert cleanup.parts[0]["target_message_id"] == 1911


async def test_second_press_and_old_press_replay_preserve_current_operation(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, download_result="file.not_saved")
    initial = await service.put(e.link, origin, file, **args)
    one = await retry_button(e, service, initial)
    failed = await service.retry(e.link, initial.id, callback_id=one, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None, **args)
    two = await retry_button(e, service, failed, identity="second-file-retry", message_id=1912)
    failed = await service.retry(e.link, initial.id, callback_id=two, chat_id=e.link.chat_id, message_id=1912, message_thread_id=None, **args)
    replay = await service.retry(e.link, initial.id, callback_id=one, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None, **args)
    assert failed == replay and failed.attempts == 3
    assert failed.operation_ids == (initial.id, hashlib.sha256((initial.id + ":retry:2").encode()).hexdigest()[:32], hashlib.sha256((initial.id + ":retry:3").encode()).hexdigest()[:32])


async def test_actual_invalid_name_falls_back_once_and_saves(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch, name="../input.txt")
    saved = await service.put(e.link, origin, file, **args)
    assert saved.state == "done" and saved.body["path"] == "uploads/telegram-911.txt"
    assert len(saved.operation_ids) == 2 and not sleeps and len(releases) == 1


async def test_live_duplicate_waits_for_original_result_without_second_download(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    original, entered, finish = e.file_service.agent_files.apply, asyncio.Event(), asyncio.Event()

    async def held(**kwargs):
        entered.set()
        await finish.wait()
        return await original(**kwargs)

    monkeypatch.setattr(e.file_service.agent_files, "apply", held)
    one = asyncio.create_task(service.put(e.link, origin, file, **args))
    async with asyncio.timeout(10):
        await entered.wait()
    two = asyncio.create_task(service.put(e.link, origin, file, **args))
    await asyncio.sleep(0)
    finish.set()
    first, second = await asyncio.gather(one, two)
    assert first == second and first.state == "done" and len(calls) == len(releases) == 1


@pytest.mark.parametrize("target", ["home", "home_generation", "instance_generation"])
@pytest.mark.parametrize("retry", [False, True])
async def test_saved_file_target_cannot_be_rebound_to_changed_home_or_incarnation(file_admission, monkeypatch, target, retry):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch, download_result="file.not_saved" if retry else None)
    initial = await service.put(e.link, origin, file, **args)
    callback = await retry_button(e, service, initial) if retry else None
    from sqlalchemy import update

    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.spaces.model import SpaceRow
    from deerflow.spaces.contract import Custody, MutationMode, Permission

    if target == "home":
        replacement = await e.files.create(actor=e.actor, name="Replacement Home", custody=Custody.personal(e.actor), mode=MutationMode.NATIVE)
        instance = await e.live.authority.instance_view(e.link)
        await e.files.registry.set_grant(
            actor=e.actor, space_id=replacement.id, expected_generation=replacement.generation, subject=instance.principal, permissions=Permission.READ | Permission.WRITE | Permission.EXPORT, acknowledge_existing_data=True
        )
    async with e.db.sessions.begin() as writer:
        if target == "home":
            await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(home_id=replacement.id))
        elif target == "home_generation":
            await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
        else:
            await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(generation=AgentInstanceRow.generation + 1))
    with pytest.raises(ValueError, match="file-target-changed"):
        if retry:
            await service.retry(e.link, initial.id, callback_id=callback, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None, **args)
        else:
            await service.put(e.link, origin, file, **args)
    assert len(calls) == 1
    async with e.db.sessions() as reader:
        actual = await reader.get(TelegramActRow, initial.id)
        assert actual.state == initial.state and actual.operation_ids == list(initial.operation_ids)
    if retry:
        assert (await service.callbacks.get(callback)).consumed_at is None


async def test_home_change_after_access_capture_refuses_saved_path(file_admission, monkeypatch):
    from sqlalchemy import update

    from app.telegram_bots import file_puts as module
    from deerflow.persistence.spaces.model import SpaceRow

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    await service.put(e.link, origin, file, **args)
    original = module.prepare_file_write

    async def changed(*args, **kwargs):
        access = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
        return access

    monkeypatch.setattr(module, "prepare_file_write", changed)
    from deerflow.agent_instances.contract import AgentDenied

    # The composite writer now refuses the changed Home before reading the Act.
    with pytest.raises(AgentDenied):
        await service.put(e.link, origin, file, **args)


@pytest.mark.parametrize(
    "state,malformed,deleted_link,removed", [("done", False, False, True), ("refused", False, False, True), ("done", False, True, True), ("uncertain", False, False, False), ("pending", False, False, False), ("done", True, False, False)]
)
async def test_settled_file_metadata_expires_but_uncertain_or_malformed_journal_stays(file_admission, monkeypatch, state, malformed, deleted_link, removed):
    from datetime import timedelta

    from app.telegram_bots.retention import sweep_rows

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, download_result=None if state == "done" else "file.not_saved")
    saved = await service.put(e.link, origin, file, **args)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, saved.id)
        row.created_at = datetime.now(UTC) - timedelta(days=401)
        row.state = state
        if state == "pending":
            row.reason = None
        if state == "refused":
            row.reason = "storage-busy"
        if deleted_link:
            row.link_id = None
        if malformed:
            row.continuation = {**row.continuation, "unexpected": "hold this"}
    counts = await sweep_rows(e.db.sessions, datetime.now(UTC), callback_max_age_days=180)
    assert counts["acts"] == int(removed)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, saved.id) is None) is removed
