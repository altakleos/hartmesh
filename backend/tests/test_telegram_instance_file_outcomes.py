"""File outcomes and saved retry controls come from actual file journals."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_puts import runner
from test_telegram_instance_publication import publication as publication

from deerflow.agent_instances.agent_files import AgentFileRefusal
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

pytestmark = pytest.mark.asyncio


async def outcome(e, monkeypatch, reason=None):
    from app.telegram_bots.file_outcomes import FileOutcomes

    service, origin, file, args, *rest = await runner(e, monkeypatch, text="Read the file")
    services = e.channel._services
    services.file_puts, services.authority, services.callbacks = service, e.live.authority, service.callbacks
    if reason:

        async def refused(**kwargs):
            raise AgentFileRefusal(reason)

        monkeypatch.setattr(e.file_service.agent_files, "apply", refused)
    saved = await service.put(e.link, origin, file, **args)
    return FileOutcomes(services), origin, file, saved


@pytest.mark.parametrize("reason,key,retry", [("files-full", "file.files_full", False), ("invalid-name", "file.bad_name", False), ("name-collision", "file.not_saved", True), ("file-too-large", "file.too_large_in", False)])
async def test_journal_refusal_selects_fixed_notice_and_exact_retry_target(file_admission, monkeypatch, reason, key, retry):
    e = file_admission
    outcomes, origin, file, saved = await outcome(e, monkeypatch, reason)
    identity = await outcomes.plan(e.channel, e.link, origin, file)
    item = await e.channel._services.outbox.get(e.link, identity)
    assert item.parts[0]["source_ref"]["key"] == key
    assert item.parts[0]["source_ref"]["values"]["file"] == (saved.body["name"] if reason == "invalid-name" else file["filename"])
    async with e.db.sessions() as reader:
        buttons = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.target_kind == "act", TelegramCallbackRow.target_id == saved.id)))
    assert bool(buttons) is retry
    if retry:
        assert len(buttons) == 1 and buttons[0].kind == "file_retry"
        assert buttons[0].payload["act_id"] == saved.id
    assert await outcomes.plan(e.channel, e.link, origin, file) == identity
    assert e.bus.inbound_queue.empty()


async def test_normal_captioned_save_needs_no_extra_notice(file_admission, monkeypatch):
    e = file_admission
    outcomes, origin, file, saved = await outcome(e, monkeypatch)
    assert saved.state == "done"
    assert await outcomes.plan(e.channel, e.link, origin, file) is None


async def test_full_files_reply_uses_current_manage_permission(file_admission, monkeypatch):
    from app.telegram_bots.render import render_plain

    e = file_admission
    outcomes, origin, file, saved = await outcome(e, monkeypatch, "files-full")
    identity = await outcomes.plan(e.channel, e.link, origin, file)
    item = await e.channel._services.outbox.get(e.link, identity)
    source = item.parts[0]["source_ref"]
    assert source["values"]["reader_is_manager"] is False
    assert "url_button" not in item.parts[0]
    assert "delete files" in render_plain(source["key"], "en", **source["values"]).lower()
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=7))
    # A replay keeps the original immutable reply and its lack of manager action.
    assert await outcomes.plan(e.channel, e.link, origin, file) == identity


@pytest.mark.parametrize("reason", ["files-full", "file-too-large"])
@pytest.mark.parametrize("remaining", [7, 3, 5])
async def test_files_action_rechecks_manage_and_inspect_inside_actual_sdk_limiter(file_admission, monkeypatch, reason, remaining):
    import asyncio

    from test_telegram_bot_adapter import message
    from test_telegram_publication_primary import _primary

    from app.telegram_bots.producer import OutboxSender

    e = file_admission
    await e.db.links.end_pending(e.link.id, skip_reason="superseded")
    e.live.config.model_extra["telegram_bots"] = {"enabled": True, "public_base_url": "https://app.example"}
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=7))
    outcomes, origin, file, saved = await outcome(e, monkeypatch, reason)
    services = e.channel._services
    services.eligibility = e.live.authority.delivery
    services.prepare_live_owner = e.live.authority.prepare_live_owner
    identity = await outcomes.plan(e.channel, e.link, origin, file)
    item = await services.outbox.get(e.link, identity)
    assert item.parts[0]["url_button"]["page"] == "web.files"
    request = e.channel.bot.request
    message(request)

    async def revoke(seconds):
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=remaining))
        e.channel.bot.rate_limiter._short_hold_until = 0

    if remaining != 7:
        e.channel.bot.rate_limiter._sleep = revoke
        e.channel.bot.rate_limiter._short_hold_until = e.channel.bot.rate_limiter._time() + 1

    async def raw(*, link_id, **kwargs):
        return await e.channel.send_part(**kwargs)

    sender = OutboxSender(services.outbox, run_id="file-outcome-fixture", deliver_part=raw, eligibility=e.live.authority.delivery, jitter=lambda: 0)
    try:
        await sender.tick()
        await asyncio.wait_for(sender.drain(), 5)
        calls = [call for call in request.calls if call.endpoint == "sendMessage"]
        if remaining != 7:
            assert not calls and (await _primary(e.db, identity)).state != "sent"
        else:
            assert len(calls) == 1 and (await _primary(e.db, identity)).state == "sent"
            button = calls[0].parameters["reply_markup"]["inline_keyboard"][0][0]
            assert button["text"] == "Open Files" and "tab=files" in button["url"]
    finally:
        await sender.stop()


async def test_saved_rename_notice_takes_actual_write_result(file_admission, monkeypatch):
    from app.telegram_bots.file_outcomes import FileOutcomes

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, name="../input.txt")
    services = e.channel._services
    services.file_puts, services.authority, services.callbacks = service, e.live.authority, service.callbacks
    saved = await service.put(e.link, origin, file, **args)
    assert saved.state == "done" and saved.body["name"] == "telegram-911.txt"
    planner = FileOutcomes(services)
    identity = await planner.plan(e.channel, e.link, origin, file)
    item = await services.outbox.get(e.link, identity)
    assert item.parts[0]["source_ref"]["key"] == "file.saved_as"
    assert item.parts[0]["source_ref"]["values"]["file"] == saved.result["path"].rsplit("/", 1)[-1]


async def test_final_outcome_writer_rolls_back_reply_and_retry_when_use_changes(file_admission, monkeypatch):
    from app.telegram_bots import file_puts
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = file_admission
    outcomes, origin, file, saved = await outcome(e, monkeypatch, "name-collision")
    original, changed = file_puts._FileReceiptGuard.check, []

    async def revoke_after_callback(guard, writer):
        exists = await writer.scalar(select(TelegramCallbackRow.id).where(TelegramCallbackRow.target_id == saved.id))
        if exists and not changed:
            changed.append(True)
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return await original(guard, writer)

    monkeypatch.setattr(file_puts._FileReceiptGuard, "check", revoke_after_callback)
    from deerflow.agent_instances.contract import AgentDenied

    with pytest.raises(AgentDenied):
        await outcomes.plan(e.channel, e.link, origin, file)
    assert changed
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.target_id == saved.id)))
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-put:%"))))


async def test_duplicate_notice_requires_actual_deduplicated_write(file_admission, monkeypatch):
    import io
    from uuid import uuid4

    from app.telegram_bots.instance_file_authority import prepare_file_write

    e = file_admission
    access = await prepare_file_write(e.live.authority, e.link, current=lambda: e.active[0])
    with io.BytesIO(b"data") as content:
        await access.apply(operation_id=uuid4().hex, name="input.txt", content=content, size=4)
    outcomes, origin, file, saved = await outcome(e, monkeypatch)
    assert saved.result["deduplicated"] is True
    identity = await outcomes.plan(e.channel, e.link, origin, file)
    item = await e.channel._services.outbox.get(e.link, identity)
    assert item.parts[0]["source_ref"]["key"] == "file.same"
    assert item.parts[0]["source_ref"]["values"]["name"] == "Fixture employee"


@pytest.mark.parametrize("failure", ["uncertain", "workflow"])
async def test_unconfirmed_write_never_claims_file_was_not_saved(file_admission, monkeypatch, failure):
    from app.telegram_bots.file_outcomes import FileOutcomes
    from app.telegram_bots.render import render_plain
    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, text="Read the file")
    services = e.channel._services
    services.file_puts, services.authority, services.callbacks = service, e.live.authority, service.callbacks
    original = e.file_service.agent_files.apply
    published = []

    async def lost_confirmation(**kwargs):
        if not published:
            published.append(await original(**kwargs))
            if failure == "workflow":
                # Reproduce a native effect whose SQL completion was lost.
                async with e.db.sessions.begin() as writer:
                    row = await writer.get(SpaceFileOperationRow, (e.home.id, kwargs["operation_id"]))
                    row.phase, row.result = "pending", None
        if failure == "workflow":
            # The actual AgentFiles boundary now observes the pending intent.
            return await original(**kwargs)
        raise RuntimeError("confirmation lost after native publication")

    monkeypatch.setattr(e.file_service.agent_files, "apply", lost_confirmation)
    if failure == "uncertain":
        with pytest.raises(RuntimeError):
            await service.put(e.link, origin, file, **args)
    saved = await service.put(e.link, origin, file, **args)
    assert saved.state == ("uncertain" if failure == "uncertain" else "refused")
    assert published[0].path == "uploads/input.txt"
    identity = await FileOutcomes(services).plan(e.channel, e.link, origin, file)
    item = await services.outbox.get(e.link, identity)
    source = item.parts[0]["source_ref"]
    assert source["key"] == ("file.unconfirmed" if failure == "uncertain" else "file.files_busy")
    assert "cannot confirm" in render_plain(source["key"], "en", **source["values"])
    async with e.db.sessions() as reader:
        buttons = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.target_kind == "act", TelegramCallbackRow.target_id == saved.id)))
    assert len(buttons) == 1 and buttons[0].kind == "file_retry"
