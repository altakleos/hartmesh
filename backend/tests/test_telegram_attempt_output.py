"""Exact attempt output and current conversation authority at SDK delivery."""

import json

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_attempt_notices import ended
from test_telegram_bot_adapter import message
from test_telegram_work_news_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.output import canonical_text_digest


async def captured_output(e):
    """Seed projection evidence only; actual worker provenance is tested separately."""
    item = await ended(e, "attempt.no_result")
    run = e.ended_run
    text = "Verified text from this attempt."
    content = json.dumps({"type": "ai", "id": "exact-assistant", "content": text})
    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: RunEventRow.__table__.create(sync, checkfirst=True))
    async with e.db.sessions.begin() as writer:
        row = await writer.get(RunRow, run.run_id)
        row.kwargs_json = {
            "output_capture": {"version": 1},
            "output_evidence": {
                "version": 1,
                "kind": "text",
                "owner_user_id": run.user_id,
                "thread_id": run.thread_id,
                "run_id": run.run_id,
                "input_message_id": None,
                "assistant_message_id": "exact-assistant",
                "event_seq": 1,
                "text_sha256": canonical_text_digest(text),
            },
        }
        row.last_ai_message = "Unqualified cached answer must never appear."
        writer.add(
            RunEventRow(
                thread_id=run.thread_id,
                run_id=run.run_id,
                user_id=run.user_id,
                event_type="llm.ai.response",
                category="message",
                content=content,
                content_sha256=canonical_text_digest(content),
                event_metadata={"caller": "lead_agent", "content_is_json": True},
                seq=1,
            )
        )
    return item, text


@pytest.mark.asyncio
async def test_no_result_delivers_exact_receipted_output(work_delivery):
    e = work_delivery
    item, text = await captured_output(e)
    # A later assistant message in the same conversation must not replace the
    # sealed source, even if a convenience cache also points elsewhere.
    async with e.db.sessions.begin() as writer:
        content = json.dumps({"type": "ai", "id": "later-assistant", "content": "Unrelated later answer"})
        writer.add(
            RunEventRow(
                thread_id=e.ended_run.thread_id,
                run_id="later-run",
                user_id=e.ended_run.user_id,
                event_type="llm.ai.response",
                category="message",
                content=content,
                content_sha256=canonical_text_digest(content),
                event_metadata={"caller": "lead_agent", "content_is_json": True},
                seq=2,
            )
        )
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2901)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    assert text in calls[0].parameters["text"]
    assert "Unqualified cached" not in calls[0].parameters["text"]
    assert "Unrelated later" not in calls[0].parameters["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["deleted", "incarnation"])
async def test_deleted_original_conversation_keeps_notice_without_link(work_delivery, change):
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    e = work_delivery
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    item = await ended(e, "attempt.no_result")
    async with e.db.sessions.begin() as writer:
        thread = await writer.get(ThreadMetaRow, e.ended_run.thread_id)
        if change == "deleted":
            await writer.delete(thread)
        else:
            thread.incarnation = "b" * 32
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2902)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    assert "Ended without a result:" in calls[0].parameters["text"]
    assert "Open conversation" not in str(calls[0].parameters)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "content", "message_id", "digest", "owner", "run", "sequence", "capture", "unsupported", "metadata"])
async def test_unprovable_attempt_output_is_omitted(work_delivery, change):
    from copy import deepcopy

    e = work_delivery
    item, text = await captured_output(e)
    async with e.db.sessions.begin() as writer:
        run = await writer.get(RunRow, e.ended_run.run_id)
        source = await writer.scalar(select(RunEventRow).where(RunEventRow.run_id == run.run_id))
        kwargs = deepcopy(run.kwargs_json)
        if change == "missing":
            await writer.delete(source)
        elif change == "content":
            source.content = "broken JSON"
        elif change == "digest":
            source.content_sha256 = "0" * 64
        elif change == "unsupported":
            source.content = json.dumps({"type": "ai", "id": "exact-assistant", "content": text, "tool_calls": [{"name": "tool"}]})
            source.content_sha256 = canonical_text_digest(source.content)
        elif change == "metadata":
            source.event_metadata = {"caller": "subagent", "content_is_json": True}
        elif change == "capture":
            kwargs.pop("output_capture")
        else:
            field = {"message_id": "assistant_message_id", "owner": "owner_user_id", "run": "run_id", "sequence": "event_seq"}[change]
            kwargs["output_evidence"][field] = 2 if change == "sequence" else "unrelated"
        run.kwargs_json = kwargs
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2903)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and "Ended without a result:" in calls[0].parameters["text"]
    assert text not in calls[0].parameters["text"]
    assert "Unqualified cached" not in calls[0].parameters["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("ceiling", ["threads:read", "runs:read"])
async def test_optional_excerpt_and_link_respect_route_ceiling(work_delivery, monkeypatch, ceiling):
    from app.gateway import authz

    e = work_delivery
    item, text = await captured_output(e)
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    original = authz.resolve_route_permissions

    async def restricted(*args, **kwargs):
        return set(await original(*args, **kwargs)) - {ceiling}

    monkeypatch.setattr(authz, "resolve_route_permissions", restricted)
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2904)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][0]
    assert text not in call.parameters["text"]
    assert ("Open conversation" in str(call.parameters)) is (ceiling != "threads:read")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["source", "thread"])
async def test_final_snapshot_never_sends_an_excerpt_after_source_loss(work_delivery, monkeypatch, change):
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    e = work_delivery
    item, _text = await captured_output(e)
    await prepare(e, item=item)
    sources = e.channel._services.work_news_sources
    original, changed = sources.settings.get, []

    async def remove(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(ThreadMetaRow, e.ended_run.thread_id) if change == "thread" else await writer.scalar(select(RunEventRow).where(RunEventRow.run_id == e.ended_run.run_id))
                await writer.delete(row)
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", remove)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_protected_excerpt_requires_show_of_the_acknowledged_teaser(work_delivery):
    from datetime import UTC, datetime

    from _telegram_bot_test_support import ManualClock
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow, TelegramOutboxRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item, text = await captured_output(e)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2910)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, show, message_id=2910)
    message(e.channel.bot.request, "editMessageText", identity=2910)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]
    assert len(calls) == 2 and text not in calls[0].parameters["text"]
    assert text in calls[1].parameters["text"]
    assert all(call.parameters.get("protect_content", True) for call in calls)


@pytest.mark.asyncio
async def test_frozen_notice_retires_instead_of_adopting_replaced_output_receipt(work_delivery):
    from copy import deepcopy

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    item, _text = await captured_output(e)
    await prepare(e, item=item)
    async with e.db.sessions.begin() as writer:
        run = await writer.get(RunRow, e.ended_run.run_id)
        source = await writer.scalar(select(RunEventRow).where(RunEventRow.run_id == run.run_id))
        replacement = "A different, coherently replaced answer."
        source.content = json.dumps({"type": "ai", "id": "replacement-ai", "content": replacement})
        source.content_sha256 = canonical_text_digest(source.content)
        kwargs = deepcopy(run.kwargs_json)
        kwargs["output_evidence"].update(assistant_message_id="replacement-ai", text_sha256=canonical_text_digest(replacement))
        run.kwargs_json = kwargs
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert (row.state, row.skip_reason) == ("skipped", "superseded")


@pytest.mark.asyncio
async def test_unchanged_receipt_can_refresh_after_source_retention(work_delivery):
    from _telegram_bot_test_support import ManualClock

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    item, text = await captured_output(e)
    await prepare(e, item=item)
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.scalar(select(RunEventRow).where(RunEventRow.run_id == e.ended_run.run_id)))
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    message(e.channel.bot.request, identity=2912)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and text not in calls[0].parameters["text"]
    assert "Ended without a result:" in calls[0].parameters["text"]
    assert "Open conversation" in str(calls[0].parameters)
