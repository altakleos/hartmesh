"""Complete Work previews bind controls to everything the person was shown."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.contract import AgentConflict
from deerflow.agent_instances.human_input_contract import Respond


@pytest.mark.asyncio
async def test_request_preview_reads_all_comments_and_preserves_action_basis(work_pages):
    from app.telegram_bots.split import visible_text
    from app.telegram_bots.work_requests import WorkRequestReader, present_request

    e = work_pages
    for index in range(52):
        await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text=f"Comment {index}: " + "<source> " * 35))
    reader = WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    source = await reader.read_request(e.link, e.request["id"])
    assert len(source.responses) == len(source.seen["response_ids"]) == 52
    preview = present_request(source, locale="en")
    text = "\n".join(visible_text(part["html"]) for part in preview.parts)
    assert "Comment 0:" in text and "Comment 51:" in text and "<source>" in text
    assert len(preview.parts) > 1
    assert all(not part.get("keyboard") for part in preview.parts[:-1])
    assert preview.keyboard[-1][0]["payload"]["seen"] == source.seen
    assert source.seen["work_revision"] > e.work["revision"]


@pytest.mark.asyncio
async def test_preview_refuses_a_changed_request_during_response_scan(work_pages, monkeypatch):
    from app.telegram_bots.work_requests import WorkRequestReader
    from deerflow.agent_instances.human_input import HumanInput

    e = work_pages
    original = HumanInput.responses
    changed = []

    async def race(self, **kwargs):
        page = await original(self, **kwargs)
        if not changed:
            changed.append(True)
            await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="A new comment"))
        return page

    monkeypatch.setattr(HumanInput, "responses", race)
    with pytest.raises(AgentConflict):
        await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, e.request["id"])
    assert changed


@pytest.mark.asyncio
async def test_concealed_request_has_no_source_words_or_action_controls(work_pages):
    from app.telegram_bots.split import visible_text
    from app.telegram_bots.work_requests import WorkRequestReader, present_request

    e = work_pages
    source = await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, e.request["id"])
    shown = present_request(source, locale="en", concealed=True, outbox_id=uuid4().hex)
    assert len(shown.parts) == 1 and visible_text(shown.parts[0]["html"]) == "A question is waiting."
    assert [[button["kind"] for button in row] for row in shown.keyboard] == [["reveal"]]
    assert "Which guide?" not in str(shown)


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose,kinds", [("decision", {"decide_own"}), ("review", {"accept_open", "changes_open"})])
async def test_management_preview_exposes_only_first_step_controls(work_pages, purpose, kinds):
    from sqlalchemy import func, select
    from test_telegram_work_actions import management_request

    from app.telegram_bots.work_requests import WorkRequestReader, present_request
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_pages
    record, request, seen = await management_request(e, purpose)
    async with e.db.sessions() as reader:
        attempts_before = await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow))
    source = await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, request["id"])
    preview = present_request(source, locale="en")
    assert {button["kind"] for row in preview.keyboard for button in row} == kinds
    assert source.seen == seen
    shown = "\n".join(part["visible_text"] for part in preview.parts)
    assert request["question"] in shown and request["reason"] in shown and request["expected_response"] in shown
    assert "acknowledge_unchecked_sources" not in str(preview)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == attempts_before
    assert (await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"]))["revision"] == record["revision"]


@pytest.mark.asyncio
async def test_request_preview_cannot_cross_the_bot_instance(work_pages):
    from dataclasses import replace

    from app.telegram_bots.work_requests import WorkRequestReader
    from deerflow.agent_instances.contract import AgentDenied

    e = work_pages
    with pytest.raises(AgentDenied):
        await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(replace(e.link, agent_ref=uuid4().hex), e.request["id"])


def test_work_preview_titles_fit_and_concealed_text_has_no_source_placeholders():
    from string import Formatter

    from app.telegram_bots.render import render_plain
    from app.telegram_bots.texts import catalog

    for locale in ("en", "zh-CN"):
        for key in ("ask.info", "ask.decision", "review.ready"):
            values = {name: "<source> " * 100 for _, name, _, _ in Formatter().parse(catalog(locale)[key]) if name}
            assert len(render_plain(key, locale, **values).splitlines()[0]) <= 60
        for key in ("concealed.question", "concealed.decision", "concealed.review"):
            assert not {name for _, name, _, _ in Formatter().parse(catalog(locale)[key]) if name}


@pytest.mark.asyncio
async def test_durable_request_plan_is_atomic_and_reuses_original_controls(work_pages, monkeypatch):
    from sqlalchemy import select

    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.producer import Outbox
    from app.telegram_bots.work_request_delivery import WorkRequestPlans
    from app.telegram_bots.work_requests import WorkRequestReader
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = work_pages
    reader = WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    callbacks = CallbackRepository(e.db.sessions)
    plans = WorkRequestPlans(outbox=Outbox(e.db.sessions), reader=reader, callbacks=callbacks)
    key = "ev:" + uuid4().hex
    identity = await plans.plan(e.link, request_id=e.request["id"], dedupe_key=key)
    assert await plans.plan(e.link, request_id=e.request["id"], dedupe_key=key) == identity
    async with e.db.sessions() as session:
        row = await session.get(TelegramOutboxRow, identity)
        controls = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == e.link.id)))
        assert row.class_ == "question" and row.state == "pending"
        assert row.request_id == e.request["id"] and row.work_id == e.work["id"]
        assert row.payload["work_request_basis"]["source"]["seen"]["request_revision"] == 1
        assert len(controls) == 2 and all(button.message_id is None for button in controls)
        assert all(not part.get("callback_group_id") for part in row.parts[:-1])
        assert row.parts[-1]["callback_group_id"] == controls[0].group_id

    async def fail(*args, **kwargs):
        raise RuntimeError("interrupted before controls were written")

    monkeypatch.setattr(callbacks, "write_group", fail)
    failed_key = "ev:" + uuid4().hex
    with pytest.raises(RuntimeError):
        await plans.plan(e.link, request_id=e.request["id"], dedupe_key=failed_key)
    async with e.db.sessions() as session:
        assert await session.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == failed_key)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition,phrase", [("cannot_provide", "I don't have this"), ("wrong_recipient", "I'm the wrong person")])
async def test_complete_preview_keeps_the_meaning_of_cannot_and_wrong_responses(work_pages, disposition, phrase):
    from app.telegram_bots.work_requests import WorkRequestReader, present_request

    e = work_pages
    await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, disposition=disposition, text="Please ask the editor."))
    source = await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, e.request["id"])
    shown = "\n".join(part["visible_text"] for part in present_request(source, locale="en").parts)
    assert phrase in shown and "Please ask the editor." in shown
