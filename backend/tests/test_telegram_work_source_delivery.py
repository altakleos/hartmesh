"""Real SDK boundary for frozen Work previews and current source authority."""

from functools import partial
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.producer import Outbox, OutboxSender
from app.telegram_bots.supervisor import TelegramSupervisor
from app.telegram_bots.work_request_delivery import WorkRequestPlans
from app.telegram_bots.work_requests import WorkRequestReader
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest_asyncio.fixture
async def work_delivery(work_pages):
    e = work_pages
    from deerflow.persistence.spaces.model import SpaceGrantRow

    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: SpaceGrantRow.__table__.create(sync, checkfirst=True))
    services = e.channel._services
    services.authority = e.live.authority
    services.outbox = Outbox(e.db.sessions)
    services.callbacks = CallbackRepository(e.db.sessions)
    e.reader = WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    e.plans = WorkRequestPlans(outbox=services.outbox, reader=e.reader, callbacks=services.callbacks)
    host = SimpleNamespace(_stopping=False, _cause=None, _send_ready=True, human_inputs=None, outbox=services.outbox, authority=e.live.authority)
    services.eligibility = partial(TelegramSupervisor.eligibility, host)
    async with e.db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    e.identity = await e.plans.plan(e.link, request_id=e.request["id"], dedupe_key="ev:" + uuid4().hex)
    e.dispatch_errors = []

    async def deliver(**kwargs):
        try:
            return await e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"})
        except Exception as exc:
            e.dispatch_errors.append(exc)
            raise

    e.sender = OutboxSender(services.outbox, run_id="work-source", deliver_part=deliver, eligibility=services.eligibility, proactive=True)
    try:
        yield e
    finally:
        await e.sender.stop()


@pytest.mark.asyncio
async def test_frozen_work_preview_reaches_sdk_only_with_its_source_reader(work_delivery):
    from app.telegram_bots.work_request_source import WorkRequestSources

    e = work_delivery
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks)
    message(e.channel.bot.request, identity=801)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as session:
        row = await session.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent" and row.message_id == 801, (row.state, row.failed_reason, row.attempts, row.parts)
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and "Which guide?" in calls[0].parameters["text"]
    assert calls[0].parameters["reply_markup"]


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", ["request", "account", "claim", "callback", "grant", "preferences", "work_revision"])
async def test_final_work_snapshot_refuses_changes_after_source_preparation(work_delivery, monkeypatch, retire):
    from datetime import UTC, datetime

    from app.telegram_bots.work_request_source import WorkRequestSources
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramPersonSettingsRow
    from deerflow.persistence.user.model import UserRow

    e = work_delivery
    sources = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_request_sources = sources
    original = sources.settings.get
    changed = []

    async def withdraw(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            changed.append(True)
            if retire == "request":
                await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Answered on the web."))
            else:
                async with e.db.sessions.begin() as writer:
                    if retire == "account":
                        (await writer.get(UserRow, e.link.owner_user_id)).released_at = datetime.now(UTC)
                    elif retire == "claim":
                        (await writer.get(TelegramOutboxRow, e.identity)).claim_id = "foreign-claim"
                    elif retire == "callback":
                        for row in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == e.link.id)):
                            row.consumed_at, row.closed_reason = datetime.now(UTC), "superseded"
                    elif retire == "grant":
                        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                    elif retire == "work_revision":
                        from deerflow.persistence.agent_instances.work import AgentWorkRow

                        (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
                    else:
                        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
        return settings

    monkeypatch.setattr(sources.settings, "get", withdraw)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        if retire == "claim":
            assert row.claim_id == "foreign-claim"
        elif retire == "work_revision":
            assert row.state == "pending"
        else:
            assert row.state == "skipped"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "reordered", "unacknowledged", "version"])
async def test_whole_preview_is_required_before_its_last_controls(work_delivery, damage):
    from copy import deepcopy
    from dataclasses import asdict, replace

    from app.telegram_bots.producer import PermanentDeliveryError, _item
    from app.telegram_bots.work_request_source import frozen
    from app.telegram_bots.work_requests import present_request

    e = work_delivery
    source = await e.reader.read_request(e.link, e.request["id"])
    source = replace(source, request={**source.request, "question": "Complete question. " * 180, "reason": "Complete reason. " * 180})
    shown = present_request(source, locale="en-US")
    recipes = []
    for index, part in enumerate(shown.parts):
        recipe = {**part, "source_ref": {"key": "work.request.presentation", "values": {"request_id": e.request["id"], "part": index}}}
        if index == len(shown.parts) - 1:
            recipe["stored_keyboard"] = True
        recipes.append(recipe)
    parts = e.channel._services.outbox._parts(recipes, 0)
    assert len(parts) > 1
    for index, part in enumerate(parts[:-1]):
        part.update(state="sent", message_ids=[900 + index], sent_at="2026-11-11T00:00:00+00:00")
    parts[-1]["state"] = "sending"
    snapshot = asdict(source)
    snapshot["responses"] = list(snapshot["responses"])
    controls = [list(line) for line in shown.keyboard]
    basis = {"version": 1, "source": snapshot, "locale": "en-US", "concealed": False, "parts": recipes, "keyboard": controls}
    async with e.db.sessions() as session:
        original = _item(await session.get(TelegramOutboxRow, e.identity))
    item = replace(original, parts=deepcopy(parts), payload={"presentation_version": 0, "work_request_basis": basis, "product_keyboard_basis": controls})
    selected = item.parts[-1]
    captured = SimpleNamespace(link=e.link, item=item, part=selected)
    frozen(captured)
    if damage == "missing":
        item.parts.pop(0)
    elif damage == "reordered":
        item.parts.reverse()
        for ordinal, part in enumerate(item.parts):
            part["ordinal"] = ordinal
    elif damage == "unacknowledged":
        item.parts[0].update(state="pending", message_ids=[])
        item.parts[0].pop("sent_at", None)
    else:
        item.parts[0]["presentation_version"] = 1
    with pytest.raises(PermanentDeliveryError):
        frozen(captured)


@pytest.mark.asyncio
async def test_work_revision_change_preserves_intent_without_dispatching_old_controls(work_delivery):
    from app.telegram_bots.work_request_source import WorkRequestSources
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
    async with e.db.sessions() as session:
        row = await session.get(TelegramOutboxRow, e.identity)
        assert row.state == "pending" and row.parts[-1]["callback_group_id"]


@pytest.mark.asyncio
async def test_final_query_does_not_multiply_preview_payload_by_spaces_and_buttons(work_delivery):
    from dataclasses import replace

    from sqlalchemy import literal

    from app.telegram_bots.work_request_source import WorkRequestSources, _RequestProjection
    from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = work_delivery
    source = await e.reader.read_request(e.link, e.request["id"])
    second = uuid4().hex
    async with e.db.sessions.begin() as writer:
        home = await writer.get(SpaceRow, e.instance.home_id)
        values = {column.name: getattr(home, column.name) for column in SpaceRow.__table__.columns}
        writer.add(SpaceRow(**{**values, "id": second, "backing_handle": uuid4().hex}))
        await writer.flush()
        for identity in (home.id, second):
            writer.add(SpaceGrantRow(space_id=identity, principal_kind="human", subject_id=e.link.owner_user_id, permissions=1))
        saved = await writer.get(TelegramOutboxRow, e.identity)
        captured = SimpleNamespace(item=SimpleNamespace(id=saved.id), part=saved.parts[-1])
    source = replace(source, request={**source.request, "sources": [{"kind": "space_file", "space_id": identity, "path": "guide.txt"} for identity in (e.instance.home_id, second)]})
    host = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks)
    projection = _RequestProjection(host, captured, saved.payload["work_request_basis"], source, None, None, False, None)
    query = projection.extend(select(*[literal(0) for _ in range(8)]).select_from(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id))
    async with e.db.sessions() as reader:
        rows = (await reader.execute(query)).all()
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_reference_access_is_rechecked_with_the_claim_before_sdk_dispatch(work_delivery, monkeypatch):
    from test_telegram_work_actions import management_request

    from app.telegram_bots.work_request_source import WorkRequestSources
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.agent_instances.work_contract import WorkSource
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.registry import SpaceRegistry

    e = work_delivery
    e.human.agents.files.registry = SimpleNamespace(_view=SpaceRegistry._view)
    async with e.db.sessions.begin() as writer:
        writer.add(SpaceGrantRow(space_id=e.instance.home_id, principal_kind="human", subject_id=e.link.owner_user_id, permissions=1))
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    _record, request, _seen = await management_request(e, "review")
    await e.human.respond(
        actor=e.actor,
        request_id=request["id"],
        request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Review this reference.", sources=[WorkSource(space_id=e.instance.home_id, path="private-guide.txt")]),
    )
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="ev:" + uuid4().hex)
    sources = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_request_sources = sources
    original = sources.settings.get
    changed = []

    async def withdraw(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                await writer.delete(await writer.get(SpaceGrantRow, (e.instance.home_id, "human", e.link.owner_user_id)))
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", withdraw)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "skipped"


@pytest.mark.asyncio
async def test_real_sdk_finishes_every_chunk_before_exposing_request_controls(work_delivery):
    from datetime import UTC, datetime

    from test_telegram_outbox_workers import Clock

    from app.telegram_bots.work_request_source import WorkRequestSources
    from deerflow.agent_instances.human_input_contract import CreateRequest
    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    record = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Check the complete reference", success_criteria="Explain the evidence"))
    request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=record["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=1,
            expected_assignment_revision=1,
            purpose="information",
            question="Complete question. " * 180,
            reason="Complete context. " * 180,
            expected_response="The full reference",
            recipient_id=e.link.owner_user_id,
        ),
    )
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="ev:" + uuid4().hex)
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks)
    clock = Clock()
    clock.now = datetime.now(UTC)
    e.sender.outbox.clock = clock
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        count = len(row.parts)
        group = row.parts[-1]["callback_group_id"]
    assert count > 1
    for index in range(count):
        message(e.channel.bot.request, identity=901 + index)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not e.dispatch_errors, e.dispatch_errors
        calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
        assert len(calls) == index + 1
        if index < count - 1:
            assert not calls[-1].parameters.get("reply_markup")
            async with e.db.sessions() as reader:
                assert all(row.message_id is None for row in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
        clock.advance(2)
    assert calls[-1].parameters["reply_markup"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent"
        assert all(row.message_id == 900 + count for row in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
