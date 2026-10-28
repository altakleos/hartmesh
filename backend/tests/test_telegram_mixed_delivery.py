"""Real SQL mixed delivery/source fences with explicitly seeded wire evidence.

The separate worker composition tests establish actual runtime provenance.
"""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_mixed_source import mixed_environment
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.telegram_bots.human_input import present_request
from app.telegram_bots.human_input_source import HumanInputSource
from app.telegram_bots.mixed_source import prepare_mixed_files
from app.telegram_bots.model_source import ModelSource
from app.telegram_bots.producer import DeliveryEligibility
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def primary(e):
    source, guard, callbacks, basis = await mixed_environment(e)
    record = source.record
    plan = await prepare_mixed_files(record, source, http_owner=e.http, guard=guard, capacity=1024, max_bytes=1024)
    view = present_request(source.request, thread_id=record.scope.thread_id, reply_to_message_id=record.origins[0].message_id, locale="en")
    identity = await e.outbox.plan_human_input_reply(record, source, parts=list(view.parts), keyboard=list(view.keyboard), callbacks=callbacks, guard=guard, file_plan=plan)
    reader = ModelSource(read_output=e.coordinator.read_output, human_input_source=HumanInputSource(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture), outbox=e.outbox, http_owner=e.http)
    return SimpleNamespace(id=identity, reader=reader, basis=basis, request=source.request, callbacks=callbacks, guard=guard)


async def capture(e, p, index):
    async with e.outbox._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, p.id, with_for_update=True)
        row.state, row.claim_owner, row.claim_id = "sending", "root-source", "claim"
        parts = deepcopy(row.parts)
        parts[index]["state"] = "sending"
        row.parts = parts
        part_id = parts[index]["part_id"]
    return await e.outbox.capture_sending_part(link_id=e.link.id, outbox_id=p.id, part_id=part_id, claim_owner="root-source", claim_id="claim", token_revision=e.link.token_revision)


async def test_saved_mixed_media_and_question_use_distinct_fresh_source_paths(coordinator_environment):
    e = coordinator_environment
    p = await primary(e)
    media = await capture(e, p, 0)
    prepared = await p.reader.read_file_part(media)
    assert prepared.record.scope.thread_id == media.item.payload["reply_basis"]["publication"]["scope"]["thread_id"]
    assert len(prepared.files) == 1 and prepared.caption_html is None and prepared.caption_text is None
    assert await e.outbox.record_file_hashes(media, {prepared.files[0]["path"]: "1" * 64})
    async with e.outbox._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, p.id, with_for_update=True)
        parts = deepcopy(row.parts)
        parts[0].update(state="sent", message_ids=[2110])
        row.parts = parts
    question = await capture(e, p, 1)
    ui = await p.reader.read_part(question)
    assert ui.human_input is True and p.request["question"] in ui.plain_text


async def test_mixed_claim_and_media_do_not_depend_on_answered_question_currency(coordinator_environment):
    from app.telegram_bots.supervisor import TelegramSupervisor

    e = coordinator_environment
    p = await primary(e)
    item = await e.outbox.get(e.link, p.id)
    seen = []

    async def allowed(*args):
        return DeliveryEligibility.allow()

    async def answered(actual, *, part):
        seen.append(part["part_id"])
        return False

    services = SimpleNamespace(_stopping=False, _cause=None, _send_ready=True, authority=SimpleNamespace(delivery=allowed), human_inputs=SimpleNamespace(step_current=answered))
    assert (await TelegramSupervisor.eligibility(services, e.link, item, None)).decision == "allow"
    assert (await TelegramSupervisor.eligibility(services, e.link, item, item.parts[0])).decision == "allow"
    assert seen == []
    stale = await TelegramSupervisor.eligibility(services, e.link, item, item.parts[-1])
    assert (stale.decision, stale.reason) == ("skip", "superseded") and seen == [item.parts[-1]["part_id"]]
