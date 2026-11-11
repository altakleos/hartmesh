"""Earlier host denials remain separate from a terminal question's controls."""

from copy import deepcopy

import pytest
from _telegram_human_input_test_support import source_basis
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import sent
from test_telegram_callback_commands import drain
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_exports import exports as exports
from test_telegram_instance_network_cards import network as network
from test_telegram_instance_network_cards import network_request, offer, permissions
from test_telegram_instance_publication import publication as publication
from test_telegram_network_denials import denial_basis

from app.telegram_bots.human_input_source import validate_request_basis
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest

pytestmark = pytest.mark.asyncio


def question_basis(record, *, files=False, network_card=False, hosts=("one.example",)):
    denial = denial_basis(record, hosts=hosts)
    question = source_basis(record, **({"payload": network_request()} if network_card else {}))
    offset = len(denial["messages"])
    for row in question["messages"]:
        row["seq"] += offset
    doc = deepcopy(question["projection"]["inventory"]["document"])
    doc["version"] = 2
    doc.pop("scan")
    doc["output_view"] = denial["projection"]["inventory"]["document"]["output_view"]
    doc["items"] = denial["projection"]["inventory"]["document"]["items"] + doc["items"]
    receipt = question["projection"]["output"]["output_evidence"]
    doc["output_receipt_sha256"] = output_receipt_digest(receipt)
    if files:
        doc["items"].append({"kind": "file", "path": "/mnt/user-data/outputs/result.txt", "surfaces": ["presentation_reference"], "witnesses": [{"kind": "root_snapshot", "root_epoch": doc["root_epoch"], "message_id": "tool-0"}]})
    question["projection"]["inventory"] = {
        "state": "matched",
        "document": doc,
        "reason": None,
        "evidence": build_event_inventory_evidence(receipt, status="success", inventory_event_seq=offset + 5, document=doc).model_dump(mode="json"),
    }
    question["messages"] = denial["messages"] + question["messages"]
    question["version"] = 2 if files else 1
    return question


@pytest.mark.parametrize("network_card", [False, True])
async def test_terminal_question_keeps_controls_and_delivers_denials_after_ack(network, network_card):
    e = network
    if network_card:
        await permissions(e, 7)
    e.seed = question_basis(e.record, network_card=network_card, hosts=("one.example", "two.example", "three.example", "four.example"))
    item = await offer(e)
    assert item.payload["reply_basis"]["version"] == 5
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    messages = [call.parameters for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(messages) == 1 and messages[0]["reply_markup"]["inline_keyboard"]
    for identity in (702, 703, 704):
        e.request.enqueue("sendMessage", result=sent(e, identity=identity))
    for _ in range(3):
        await drain(e)
    messages = [call.parameters for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(messages) == 4
    for part, host in zip(messages[1:], ("one.example", "two.example", "three.example"), strict=True):
        assert host in part["text"] and "reply_markup" not in part
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        controls = [row for row in await reader.scalars(select(TelegramCallbackRow)) if row.payload.get("anchor_outbox_id") == item.id]
        assert controls and all(row.consumed_at is None for row in controls)
        notices = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:network-denials:"))))
        assert len(notices) == 1 and notices[0].state == "sent"


async def test_mixed_question_delivers_file_then_controls_then_denial(network, exports):
    from app.telegram_bots.files import FileBytesBudget

    e = network
    assert exports is e
    e.services.file_bytes_budget = FileBytesBudget(50_000_000)
    e.seed = question_basis(e.record, files=True)
    item = await offer(e)
    assert item.payload["reply_basis"]["version"] == 7
    e.request.enqueue("sendDocument", result=sent(e, identity=701))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    for _ in range(3):
        await drain(e)
    calls = [call for call in e.request.calls if call.endpoint.startswith("send")]
    assert [call.endpoint for call in calls] == ["sendDocument", "sendMessage", "sendMessage"]
    assert calls[1].parameters["reply_markup"]["inline_keyboard"]
    assert "one.example" in calls[2].parameters["text"] and "reply_markup" not in calls[2].parameters
    assert e.native_reads == ["outputs/result.txt"] and e.services.file_bytes_budget.used == 0


@pytest.mark.parametrize("change", ["unissued", "other_artifact"])
async def test_question_never_discards_unqualified_or_other_structured_content(network, change):
    from deerflow.runtime.runs.output_inventory import presentation_fragment_digest

    e = network
    basis = question_basis(e.record)
    result = basis["messages"][2]
    if change == "unissued":
        result["metadata"].clear()
    else:
        result["content"]["artifact"]["other_output"] = {"value": "must remain accounted for"}
        result["metadata"]["tool_artifact_enrichment"]["added_keys"].insert(0, "other_output")
        projection = basis["projection"]
        projection["inventory"]["document"]["items"][0]["value_sha256"] = presentation_fragment_digest(result["content"]["artifact"])
        projection["inventory"]["evidence"] = build_event_inventory_evidence(projection["output"]["output_evidence"], status="success", inventory_event_seq=8, document=projection["inventory"]["document"]).model_dump(mode="json")
    with pytest.raises(ValueError, match="human-input-source-conflict"):
        validate_request_basis(e.record, basis)


async def test_answered_question_does_not_cancel_the_separate_denial_notice(network):
    e = network
    e.seed = question_basis(e.record)
    item = await offer(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    async with e.db.sessions() as reader:
        identity = await reader.scalar(select(TelegramCallbackRow.id).where(TelegramCallbackRow.kind == "clarify_option"))
    control = await e.services.callbacks.get(identity)
    await e.services.human_inputs.execute_callback(e.channel, control, update_id=1501)
    e.services.turns.accept_staged.assert_awaited_once()
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await drain(e)
    messages = [call.parameters for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(messages) == 2 and "one.example" in messages[-1]["text"]
    original = await e.services.outbox.get(e.link, item.id)
    assert original.parts[-1]["message_ids"] == [701]


async def test_question_and_notice_share_rollback_and_original_source_guard(network, monkeypatch):
    e = network
    e.seed = question_basis(e.record)
    original = e.services.outbox.inbox.seal_publication
    reached = []

    async def retire(*args, **kwargs):
        await original(*args, **kwargs)
        reached.append(True)
        e.active[0] = False

    monkeypatch.setattr(e.services.outbox.inbox, "seal_publication", retire)
    try:
        with pytest.raises(ValueError, match="human-input-source-conflict"):
            await offer(e)
        assert reached == [True]
        async with e.db.sessions() as reader:
            keys = ["reply:" + e.record.turn_id, "notice:network-denials:" + e.record.publication_id]
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.in_(keys))))
            assert not list(await reader.scalars(select(TelegramCallbackRow)))
    finally:
        e.active[0] = True


async def test_cancelled_question_writer_keeps_both_plans_owned_until_settlement(network, monkeypatch):
    import asyncio

    e = network
    e.seed = question_basis(e.record)
    original = e.services.outbox.inbox.seal_publication
    entered, release = asyncio.Event(), asyncio.Event()

    async def held(*args, **kwargs):
        await original(*args, **kwargs)
        entered.set()
        await release.wait()

    monkeypatch.setattr(e.services.outbox.inbox, "seal_publication", held)
    task = asyncio.create_task(offer(e))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        for _ in range(2):
            task.cancel()
            await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with e.db.sessions() as reader:
            keys = ["reply:" + e.record.turn_id, "notice:network-denials:" + e.record.publication_id]
            assert len(list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.in_(keys))))) == 2
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
