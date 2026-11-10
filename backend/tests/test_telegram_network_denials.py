"""Verified denial trailers over actual SQL/SDK; terminal protocol seeds are explicit."""

import pytest
from _telegram_repository_test_support import db as db
from langchain_core.messages import AIMessage, ToolMessage
from sqlalchemy import select, update
from test_telegram_bot_handlers import sent
from test_telegram_callback_commands import drain
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_exports import exports as exports
from test_telegram_instance_network_cards import network as network
from test_telegram_instance_network_cards import permissions
from test_telegram_instance_publication import publication as publication
from test_telegram_publication_observation import output_projection

from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest, presentation_fragment_digest

pytestmark = pytest.mark.asyncio


def denial_basis(record, *, hosts=("one.example",), text="The answer is ready."):
    projection = output_projection(record, text=text)
    receipt = projection["output"]["output_evidence"]
    receipt["event_seq"] = len(hosts) * 3 + 3
    document = projection["inventory"]["document"]
    document["version"] = 2
    document.pop("scan")
    document["output_view"] = {"kind": "no_admitted_output_view", "scope": {"version": 1, "kind": "managed_output_routing", "virtual_root": "/mnt/user-data/outputs"}}
    document["output_receipt_sha256"] = output_receipt_digest(receipt)
    messages = []
    for index, host in enumerate(hosts):
        call = "call-" + str(index)
        ai = AIMessage("", id="ai-" + str(index), tool_calls=[{"id": call, "name": "bash", "args": {"command": "a scripted tool call"}}])
        tool = ToolMessage("Tool finished", id="tool-" + str(index), tool_call_id=call, name="bash", artifact={"sandbox_network_denied": {"host": host, "port": 443}})
        raw = tool.model_copy(update={"artifact": None})
        metadata = {"tool_artifact_enrichment": {"version": 1, "previous_sha256": presentation_fragment_digest({}), "added_keys": ["sandbox_network_denied"]}}
        for message, seq, kind, metadata in ((ai, index * 3 + 1, "llm.ai.response", {"caller": "lead_agent"}), (raw, index * 3 + 2, "llm.tool.result", {}), (tool, index * 3 + 3, "llm.tool.result", metadata)):
            messages.append(
                {
                    "thread_id": record.scope.thread_id,
                    "run_id": record.run_id,
                    "user_id": record.scope.owner_user_id,
                    "seq": seq,
                    "category": "message",
                    "event_type": kind,
                    "content": message.model_dump(),
                    "metadata": metadata,
                    "created_at": "2026-11-10T00:00:00+00:00",
                }
            )
        document["items"].append({"kind": "structured", "carrier": "message_artifact", "value_sha256": presentation_fragment_digest(tool.artifact), "witness": {"kind": "message_event", "event_seq": index * 3 + 3, "message_id": tool.id}})
    projection["inventory"]["evidence"] = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=receipt["event_seq"] + 1, document=document).model_dump(mode="json")
    from app.telegram_bots.inbox import publication_identity

    return {"version": 1, "publication": publication_identity(record), "projection": projection, "messages": messages}


async def test_turn_delivers_three_distinct_blocked_sites_after_answer_and_no_act(network):
    e = network
    e.seed = denial_basis(e.record, hosts=("one.example", "two.example", "one.example", "three.example", "four.example", "five.example"))
    result = await e.coordinator.observe(e.record, e.link, max_pages=1)
    assert result.state == "planned"
    from app.telegram_bots.network_notices import validate_notice
    from app.telegram_bots.producer import _item
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    async with e.db.sessions() as reader:
        notes = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:network-denials:"))))
        assert len(notes) == 1
        notice = _item(notes[0])
    assert validate_notice(notice).kind == "network_denials"
    assert (await e.services.eligibility(e.link, notice, None)).decision == "wait"
    for identity in range(701, 705):
        e.request.enqueue("sendMessage", result=sent(e, identity=identity))
    for _ in range(5):
        await drain(e)
    messages = [call.parameters for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(messages) == 4
    assert messages[0]["text"] == "The answer is ready."
    for part, host in zip(messages[1:], ("one.example", "two.example", "three.example"), strict=True):
        assert host in part["text"] and "Ask " in part["text"]
        assert "reply_markup" not in part or not part["reply_markup"]["inline_keyboard"]
    assert "four.example" not in str(messages) and "five.example" not in str(messages)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


async def test_manager_dropped_denial_explains_how_to_request_access_without_a_card(network):
    e = network
    await permissions(e, 7)
    e.seed = denial_basis(e.record)
    assert (await e.coordinator.observe(e.record, e.link, max_pages=1)).state == "planned"
    for identity in (701, 702):
        e.request.enqueue("sendMessage", result=sent(e, identity=identity))
    for _ in range(3):
        await drain(e)
    messages = [call.parameters for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(messages) == 2
    assert "allow the site when asked" in messages[-1]["text"] and "permanent list" in messages[-1]["text"]
    assert "reply_markup" not in messages[-1] or not messages[-1]["reply_markup"]["inline_keyboard"]


@pytest.mark.parametrize("change", ["disabled", "released", "claim", "parent", "manage", "use", "protected", "lifetime"])
async def test_last_sdk_snapshot_fences_notice_scope_and_wording(network, change):
    from datetime import UTC, datetime

    from sqlalchemy import event, insert

    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramOutboxRow
    from deerflow.persistence.user.access import DisabledIdentityRow
    from deerflow.persistence.user.model import UserRow

    e = network
    await permissions(e, 7)
    e.seed = denial_basis(e.record)
    primary = (await e.coordinator.observe(e.record, e.link, max_pages=1)).outbox_id
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    assert len([call for call in e.request.calls if call.endpoint == "sendMessage"]) == 1
    limiter, changed = e.channel.bot.rate_limiter, []

    def alter(connection, cursor, statement, parameters, context, many):
        if "JOIN telegram_outbox" not in statement or "LEFT OUTER JOIN telegram_outbox AS" not in statement or changed:
            return
        changed.append(True)
        if change == "disabled":
            connection.execute(insert(DisabledIdentityRow).values(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=datetime.now(UTC)))
        elif change == "released":
            connection.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(released_at=datetime.now(UTC)))
        elif change in {"manage", "use"}:
            connection.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=1 if change == "manage" else 2))
        elif change == "protected":
            connection.execute(insert(TelegramInstancePolicyRow).values(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
        elif change == "lifetime":
            e.active[0] = False
        elif change == "claim":
            connection.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id).values(claim_id="retired-claim"))
        else:
            connection.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == primary).values(state="skipped"))

    async def waited(delay):
        limiter._short_hold_until = 0
        event.listen(e.db.engine.sync_engine, "before_cursor_execute", alter)

    limiter._sleep = waited
    limiter._short_hold_until = limiter._time() + 10
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    try:
        await drain(e)
        assert changed == [True]
        assert len([call for call in e.request.calls if call.endpoint == "sendMessage"]) == 1
    finally:
        if event.contains(e.db.engine.sync_engine, "before_cursor_execute", alter):
            event.remove(e.db.engine.sync_engine, "before_cursor_execute", alter)


async def test_saved_notice_renders_current_manager_wording_and_does_not_repeat_acknowledgements(network):
    e = network
    e.seed = denial_basis(e.record)
    result = await e.coordinator.observe(e.record, e.link, max_pages=1)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    await permissions(e, 7)
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await drain(e)
    for _ in range(2):
        await e.coordinator.observe(e.record, e.link, max_pages=1)
        await drain(e)
    messages = [call.parameters for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(messages) == 2 and "allow the site when asked" in messages[-1]["text"]
    primary = await e.services.outbox.get(e.link, result.outbox_id)
    assert primary.parts[0]["message_ids"] == [701]


async def test_notice_waits_for_real_instance_file_export_and_caption_ack(network, exports):
    from app.telegram_bots.files import FileBytesBudget

    e = network
    assert exports is e
    e.seed = denial_basis(e.record)
    projection = e.seed["projection"]
    doc = projection["inventory"]["document"]
    witness = {"kind": "root_snapshot", "root_epoch": doc["root_epoch"], "message_id": projection["output"]["output_evidence"]["assistant_message_id"]}
    doc["items"].append({"kind": "file", "path": "/mnt/user-data/outputs/result.txt", "surfaces": ["presentation_reference"], "witnesses": [witness]})
    projection["inventory"]["evidence"] = build_event_inventory_evidence(projection["output"]["output_evidence"], status="success", inventory_event_seq=projection["output"]["output_evidence"]["event_seq"] + 1, document=doc).model_dump(
        mode="json"
    )
    e.services.file_bytes_budget = FileBytesBudget(50_000_000)
    e.coordinator.file_output_planner = e.services.model_source.plan_file_reply
    result = await e.coordinator.observe(e.record, e.link, max_pages=1)
    assert result.state == "planned"
    e.request.enqueue("sendDocument", result=sent(e, identity=701))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    for _ in range(3):
        await drain(e)
    messages = [call for call in e.request.calls if call.endpoint.startswith("send")]
    assert [call.endpoint for call in messages] == ["sendDocument", "sendMessage"]
    assert messages[0].parameters["caption"] == "The answer is ready."
    assert "one.example" in messages[1].parameters["text"]
    assert e.native_reads == ["outputs/result.txt"]
    assert e.services.file_bytes_budget.used == 0


async def test_completed_primary_is_not_reapplied_to_backfill_new_notice_delivery(network):
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = network
    e.seed = denial_basis(e.record)
    reader = e.services.outbox.network_denials
    # Reproduce a primary created before denial trailers were supported.
    e.services.outbox.network_denials = None
    result = await e.coordinator.observe(e.record, e.link, max_pages=1)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    e.services.outbox.network_denials = reader
    again = await e.coordinator.observe(e.record, e.link, max_pages=1)
    assert result.outbox_id == again.outbox_id
    await drain(e)
    assert len([call for call in e.request.calls if call.endpoint == "sendMessage"]) == 1
    async with e.db.sessions() as session:
        assert not list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:network-denials:"))))


async def test_lost_publication_lifetime_rolls_back_primary_and_notices_together(network, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = network
    e.seed = denial_basis(e.record)
    original = e.services.outbox.inbox.seal_publication
    reached = []

    async def retire(*args, **kwargs):
        await original(*args, **kwargs)
        reached.append(True)
        e.active[0] = False

    monkeypatch.setattr(e.services.outbox.inbox, "seal_publication", retire)
    try:
        from app.telegram_bots.publication import PublicationRefused

        with pytest.raises(PublicationRefused):
            await e.coordinator.observe(e.record, e.link, max_pages=1)
        assert reached == [True]
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.in_(["reply:" + e.record.turn_id, "notice:network-denials:" + e.record.publication_id]))))
    finally:
        e.active[0] = True


@pytest.mark.parametrize("publication", [77], indirect=True)
async def test_missing_topic_relocates_only_unacknowledged_host_notices(network):
    from test_telegram_bot_adapter import fail

    from app.telegram_bots.network_notices import validate_notice
    from app.telegram_bots.producer import _item
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = network
    assert e.record.scope.message_thread_id == 77
    e.seed = denial_basis(e.record, hosts=("first.example", "second.example"))
    assert (await e.coordinator.observe(e.record, e.link, max_pages=1)).state == "planned"
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    fail(e.request, "sendMessage", code=400, description="Bad Request: message thread not found")
    await drain(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    for _ in range(3):
        await drain(e)
    messages = [call.parameters for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(messages) == 4
    assert ["first.example" in part["text"] for part in messages] == [False, True, False, False]
    assert "second.example" in messages[-1]["text"] and "message_thread_id" not in messages[-1]
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:network-denials:")))
        assert row.state == "sent" and validate_notice(_item(row)).kind == "network_denials"
        assert len([part for part in row.parts if part["state"] == "sent"]) == 2


async def test_unrelated_artifact_keys_do_not_block_answer_or_host_notice(network):
    from app.telegram_bots.network_notices import validate_notice
    from app.telegram_bots.producer import _item
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = network
    e.seed = denial_basis(e.record)
    raw, enriched = e.seed["messages"][1:]
    raw["content"]["artifact"] = {"bytes": 7, "token": "unrelated result field"}
    enriched["content"]["artifact"].update(raw["content"]["artifact"])
    enriched["metadata"]["tool_artifact_enrichment"]["previous_sha256"] = presentation_fragment_digest(raw["content"]["artifact"])
    projection = e.seed["projection"]
    projection["inventory"]["document"]["items"][0]["value_sha256"] = presentation_fragment_digest(enriched["content"]["artifact"])
    projection["inventory"]["evidence"] = build_event_inventory_evidence(
        projection["output"]["output_evidence"], status="success", inventory_event_seq=projection["output"]["output_evidence"]["event_seq"] + 1, document=projection["inventory"]["document"]
    ).model_dump(mode="json")
    assert (await e.coordinator.observe(e.record, e.link, max_pages=1)).state == "planned"
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:network-denials:")))
        assert validate_notice(_item(row)).kind == "network_denials"
        assert "messages" not in row.payload["reply_basis"]["outcome"]["denial_basis"]
    for identity in (701, 702):
        e.request.enqueue("sendMessage", result=sent(e, identity=identity))
    for _ in range(3):
        await drain(e)
    messages = [call.parameters for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(messages) == 2 and "one.example" in messages[-1]["text"]
    assert "unrelated result field" not in str(messages)
