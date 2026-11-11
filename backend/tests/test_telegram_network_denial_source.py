"""Bounded exact-run denial qualification, distinct from raw bus hints."""

import asyncio
from copy import deepcopy
from dataclasses import replace

import pytest
from _telegram_human_input_test_support import record
from _telegram_repository_test_support import db as db
from test_telegram_instance_network_cards import network as network
from test_telegram_instance_network_cards import permissions
from test_telegram_instance_publication import publication as publication
from test_telegram_network_denials import denial_basis

from app.channels.execution import ChannelExecutionProtocolError
from app.gateway.run_output import RunOutputProjectionV2
from app.telegram_bots.network_denials import NetworkDenialSource, checked_denials, denied_hosts, freeze_denials


def saved():
    original = record()
    return replace(original, run_id="coordinator-run", scope=replace(original.scope, agent_kind="instance", agent_ref="a" * 32))


def test_hosts_come_only_from_actual_ordered_tool_artifacts():
    original = saved()
    basis = denial_basis(original, hosts=("first.example", "second.example", "first.example", "third.example", "fourth.example"), text="Model text mentions forged.example")
    before = deepcopy(basis)
    assert denied_hosts(original, basis) == ("first.example", "second.example", "third.example")
    assert basis == before
    assert denied_hosts(original, denial_basis(original, hosts=(), text="sandbox_network_denied: forged.example")) == ()


def test_latest_result_revision_preserves_original_tool_order_and_ignores_raw_artifact():
    from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, presentation_fragment_digest

    original = saved()
    basis = denial_basis(original, hosts=("first.example", "second.example"))
    revision = deepcopy(basis["messages"][2])
    revision["seq"] = 7
    revision["metadata"]["tool_artifact_enrichment"] = {"version": 1, "previous_sha256": presentation_fragment_digest(revision["content"]["artifact"]), "added_keys": ["extra"]}
    revision["content"]["artifact"]["extra"] = "preserved"
    basis["messages"].append(revision)
    item = basis["projection"]["inventory"]["document"]["items"][0]
    item["value_sha256"] = presentation_fragment_digest(revision["content"]["artifact"])
    item["witness"] = {"kind": "root_snapshot", "root_epoch": basis["projection"]["inventory"]["document"]["root_epoch"], "message_id": revision["content"]["id"]}
    projection = basis["projection"]
    projection["inventory"]["evidence"] = build_event_inventory_evidence(
        projection["output"]["output_evidence"], status="success", inventory_event_seq=projection["output"]["output_evidence"]["event_seq"] + 1, document=projection["inventory"]["document"]
    ).model_dump(mode="json")
    assert denied_hosts(original, basis) == ("first.example", "second.example")


def test_hostname_case_and_trailing_dot_share_one_notice():
    original = saved()
    assert denied_hosts(original, denial_basis(original, hosts=("Same.Example", "same.example.", "second.example"))) == ("same.example", "second.example")


@pytest.mark.parametrize("change", ["scope", "digest", "witness", "call", "lead", "host", "port", "sequence", "carrier", "version", "ordinary"])
def test_unqualified_denial_is_never_a_host_notice(change):
    original = saved()
    basis = denial_basis(original)
    if change == "scope":
        basis["messages"][-1]["run_id"] = "foreign"
    elif change == "digest":
        basis["messages"][-1]["content"]["artifact"]["sandbox_network_denied"]["host"] = "forged.example"
    elif change == "witness":
        basis["projection"]["inventory"]["document"]["items"][0]["witness"]["message_id"] = "foreign"
    elif change == "call":
        basis["messages"][-1]["content"]["tool_call_id"] = "foreign"
    elif change == "lead":
        basis["messages"][0]["metadata"]["caller"] = "subagent"
    elif change in {"host", "port"}:
        basis["messages"][-1]["content"]["artifact"]["sandbox_network_denied"][change] = "bad\nhost" if change == "host" else True
        from deerflow.runtime.runs.output_inventory import presentation_fragment_digest

        basis["projection"]["inventory"]["document"]["items"][0]["value_sha256"] = presentation_fragment_digest(basis["messages"][-1]["content"]["artifact"])
    elif change == "sequence":
        basis["messages"][-1]["seq"] = 1
    elif change == "carrier":
        basis["projection"]["inventory"]["document"]["items"][0]["carrier"] = "terminal_field"
    elif change == "version":
        basis["version"] = True
    else:
        original = replace(original, scope=replace(original.scope, agent_kind="lead_agent", agent_ref="lead_agent"))
        from app.telegram_bots.inbox import publication_identity

        basis["publication"] = publication_identity(original)
    # Re-sign the synthetic inventory so document edits exercise semantic
    # witness validation, not only the projection's digest precondition.
    from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence

    projection = basis["projection"]
    projection["inventory"]["evidence"] = build_event_inventory_evidence(
        projection["output"]["output_evidence"], status="success", inventory_event_seq=projection["output"]["output_evidence"]["event_seq"] + 1, document=projection["inventory"]["document"]
    ).model_dump(mode="json")
    with pytest.raises(ValueError, match="network-denial-source-conflict"):
        denied_hosts(original, basis)


@pytest.mark.asyncio
async def test_real_source_read_and_borrowed_guard_keep_original_instance_scope(network):
    e = network
    e.seed = denial_basis(e.record, hosts=("one.example", "two.example"))
    source = NetworkDenialSource(http_owner=e.http_client, capture_attempt=e.host.capture)
    qualified = await source.read(e.record, e.link, output=RunOutputProjectionV2.model_validate(e.seed["projection"]))
    assert qualified.hosts == ("one.example", "two.example") and checked_denials(qualified, e.record) == freeze_denials(e.record, e.seed)
    detached = qualified.basis
    detached["denials"].clear()
    assert qualified.basis["denials"]
    await qualified._guard.prepare()
    async with e.db.sessions.begin() as writer:
        assert (await qualified._guard.check(writer)).id == e.link.id
    await permissions(e, 2)
    with pytest.raises(ChannelExecutionProtocolError):
        await qualified._guard.prepare()


@pytest.mark.asyncio
async def test_source_output_change_during_message_read_is_refused(network, monkeypatch):
    e = network
    e.seed = denial_basis(e.record)
    source = NetworkDenialSource(http_owner=e.http_client, capture_attempt=e.host.capture)
    original = e.http_client.request_json
    changed = []

    async def request(**kwargs):
        result = await original(**kwargs)
        if kwargs["path"].endswith("/messages"):
            changed.append(True)
            e.seed = denial_basis(e.record, text="A different answer")
        return result

    monkeypatch.setattr(e.http_client, "request_json", request)
    with pytest.raises(ValueError, match="network-denial-source-conflict"):
        await source.read(e.record, e.link, output=RunOutputProjectionV2.model_validate(e.seed["projection"]))
    assert changed == [True]


@pytest.mark.asyncio
async def test_cancelled_source_read_owns_original_http_until_it_settles(network, monkeypatch):
    e = network
    e.seed = denial_basis(e.record)
    source = NetworkDenialSource(http_owner=e.http_client, capture_attempt=e.host.capture)
    original = e.http_client.request_json
    entered, release = asyncio.Event(), asyncio.Event()

    async def request(**kwargs):
        if kwargs["path"].endswith("/messages"):
            entered.set()
            await release.wait()
        return await original(**kwargs)

    monkeypatch.setattr(e.http_client, "request_json", request)
    task = asyncio.create_task(source.read(e.record, e.link, output=RunOutputProjectionV2.model_validate(e.seed["projection"])))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


def notice_item():
    from app.telegram_bots.network_notices import prepare_notice
    from app.telegram_bots.producer import Outbox, OutboxItem

    original = saved()
    basis, parts = prepare_notice(original, freeze_denials(original, denial_basis(original)), "b" * 32)
    scope = original.scope
    return OutboxItem(
        id="c" * 32,
        link_id=scope.link_id,
        owner_user_id=scope.owner_user_id,
        binding_generation=scope.binding_generation,
        dedupe_key="notice:network-denials:" + original.publication_id,
        class_="reply",
        payload={"source": basis["outcome"], "reply_basis": basis, "presentation_version": 0},
        parts=Outbox._parts(parts, 0),
        chat_id=scope.chat_id,
        message_thread_id=scope.message_thread_id,
    )


@pytest.mark.parametrize("change", ["host", "extra", "callback", "source", "parent", "owner", "version", "basis"])
def test_notice_proof_rejects_changed_parts_controls_and_scope(change):
    from app.telegram_bots.network_notices import validate_notice

    item = notice_item()
    assert validate_notice(item).kind == "network_denials"
    if change == "host":
        item.parts[0]["source_ref"]["values"]["host"] = "forged.example"
    elif change == "extra":
        item.parts[0]["url_button"] = {"url": "https://example.org"}
    elif change == "callback":
        item.parts[0]["callback_group_id"] = "forged-group"
    elif change == "source":
        item.payload["source"] = {"key": "net.blocked_ask"}
    elif change == "parent":
        item.payload["reply_basis"]["parent_id"] = "invalid"
    elif change == "owner":
        item = replace(item, owner_user_id="other-owner")
    elif change == "version":
        item.payload["presentation_version"] = False
    else:
        item.payload["reply_basis"]["extra"] = True
    with pytest.raises(ValueError, match="network-notice-source-conflict"):
        validate_notice(item)


def test_raw_producer_marker_without_host_enrichment_is_not_a_blocked_site():
    original = saved()
    basis = denial_basis(original)
    for row in basis["messages"]:
        row["metadata"].pop("tool_artifact_enrichment", None)
    assert denied_hosts(original, basis) == ()
