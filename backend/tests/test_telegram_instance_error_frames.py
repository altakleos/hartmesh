"""Instance interruption replies are qualified by exact host streams, not wording."""

import json
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_instance_publication import publication as publication
from test_telegram_publication_observation import ACK, output_projection, run_row
from test_telegram_publication_primary import _primary

from app.channels.execution import ChannelExecutionProtocolError
from app.channels.run_errors import saved_run_error
from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.errors import classify_run
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.producer import _item, validate_publication_outcome
from app.telegram_bots.progress import RunObserver
from app.telegram_bots.publication import channel_execution_scope, failure_button, fixed_source
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

pytestmark = pytest.mark.asyncio


async def observed_error(e, record, frame, *, event="error", status="error", reason=None, recovery=False):
    """Use the actual observer/facade/SQL authority with seeded host HTTP frames."""
    asgi = httpx.ASGITransport(app=e.http_app)
    row = run_row(record, run_id=record.run_id)
    row.update(status=status, stop_reason=reason, metadata={"channel_turn_id": record.publication_id})

    e.error_requests = []

    async def respond(request):
        e.error_requests.append((request.method, request.url.path))
        if request.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=output_projection(record, failed=True))
        if "/runs/" in request.url.path:
            if request.url.path.endswith("/join"):
                body = f"event: {event}\ndata: {json.dumps(frame)}\n\n".encode()
                return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=body)
            return httpx.Response(200, headers=ACK, json=row)
        return await asgi.handle_async_request(request)

    old = e.http_client
    e.http_client = GatewayClientOwner(gateway_url="http://gateway.example", langgraph_url="http://runtime.example/api", transport=httpx.MockTransport(respond))
    assert e.http_client.endpoint_id == old.endpoint_id
    e.threads.client = e.host._client = e.coordinator._http_owner = e.http_client
    await old.stop()
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http_client, capture_attempt=e.host.capture, is_current=lambda record: e.active[0])
    try:
        if recovery:
            services = e.channel._services
            services.authority = e.live.authority
            services.callbacks = CallbackRepository(e.db.sessions)
            services.product_name = "HartMesh"
            e.coordinator.host = services
            return await e.coordinator.observe(record, e.link, max_pages=1, closing="chat.restarted")
        return await observer.read_run(record, e.link)
    finally:
        await observer.drain()
        await asgi.aclose()


async def bound(e):
    record = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    return await e.inbox.bind_publication_run(record, run_id="coordinator-run", guard=attempt.read_guard)


@pytest.mark.parametrize(
    "name,reason,key",
    [
        ("ResidentInterrupted", "lifecycle", "refused.interrupted_manager"),
        ("ResidentInterrupted", "storage-recovery", "refused.interrupted_recovery"),
        ("ResidentInterrupted", "operator", "refused.interrupted"),
        ("ResidentInterrupted", "container-absent", "refused.interrupted"),
        ("ResidentInterrupted", None, "refused.interrupted"),
        ("Refusal", "managers-only", "net.blocked_ask"),
        ("OtherError", "lifecycle", "error.generic"),
    ],
)
async def test_observer_and_saved_outcome_keep_exact_error_evidence(publication, name, reason, key):
    e = publication
    record = await bound(e)
    run = await observed_error(e, record, {"name": name, "reason": reason, "message": "PRIVATE diagnostic"})
    scope = channel_execution_scope(record)
    failure = classify_run(run, scope)
    assert failure.text_key == key and not failure.retry_later
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    callbacks = CallbackRepository(e.db.sessions)
    source = {"key": key, "values": {"name": "Fixture employee"}}
    identity = await e.channel._services.outbox.plan_publication_outcome(record, run=run, source_ref=source, keyboard=[], callbacks=callbacks, guard=attempt.read_guard)
    item = _item(await _primary(e.db, identity))
    assert validate_publication_outcome(item).source_ref == source
    assert "PRIVATE" not in str(item.payload)
    assert "error" in item.payload["turn_outcome_basis"]["proof"]
    # Saved host data is not itself a live capability, and cannot cross instances.
    with pytest.raises(ChannelExecutionProtocolError):
        classify_run(replace(run, error_evidence=saved_run_error(run.error_evidence, scope, record.run_id)), scope)
    with pytest.raises(ChannelExecutionProtocolError):
        classify_run(run, replace(scope, instance_id="f" * 32))
    item.payload["turn_outcome_basis"]["proof"]["error"]["run_id"] = "another"
    with pytest.raises(ValueError):
        validate_publication_outcome(item)


@pytest.mark.parametrize(
    "event,frame,status,reason,key",
    [
        ("error", {"message": "ResidentInterrupted lifecycle"}, "error", None, "error.generic"),
        ("custom", {"name": "ResidentInterrupted", "reason": "lifecycle"}, "error", None, "error.generic"),
        ("error", {"name": "ResidentInterrupted", "reason": "lifecycle"}, "success", None, None),
        ("error", {"name": "ResidentInterrupted", "reason": "lifecycle"}, "interrupted", None, "error.generic"),
        ("error", {"name": "ResidentInterrupted", "reason": "lifecycle"}, "error", "gateway_stopped", "chat.restarted"),
    ],
)
async def test_only_structured_error_on_error_status_changes_wording(publication, event, frame, status, reason, key):
    e = publication
    record = await bound(e)
    run = await observed_error(e, record, frame, event=event, status=status, reason=reason)
    result = classify_run(run, channel_execution_scope(record))
    assert (result.text_key if result else None) == key


@pytest.mark.parametrize("permissions,files", [(1, False), (3, True), (7, True)])
async def test_recovery_files_link_needs_inspect_without_manage(publication, permissions, files):
    from app.telegram_bots.render import render_plain

    e = publication
    e.live.config.model_extra["telegram_bots"] = {"enabled": True, "public_base_url": "https://app.example"}
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=permissions))
    host = SimpleNamespace(authority=e.live.authority, product_name="HartMesh", _app_config=e.live.config)
    source = await fixed_source(host, e.link, "refused.interrupted_recovery")
    for locale in ("en", "zh-CN"):
        text = render_plain(source["key"], locale, **source["values"])
        assert (await e.live.authority.instance_view(e.link)).name in text
    assert bool(await failure_button(host, e.link, source["key"])) is files


async def test_public_provider_identity_is_used_without_private_configuration(publication, tmp_path, monkeypatch):
    from PIL import Image

    from app.telegram_bots.render import render_plain
    from deerflow.config.product_identity import mounted_identity

    e = publication
    root = tmp_path / "public-identity"
    root.mkdir()
    assets = root / "assets"
    assets.mkdir()
    Image.new("RGB", (64, 64), "navy").save(assets / "mark.png")
    (root / "product.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "public": True,
                "product": {"name": "Example Product", "logo": "assets/mark.png", "browser_icon": "assets/mark.png"},
                "provider": {"name": "Example Provider", "logo": "assets/mark.png", "support_url": "https://support.example.test"},
            }
        )
    )
    monkeypatch.setenv("HARTMESH_PRODUCT_PAYLOAD", str(root))
    mounted_identity.cache_clear()
    host = SimpleNamespace(authority=e.live.authority, product_name="Example Product", _app_config=None)
    try:
        source = await fixed_source(host, e.link, "refused.interrupted_recovery")
        assert source["values"]["provider"] == "Example Provider"
        assert render_plain(source["key"], "en", **source["values"]).startswith("Example Provider restored or repaired")
        source = await fixed_source(host, e.link, "net.blocked_ask")
        text = render_plain(source["key"], "en", **source["values"])
        assert "a site" in text and "None" not in text
    finally:
        mounted_identity.cache_clear()


@pytest.mark.parametrize("name,reason,key", [("ResidentInterrupted", "storage-recovery", "refused.interrupted_recovery"), ("Refusal", "managers-only", "net.blocked_ask")])
async def test_recovery_reads_original_error_stream_and_plans_once_without_dispatch(publication, name, reason, key):
    e = publication
    record = await bound(e)
    before = e.bus.inbound_queue.qsize()
    first = await observed_error(e, record, {"name": name, "reason": reason}, recovery=True)
    assert first.state == "planned"
    assert any(path.endswith("/join") for _, path in e.error_requests)
    assert all(method == "GET" for method, _ in e.error_requests)
    item = _item(await _primary(e.db, first.outbox_id))
    assert validate_publication_outcome(item).source_ref["key"] == key
    if key == "net.blocked_ask":
        assert not item.payload.get("product_keyboard_basis")
    again = await observed_error(e, record, {"name": name, "reason": reason}, recovery=True)
    assert again.state == "planned" and again.outbox_id == first.outbox_id
    assert e.bus.inbound_queue.qsize() == before
