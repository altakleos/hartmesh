"""A chat turn on everything real but the far side of HTTP, for the ways a turn can end.

Real: the channel and its pinned Application, ChatTurns, the queue, ThreadCoordinator,
GatewayClientOwner, the attempt authority, PublicationCoordinator, RunObserver, the manager's
worker loop, the bus, the inbox, the outbox and SQL.

Scripted: the Gateway's answers and the Bot API. The live preview is a stand-in, because no
ending depends on it.
"""

import asyncio
import importlib
import json
import re
from types import SimpleNamespace

import httpx
from _telegram_repository_test_support import code_of
from sqlalchemy import select
from test_telegram_bot_handlers import ingress
from test_telegram_chat_turns import ControlledView
from test_telegram_chat_turns import message as text_message
from test_telegram_publication_observation import ACK, output_projection

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


class Gateway:
    """Threads and runs, answered as the real routes answer them."""

    def __init__(self, owner):
        self.owner = owner
        self.threads = {}
        self.deleted = set()
        self.runs = {}
        self.calls = []
        self.fault = None
        self.stream_status = None
        self.post_gate = None
        self.count = 0

    def thread_json(self, identity):
        return {"thread_id": identity, "owner_user_id": self.owner, "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}, "status": "idle", "created_at": "2026-10-25T12:00:00Z", "updated_at": "2026-10-25T12:00:00Z"}

    def start(self, publication_id, thread_id):
        self.count += 1
        row = {
            # The fixture's output evidence is bound to this exact run identity.
            "run_id": "coordinator-run" if self.count == 1 else f"run-{self.count:04d}",
            "thread_id": thread_id,
            "assistant_id": "lead_agent",
            "status": "running",
            "created_at": f"2026-10-25T12:00:{self.count:02d}Z",
            "metadata": {"channel_turn_id": publication_id},
            "kwargs": {"channel_execution": {"version": 1, "publication_id": publication_id, "owner_user_id": self.owner, "thread_id": thread_id, "assistant_id": "lead_agent", "agent_name": None}},
        }
        self.runs[publication_id] = {"row": row, "output": None}

    def finish(self, publication_id, *, status="success", stop_reason=None, output=None):
        entry = self.runs[publication_id]
        entry["row"] = {**entry["row"], "status": status, "stop_reason": stop_reason}
        entry["output"] = output

    def posts(self):
        return sum(1 for _method, path in self.calls if path.endswith("/runs/stream"))

    def _by_run(self, run_id):
        return next((entry for entry in self.runs.values() if entry["row"]["run_id"] == run_id), None)

    async def __call__(self, request):
        path, method = request.url.path, request.method
        self.calls.append((method, path))
        if self.fault is not None:
            response = self.fault(request)
            if response is not None:
                return response
        if method == "POST" and path == "/api/threads":
            identity = json.loads(request.content)["thread_id"]
            self.threads[identity] = True
            return httpx.Response(200, json=self.thread_json(identity))
        if (match := re.fullmatch(r"/api/threads/([^/]+)", path)) and method == "GET":
            if match.group(1) not in self.threads or match.group(1) in self.deleted:
                return httpx.Response(404, json={"detail": "Thread not found"})
            return httpx.Response(200, json=self.thread_json(match.group(1)))
        if re.fullmatch(r"/api/threads/([^/]+)/runs/by-idempotency-key", path) and method == "GET":
            entry = self.runs.get(request.headers.get("idempotency-key"))
            return httpx.Response(200, headers=ACK, json=entry["row"]) if entry is not None else httpx.Response(404, headers=ACK, json={"lookup": "idempotency-key", "detail": "Run not found"})
        if (match := re.fullmatch(r"/api/threads/([^/]+)/runs/page", path)) and method == "GET":
            data = sorted((entry["row"] for entry in self.runs.values() if entry["row"]["thread_id"] == match.group(1)), key=lambda row: (row["created_at"], row["run_id"]), reverse=True)
            return httpx.Response(200, headers=ACK, json={"data": data, "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        if (match := re.fullmatch(r"/api/threads/([^/]+)/runs/stream", path)) and method == "POST":
            if self.post_gate is not None:
                await self.post_gate.wait()
            if self.stream_status is not None:
                return httpx.Response(self.stream_status, json={"detail": "scripted refusal"})
            self.start(request.headers.get("idempotency-key"), match.group(1))
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=b"event: end\ndata: null\n\n")
        if re.fullmatch(r"/api/threads/([^/]+)/runs/([^/]+)/cancel", path) and method == "POST":
            return httpx.Response(202, json={})
        if re.fullmatch(r"/api/threads/([^/]+)/runs/([^/]+)/join", path) and method == "GET":
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=b"")
        if (match := re.fullmatch(r"/api/threads/([^/]+)/runs/([^/]+)/output", path)) and method == "GET":
            entry = self._by_run(match.group(2))
            return httpx.Response(200, headers=ACK, json=entry["output"]) if entry is not None and entry["output"] is not None else httpx.Response(404, headers=ACK, json={})
        if (match := re.fullmatch(r"/api/threads/([^/]+)/runs/([^/]+)", path)) and method == "GET":
            entry = self._by_run(match.group(2))
            return httpx.Response(200, headers=ACK, json=entry["row"]) if entry is not None else httpx.Response(404, headers=ACK, json={})
        raise AssertionError(f"Unscripted Gateway request {method} {path}")


def coordinator_for(services, bus, http):
    from app.telegram_bots.publication import PublicationCoordinator

    publications = PublicationCoordinator(inbox=services.outbox.inbox, outbox=services.outbox, bus=bus, http_owner=http, capture_attempt=services.publication_authority.capture)
    publications.host = services
    return publications


async def stack(db, monkeypatch, tmp_path, *, with_worker=True):
    from app.channels.message_bus import MessageBus
    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.gateway_client import GatewayClientOwner
    from app.telegram_bots.publication_authority import PublicationAttemptAuthority
    from app.telegram_bots.threads import ThreadCoordinator
    from app.telegram_bots.turns import ChatTurns

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    gateway = Gateway(e.link.owner_user_id)
    http = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://gateway.example/api", transport=httpx.MockTransport(gateway))
    services = e.services
    services.channel = lambda identity: e.channel if identity == e.link.id else None
    services.callbacks = CallbackRepository(services.outbox.sessions)
    services.thread_client = http
    services.threads = ThreadCoordinator(links=db.links, authority=e.live.authority, client=http, channel=services.channel, send_allowed=services.send_allowed, outbox=services.outbox, product_name="HartMesh")
    services.publication_authority = PublicationAttemptAuthority(links=db.links, authority=e.live.authority, client=http, channel=services.channel, create_allowed=services.send_allowed, read_allowed=lambda: True)
    bus = MessageBus(inbound_queue_maxsize=16)
    services.publications = coordinator_for(services, bus, http)
    turns = ChatTurns(services)
    services.turns = turns
    monkeypatch.setattr(importlib.import_module("app.telegram_bots.streaming"), "LivePresentation", ControlledView)

    async def to_channel(message):
        await e.channel.send(message)

    bus.subscribe_outbound(to_channel)
    s = SimpleNamespace(e=e, db=db, gateway=gateway, http=http, services=services, bus=bus, turns=turns, tmp_path=tmp_path, workers=[])

    async def close():
        for worker in s.workers:
            worker.cancel()
        await asyncio.gather(*s.workers, return_exceptions=True)
        await s.turns.stop()
        await services.publications.stop()
        await services.threads.stop()
        await http.stop()
        await e.channel.stop()

    s.close = close
    if with_worker:
        start_worker(s)
    return s


def start_worker(s):
    from app.channels.manager import ChannelManager
    from app.channels.store import ChannelStore

    manager = ChannelManager(s.bus, ChannelStore(s.tmp_path / f"store-{len(s.workers)}.json"), langgraph_url="https://gateway.example/api", gateway_url="https://gateway.example")
    manager._running = True
    s.workers.append(asyncio.create_task(manager._worker_loop(0), name="test-manager-worker"))


async def send(s, identity, content):
    """One gesture through the real Application, as the poller delivers it."""
    return await s.e.channel.handle_update(text_message(s.e, identity, content=content))


async def planned_keys(s):
    async with s.db.sessions() as session:
        rows = list(await session.scalars(select(TelegramOutboxRow).order_by(TelegramOutboxRow.created_at)))
    return [key for row in rows for key in (part.get("source_ref", {}).get("key") for part in row.parts) if key not in {"hello.chat", "hello.privacy"}]


async def mark(s, message_id):
    async with s.db.sessions() as session:
        return await session.get(TelegramInboundMarkRow, (s.e.link.id, message_id, s.e.link.chat_id))


async def until(predicate, seconds=8):
    async with asyncio.timeout(seconds):
        while True:
            value = predicate()
            if asyncio.iscoroutine(value):
                value = await value
            if value:
                return value
            await asyncio.sleep(0.01)


def only_turn(s):
    return next(iter(s.turns._turns.values()), None)


async def past_the_limit(s, turn, *, seconds=None):
    """Let the driver reach its wait, then move the clock past the limit."""
    await asyncio.sleep(0.2)
    s.e.clock.advance(1)
    turn.wake.set()
    await asyncio.sleep(0.2)
    s.e.clock.advance(seconds if seconds is not None else s.services.config.queue.max_wait_seconds + 1)
    turn.wake.set()


async def press_try_again(s, publication_id):
    async with s.db.sessions() as session:
        control = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message", TelegramCallbackRow.target_id == publication_id))
        callback_id, group_id, parent_id = control.id, control.group_id, control.payload["anchor_outbox_id"]
    assert await s.services.callbacks.adopt(s.e.channel.link, group_id, 1700, chat_id=s.e.link.chat_id, message_thread_id=None, outbox_id=parent_id, presentation_version=0, check_current=lambda: True)
    await s.turns.execute_callback(s.e.channel, await s.services.callbacks.get(callback_id), update_id=1701, query_id="try-again")


async def rebind(s, link):
    """The real unbind, with the code that binds again."""
    gone = await s.db.links.unbind(link, actor_id=link.owner_user_id)
    fresh = await s.db.links.mint_code(gone)
    s.e.channel.link = await s.db.links.get(link.id)
    return fresh


async def bound_again(s, link, fresh):
    saved = await s.db.links.bind(
        await s.db.links.get(link.id),
        telegram_user_id=link.telegram_user_id,
        chat_id=link.chat_id,
        first_name="Fixture",
        username=None,
        code=code_of(fresh),
        start_identity={"update_id": 9001, "message_id": 9001, "chat_id": link.chat_id},
        hello_parts=[{"source_ref": {"key": "hello.chat", "values": {"product": "HartMesh", "email": "fixture@example.test"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
    )
    s.e.channel.link = saved


async def restart(s):
    """The process is gone: its turns, queue and bus with it. SQL stays."""
    from app.telegram_bots.recovery import PublicationRecoveryService
    from app.telegram_bots.turns import ChatTurns

    services = s.services
    await s.turns.stop()
    await services.publications.stop()
    s.bus.discard_pending_inbound()
    s.turns = services.turns = ChatTurns(services)
    services.publications = coordinator_for(services, s.bus, s.http)
    return PublicationRecoveryService(services.outbox.inbox, services.outbox, services.publications, s.db.links, lambda: True, owned=lambda record, link: s.turns.owns_publication(record, link))


def stopped_output(record, status):
    """What the output route projects for a run that ended without an answer."""
    from deerflow.runtime.runs.output_inventory import build_unavailable_inventory_evidence

    value = output_projection(record, failed=True)
    if status == "error":
        return value
    receipt = {**value["output"]["output_evidence"], "reason": "cancelled"}
    evidence = build_unavailable_inventory_evidence(receipt, status=status, reason="prestart_not_enumerated")
    value["output"].update(run_status=status, reason="cancelled", output_evidence=receipt)
    value["inventory"] = {"state": "unavailable", "evidence": evidence.model_dump(mode="json"), "document": None, "reason": evidence.reason}
    return value
