"""Socket-free Bot API transport shared by the Telegram bot tests."""

from __future__ import annotations

import asyncio
import json
import secrets
from collections import defaultdict, deque
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from telegram import Update
from telegram.ext import ExtBot
from telegram.request import BaseRequest, RequestData


@dataclass
class ScriptedReply:
    status: int
    body: dict[str, Any]
    release: asyncio.Event | None = None
    started: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(frozen=True)
class RecordedCall:
    endpoint: str
    method: str
    parameters: dict[str, Any]


class RecordingRequest(BaseRequest):
    """Exercise library serialization and errors without opening a socket.

    Each endpoint has an explicit FIFO script; unexpected calls fail. Replies
    may wait for release events, including cancellation tests. Record endpoint
    names only, never URLs containing the runtime token.
    """

    def __init__(self) -> None:
        self.calls: list[RecordedCall] = []
        self.replies: dict[str, deque[ScriptedReply]] = defaultdict(deque)
        self.initialized = False
        self.closed = False

    @property
    def read_timeout(self) -> float:
        return 5.0

    def enqueue(self, endpoint: str, *, result: Any = True, status: int = 200, body: dict[str, Any] | None = None, release: asyncio.Event | None = None) -> ScriptedReply:
        reply = ScriptedReply(status, deepcopy(body) if body is not None else {"ok": True, "result": deepcopy(result)}, release)
        self.replies[endpoint].append(reply)
        return reply

    def enqueue_get_me(self, *, bot_id: int = 123456, **fields: Any) -> ScriptedReply:
        return self.enqueue("getMe", result={"id": bot_id, "is_bot": True, "first_name": "Offline bot", "username": "offline_test_bot", **fields})

    async def initialize(self) -> None:
        self.initialized = True

    async def shutdown(self) -> None:
        self.closed = True

    async def do_request(self, url: str, method: str, request_data: RequestData | None = None, **kwargs: Any) -> tuple[int, bytes]:
        endpoint = url.rsplit("/", 1)[-1]
        self.calls.append(RecordedCall(endpoint, method, deepcopy(request_data.parameters) if request_data else {}))
        if not self.replies[endpoint]:
            raise AssertionError(f"Unscripted Bot API endpoint: {endpoint}")
        reply = self.replies[endpoint].popleft()
        reply.started.set()
        try:
            if reply.release is not None:
                await reply.release.wait()
            return reply.status, json.dumps(reply.body).encode("utf-8")
        except asyncio.CancelledError:
            reply.cancelled.set()
            raise


def offline_bot(request: RecordingRequest, *, updates_request: RecordingRequest | None = None, **kwargs: Any) -> ExtBot:
    """Both transports stay offline; synthesize credentials only at runtime."""
    token = str(secrets.randbelow(900000) + 100000) + ":" + secrets.token_urlsafe(30)
    return ExtBot(token, request=request, get_updates_request=updates_request if updates_request is not None else RecordingRequest(), **kwargs)


class ManualClock:
    """Independent wall/monotonic clocks and event-controlled async sleeps."""

    def __init__(self, now: datetime | None = None) -> None:
        self._now = now or datetime(2030, 1, 1, tzinfo=UTC)
        self._monotonic = 0.0
        self.sleeps: list[float] = []
        self._waiters: list[tuple[float, asyncio.Future]] = []
        self._changed = asyncio.Event()

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self._changed.set()
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        waiter = asyncio.get_running_loop().create_future()
        entry = (self._monotonic + seconds, waiter)
        self._waiters.append(entry)
        try:
            await waiter
        finally:
            self._waiters.remove(entry)

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("clock cannot move backward")
        self._monotonic += seconds
        self._now += timedelta(seconds=seconds)
        for deadline, waiter in self._waiters:
            if deadline <= self._monotonic and not waiter.done():
                waiter.set_result(None)

    async def wait_for_sleep(self, count: int = 1) -> None:
        async def ready():
            while len(self.sleeps) < count:
                self._changed.clear()
                if len(self.sleeps) < count:
                    await self._changed.wait()

        await asyncio.wait_for(ready(), 2)


def update_payload(update_id: int, *, message_id: int | None = None, account: int = 7_000_000_001, topic: int | None = None, text: str = "Synthetic message") -> dict:
    message = {"message_id": message_id if message_id is not None else update_id, "date": 1_893_456_000, "chat": {"id": account, "type": "private"}, "from": {"id": account, "is_bot": False, "first_name": "Fixture"}, "text": text}
    if topic is not None:
        message["message_thread_id"] = topic
    return {"update_id": update_id, "message": message}


def make_update(update_id: int, **kwargs: Any) -> Update:
    return Update.de_json(update_payload(update_id, **kwargs), None)
