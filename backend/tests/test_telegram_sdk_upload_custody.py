"""Real pinned SDK/HTTPX/httpcore pool ownership over a socket-free network backend.

Only the network backend is replaced; HTTP transport/pool, HTTP/1.1 multipart,
SDK serialization and the actual upload leaf run. Private inspection is confined
to this regression evidence and must not become production pool cleanup.
"""

import asyncio
import hashlib
import json
import sys

import httpcore
import httpx
import pytest
from _telegram_bot_test_support import offline_bot

from app.telegram_bots.adapter import BoundedRateLimiter
from app.telegram_bots.capped_download import ManagedHTTPXRequest
from app.telegram_bots.files import FileBytesBudget, OutputBatch
from app.telegram_bots.outbound_upload import upload_output
from deerflow.utils.file_io import await_drained


def reply(result):
    body = json.dumps({"ok": True, "result": result}, separators=(",", ":")).encode()
    return b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body


def rate_limited_reply():
    body = json.dumps({"ok": False, "error_code": 429, "description": "Too Many Requests", "parameters": {"retry_after": 1}}, separators=(",", ":")).encode()
    return b"HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body


class RecordedNetworkStream(httpcore.AsyncMockStream):
    def __init__(self, response, *, payload_size, payload_sha256, fail_close, expected_chunks=None, write_entered=None, write_release=None):
        super().__init__([response])
        self.payload_size, self.payload_sha256, self.fail_close = payload_size, payload_sha256, fail_close
        self.payload_writes = self.close_calls = 0
        self.expected_chunks = expected_chunks or ((payload_size, payload_sha256),)
        self.payload_chunk_lengths = []
        self.write_entered, self.write_release = write_entered, write_release
        self.filename_sent = False

    async def write(self, buffer, timeout=None):
        self.filename_sent = self.filename_sent or b'filename="result.bin"' in buffer
        index = len(self.payload_chunk_lengths)
        if index < len(self.expected_chunks) and (len(buffer), hashlib.sha256(buffer).hexdigest()) == self.expected_chunks[index]:
            self.payload_chunk_lengths.append(len(buffer))
            if self.write_entered is not None:
                self.write_entered.set()
                await await_drained(self.write_release.wait())
            if len(self.payload_chunk_lengths) == len(self.expected_chunks):
                self.payload_writes += 1
        await super().write(buffer, timeout=timeout)

    async def aclose(self):
        self.close_calls += 1
        if self.fail_close and self.close_calls == 1:
            raise httpcore.ReadError("synthetic network-stream close failure")
        await super().aclose()


class RecordedNetworkBackend(httpcore.AsyncMockBackend):
    def __init__(self, *, payload_size, payload_sha256, fail_close, expected_chunks=None, short_retry=False, write_entered=None, write_release=None):
        super().__init__([])
        self.payload_size, self.payload_sha256, self.fail_close = payload_size, payload_sha256, fail_close
        self.streams = []
        self.expected_chunks, self.short_retry = expected_chunks, short_retry
        self.write_entered, self.write_release = write_entered, write_release

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        result = {"id": 100001, "is_bot": True, "first_name": "Offline", "username": "fixture_bot"} if not self.streams else {"message_id": 10, "date": 1, "chat": {"id": 7000000001, "type": "private"}}
        stream = RecordedNetworkStream(
            rate_limited_reply() if self.short_retry and len(self.streams) == 1 else reply(result),
            payload_size=self.payload_size,
            payload_sha256=self.payload_sha256,
            fail_close=self.fail_close and bool(self.streams),
            expected_chunks=self.expected_chunks,
            write_entered=self.write_entered if self.streams else None,
            write_release=self.write_release if self.streams else None,
        )
        self.streams.append(stream)
        return stream


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_close", [False, True])
async def test_real_sdk_upload_drops_leased_bytes_even_when_pool_retains_failed_response(fail_close):
    payload = bytes(bytearray(range(256))) * 64
    original_references = sys.getrefcount(payload)
    backend = RecordedNetworkBackend(payload_size=len(payload), payload_sha256=hashlib.sha256(payload).hexdigest(), fail_close=fail_close)

    def build():
        transport = httpx.AsyncHTTPTransport(verify=False)
        # Test-only injection uses the real cached pool and HTTP/1.1 stack.
        transport._pool._network_backend = backend
        request = ManagedHTTPXRequest(httpx_kwargs={"transport": transport, "trust_env": False})
        bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
        return transport, request, bot

    transport, request, bot = await asyncio.to_thread(build)
    budget = FileBytesBudget(len(payload))
    batch = OutputBatch(await budget.reserve_batch([len(payload)], timeout=1))
    await batch.keep(0, payload)
    try:
        async with bot:
            outcome = await upload_output(
                bot,
                kind="document",
                files=(("result.bin", batch.data(0)),),
                chat_id=7000000001,
                message_thread_id=None,
                reply_to_message_id=None,
                silent=False,
                protect_content=False,
                caption=None,
                plain=False,
                before_dispatch=lambda: asyncio.sleep(0),
            )
            assert outcome.failure == ("network" if fail_close else None)
            assert len(backend.streams) == 2 and backend.streams[1].payload_writes == 1
            if fail_close:
                assert backend.streams[1].close_calls == 1 and len(transport._pool._requests) == 1
            else:
                assert transport._pool._requests == []
            await batch.aclose()
            assert budget.used == 0
            remaining_references = sys.getrefcount(payload)
            assert remaining_references == original_references, "The actual HTTPX/httpcore multipart owner still retains leased upload bytes after the SDK leaf drained"
    finally:
        await batch.aclose()
        await request.shutdown()


def multipart_chunks(payload):
    return tuple((len(payload[offset : offset + 65_536]), hashlib.sha256(payload[offset : offset + 65_536]).hexdigest()) for offset in range(0, len(payload), 65_536))


async def real_bot(backend):
    def build():
        transport = httpx.AsyncHTTPTransport(verify=False)
        transport._pool._network_backend = backend
        request = ManagedHTTPXRequest(httpx_kwargs={"transport": transport, "trust_env": False})
        return transport, request, offline_bot(request, rate_limiter=BoundedRateLimiter())

    return await asyncio.to_thread(build)


@pytest.mark.asyncio
@pytest.mark.parametrize("short_retry", [False, True])
async def test_real_sdk_streams_every_bounded_chunk_and_rewinds_for_short_retry(short_retry):
    payload = bytes(bytearray(range(256))) * 512 + b"the-end"
    original_references = sys.getrefcount(payload)
    backend = RecordedNetworkBackend(payload_size=len(payload), payload_sha256=hashlib.sha256(payload).hexdigest(), fail_close=False, expected_chunks=multipart_chunks(payload), short_retry=short_retry)
    transport, request, bot = await real_bot(backend)
    budget = FileBytesBudget(len(payload))
    batch = OutputBatch(await budget.reserve_batch([len(payload)], timeout=1))
    await batch.keep(0, payload)
    try:
        async with bot:
            outcome = await upload_output(
                bot,
                kind="document",
                files=(("result.bin", batch.data(0)),),
                chat_id=7000000001,
                message_thread_id=None,
                reply_to_message_id=None,
                silent=False,
                protect_content=False,
                caption=None,
                plain=False,
                before_dispatch=lambda: asyncio.sleep(0),
            )
            assert outcome.message_ids == (10,) and outcome.failure is None, outcome
            assert len(backend.streams) == (3 if short_retry else 2)
            for stream in backend.streams[1:]:
                assert stream.payload_writes == 1 and stream.payload_chunk_lengths == [65_536, 65_536, 7] and stream.filename_sent
            assert budget.used == len(payload) and transport._pool._requests == []
            await batch.aclose()
            remaining_references = sys.getrefcount(payload)
            assert budget.used == 0 and remaining_references == original_references
    finally:
        await batch.aclose()
        await request.shutdown()


@pytest.mark.asyncio
async def test_real_sdk_cancel_during_wire_write_drains_before_reader_and_budget_release():
    payload = bytes(bytearray(range(256))) * 512 + b"the-end"
    original_references = sys.getrefcount(payload)
    entered, release = asyncio.Event(), asyncio.Event()
    backend = RecordedNetworkBackend(payload_size=len(payload), payload_sha256=hashlib.sha256(payload).hexdigest(), fail_close=False, expected_chunks=multipart_chunks(payload), write_entered=entered, write_release=release)
    _transport, request, bot = await real_bot(backend)
    budget = FileBytesBudget(len(payload))
    batch = OutputBatch(await budget.reserve_batch([len(payload)], timeout=1))
    await batch.keep(0, payload)
    task = None
    try:
        async with bot:
            task = asyncio.create_task(
                upload_output(
                    bot,
                    kind="document",
                    files=(("result.bin", batch.data(0)),),
                    chat_id=7000000001,
                    message_thread_id=None,
                    reply_to_message_id=None,
                    silent=False,
                    protect_content=False,
                    caption=None,
                    plain=False,
                    before_dispatch=lambda: asyncio.sleep(0),
                )
            )
            await asyncio.wait_for(entered.wait(), 15)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and budget.used == len(payload)
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert backend.streams[1].payload_chunk_lengths == [65_536]
            await batch.aclose()
            remaining_references = sys.getrefcount(payload)
            assert budget.used == 0 and remaining_references == original_references
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await batch.aclose()
        await request.shutdown()
