"""The output facade uses the existing loop-safe, cancellation-owned HTTP owner."""

import asyncio
import threading

import httpx
import pytest
from test_channel_execution_output import ACK, TEXT, client_for, projection

from app.channels.execution import ChannelExecutionProtocolError
from app.telegram_bots import gateway_client

pytestmark = pytest.mark.asyncio


async def test_exact_output_decode_runs_off_loop_and_retains_original_read_lifetime(tmp_path, monkeypatch):
    path = tmp_path / "small-output-fixture.json"
    await asyncio.to_thread(path.write_text, "{}", encoding="utf-8")
    loop, thread = asyncio.get_running_loop(), threading.get_ident()
    workers = []
    actual = gateway_client._decode

    def decode(value):
        path.read_text(encoding="utf-8")
        workers.append(threading.get_ident())
        return actual(value)

    monkeypatch.setattr(gateway_client, "_decode", decode)
    calls = []

    async def respond(request):
        assert asyncio.get_running_loop() is loop
        calls.append(request)
        return httpx.Response(200, headers=ACK, json=projection())

    client, owner, admission, _ = client_for(respond)
    try:
        owner.close_run_admission()
        admission.permitted.remove("run_create")
        result = await client.get_run_output("run")
        assert result.source.text == TEXT
        assert workers and all(identity != thread for identity in workers)
        assert len(calls) == 1 and calls[0].method == "GET"
    finally:
        await owner.stop()


async def test_output_stop_joins_an_already_started_decoder_worker(tmp_path, monkeypatch):
    path = tmp_path / "small-output-fixture.json"
    await asyncio.to_thread(path.write_text, "{}", encoding="utf-8")
    entered, release = threading.Event(), threading.Event()
    actual = gateway_client._decode

    def decode(value):
        path.read_text(encoding="utf-8")
        entered.set()
        release.wait(10)
        return actual(value)

    monkeypatch.setattr(gateway_client, "_decode", decode)
    client, owner, _, _ = client_for(lambda request: httpx.Response(200, headers=ACK, json=projection()))
    reading = asyncio.create_task(client.get_run_output("run"))
    closing = None
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        closing = asyncio.create_task(owner.stop())
        await asyncio.sleep(0)
        closing.cancel()
        await asyncio.gather(closing, return_exceptions=True)
        closing = asyncio.create_task(owner.stop())
        done, _ = await asyncio.wait({closing}, timeout=0.1)
        assert not done, "The decoder worker must retire before client close completes"
        release.set()
        await asyncio.wait_for(closing, 3)
        (result,) = await asyncio.gather(reading, return_exceptions=True)
        assert isinstance(result, (asyncio.CancelledError, ChannelExecutionProtocolError))
    finally:
        release.set()
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        await asyncio.gather(reading, return_exceptions=True)
        await owner.stop()
