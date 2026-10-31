"""The actual provider SDKs under strict I/O: cold import and TLS setup stay off-loop."""

from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from app.telegram_bots import voice


@pytest.mark.asyncio
async def test_openai_note_keeps_sdk_setup_off_the_gateway_loop(monkeypatch):
    import openai

    def respond(request):
        return httpx.Response(200, json={"text": "heard"})

    actual = openai.AsyncOpenAI
    monkeypatch.setattr(openai, "AsyncOpenAI", lambda **kwargs: actual(http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond)), **kwargs))
    assert await voice.OpenAITranscriber(api_key="synthetic-key", model="synthetic-model").transcribe(b"audio", timeout=5) == "heard"


@pytest.mark.asyncio
async def test_gemini_note_keeps_sdk_setup_off_the_gateway_loop(monkeypatch):
    from google.genai import types

    def respond(request):
        return httpx.Response(200, json={"candidates": [{"content": {"role": "model", "parts": [{"text": "heard"}]}}]})

    actual = types.HttpOptions
    monkeypatch.setattr(types, "HttpOptions", lambda **kwargs: actual(httpx_async_client=httpx.AsyncClient(transport=httpx.MockTransport(respond)), **kwargs))
    assert await voice.GeminiTranscriber(api_key="synthetic-key", model="synthetic-model").transcribe(b"audio", timeout=5) == "heard"


def test_local_midnight_is_the_persons_own_day():
    moment = datetime(2030, 6, 15, 16, 0, tzinfo=UTC)
    assert voice._local_midnight(SimpleNamespace(timezone="Asia/Tokyo"), moment) == datetime(2030, 6, 15, 15, 0, tzinfo=UTC)
