"""The direct reader owns extraction capacity until its synchronous worker exits."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from deerflow.community.direct_fetch import tools as fetch_tools
from deerflow.community.direct_fetch.client import FetchedPage


def _page(text: str = "<p>article</p>", content_type: str = "text/html") -> FetchedPage:
    return FetchedPage(url="https://example.org/", status_code=200, content_type=content_type, text=text)


@pytest.mark.anyio
async def test_extraction_and_markdown_conversion_share_a_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    loop_thread = threading.get_ident()
    threads: list[int] = []

    def markdown() -> str:
        threads.append(threading.get_ident())
        return "x" * 5000

    def extract(text: str) -> SimpleNamespace:
        assert text == "<p>article</p>"
        threads.append(threading.get_ident())
        return SimpleNamespace(to_markdown=markdown)

    monkeypatch.setattr(fetch_tools._readability, "extract_article", extract)
    assert await fetch_tools._extract(_page()) == "x" * 4096
    assert len(threads) == 2
    assert threads[0] == threads[1] != loop_thread


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["article", "markdown"])
async def test_cancelled_extraction_keeps_capacity_until_its_worker_finishes(monkeypatch: pytest.MonkeyPatch, phase: str) -> None:
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    attempted = asyncio.Event()
    release = threading.Event()
    lock = threading.Lock()
    active = peak = workers = acquisitions = fetches = 0

    class ObservedSlots(asyncio.Semaphore):
        async def acquire(self) -> bool:
            nonlocal acquisitions
            acquisitions += 1
            if acquisitions == 8:
                attempted.set()
            return await super().acquire()

    slots = ObservedSlots(4)

    def hold_worker() -> None:
        nonlocal active, peak, workers
        # A synchronous Markdown conversion on the event loop must fail before
        # waiting, otherwise the test itself would prevent its release signal.
        assert threading.get_ident() != loop_thread
        with lock:
            active += 1
            workers += 1
            peak = max(peak, active)
            if workers == 4:
                loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(10), "test did not release extraction workers"
        finally:
            with lock:
                active -= 1

    def markdown() -> str:
        if phase == "markdown":
            hold_worker()
        return "article"

    def extract(_text: str) -> SimpleNamespace:
        if phase == "article":
            hold_worker()
        return SimpleNamespace(to_markdown=markdown)

    async def fetch(_url: str) -> FetchedPage:
        nonlocal fetches
        fetches += 1
        return _page()

    loop_thread = threading.get_ident()
    monkeypatch.setattr(fetch_tools, "_fetch_slots", lambda: slots)
    monkeypatch.setattr(fetch_tools, "get_app_config", lambda: None)
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _cfg: SimpleNamespace(fetch=fetch))
    monkeypatch.setattr(fetch_tools._readability, "extract_article", extract)
    first = [asyncio.create_task(fetch_tools.web_fetch_tool.coroutine(f"https://example.org/{index}")) for index in range(4)]
    second = []
    try:
        await asyncio.wait_for(started.wait(), 5)
        for task in first:
            task.cancel()
        await asyncio.sleep(0)  # Deliver cancellation while all four workers remain held.
        for task in first:
            task.cancel()  # Repeated cancellation must not unwind capacity either.
        second = [asyncio.create_task(fetch_tools.web_fetch_tool.coroutine(f"https://example.org/next/{index}")) for index in range(4)]
        await asyncio.wait_for(attempted.wait(), 5)
        assert all(not task.done() for task in first), "cancellation escaped while an owned worker was still running"
        assert fetches == 4, "replacement requests started while cancelled workers still owned all slots"
    finally:
        release.set()
        results = await asyncio.gather(*first, *second, return_exceptions=True)

    assert all(isinstance(result, asyncio.CancelledError) for result in results[:4])
    assert results[4:] == ["article"] * 4
    assert workers == 8 and peak == 4 and active == 0


@pytest.mark.anyio
async def test_cancellation_during_network_io_still_releases_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    fetching = asyncio.Event()
    slots = asyncio.Semaphore(1)

    async def fetch(_url: str) -> FetchedPage:
        fetching.set()
        await asyncio.Event().wait()
        raise AssertionError("network request should have been cancelled")

    monkeypatch.setattr(fetch_tools, "_fetch_slots", lambda: slots)
    monkeypatch.setattr(fetch_tools, "get_app_config", lambda: None)
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _cfg: SimpleNamespace(fetch=fetch))
    task = asyncio.create_task(fetch_tools.web_fetch_tool.coroutine("https://example.org/"))
    await asyncio.wait_for(fetching.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    await asyncio.wait_for(slots.acquire(), 5)
    slots.release()


@pytest.mark.anyio
async def test_cancelled_queued_fetch_never_starts_network_work(monkeypatch: pytest.MonkeyPatch) -> None:
    slots = asyncio.Semaphore(1)
    await slots.acquire()

    def client(_cfg: object) -> None:
        raise AssertionError("a cancelled queued fetch must not start a request")

    monkeypatch.setattr(fetch_tools, "_fetch_slots", lambda: slots)
    monkeypatch.setattr(fetch_tools, "get_app_config", lambda: None)
    monkeypatch.setattr(fetch_tools, "_client_from_config", client)
    task = asyncio.create_task(fetch_tools.web_fetch_tool.coroutine("https://example.org/"))
    try:
        await asyncio.sleep(0)  # Let the task reach the occupied slot.
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    finally:
        slots.release()


@pytest.mark.anyio
async def test_network_failure_releases_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    slots = asyncio.Semaphore(1)

    async def fetch(_url: str) -> FetchedPage:
        raise OSError("network failed")

    monkeypatch.setattr(fetch_tools, "_fetch_slots", lambda: slots)
    monkeypatch.setattr(fetch_tools, "get_app_config", lambda: None)
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _cfg: SimpleNamespace(fetch=fetch))
    with pytest.raises(OSError, match="network failed"):
        await fetch_tools.web_fetch_tool.coroutine("https://example.org/")
    await asyncio.wait_for(slots.acquire(), 5)
    slots.release()


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["article", "markdown"])
async def test_worker_failures_release_capacity(monkeypatch: pytest.MonkeyPatch, phase: str) -> None:
    slots = asyncio.Semaphore(1)

    def markdown() -> str:
        raise ValueError("conversion failed")

    def extract(_text: str) -> SimpleNamespace:
        if phase == "article":
            raise ValueError("conversion failed")
        return SimpleNamespace(to_markdown=markdown)

    async def fetch(_url: str) -> FetchedPage:
        return _page()

    monkeypatch.setattr(fetch_tools, "_fetch_slots", lambda: slots)
    monkeypatch.setattr(fetch_tools, "get_app_config", lambda: None)
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _cfg: SimpleNamespace(fetch=fetch))
    monkeypatch.setattr(fetch_tools._readability, "extract_article", extract)
    with pytest.raises(ValueError, match="conversion failed"):
        await fetch_tools.web_fetch_tool.coroutine("https://example.org/")
    await asyncio.wait_for(slots.acquire(), 5)
    slots.release()


@pytest.mark.anyio
async def test_plain_text_skips_extraction_and_keeps_the_result_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    def extract(_text: str) -> None:
        raise AssertionError("plain text should not need HTML extraction")

    monkeypatch.setattr(fetch_tools._readability, "extract_article", extract)
    assert await fetch_tools._extract(_page("x" * 5000, "text/plain")) == "x" * 4096
