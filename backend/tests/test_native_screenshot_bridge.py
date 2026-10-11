"""File effects run on the trusted host's loop and drain before close."""

import asyncio

import pytest

from deerflow.spaces.chat_bridge import ChatStorageHost


@pytest.mark.asyncio
async def test_screenshot_publication_uses_the_owned_loop_and_exact_arguments():
    calls = []
    loop = asyncio.get_running_loop()

    async def prepare(*_args):
        pytest.fail("Publication must not be confused with path preparation")

    async def publish(user_id, thread_id, *, name, content, frames):
        assert asyncio.get_running_loop() is loop
        calls.append((user_id, thread_id, name, content, frames))
        return "capture_1.png"

    host = ChatStorageHost(loop, prepare, publish=publish)
    try:
        assert await host.publish_async("alice", "chat-a", name="capture.png", content=b"PNG", frames=False) == "capture_1.png"
        assert calls == [("alice", "chat-a", "capture.png", b"PNG", False)]
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_cancelled_screenshot_call_drains_publisher_before_returning():
    entered, release, drained = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def publish(*_args, **_kwargs):
        entered.set()
        try:
            await release.wait()
        finally:
            await release.wait()
            drained.set()

    host = ChatStorageHost(asyncio.get_running_loop(), lambda *_args: None, publish=publish)
    task = asyncio.create_task(host.publish_async("alice", "chat-a", name="capture.png", content=b"PNG", frames=False))
    try:
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert drained.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await host.close()


@pytest.mark.asyncio
async def test_enabled_native_screenshots_without_owning_host_refuse_caller_paths(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from deerflow.community.browser_automation import tools
    from deerflow.spaces import chat_screenshots

    monkeypatch.setattr(chat_screenshots, "get_chat_storage_host", lambda: None)
    monkeypatch.setattr(chat_screenshots, "get_app_config", lambda: SimpleNamespace(storage_spaces=SimpleNamespace(enabled=True)))
    unqualified = tmp_path / "unqualified"
    runtime = SimpleNamespace(context={"thread_id": "chat-a"}, state={"thread_data": {"outputs_path": str(unqualified)}})
    result = await tools.browser_screenshot_tool.coroutine(runtime=runtime, tool_call_id="capture")
    assert "owning Gateway host" in result.update["messages"][0].content
    assert not unqualified.exists()


@pytest.mark.asyncio
async def test_ai_employee_execution_never_selects_human_screenshot_storage(monkeypatch):
    from deerflow_extension_api.storage import StorageUnsupported

    from deerflow.spaces import chat_screenshots

    monkeypatch.setattr(chat_screenshots, "current_environment", lambda: object())
    with pytest.raises(StorageUnsupported, match="AI employee"):
        await chat_screenshots.native_screenshot_writer("chat-a")
