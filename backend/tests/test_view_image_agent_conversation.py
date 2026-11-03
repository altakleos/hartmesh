"""A photo in an AI assistant or AI employee conversation reaches the model.

The agent's Home is only reachable through its workspace, which the run starts
before the first model call. Until a workspace tool has run, the conversation has
no saved workspace id, or has one from a replaced workspace. The real execution,
provider, lease and middleware run here; the workspace is a stand-in that reads
the Home directory, as its mount does.
"""

import asyncio
import base64
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, operation
from langchain_core.messages import AIMessage, ToolMessage
from sqlalchemy import select
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_view_image_middleware import _image_urls, _model_request, _view_image_call

import deerflow.sandbox.tools  # noqa: F401  # bind the real authorization gate before anything patches it
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agent_instances.runtime import InstanceSandboxProvider, execution_scope
from deerflow.agents.middlewares.view_image_middleware import ViewImageMiddleware
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.sandbox.lease import ensure_sandbox_lease_owner, get_sandbox_lease_manager
from deerflow.sandbox.middleware import SandboxMiddleware
from deerflow.sandbox.overwrite import unwrap_sandbox
from deerflow.tools.builtins.view_image_tool import view_image_tool

JPEG = b"\xff\xd8\xff\xe0" + b"stone" * 8
UPLOAD = "/mnt/user-data/uploads/stone.jpg"
CURRENT = "attachment-now"


class Workspace:
    """The agent's running workspace: its Home holds what the tool asks for."""

    def __init__(self, home):
        self.id, self.home = CURRENT, home

    def download_file(self, path):
        name = path.removeprefix("/mnt/user-data/uploads/")
        if name == path or "/" in name:
            raise FileNotFoundError(path)
        return (self.home / "uploads" / name).read_bytes()

    def release_command_scope(self, scope_id):
        """The run's lease ends; this stand-in keeps no shell sessions."""


async def _agent_chat(instances):
    agents, authority, _, sf = await conversations(instances)
    instance = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=instance.id, thread_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    async with sf() as session:
        backing = await session.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == instance.home_id))
    home = instances[3].verified[backing.slot_id].data_path
    (home / "uploads").mkdir(exist_ok=True)
    (home / "uploads" / "stone.jpg").write_bytes(JPEG)
    return execution, home


async def _view_then_answer(execution, home, saved, *, sync):
    """One tool call and the model call after it, as the graph runs them."""
    context = {AGENT_EXECUTION_CONTEXT_KEY: execution, "thread_id": execution.thread_id, "user_id": ALICE.subject_id}
    owner_id = ensure_sandbox_lease_owner(context)
    state = {"thread_data": execution.thread_paths, "messages": [], **saved}
    runtime = SimpleNamespace(context=context, state=state, config={})
    request = SimpleNamespace(runtime=runtime, tool_call=_view_image_call("call-image", UPLOAD))
    seen = []

    async def model(model_request):
        seen.append(model_request)
        return AIMessage(content="An amethyst geode.")

    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, Workspace(home))
        try:
            if sync:
                # A synchronous tool runs off the authority loop, as a graph worker thread does.
                result = await asyncio.to_thread(SandboxMiddleware().wrap_tool_call, request, lambda call: view_image_tool.func(runtime=call.runtime, image_path=UPLOAD, tool_call_id="call-image"))
            else:

                async def handler(call):
                    return await view_image_tool.coroutine(call.runtime, UPLOAD, "call-image")

                result = await SandboxMiddleware().awrap_tool_call(request, handler)
            assert result.update["messages"][0].content == "Successfully read image"
            sandbox, _ = unwrap_sandbox(result.update.get("sandbox"))
            assert sandbox == {"sandbox_id": CURRENT}
            assert result.update["viewed_images"][UPLOAD]["source_sandbox_id"] == CURRENT
            messages = [AIMessage(content="", tool_calls=[_view_image_call("call-image", UPLOAD)]), ToolMessage(content="Successfully read image", tool_call_id="call-image")]
            model_request = _model_request(messages, result.update["viewed_images"])
            model_request.state.update(thread_data=execution.thread_paths, sandbox=sandbox)
            model_request.runtime.context = context
            await ViewImageMiddleware().awrap_model_call(model_request, model)
        finally:
            await get_sandbox_lease_manager(environment.provider).release_async(owner_id)
    return _image_urls(seen[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize(
    "saved",
    [{}, {"sandbox": {"sandbox_id": "attachment-replaced"}}],
    ids=["new-conversation", "workspace-replaced"],
)
async def test_a_photo_reaches_the_model_before_any_workspace_tool_has_run(instances, saved, sync):
    execution, home = await _agent_chat(instances)

    urls = await _view_then_answer(execution, home, saved, sync=sync)

    assert urls == ["data:image/jpeg;base64," + base64.b64encode(JPEG).decode()]


class RefusingProvider:
    """An ordinary chat's provider: an image view must not start a sandbox."""

    def __init__(self):
        self.acquired = 0

    def get(self, sandbox_id):
        return None

    def acquire(self, *args, **kwargs):
        self.acquired += 1
        raise AssertionError("an ordinary chat started a sandbox to view an image")

    async def acquire_async(self, *args, **kwargs):
        return self.acquire()


@pytest.mark.asyncio
async def test_an_ordinary_chat_reads_its_host_copy_without_starting_a_sandbox(tmp_path, monkeypatch):
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    (uploads / "stone.jpg").write_bytes(JPEG)
    thread_data = {"workspace_path": str(tmp_path / "workspace"), "uploads_path": str(uploads), "outputs_path": str(tmp_path / "outputs")}
    provider = RefusingProvider()
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_baseline_sandbox_provider", lambda **kwargs: provider)
    runtime = SimpleNamespace(state={"thread_data": thread_data}, context={"thread_id": "thread-1"}, config={})

    results = [
        await view_image_tool.coroutine(runtime, UPLOAD, "call-async"),
        await asyncio.to_thread(view_image_tool.func, runtime=runtime, image_path=UPLOAD, tool_call_id="call-sync"),
    ]

    assert [result.update["messages"][0].content for result in results] == ["Successfully read image"] * 2
    assert provider.acquired == 0
