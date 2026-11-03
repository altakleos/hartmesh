"""A photo in an AI assistant or AI employee conversation reaches the model.

The agent's Home is only reachable through its workspace, which the run starts
before the graph. A conversation may have no saved workspace id yet, or one from
a replaced workspace. A real graph runs the sandbox and image middleware, the
image tool and the thread-state reducers over a real AI assistant execution, with
two photos viewed in one step. The workspace is a stand-in that reads the Home
directory, as its mount does.
"""

import asyncio
import base64
from types import SimpleNamespace

import pytest
from _agent_e2e_helpers import FakeToolCallingModel
from _storage_spaces_test_support import ALICE, operation
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from sqlalchemy import select
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances

import deerflow.sandbox.tools  # noqa: F401  # bind the real authorization gate before anything patches it
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agent_instances.runtime import InstanceSandboxProvider, execution_scope
from deerflow.agents.middlewares.view_image_middleware import ViewImageMiddleware
from deerflow.agents.thread_state import ThreadState
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.sandbox.lease import release_sandbox_execution_lease_async
from deerflow.sandbox.middleware import SandboxMiddleware
from deerflow.tools.builtins.view_image_tool import view_image_tool

PHOTOS = {"stone.jpg": b"\xff\xd8\xff\xe0" + b"stone" * 8, "geode.jpg": b"\xff\xd8\xff\xe0" + b"geode" * 8}
UPLOAD = "/mnt/user-data/uploads/stone.jpg"
JPEG = PHOTOS["stone.jpg"]
CURRENT = "attachment-now"


class RecordingModel(FakeToolCallingModel):
    """Answers from a script and keeps what each model call was sent."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        object.__setattr__(self, "received", [])

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.received.append(list(messages))
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def _viewing_both_photos():
    calls = [{"name": "view_image", "id": "call-" + name, "args": {"image_path": "/mnt/user-data/uploads/" + name}} for name in PHOTOS]
    return RecordingModel(responses=[AIMessage(content="", tool_calls=calls), AIMessage(content="An amethyst and a geode.")])


def _graph(model):
    return create_agent(model=model, tools=[view_image_tool], middleware=[SandboxMiddleware(), ViewImageMiddleware()], state_schema=ThreadState)


def _image_urls(messages):
    return [block["image_url"]["url"] for message in messages if isinstance(message, HumanMessage) and isinstance(message.content, list) for block in message.content if isinstance(block, dict) and block.get("type") == "image_url"]


def _data_url(content):
    return "data:image/jpeg;base64," + base64.b64encode(content).decode()


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
    for name, content in PHOTOS.items():
        (home / "uploads" / name).write_bytes(content)
    return execution, home


async def _turn(execution, home, saved, *, sync):
    """One turn as the worker runs it: the workspace is started before the graph."""
    context = {AGENT_EXECUTION_CONTEXT_KEY: execution, "thread_id": execution.thread_id, "user_id": ALICE.subject_id}
    model = _viewing_both_photos()
    graph = _graph(model)
    inputs = {"messages": [HumanMessage(content="What are these?")], "thread_data": execution.thread_paths, **saved}
    config = {"configurable": {"thread_id": execution.thread_id}}
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, Workspace(home))
        try:
            if sync:
                # A synchronous graph runs off the authority loop, as embedded callers do.
                state = await asyncio.to_thread(graph.invoke, inputs, config, context=context)
            else:
                state = await graph.ainvoke(inputs, config, context=context)
        finally:
            await release_sandbox_execution_lease_async(context)
    return state, model


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize(
    "saved",
    [{}, {"sandbox": {"sandbox_id": "attachment-replaced"}}, {"sandbox": {"sandbox_id": CURRENT}}],
    ids=["new-conversation", "workspace-replaced", "workspace-current"],
)
async def test_two_photos_reach_the_model_whatever_workspace_the_conversation_saved(instances, saved, sync):
    execution, home = await _agent_chat(instances)

    state, model = await _turn(execution, home, saved, sync=sync)

    assert [message.content for message in state["messages"] if isinstance(message, ToolMessage)] == ["Successfully read image"] * 2
    assert state["sandbox"] == {"sandbox_id": CURRENT}
    assert sorted(_image_urls(model.received[1])) == sorted(_data_url(content) for content in PHOTOS.values())


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

    assert await SandboxMiddleware().abefore_agent({"messages": [], "thread_data": thread_data}, runtime) is None
    assert await asyncio.to_thread(SandboxMiddleware().before_agent, {"messages": [], "thread_data": thread_data}, runtime) is None
    results = [
        await view_image_tool.coroutine(runtime, UPLOAD, "call-async"),
        await asyncio.to_thread(view_image_tool.func, runtime=runtime, image_path=UPLOAD, tool_call_id="call-sync"),
    ]

    assert [result.update["messages"][0].content for result in results] == ["Successfully read image"] * 2
    assert provider.acquired == 0
