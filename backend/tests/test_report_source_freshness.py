"""The installed report handoff compares only the draft's currently admitted live inputs."""

import hashlib
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from _projection_support import legacy_report_package as legacy_report_package
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph.message import add_messages

from deerflow.config.paths import Paths, paths_scope

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Report file evidence requires descriptor-relative no-follow opens")
native_filesystem = pytest.mark.skipif(sys.platform != "linux", reason="Native Space evidence uses Linux openat2")

OWNER = "test-user-autouse"
REPORT = "reports/august/drafts/draft-1/august"
VIEW = "/mnt/user-data/outputs/" + REPORT + ".view.json"


@pytest.fixture(autouse=True)
def mounted_provider(monkeypatch, legacy_report_package):
    from hartmesh_legacy_report import freshness

    monkeypatch.setattr(freshness, "get_initialized_sandbox_provider", lambda: SimpleNamespace(uses_personal_and_shared_mounts=True))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def publish(paths, sources, *, presented=VIEW):
    base = paths.sandbox_outputs_dir("chat", user_id=OWNER) / REPORT
    rows = [{"name": Path(path.split("::")[0]).name, "sha256": sha(data), "sheet": path.partition("::")[2] or None} for path, data in sources]
    raw = json.dumps({"version": 1, "meta": {"draft": 1, "inputs": rows, "build": {"sources": [path for path, _ in sources]}}}).encode()
    evidence = json.dumps({"version": 1, "report_sha256": sha(raw), "fresh_inputs": True, "sources": [path for path, _ in sources], "inputs": rows}).encode()
    write(base.parent / "input-sources.json", evidence)
    write(base.with_suffix(".report.json"), raw)
    write(base.with_suffix(".view.json"), b"{}")
    write(base.parent / "renders.json", json.dumps({"version": 2, "base": "august", "bundle": "draft-1", "draft": 1, "sha256": {"august.report.json": sha(raw), "august.view.json": sha(b"{}"), "input-sources.json": sha(evidence)}}).encode())
    return [HumanMessage("Build the August report.", id="h1"), ToolMessage("Presented", name="bash", id="t1", tool_call_id="call1", additional_kwargs={"presented_files": [presented]}), AIMessage("Here is the report.", id="a1")]


def next_turn(messages):
    return [*messages, HumanMessage("Make the summary shorter.", id="h2"), AIMessage("Here is the shorter summary.", id="a2")]


async def run(messages, **context):
    from hartmesh_legacy_report.freshness import ReportFreshnessMiddleware

    result = await ReportFreshnessMiddleware().aafter_agent({"messages": messages}, SimpleNamespace(context={"thread_id": "chat", **context}))
    return add_messages(messages, result["messages"]) if result else messages


@pytest.mark.asyncio
async def test_changed_live_bytes_appear_in_next_reply_and_preserve_draft(tmp_path):
    paths = Paths(tmp_path)
    personal = write(paths.user_files_dir(OWNER) / "Sales/Jobs.xlsx", b"old")
    shared = write(paths.shared_dir() / "Jobs.xlsx", b"new")
    imported = write(paths.sandbox_uploads_dir("chat", user_id=OWNER) / "copy.xlsx", b"old")
    sources = [("/mnt/user-data/files/Sales/Jobs.xlsx::Jobs", b"old"), ("/mnt/user-data/shared/Jobs.xlsx", b"new"), ("/mnt/user-data/uploads/copy.xlsx", b"old")]
    with paths_scope(paths):
        first = await run(publish(paths, sources))
        draft = paths.sandbox_outputs_dir("chat", user_id=OWNER) / (REPORT + ".report.json")
        snapshot = draft.read_bytes()
        assert (await run(next_turn(first)))[-1].content == "Here is the shorter summary."
        personal.with_suffix(".tmp").write_bytes(b"new")
        personal.with_suffix(".tmp").replace(personal)
        shared.write_bytes(b"old")
        imported.write_bytes(b"new")
        answer = (await run(next_turn(first)))[-1].content
        assert "My files/Sales/Jobs.xlsx" in answer and "Shared/Jobs.xlsx" in answer
        assert "rebuild" in answer and "copy.xlsx" not in answer
        assert draft.read_bytes() == snapshot


@pytest.mark.asyncio
async def test_missing_files_links_unsafe_and_forged_handoffs_are_silent(tmp_path):
    paths = Paths(tmp_path)
    root = paths.user_files_dir(OWNER)
    outside = write(tmp_path / "hidden.csv", b"changed")
    root.mkdir(parents=True)
    (root / "link.csv").symlink_to(outside)
    sources = [(path, b"old") for path in ("/mnt/user-data/files/missing.csv", "/mnt/user-data/files/link.csv", "/mnt/user-data/files/../hidden.csv", str(outside), "/mnt/user-data/workspace/hidden.csv")]
    with paths_scope(paths):
        first = await run(publish(paths, sources))
        assert (await run(next_turn(first)))[-1].content == "Here is the shorter summary."


def test_gateway_strips_presentation_metadata_on_replayed_messages():
    from app.gateway.services import _normalize_input_messages

    kwargs = {"presented_files": [VIEW], "presented_by": "runtime"}
    [message] = _normalize_input_messages([AIMessage("Delivered", additional_kwargs=kwargs)], location="input.messages")
    assert not kwargs.keys() & message.additional_kwargs.keys()


def volume(root, name):
    from deerflow.spaces.backings import VerifiedVolume

    data, control = root / name / "data", root / name / "control"
    data.mkdir(parents=True)
    control.mkdir(mode=0o700)
    spec = SimpleNamespace(slot_id=name, filesystem_uuid=name, mount_path=data.parent)
    return VerifiedVolume(spec, 1, data.stat().st_dev, data.stat().st_ino, control.stat().st_ino, 100, 100, 100, 100, 1, 1)


@pytest.mark.asyncio
@native_filesystem
async def test_managed_scope_refresh_after_hash_and_shared_reassignment(tmp_path, monkeypatch):
    from hartmesh_legacy_report import freshness

    from deerflow.spaces.chat_paths import ChatStoragePaths

    base = Paths(tmp_path)
    personal, company, other = (volume(tmp_path, name) for name in ("personal", "company", "other"))
    active = ChatStoragePaths(base, user_id=OWNER, user=personal, company=company)
    revoked = ChatStoragePaths(base, user_id=OWNER, user=personal)
    replacement = ChatStoragePaths(base, user_id=OWNER, user=personal, company=other)
    mount_sequence = []

    async def prepare(owner, thread):
        assert owner == OWNER and thread == "chat"
        selected = mount_sequence.pop(0)
        return SimpleNamespace(paths=selected, mounts=selected.mounts(thread, user_id=owner))

    monkeypatch.setattr(freshness, "get_chat_storage_host", lambda: SimpleNamespace(prepare_async=prepare))
    source = write(company.data_path / "shared/work.csv", b"old")
    write(other.data_path / "shared/work.csv", b"new")
    with paths_scope(active):
        mount_sequence.extend([active] * 3)
        first = await run(publish(active, [("/mnt/user-data/shared/work.csv", b"old")]))
        assert not mount_sequence
        source.write_bytes(b"new")
        mount_sequence.extend([active, active])
        assert "Shared/work.csv" in (await run(next_turn(first)))[-1].content
        mount_sequence.extend([active, revoked])
        assert "Shared" not in (await run(next_turn(first)))[-1].content
        assert not mount_sequence
    with paths_scope(replacement):
        mount_sequence.extend([replacement, replacement])
        assert "Shared" not in (await run(next_turn(first)))[-1].content
    assert not mount_sequence


@pytest.mark.asyncio
async def test_new_presentation_replaces_old_evidence_and_filename_only_copy_is_silent(tmp_path):
    paths = Paths(tmp_path)
    source = write(paths.shared_dir() / "jobs.csv", b"old")
    with paths_scope(paths):
        first = await run(publish(paths, [("/mnt/user-data/shared/jobs.csv", b"old")]))
        source.write_bytes(b"new")
        copy = write(paths.sandbox_uploads_dir("chat", user_id=OWNER) / "jobs.csv", b"old")
        latest = publish(paths, [("/mnt/user-data/uploads/jobs.csv", copy.read_bytes())])
        latest = [m.model_copy(update={"id": "new-" + m.id}) for m in latest]
        current = await run(first + latest)
        assert (await run(next_turn(current)))[-1].content == "Here is the shorter summary."


async def exercise_web_stream(paths, source, owner):
    from deerflow_extension_api import AgentBuildContext, AgentScope
    from langchain.agents import create_agent
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langgraph.checkpoint.memory import InMemorySaver

    from deerflow.agents.thread_state import ThreadState
    from deerflow.extensions.isolation import IsolatedMiddleware
    from deerflow.extensions.loader import ExtensionSpec, load_extensions
    from deerflow.runtime.runs.worker import _LargeFileToolChunkBatcher, _publish_stream_item
    from deerflow.runtime.serialization import serialize
    from deerflow.runtime.stream_bridge import END_SENTINEL, MemoryStreamBridge
    from deerflow.tools.presentation import with_presentation
    from deerflow.tools.types import Runtime

    @tool("bash")
    def finish_export(present: list[str], runtime: Runtime):
        """Hand over the files written by the export command."""
        return with_presentation(runtime, "Wrote the report.", present=present, tool_call_id=runtime.tool_call_id)

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    loaded, diagnostics = load_extensions([ExtensionSpec(use="hartmesh_legacy_report:install", required=True, config={"enabled": True})])
    assert loaded.middleware_contributors == ()
    [(name, contributor)] = loaded.for_web_chat_lead().middleware_contributors
    [placement] = contributor.contribute_middlewares(loaded.app_store, AgentBuildContext(scope=AgentScope.LEAD))
    assert placement.scope == AgentScope.LEAD
    handoff = AIMessage("", tool_calls=[{"name": "bash", "args": {"present": [VIEW]}, "id": "call1", "type": "tool_call"}])
    model = Model(responses=[handoff, AIMessage("Draft ready."), AIMessage("Here is your summary.")])
    middleware = IsolatedMiddleware(placement.middleware, name, diagnostics.append)
    agent = create_agent(model=model, tools=[finish_export], state_schema=ThreadState, middleware=[middleware], checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": "chat"}}
    context = {"thread_id": "chat", "user_id": owner}
    first = await agent.ainvoke({"messages": [HumanMessage("Build the report.")], "thread_data": {"outputs_path": str(paths.sandbox_outputs_dir("chat", user_id=owner))}}, config=config, context=context)
    assert any(isinstance(m, ToolMessage) and m.additional_kwargs.get("presented_files") == [VIEW] for m in first["messages"])
    source.write_bytes(source.read_bytes() + b"\n")

    bridge = MemoryStreamBridge()
    batcher = _LargeFileToolChunkBatcher()
    # CHAT_RUN_STREAM_MODES uses messages-tuple, updates and custom, omitting values.
    async for mode, chunk in agent.astream({"messages": [HumanMessage("Make it shorter.", id="human-shorter")]}, config=config, context=context, stream_mode=["messages", "updates", "custom"]):
        await _publish_stream_item(bridge=bridge, run_id="run", mode=mode, chunk=chunk, namespace=(), file_tool_chunk_batcher=batcher, subagent_events=None)
    await bridge.publish_end("run")
    frames = [{"event": frame.event, "data": frame.data} async for frame in bridge.subscribe("run") if frame is not END_SENTINEL]
    final = (await agent.aget_state(config)).values
    assert not diagnostics
    return serialize(first, mode="values"), frames, serialize(final, mode="values")


@pytest.mark.asyncio
async def test_installed_extension_web_stream_and_settled_history(tmp_path):
    paths = Paths(tmp_path)
    with paths_scope(paths):
        source = write(paths.shared_dir() / "jobs.csv", b"old")
        publish(paths, [("/mnt/user-data/shared/jobs.csv", b"old")])
        _, frames, final = await exercise_web_stream(paths, source, OWNER)
    assert {"messages", "updates"} <= {frame["event"] for frame in frames}
    [answer] = [m for m in final["messages"] if "Here is your summary." in m["content"]]
    assert answer["content"].count("Shared/jobs.csv") == 1 and "rebuild" in answer["content"]
    assert any(frame["event"] == "updates" and "Shared/jobs.csv" in json.dumps(frame["data"]) and answer["id"] in json.dumps(frame["data"]) for frame in frames)


def test_ordinary_provider_mount_capability_is_distinct_from_threads():
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
    from deerflow.community.aio_sandbox.remote_backend import RemoteSandboxBackend
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider

    provider = object.__new__(AioSandboxProvider)
    provider._config = {"thread_data_mounts": True}
    provider._backend = object.__new__(RemoteSandboxBackend)
    assert provider.uses_thread_data_mounts and not provider.uses_personal_and_shared_mounts
    provider._backend = object.__new__(LocalContainerBackend)
    assert provider.uses_personal_and_shared_mounts and LocalSandboxProvider.uses_personal_and_shared_mounts


@pytest.mark.asyncio
async def test_only_local_provider_accepts_its_exact_host_path_and_no_other_owner(tmp_path, monkeypatch):
    from hartmesh_legacy_report import freshness

    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider

    provider = object.__new__(LocalSandboxProvider)
    monkeypatch.setattr(freshness, "get_initialized_sandbox_provider", lambda: provider)
    paths = Paths(tmp_path)
    own = write(paths.user_files_dir(OWNER) / "mine.csv", b"old")
    other = write(paths.user_files_dir("other") / "theirs.csv", b"new")
    with paths_scope(paths):
        first = await run(publish(paths, [(str(own), b"old"), (str(other), b"old")]))
        own.write_bytes(b"new")
        answer = (await run(next_turn(first)))[-1].content
        assert "My files/mine.csv" in answer and "theirs" not in answer and str(tmp_path) not in answer
        monkeypatch.setattr(freshness, "get_initialized_sandbox_provider", lambda: SimpleNamespace(uses_thread_data_mounts=True, uses_personal_and_shared_mounts=False))
        assert (await run(next_turn(first)))[-1].content == "Here is the shorter summary."


@pytest.mark.asyncio
async def test_tenth_input_and_implicit_workbook_sheet_and_unchecked_budget(tmp_path, monkeypatch):
    from hartmesh_legacy_report import freshness

    paths = Paths(tmp_path)
    sources = [(f"/mnt/user-data/uploads/{i}.csv", b"copy") for i in range(8)]
    sources += [("/mnt/user-data/shared/ninth.xlsx", b"old"), ("/mnt/user-data/files/tenth.xlsx::Second", b"old")]
    ninth = write(paths.shared_dir() / "ninth.xlsx", b"old")
    tenth = write(paths.user_files_dir(OWNER) / "tenth.xlsx", b"old")
    with paths_scope(paths):
        messages = publish(paths, sources)
        folder = paths.sandbox_outputs_dir("chat", user_id=OWNER) / Path(REPORT).parent
        canonical = folder / "august.report.json"
        report = json.loads(canonical.read_bytes())
        report["meta"]["inputs"][8]["sheet"] = "Jobs"
        # The compact supporting member is sufficient even with a huge Rows section.
        encoded = json.dumps({**report, "raw_rows": ["x" * 5000]}).encode()
        canonical.write_bytes(encoded)
        proof = {"version": 1, "report_sha256": sha(encoded), "sources": report["meta"]["build"]["sources"], "inputs": report["meta"]["inputs"]}
        raw = json.dumps(proof).encode()
        write(folder / "input-sources.json", raw)
        manifest = json.loads((folder / "renders.json").read_bytes())
        manifest["sha256"].update({"august.report.json": sha(encoded), "input-sources.json": sha(raw)})
        (folder / "renders.json").write_text(json.dumps(manifest), encoding="utf-8")
        monkeypatch.setattr(freshness, "_REPORT_LIMIT", 64)
        first = await run(messages)
        ninth.write_bytes(b"new")
        tenth.write_bytes(b"new")
        answer = (await run(next_turn(first)))[-1].content
        assert "Shared/ninth.xlsx" in answer and "My files/tenth.xlsx" in answer
        monkeypatch.setattr(freshness, "_INPUT_LIMIT", 3)
        limited = (await run(next_turn(first)))[-1].content
        assert "Shared/ninth.xlsx" in limited and "could not compare" in limited and "My files/tenth.xlsx" in limited
        tenth.unlink()
        after_removal = (await run(next_turn(first)))[-1].content
        assert "tenth" not in after_removal


@pytest.mark.asyncio
async def test_older_oversized_canonical_handoff_explains_explicit_check_without_source_names(tmp_path, monkeypatch):
    from hartmesh_legacy_report import freshness

    paths = Paths(tmp_path)
    with paths_scope(paths):
        messages = publish(paths, [("/mnt/user-data/shared/private.csv", b"old")], presented=VIEW.replace(".view.json", ".report.json"))
        monkeypatch.setattr(freshness, "_REPORT_LIMIT", 4)
        first = await run(messages)
        reply = (await run(next_turn(first)))[-1].content
        assert "too large" in reply and "check the draft explicitly" in reply and "private.csv" not in reply
        (paths.sandbox_outputs_dir("chat", user_id=OWNER) / (REPORT + ".report.json")).unlink()
        assert (await run(next_turn(first)))[-1].content == "Here is the shorter summary."


@pytest.mark.asyncio
@native_filesystem
@pytest.mark.parametrize("retained_render", [False, True])
async def test_representing_a_historical_bundle_does_not_rebind_shared_provenance(tmp_path, monkeypatch, retained_render):
    from hartmesh_legacy_report import freshness

    from deerflow.spaces.chat_paths import ChatStoragePaths

    base = Paths(tmp_path)
    personal, company, other = (volume(tmp_path, name) for name in ("personal", "company", "replacement"))
    earlier = ChatStoragePaths(base, user_id=OWNER, user=personal, company=company)
    later = ChatStoragePaths(base, user_id=OWNER, user=personal, company=other)
    active = earlier

    async def prepare(owner, thread):
        return SimpleNamespace(paths=active, mounts=active.mounts(thread, user_id=owner))

    monkeypatch.setattr(freshness, "get_chat_storage_host", lambda: SimpleNamespace(prepare_async=prepare))
    write(company.data_path / "shared/jobs.csv", b"old")
    write(other.data_path / "shared/jobs.csv", b"another file")
    with paths_scope(earlier):
        first = await run(publish(earlier, [("/mnt/user-data/shared/jobs.csv", b"old")]))
    active = later
    with paths_scope(later):
        shown_view = VIEW
        tool = "present_files"
        if retained_render:
            import shutil

            original = later.sandbox_outputs_dir("chat", user_id=OWNER) / Path(REPORT).parent
            regenerated = original.with_name("new-formats")
            shutil.copytree(original, regenerated)
            proof = json.loads((regenerated / "input-sources.json").read_bytes())
            raw = json.dumps({**proof, "fresh_inputs": False}).encode()
            write(regenerated / "input-sources.json", raw)
            manifest = json.loads((regenerated / "renders.json").read_bytes())
            manifest["sha256"]["input-sources.json"] = sha(raw)
            write(regenerated / "renders.json", json.dumps({**manifest, "bundle": "new-formats"}).encode())
            tool, shown_view = "bash", VIEW.replace("draft-1", "new-formats")
        again = ToolMessage("Attached", name=tool, tool_call_id="again", id="t2", additional_kwargs={"presented_files": [shown_view]})
        shown = await run([*first, HumanMessage("Show that draft again.", id="h2"), again, AIMessage("Here it is.", id="a2")])
        assert shown[-1].content == "Here it is."
        assert (await run(next_turn(shown)))[-1].content == "Here is the shorter summary."
