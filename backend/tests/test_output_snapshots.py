"""Output delivery has a scan budget independent of scratch workspace files."""

from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage

from deerflow.agents.middlewares.runtime_delivery_middleware import RuntimeDeliveryMiddleware
from deerflow.config.paths import Paths
from deerflow.runtime.runs.worker import _delivery_content_with_outputs, _delivery_error, _produced_output_paths
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.workspace_changes import WorkspaceChangeLimits, WorkspaceSnapshot, recorder, scanner


@pytest.fixture
def paths(tmp_path, monkeypatch):
    paths = Paths(tmp_path)
    monkeypatch.setattr(recorder, "get_paths", lambda: paths)
    paths.ensure_thread_dirs("thread", user_id=get_effective_user_id())
    return paths


@pytest.mark.anyio
async def test_large_workspace_cannot_hide_output_from_handover_or_verification(paths, monkeypatch):
    user_id = get_effective_user_id()
    workspace = paths.sandbox_work_dir("thread", user_id=user_id)
    for index in range(2001):
        (workspace / f"scratch-{index}.txt").write_text("scratch", encoding="utf-8")
    original = scanner._snapshot_file

    def output_only(root, *args, **kwargs):
        assert root.name == "outputs", "delivery must not hash scratch workspace bytes"
        return original(root, *args, **kwargs)

    monkeypatch.setattr(scanner, "_snapshot_file", output_only)
    middleware = RuntimeDeliveryMiddleware()
    runtime = SimpleNamespace(context={"thread_id": "thread", "run_id": "run"})
    before = await recorder.capture_output_snapshot("thread", user_id=user_id)
    await middleware.abefore_agent({}, runtime)
    (paths.sandbox_outputs_dir("thread", user_id=user_id) / "report.pdf").write_bytes(b"%PDF-1.7 result")
    update = await middleware.aafter_agent({"messages": [AIMessage("Done", id="answer")]}, runtime)
    assert update["artifacts"] == ["/mnt/user-data/outputs/report.pdf"]
    produced = await _produced_output_paths(before, thread_id="thread", user_id=user_id)
    assert produced == update["artifacts"]


@pytest.mark.anyio
async def test_output_limit_is_explicit_and_never_means_no_outputs(paths):
    user_id = get_effective_user_id()
    outputs = paths.sandbox_outputs_dir("thread", user_id=user_id)
    for index in range(2):
        (outputs / f"{index}.txt").write_text("result", encoding="utf-8")
    limits = WorkspaceChangeLimits(max_scanned_files=2)
    exact = await recorder.capture_output_snapshot("thread", user_id=user_id, limits=limits)
    assert not exact.truncated
    (outputs / "third.txt").write_text("result", encoding="utf-8")
    over = await recorder.capture_output_snapshot("thread", user_id=user_id, limits=limits)
    assert over.truncated
    produced = await _produced_output_paths(over, thread_id="thread", user_id=user_id)
    assert produced is None
    verdict = _delivery_content_with_outputs({"presented": 0}, produced)
    assert verdict["verification"]["scan_complete"] is False
    assert _delivery_error(verdict) is not None


@pytest.mark.anyio
async def test_output_snapshot_failure_is_unknown_not_empty(paths, monkeypatch):
    async def broken(*args, **kwargs):
        raise OSError("unreadable outputs")

    monkeypatch.setattr("deerflow.runtime.runs.worker.capture_output_snapshot", broken)
    assert await _produced_output_paths(WorkspaceSnapshot(), thread_id="thread", user_id=get_effective_user_id()) is None
    handed_over = ["/mnt/user-data/outputs/report.pdf"]
    verdict = _delivery_content_with_outputs({"presented": 0}, None, handed_over)
    assert verdict["presented_by_runtime"] == handed_over
    assert verdict["presented_paths"] == handed_over
    assert _delivery_error(verdict) is not None


@pytest.mark.anyio
async def test_custom_tool_spill_is_excluded_from_runtime_handover(paths):
    middleware = RuntimeDeliveryMiddleware(extra_excluded_dir_names=frozenset({"tool-cache"}))
    runtime = SimpleNamespace(context={"thread_id": "thread", "run_id": "run"})
    await middleware.abefore_agent({}, runtime)
    spill = paths.sandbox_outputs_dir("thread", user_id=get_effective_user_id()) / "tool-cache"
    spill.mkdir()
    (spill / "tool.txt").write_text("tool feedback", encoding="utf-8")
    assert await middleware.aafter_agent({"messages": [AIMessage("Done", id="answer")]}, runtime) is None


@pytest.mark.anyio
async def test_output_snapshot_keeps_user_scope_and_never_reads_symlink_target(paths, tmp_path):
    user_id = get_effective_user_id()
    outputs = paths.sandbox_outputs_dir("thread", user_id=user_id)
    secret = tmp_path / "outside.txt"
    secret.write_text("outside", encoding="utf-8")
    (outputs / "link.txt").symlink_to(secret)
    peer = paths.sandbox_outputs_dir("thread", user_id="another-user")
    peer.mkdir(parents=True)
    (peer / "peer.txt").write_text("private", encoding="utf-8")
    after = await recorder.capture_output_snapshot("thread", user_id=user_id)
    assert set(after.files) == {"/mnt/user-data/outputs/link.txt"}
    assert after.files["/mnt/user-data/outputs/link.txt"].sha256 is None
    assert await _produced_output_paths(WorkspaceSnapshot(), thread_id="thread", user_id=user_id) == []


@pytest.mark.anyio
async def test_empty_directories_also_have_a_scan_budget(paths):
    outputs = paths.sandbox_outputs_dir("thread", user_id=get_effective_user_id())
    for index in range(3):
        (outputs / str(index)).mkdir()
    snapshot = await recorder.capture_output_snapshot("thread", user_id=get_effective_user_id(), limits=WorkspaceChangeLimits(max_scanned_files=2))
    assert snapshot.truncated
    assert snapshot.files == {}


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["read", "walk"])
async def test_output_scan_errors_are_incomplete_not_empty(paths, monkeypatch, failure):
    outputs = paths.sandbox_outputs_dir("thread", user_id=get_effective_user_id())
    (outputs / "report.txt").write_text("result", encoding="utf-8")
    if failure == "read":
        monkeypatch.setattr(scanner, "_snapshot_file", lambda *args, **kwargs: None)
    else:

        def denied_walk(*args, onerror, **kwargs):
            onerror(PermissionError("read refused"))
            return iter(())

        monkeypatch.setattr(scanner.os, "fwalk", denied_walk)
    assert await _produced_output_paths(WorkspaceSnapshot(), thread_id="thread", user_id=get_effective_user_id()) is None
