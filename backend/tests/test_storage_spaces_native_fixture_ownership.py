"""An ambiguous create must not permit deleting an owned fixture's backing."""

import ast
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from _storage_spaces_native_test_support import prepare_consumer_container


def test_create_effect_before_lost_acknowledgement_preserves_fixture_uncertainty(tmp_path):
    plan = SimpleNamespace(id="a" * 32, host_id="fixture", views=())
    effect = tmp_path / "created-container"
    containers, containment = [], {"confirmed": True}

    def create_then_error(arguments, **_kwargs):
        effect.write_text("task-owned stopped container", encoding="utf-8")
        raise subprocess.TimeoutExpired(arguments, 30)

    with pytest.raises(subprocess.TimeoutExpired):
        prepare_consumer_container(plan, "fixture-image", "pass", containers, containment, runner=create_then_error)
    assert effect.exists() and not containers
    assert containment["confirmed"] is False


@pytest.mark.parametrize("previously_confirmed", [True, False])
def test_exact_capture_keeps_owned_identity_and_never_clears_earlier_uncertainty(previously_confirmed):
    plan = SimpleNamespace(id="a" * 32, host_id="fixture", views=())
    containers, containment = [], {"confirmed": previously_confirmed}

    def acknowledged(arguments, **_kwargs):
        return subprocess.CompletedProcess(arguments, 0, stdout="c" * 64 + "\n")

    assert prepare_consumer_container(plan, "fixture-image", "pass", containers, containment, runner=acknowledged) == "c" * 64
    assert containers == [("c" * 64, plan.id)]
    assert containment["confirmed"] is previously_confirmed


def test_resident_native_fixture_uses_real_provider_registry_and_dedicated_inodes():
    # Parse as data. Importing/collecting the mounted-volume suite locally is
    # forbidden, including merely to borrow one of its fixtures.
    tree = ast.parse(Path(__file__).with_name("test_storage_spaces_volume_native.py").read_text(encoding="utf-8"))
    functions = {node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    setup = ast.unparse(functions["resident_runtime"])
    assert "run_file_io(AioSandboxProvider)" in setup and "ResidentRegistry(" in setup and "RunManager()" in setup
    assert "qualified_attachments(" in setup and "settle_contained(" in setup
    assert "begin_restore(" in setup and "finish_restore(" in setup and "retained_claims(" in setup
    assert "external_native_claims" in setup and "shutdown" in setup and "unregister" in setup
    assert "4096" in ast.unparse(functions["resident_prepared"])
    native = ast.unparse(functions["test_agent_two_chats_use_one_qualified_home_with_real_aio_protocol"])
    assert "resident_runtime" in native and "object.__new__" not in native and "native_prepare(" in native
    containment = ast.unparse(functions["test_host_containment_removes_the_real_container_and_the_next_run_attaches_afresh"])
    for fact in ("contain_host", "fenced_by_kind", "fenced_by_id", "fence_reason", "fenced_at", "container_gone"):
        assert fact in containment
