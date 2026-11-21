"""The mounted Shared fixture owns its post-crash recovery and retires only its reader."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.mark.parametrize("store, budget", [("redis", 480), ("memory", 240)])
def test_waits_for_the_effective_recovery_window_then_retires_the_reader(store, budget):
    from _compose_shared_reader import retire_reader

    compose, session = Mock(), Mock()
    compose.gateway.return_value = json.dumps({"store": store, "lease_seconds": 120, "recheck_seconds": 60, "startup_seconds": 120, "idle_seconds": 1800})
    compose.invoke.side_effect = [SimpleNamespace(stdout="original-container\nanother-container\n"), SimpleNamespace(stdout="another-container\n")]
    events = []

    def wait_ready(person, *, timeout=90):
        assert person is session
        events.append(("ready", timeout))
        if len(events) == 1:
            # The observed 90 s Home readiness check expired before eligibility.
            assert timeout > (120 * 2 + 60 if store == "redis" else 90)
        return {"execution": "ready"}

    def request(method, path, *, expected=200):
        events.append((method, path, expected))
        reply = Mock()
        reply.json.return_value = {"success": True}
        return reply

    session.request.side_effect = request
    evidence = {}
    retire_reader(compose, session, "reader-thread", "original-container", evidence, wait_ready=wait_ready)
    assert events == [("ready", budget), ("DELETE", "/api/threads/reader-thread", 200), ("GET", "/api/langgraph/threads/reader-thread", 404), ("ready", 90)]
    assert compose.invoke.call_args.args == (["docker", "ps", "-aq", "--no-trunc"],)
    assert evidence["reader_retirement"]["container_present_before_delete"]
    assert evidence["reader_retirement"]["container_absent"] and evidence["reader_retirement"]["execution_after"] == "ready"
    assert evidence["reader_retirement"]["eligibility_wait_seconds"] >= 0
    assert evidence["reader_retirement"]["authenticated_delete_seconds"] >= 0


@pytest.mark.parametrize("problem", ["unavailable", "forbidden", "still-present"])
def test_incomplete_cleanup_cannot_admit_the_next_rehearsal(problem):
    from _compose_shared_reader import retire_reader

    compose, session = Mock(), Mock()
    compose.gateway.return_value = json.dumps({"store": "redis", "lease_seconds": 120, "recheck_seconds": 60, "startup_seconds": 120, "idle_seconds": 0})
    compose.invoke.return_value.stdout = "original-container\n"
    ready = Mock(return_value={"execution": "ready"})
    if problem == "unavailable":
        ready.side_effect = AssertionError("execution unavailable")
    if problem == "forbidden":
        session.request.side_effect = AssertionError("403")
    session.request.return_value.json.return_value = {"success": True}
    with pytest.raises(AssertionError):
        retire_reader(compose, session, "reader-thread", "original-container", {}, wait_ready=ready)
    assert ready.call_count == 1
    assert session.request.call_count == {"unavailable": 0, "forbidden": 1, "still-present": 2}[problem]


def test_installed_policy_uses_runtime_inference_and_emits_only_numeric_limits_and_store(monkeypatch, capsys):
    from _compose_shared_reader import POLICY

    from deerflow import config
    from deerflow.config.stream_bridge_config import StreamBridgeConfig

    app = SimpleNamespace(sandbox=SimpleNamespace(ownership=None, idle_timeout=1800), stream_bridge=StreamBridgeConfig(type="redis", redis_url="redis://private.example/1"))
    monkeypatch.setattr(config, "get_app_config", lambda: app)
    exec(compile(POLICY, "<mounted reader policy>", "exec"), {})
    assert json.loads(capsys.readouterr().out) == {"store": "redis", "lease_seconds": 120, "recheck_seconds": 60, "startup_seconds": 120, "idle_seconds": 1800}
