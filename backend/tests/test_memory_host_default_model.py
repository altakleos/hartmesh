"""The model DeerMem's memory updates call is the host's default model as configured now.

The memory manager is built once per process, and the updater keeps what it
was handed. Handing it a model built then froze that model's key for the
process's life: a key replaced or removed afterwards (a ``config.yaml`` edit,
or a provider key an administrator changed in the product) went on being
used by every memory update until a restart. The hook hands over the
default model *as it is configured at each call* instead.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from deerflow.agents.memory import manager


class _Model:
    def __init__(self, key: str) -> None:
        self.key = key
        self.calls: list[tuple[object, object]] = []

    def invoke(self, prompt, config=None, **kwargs):
        self.calls.append((prompt, config))
        return SimpleNamespace(content=f"answered with {self.key}")


def test_each_memory_update_calls_the_default_model_as_configured_at_that_moment(monkeypatch: pytest.MonkeyPatch) -> None:
    configured = {"key": "first"}
    built: list[_Model] = []

    def create_chat_model(name=None, **kwargs):
        assert name is None, "the host's default model"
        model = _Model(configured["key"])
        built.append(model)
        return model

    monkeypatch.setattr("deerflow.models.create_chat_model", create_chat_model)
    host_llm = manager._host_default_llm()

    assert host_llm.invoke("prompt one", config={"run_name": "memory_agent"}).content == "answered with first"
    configured["key"] = "replaced"
    assert host_llm.invoke("prompt two", config={"run_name": "memory_agent"}).content == "answered with replaced"
    assert [model.calls for model in built[-2:]] == [[("prompt one", {"run_name": "memory_agent"})], [("prompt two", {"run_name": "memory_agent"})]]


def test_a_deployment_that_starts_with_no_model_gains_memory_updates_once_it_has_one(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    available = {"model": None}

    def create_chat_model(name=None, **kwargs):
        if available["model"] is None:
            raise ValueError("No chat models are configured")
        return available["model"]

    monkeypatch.setattr("deerflow.models.create_chat_model", create_chat_model)
    with caplog.at_level("WARNING"):
        host_llm = manager._host_default_llm()
    assert host_llm is not None
    assert "memory extraction" in caplog.text, "the operator still learns that no model exists now"

    with pytest.raises(ValueError):
        host_llm.invoke("too early")
    available["model"] = _Model("added later")
    assert host_llm.invoke("now").content == "answered with added later"


def test_deermem_hands_the_updater_the_current_default_model(monkeypatch: pytest.MonkeyPatch) -> None:
    from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem

    configured = {"key": "first"}
    monkeypatch.setattr("deerflow.models.create_chat_model", lambda name=None, **kwargs: _Model(configured["key"]))
    deer_mem = DeerMem.from_config({}, mode="middleware", host_llm_factory=manager._host_default_llm)
    updater_model = deer_mem._updater._llm
    configured["key"] = "replaced"
    assert updater_model.invoke("prompt").content == "answered with replaced"
