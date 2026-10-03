"""Library guidance follows a verified image declaration and the live provider."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError

from deerflow.config.app_config import AppConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.sandbox import sandbox_provider
from deerflow.sandbox.preinstalled import GUARANTEED_IMPORTS, preinstalled_libraries_section

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "docker" / "sandbox" / "Dockerfile"
AIO = "deerflow.community.aio_sandbox:AioSandboxProvider"
LOCAL = "deerflow.sandbox.local:LocalSandboxProvider"
VERIFIED_IMAGE = "registry.example/verified-sandbox@sha256:" + "a" * 64


@pytest.fixture(autouse=True)
def no_provider_initialization(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(sandbox_provider, "_default_sandbox_provider", None)

    def unexpected_initialization(*args, **kwargs):
        pytest.fail("rendering a prompt must not initialize a sandbox provider")

    monkeypatch.setattr(sandbox_provider, "resolve_class", unexpected_initialization)


@pytest.fixture
def render_prompt(monkeypatch: pytest.MonkeyPatch):
    from deerflow.agents.lead_agent import prompt as prompt_module

    monkeypatch.setattr(prompt_module, "get_skills_prompt_section", lambda *args, **kwargs: "")
    monkeypatch.setattr(prompt_module, "get_deferred_tools_prompt_section", lambda **kwargs: "")
    monkeypatch.setattr(prompt_module, "_build_acp_section", lambda **kwargs: "")
    monkeypatch.setattr(prompt_module, "_get_memory_context", lambda agent_name=None, **kwargs: "")
    monkeypatch.setattr(prompt_module, "get_agent_soul", lambda agent_name=None, **kwargs: "")

    def render(sandbox: SandboxConfig, *, bash_available=True, injected=True):
        config = AppConfig(sandbox=sandbox)
        monkeypatch.setattr("deerflow.config.get_app_config", lambda: config)
        return prompt_module.apply_prompt_template(app_config=config if injected else None, bash_available=bash_available)

    return render


def _verified_config(**overrides) -> SandboxConfig:
    return SandboxConfig(**{"use": AIO, "image": VERIFIED_IMAGE, "python_libraries_profile": "hartmesh", **overrides})


def test_the_prompt_promises_exactly_what_the_image_proves() -> None:
    text = DOCKERFILE.read_text(encoding="utf-8")
    matches = [m for m in re.findall(r"""python3 -c ['"]import ([^'"]+)['"]""", text) if "," in m]
    assert len(matches) == 1, "expected exactly one multi-library build assertion"
    assert list(GUARANTEED_IMPORTS) == [name.strip() for name in matches[0].split(",")]


@pytest.mark.parametrize("injected", [False, True])
def test_verified_profile_reaches_prompt_without_initializing_provider(render_prompt, injected):
    prompt = render_prompt(_verified_config(), injected=injected)
    for name in GUARANTEED_IMPORTS:
        assert name in prompt
    assert "python-docx" in prompt
    assert "Always importable in the sandbox:" in prompt
    assert "Do not run commands to check whether these exist" in prompt
    assert "import it and handle the failure" in prompt


@pytest.mark.parametrize(
    "sandbox",
    [SandboxConfig(use=LOCAL), SandboxConfig(use=AIO), SandboxConfig(use=AIO, image=VERIFIED_IMAGE), SandboxConfig(use=AIO, image="custom/sandbox:latest"), SandboxConfig(use="custom.provider:Sandbox")],
)
def test_unknown_environments_make_no_image_promise(render_prompt, sandbox):
    prompt = render_prompt(sandbox)
    assert "Always importable in the sandbox:" not in prompt
    assert "Python library availability depends on the configured environment" in prompt
    assert "handle a missing dependency" in prompt


@pytest.mark.parametrize("sandbox", [SandboxConfig(use=LOCAL), _verified_config()])
def test_no_bash_omits_library_execution_guidance(render_prompt, sandbox):
    prompt = render_prompt(sandbox, bash_available=False)
    assert "Always importable in the sandbox:" not in prompt
    assert "Python library availability depends" not in prompt
    assert "Do not run commands to check whether these exist" not in prompt


def test_section_without_configuration_is_honest():
    assert "Always importable in the sandbox:" not in preinstalled_libraries_section()


@pytest.mark.parametrize(
    "overrides",
    [
        {"use": LOCAL},
        {"use": "custom.provider:Sandbox"},
        {"provisioner_url": "http://provisioner:8002"},
        {"image": None},
        {"image": "custom/sandbox:latest"},
        {"image": "custom/sandbox@sha256:short"},
        {"python_libraries_profile": "unknown"},
    ],
)
def test_profile_rejects_unverified_provider_or_image_modes(overrides):
    with pytest.raises(ValidationError, match="python_libraries_profile"):
        _verified_config(**overrides)


@pytest.mark.parametrize("initial_profile,new_profile", [(None, "hartmesh"), ("hartmesh", None)])
def test_live_aio_profile_stays_with_its_startup_image_after_config_reload(monkeypatch, render_prompt, initial_profile, new_profile):
    from deerflow.community.aio_sandbox import aio_sandbox_provider

    initial = _verified_config(python_libraries_profile=initial_profile)
    monkeypatch.setattr(aio_sandbox_provider, "get_app_config", lambda: SimpleNamespace(sandbox=initial))
    # Run the real configuration loader without constructing Docker clients,
    # ownership stores or background lifecycle threads.
    provider = object.__new__(aio_sandbox_provider.AioSandboxProvider)
    provider._config = provider._load_config()
    assert provider.python_libraries_profile == initial_profile
    monkeypatch.setattr(sandbox_provider, "_default_sandbox_provider", provider)
    reloaded = _verified_config(python_libraries_profile=new_profile, image="registry.example/other@sha256:" + "b" * 64)
    prompt = render_prompt(reloaded)
    assert ("Always importable in the sandbox:" in prompt) == (initial_profile == "hartmesh")
    assert provider._config["image"] == VERIFIED_IMAGE


def test_live_local_provider_overrides_reloaded_hartmesh_declaration(monkeypatch, render_prompt):
    from deerflow.sandbox.local import LocalSandboxProvider

    provider = object.__new__(LocalSandboxProvider)
    monkeypatch.setattr(sandbox_provider, "_default_sandbox_provider", provider)
    prompt = render_prompt(_verified_config())
    assert "Always importable in the sandbox:" not in prompt
    assert "Python library availability depends" in prompt


def test_shipped_compose_profile_declares_verified_sandbox():
    config = yaml.safe_load((ROOT / "deploy" / "compose" / "config.yaml").read_text(encoding="utf-8"))
    sandbox = SandboxConfig.model_validate(config["sandbox"])
    assert sandbox.python_libraries_profile == "hartmesh"
    assert sandbox.image.startswith("ghcr.io/altakleos/hartmesh-sandbox@sha256:")
