"""Operator-managed model configuration for the tenant VM compose profile.

The profile renders the tenant's effective ``config.yaml`` at every Gateway
start from release material: ``deploy/compose/config.yaml`` plus the provider
catalog under ``deploy/compose/providers``. Both are mounted read-only from the
bundle, so choosing which models a tenant gets used to mean editing release
files and cutting a release.

These tests pin the escape hatch: one optional ``.env`` key,
``HARTMESH_MODELS_FILE``, naming a YAML file on the tenant's own data disk that
declares ``models:`` and nothing else. Absent, nothing changes. Present, that
list is the whole rendered ``models:`` section -- provider keys no longer add
bundled models to it -- and it is validated (shape, installed client class,
unique names, resolvable credential references) before anything is written.

Model ids here are fictitious on purpose: a test that passed by naming a real
provider model could also pass by someone extending the bundled catalog, which
is the thing this contract exists to avoid.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

# Several tests here load a rendered profile with ``AppConfig.from_file``, which
# writes process-wide singletons ``reset_app_config()`` does not restore.
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
COMPOSE = PROFILE / "compose.yaml"
TEMPLATE = PROFILE / "config.yaml"
CATALOG = PROFILE / "providers"
README = PROFILE / "README.md"
MODELS_ENV = "HARTMESH_MODELS_FILE"
OPERATOR_MOUNT = "${HARTMESH_DATA_DIR}/operator"

# Two client families the release already installs (langchain-openai and
# langchain-anthropic are both harness dependencies), each pointed at an
# endpoint and a model id that appear in no bundled fragment, both drawing on
# one credential: several providers, several models, one key.
ACME_CHAT = {
    "name": "acme-lightning-1",
    "display_name": "Acme Lightning 1",
    "description": "Fictitious OpenAI-compatible endpoint",
    "use": "langchain_openai:ChatOpenAI",
    "model": "acme/lightning-1-2099",
    "base_url": "https://api.acme.invalid/openai",
    "api_key": "$ACME_API_KEY",
    "context_window": 262144,
    "max_tokens": 4096,
    "pricing": {"currency": "USD", "input_per_million": 1.25, "output_per_million": 5.0, "input_cache_hit_per_million": 0.125},
}
ACME_REASONER = {
    "name": "acme-anvil-9",
    "display_name": "Acme Anvil 9",
    "use": "langchain_anthropic:ChatAnthropic",
    "model": "acme-anvil-9-20991231",
    "base_url": "https://api.acme.invalid/anthropic",
    "api_key": "$ACME_API_KEY",
    "context_window": 200000,
    "max_tokens": 8192,
    "supports_thinking": True,
    "thinking": {"type": "enabled", "budget_tokens": 2048},
    "pricing": {"currency": "USD", "input_per_million": 3.0, "output_per_million": 15.0},
}


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def render_config() -> Iterator[ModuleType]:
    module = _load_module("hartmesh_render_config_operator_test", PROFILE / "gateway" / "render_config.py")
    try:
        yield module
    finally:
        sys.modules.pop("hartmesh_render_config_operator_test", None)


@pytest.fixture(scope="module")
def catalog(render_config: ModuleType) -> tuple:
    return render_config.load_catalog(CATALOG)


def _base_environ(**extra: str) -> dict[str, str]:
    environ = {
        "DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow",
        "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0",
    }
    environ.update(extra)
    return environ


def _write_models(tmp_path: Path, body: str, *, name: str = "models.yaml") -> Path:
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return path


def _operator_document(*models: dict[str, Any]) -> str:
    return yaml.safe_dump({"models": list(models)}, sort_keys=False, allow_unicode=True)


def _render(render_config: ModuleType, catalog: tuple, environ: dict[str, str]) -> dict[str, Any]:
    rendered, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), catalog, environ)
    return yaml.safe_load(rendered)


# ── 1. the interface and its default ─────────────────────────────────────────


def test_the_key_is_optional_and_its_absence_renders_the_bundled_catalogue(render_config: ModuleType, catalog: tuple) -> None:
    """No custom source: provider-key discovery, identities and order as before."""
    keys = {"OPENAI_API_KEY": "secret", "ANTHROPIC_API_KEY": "secret", "TAVILY_API_KEY": "secret"}
    document = _render(render_config, catalog, _base_environ(**keys))
    assert [model["name"] for model in document["models"]] == ["gpt-4", "gpt-5-responses", "claude-sonnet-4"]
    # An empty value is the same as an unset one, so a commented-out or blanked
    # key in an existing .env is still "no custom source" rather than a refusal.
    blank = _render(render_config, catalog, _base_environ(**keys, HARTMESH_MODELS_FILE="   "))
    assert blank == document


def test_no_new_value_is_required_of_an_existing_tenant_env() -> None:
    example = (PROFILE / ".env.example").read_text(encoding="utf-8")
    assert MODELS_ENV not in example, "the key is an escape hatch, not part of what onboarding writes"
    assert "${" + MODELS_ENV not in COMPOSE.read_text(encoding="utf-8"), "it reaches the Gateway through env_file; compose.yaml interpolating it would make an unset key a render-time hole"


def test_compose_mounts_the_operator_directory_read_only_beside_the_data_directory() -> None:
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    volumes = compose["services"]["gateway"]["volumes"]
    mount = next(volume for volume in volumes if volume.startswith(OPERATOR_MOUNT + ":"))
    source, target, mode = mount.split(":")
    assert source == target == OPERATOR_MOUNT, "same path inside and out, so the .env value is a real host path"
    assert mode == "ro", "operator material is deployment input; the Gateway never writes it"
    assert not target.startswith("${HARTMESH_DATA_DIR}/home"), "it must sit outside DEER_FLOW_HOME, which is what sandboxes bind-mount from"
    assert compose["services"]["gateway"]["volumes"].count(mount) == 1
    for service, definition in compose["services"].items():
        if service != "gateway":
            assert not any("operator" in volume for volume in definition.get("volumes", []) if isinstance(volume, str)), service


def test_the_readme_documents_the_key_and_its_directory() -> None:
    readme = README.read_text(encoding="utf-8")
    assert MODELS_ENV in readme
    assert "/srv/hartmesh/operator" in readme


# ── 2. authoritative selection ───────────────────────────────────────────────


def test_the_operator_list_is_the_whole_model_section_even_with_provider_keys_present(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    path = _write_models(tmp_path, _operator_document(ACME_CHAT, ACME_REASONER))
    environ = _base_environ(OPENAI_API_KEY="secret", ANTHROPIC_API_KEY="secret", TAVILY_API_KEY="secret", ACME_API_KEY="secret", **{MODELS_ENV: str(path)})
    document = _render(render_config, catalog, environ)
    assert [model["name"] for model in document["models"]] == ["acme-lightning-1", "acme-anvil-9"]
    # The keys are present and still buy nothing on the model side ...
    assert "gpt-4" not in yaml.safe_dump(document["models"])
    # ... while the separate tool-provider behaviour is untouched.
    tools = {tool["name"]: tool for tool in document["tools"]}
    assert tools["web_search"]["use"] == "deerflow.community.tavily.tools:web_search_tool"
    assert tools["web_fetch"]["use"] == "deerflow.community.jina_ai.tools:web_fetch_tool"
    # ... and so is every non-model setting of the profile.
    baseline = _render(render_config, catalog, _base_environ(OPENAI_API_KEY="secret", ANTHROPIC_API_KEY="secret", TAVILY_API_KEY="secret"))
    for section in ("auth", "database", "sandbox", "skills", "tools", "tool_plane", "deployment", "checkpointer"):
        assert document[section] == baseline[section], section
    assert "secret" not in yaml.safe_dump(document)


def test_a_provider_key_alone_adds_nothing_to_a_configured_list(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    path = _write_models(tmp_path, _operator_document(ACME_CHAT))
    without = _render(render_config, catalog, _base_environ(ACME_API_KEY="secret", **{MODELS_ENV: str(path)}))
    with_keys = _render(render_config, catalog, _base_environ(ACME_API_KEY="secret", OPENAI_API_KEY="secret", GEMINI_API_KEY="secret", **{MODELS_ENV: str(path)}))
    assert without["models"] == with_keys["models"] == [ACME_CHAT]


def test_operator_models_survive_the_config_parser_and_reach_the_model_list(render_config: ModuleType, catalog: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The shipped parser and the shipped ``GET /api/models`` projection."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.gateway.deps import get_config
    from app.gateway.routers.models import router
    from deerflow.config.app_config import AppConfig

    environ = _base_environ(ACME_API_KEY="not-a-real-key", **{MODELS_ENV: str(_write_models(tmp_path, _operator_document(ACME_CHAT, ACME_REASONER)))})
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    rendered, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), catalog, environ)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(rendered, encoding="utf-8")
    config = AppConfig.from_file(str(config_path))

    assert [model.name for model in config.models] == ["acme-lightning-1", "acme-anvil-9"]
    assert config.models[0].model == "acme/lightning-1-2099"
    assert config.models[0].context_window == 262144
    assert config.models[1].supports_thinking is True
    assert config.get_model_config("acme-anvil-9") is not None
    # The default model is the first entry, so the operator's order is the
    # tenant's default selection.
    assert config.models[0].name == "acme-lightning-1"

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_config] = lambda: config
    body = TestClient(app).get("/api/models").json()
    assert [model["name"] for model in body["models"]] == ["acme-lightning-1", "acme-anvil-9"]
    assert body["models"][0]["display_name"] == "Acme Lightning 1"
    serialized = yaml.safe_dump(body)
    assert "not-a-real-key" not in serialized and "api_key" not in serialized
    assert "pricing" not in serialized, "price metadata is for the cost display, not the model list"


# ── 3. empty-file and empty-list semantics ───────────────────────────────────


def test_an_empty_list_configures_no_models_and_is_not_an_absent_source(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    path = _write_models(tmp_path, "models: []\n")
    document = _render(render_config, catalog, _base_environ(OPENAI_API_KEY="secret", **{MODELS_ENV: str(path)}))
    assert document["models"] == [], "an explicit empty list is a choice: this tenant gets no models, key or no key"


def test_an_empty_file_refuses_and_names_the_empty_list(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    for body in ("", "   \n", "# only a comment\n"):
        path = _write_models(tmp_path, body)
        with pytest.raises(render_config.RenderError, match=r"models: \[\]"):
            _render(render_config, catalog, _base_environ(**{MODELS_ENV: str(path)}))


def test_a_models_key_with_no_value_refuses_rather_than_meaning_either_thing(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    path = _write_models(tmp_path, "models:\n")
    with pytest.raises(render_config.RenderError, match="list"):
        _render(render_config, catalog, _base_environ(**{MODELS_ENV: str(path)}))


# ── 4. refusals ──────────────────────────────────────────────────────────────


def test_a_missing_or_unreadable_source_refuses_instead_of_falling_back(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    missing = tmp_path / "typo.yaml"
    with pytest.raises(render_config.RenderError, match="operator") as missing_error:
        _render(render_config, catalog, _base_environ(**{MODELS_ENV: str(missing)}))
    assert str(missing) in str(missing_error.value)

    unreadable = tmp_path / "a-directory"
    unreadable.mkdir()
    with pytest.raises(render_config.RenderError):
        _render(render_config, catalog, _base_environ(**{MODELS_ENV: str(unreadable)}))


def test_a_source_that_is_not_the_documented_shape_refuses(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    cases = {
        "- name: x\n": "mapping",
        "models: {}\n": "list",
        "models:\n  - name: x\n    use: langchain_openai:ChatOpenAI\n    model: x\nsandbox:\n  image: evil\n": "unknown keys",
        "models:\n  - name: x\n    use: langchain_openai:ChatOpenAI\n    model: x\nauth:\n  local:\n    source_max_failures: 1\n": "unknown keys",
        "models:\n  - use: langchain_openai:ChatOpenAI\n    model: x\n": "named entries",
        "models:\n  - name: x\n    model: x\n": "use",
        "models:\n  - name: x\n    use: langchain_openai:ChatOpenAI\n": "model",
        "not: yaml: at all\n": "is not valid YAML",
    }
    for body, expected in cases.items():
        path = _write_models(tmp_path, body)
        with pytest.raises(render_config.RenderError) as error:
            _render(render_config, catalog, _base_environ(**{MODELS_ENV: str(path)}))
        assert expected in str(error.value), body


def test_a_duplicate_identity_refuses_and_names_it(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    twin = {**ACME_REASONER, "name": ACME_CHAT["name"]}
    path = _write_models(tmp_path, _operator_document(ACME_CHAT, twin))
    with pytest.raises(render_config.RenderError, match="acme-lightning-1"):
        _render(render_config, catalog, _base_environ(ACME_API_KEY="secret", **{MODELS_ENV: str(path)}))


def test_an_unresolved_credential_reference_refuses_and_prints_no_value(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    path = _write_models(tmp_path, _operator_document(ACME_CHAT))
    with pytest.raises(render_config.RenderError, match="ACME_API_KEY") as error:
        _render(render_config, catalog, _base_environ(**{MODELS_ENV: str(path)}))
    assert "secret" not in str(error.value)
    # The reference itself is what is written; the Gateway expands it.
    document = _render(render_config, catalog, _base_environ(ACME_API_KEY="secret", **{MODELS_ENV: str(path)}))
    assert document["models"][0]["api_key"] == "$ACME_API_KEY"


def test_a_client_class_the_release_does_not_install_refuses(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    cases = {
        "langchain_nonesuch:ChatNonesuch": "langchain_nonesuch",
        "langchain_openai:ChatNonesuch": "ChatNonesuch",
        "langchain_openai": "langchain_openai",
        "deerflow.config.model_config:ModelConfig": "BaseChatModel",
    }
    for use, expected in cases.items():
        path = _write_models(tmp_path, _operator_document({**ACME_CHAT, "use": use}))
        with pytest.raises(render_config.RenderError, match="acme-lightning-1") as error:
            _render(render_config, catalog, _base_environ(ACME_API_KEY="secret", **{MODELS_ENV: str(path)}))
        assert expected in str(error.value), use


def test_the_bundled_catalogue_is_never_client_checked(render_config: ModuleType, catalog: tuple) -> None:
    """The check is the operator file's, so no bundled fragment's import cost
    or availability becomes a new start requirement for existing tenants."""
    every_key = {fragment.env: "secret" for fragment in catalog}
    document = _render(render_config, catalog, _base_environ(**every_key))
    assert len(document["models"]) > 10


def test_a_refusal_leaves_the_last_valid_rendered_file_untouched(render_config: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``main`` writes atomically, so a bad update never truncates what runs."""
    output = tmp_path / "home" / "config.yaml"
    good = _write_models(tmp_path, _operator_document(ACME_CHAT), name="good.yaml")
    for name, value in _base_environ(ACME_API_KEY="secret", **{MODELS_ENV: str(good)}).items():
        monkeypatch.setenv(name, value)
    argv = ["--template", str(TEMPLATE), "--catalog", str(CATALOG), "--output", str(output)]
    assert render_config.main(argv) == 0
    valid = output.read_text(encoding="utf-8")
    assert "acme-lightning-1" in valid

    bad = _write_models(tmp_path, "models:\n  - name: broken\n", name="bad.yaml")
    monkeypatch.setenv(MODELS_ENV, str(bad))
    assert render_config.main(argv) == 1
    assert output.read_text(encoding="utf-8") == valid
    assert list(output.parent.iterdir()) == [output], "no half-written temporary file is left behind"


def test_check_mode_validates_an_update_without_writing_anything(render_config: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The documented pre-flight: validate the new file while the Gateway still
    runs the old one, so an invalid update never becomes a restart loop."""
    path = _write_models(tmp_path, _operator_document(ACME_CHAT))
    for name, value in _base_environ(ACME_API_KEY="secret", **{MODELS_ENV: str(path)}).items():
        monkeypatch.setenv(name, value)
    argv = ["--template", str(TEMPLATE), "--catalog", str(CATALOG), "--check"]
    assert render_config.main(argv) == 0
    assert list(tmp_path.iterdir()) == [path]

    path.write_text("models:\n  - name: broken\n", encoding="utf-8")
    assert render_config.main(argv) == 1


def test_rendering_opens_no_socket_at_all(render_config: ModuleType, catalog: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Boot must not depend on reaching a provider or a price service. The
    endpoints below are unresolvable on purpose, and the render still succeeds
    -- because it never asks anything about them."""
    import socket

    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("render_config opened a network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)

    path = _write_models(tmp_path, _operator_document(ACME_CHAT, ACME_REASONER))
    document = _render(render_config, catalog, _base_environ(ACME_API_KEY="secret", **{MODELS_ENV: str(path)}))
    assert [model["name"] for model in document["models"]] == ["acme-lightning-1", "acme-anvil-9"]


# ── 5. offline client construction ───────────────────────────────────────────


def _config_from(render_config: ModuleType, catalog: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *models: dict[str, Any]):
    from deerflow.config.app_config import AppConfig

    environ = _base_environ(ACME_API_KEY="not-a-real-key", **{MODELS_ENV: str(_write_models(tmp_path, _operator_document(*models)))})
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    rendered, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), catalog, environ)
    path = tmp_path / "config.yaml"
    path.write_text(rendered, encoding="utf-8")
    return AppConfig.from_file(str(path))


def test_both_installed_client_families_build_offline_from_operator_models(render_config: ModuleType, catalog: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Catalogue metadata is one thing; a constructible client is another. This
    is the second: no network, no provider discovery, no price lookup."""
    from deerflow.models.factory import create_chat_model

    config = _config_from(render_config, catalog, tmp_path, monkeypatch, ACME_CHAT, ACME_REASONER)

    chat = create_chat_model("acme-lightning-1", app_config=config, attach_tracing=False)
    assert chat.model_name == "acme/lightning-1-2099"
    assert "api.acme.invalid" in str(chat.openai_api_base)
    assert chat.max_tokens == 4096

    reasoner = create_chat_model("acme-anvil-9", app_config=config, thinking_enabled=True, attach_tracing=False)
    assert reasoner.model == "acme-anvil-9-20991231"
    assert "api.acme.invalid" in str(reasoner.anthropic_api_url)
    assert reasoner.thinking == {"type": "enabled", "budget_tokens": 2048}

    for client in (chat, reasoner):
        serialized = client.model_dump()
        assert "pricing" not in serialized and "context_window" not in serialized, "presentation and UI metadata must not reach the provider request"


def test_an_unknown_model_name_raises_rather_than_substituting_another(render_config: ModuleType, catalog: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """What an older thread meets when its model is removed from the file: an
    error naming the identity, never a silent remap onto a surviving model."""
    from deerflow.models.factory import create_chat_model

    config = _config_from(render_config, catalog, tmp_path, monkeypatch, ACME_CHAT)
    with pytest.raises(ValueError, match="acme-anvil-9"):
        create_chat_model("acme-anvil-9", app_config=config, attach_tracing=False)


# ── 6. pricing ───────────────────────────────────────────────────────────────


def test_operator_pricing_reaches_the_cost_display_and_nothing_else(render_config: ModuleType, catalog: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Prices are configuration like the rest: the console reads them from the
    rendered file, so an operator repricing a model needs no release."""
    from app.gateway.routers.console import _build_pricing_map, _lookup_pricing, _token_cost
    from deerflow.config.app_config import set_app_config

    config = _config_from(render_config, catalog, tmp_path, monkeypatch, ACME_CHAT, ACME_REASONER)
    set_app_config(config)
    pricing = _build_pricing_map()

    # Keyed by the config name and by the provider id the usage buckets carry.
    price = _lookup_pricing(pricing, "acme-lightning-1")
    assert price is not None and price == _lookup_pricing(pricing, "acme/lightning-1-2099")
    assert price.currency == "USD"
    expected = ACME_CHAT["pricing"]
    assert _token_cost(1_000_000, 0, price) == pytest.approx(expected["input_per_million"])
    assert _token_cost(0, 1_000_000, price) == pytest.approx(expected["output_per_million"])
    assert _token_cost(1_000_000, 0, price, cache_read_tokens=1_000_000) == pytest.approx(expected["input_cache_hit_per_million"])

    # A model the operator left unpriced simply has no estimate.
    unpriced = {key: value for key, value in ACME_CHAT.items() if key != "pricing"}
    plain = _config_from(render_config, catalog, tmp_path, monkeypatch, unpriced)
    set_app_config(plain)
    assert _lookup_pricing(_build_pricing_map(), "acme-lightning-1") is None


# ── 7. configuration-only change on unchanged release bytes ──────────────────


def _bundle_digest() -> str:
    digest = hashlib.sha256()
    for path in [TEMPLATE, *sorted(CATALOG.rglob("*.yaml"))]:
        digest.update(path.read_bytes())
    return digest.hexdigest()


def test_three_operator_edits_change_the_render_and_nothing_in_the_bundle(render_config: ModuleType, catalog: tuple, tmp_path: Path) -> None:
    """Add a model, change a request setting and a price, remove a model --
    each visible in the next render, with the release material byte-identical
    throughout. The live half of this (restart, container recreation, bundle
    replacement) is in the README's proof record."""
    before = _bundle_digest()
    path = tmp_path / "models.yaml"
    environ = _base_environ(ACME_API_KEY="secret", **{MODELS_ENV: str(path)})

    path.write_text(_operator_document(ACME_CHAT), encoding="utf-8")
    assert [model["name"] for model in _render(render_config, catalog, environ)["models"]] == ["acme-lightning-1"]

    path.write_text(_operator_document(ACME_CHAT, ACME_REASONER), encoding="utf-8")
    assert [model["name"] for model in _render(render_config, catalog, environ)["models"]] == ["acme-lightning-1", "acme-anvil-9"]

    repriced = {**ACME_CHAT, "max_tokens": 16384, "pricing": {**ACME_CHAT["pricing"], "output_per_million": 4.0}}
    path.write_text(_operator_document(repriced, ACME_REASONER), encoding="utf-8")
    models = _render(render_config, catalog, environ)["models"]
    assert models[0]["max_tokens"] == 16384
    assert models[0]["pricing"]["output_per_million"] == 4.0

    path.write_text(_operator_document(ACME_REASONER), encoding="utf-8")
    assert [model["name"] for model in _render(render_config, catalog, environ)["models"]] == ["acme-anvil-9"]

    assert _bundle_digest() == before, "operator input alone; the bundle is release material"
