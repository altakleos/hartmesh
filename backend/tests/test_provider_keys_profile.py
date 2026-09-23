"""How the compose profile turns provider keys in the product on, and nothing else does.

The Gateway manages keys in the product only when ``HARTMESH_PROFILE_DIR``
names a directory with the profile's renderer, template and catalog, which
only the profile's ``compose.yaml`` sets. The wrapping key reaches the
Gateway alone. At start the stored keys are applied before anything is
built from the config (the served proof is the compose stack; here, the
order is pinned where it is decided).
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from app.gateway.provider_keys.cipher import PREVIOUS_WRAPPING_KEY_ENV, WRAPPING_KEY_ENV
from app.gateway.provider_keys.profile import PROFILE_DIR_ENV, ProfileRenderer

BACKEND = Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND.parent
PROFILE = REPO_ROOT / "deploy" / "compose"


def _compose() -> dict:
    return yaml.safe_load((PROFILE / "compose.yaml").read_text(encoding="utf-8"))


def test_only_the_gateway_is_given_the_profile_directory_and_the_wrapping_key() -> None:
    services = _compose()["services"]
    environment = services["gateway"]["environment"]
    assert environment[PROFILE_DIR_ENV] == "/opt/hartmesh"
    assert environment[WRAPPING_KEY_ENV] == "${" + WRAPPING_KEY_ENV + ":-}"
    assert environment[PREVIOUS_WRAPPING_KEY_ENV] == "${" + PREVIOUS_WRAPPING_KEY_ENV + ":-}"
    for name, service in services.items():
        if name != "gateway":
            assert WRAPPING_KEY_ENV not in str(service.get("environment", {})), name
            assert PROFILE_DIR_ENV not in str(service.get("environment", {})), name


def test_the_profile_directory_the_gateway_is_given_holds_what_the_renderer_needs() -> None:
    volumes = _compose()["services"]["gateway"]["volumes"]
    mounted = {volume.split(":")[1] for volume in volumes if isinstance(volume, str) and volume.endswith(":ro")}
    assert {"/opt/hartmesh/gateway", "/opt/hartmesh/config.yaml", "/opt/hartmesh/providers"} <= mounted
    assert (PROFILE / "gateway" / "render_config.py").is_file() and (PROFILE / "config.yaml").is_file()


def test_every_catalog_fragment_is_one_provider_with_a_name_of_its_own() -> None:
    providers = ProfileRenderer(PROFILE).providers
    fragments = sorted((PROFILE / "providers").rglob("*.yaml"))
    assert len(providers) == len(fragments) == 20
    assert len({provider.id for provider in providers}) == len(providers)
    assert len({provider.variable for provider in providers}) == len(providers)
    assert {provider.kind for provider in providers} == {"models", "tools"}
    assert all(re.fullmatch(r"[a-z][a-z0-9-]*", provider.id) for provider in providers)
    by_id = {provider.id: provider.variable for provider in providers}
    assert (by_id["openai"], by_id["anthropic"], by_id["tavily"], by_id["tencent-wsa"]) == ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "TAVILY_API_KEY", "TENCENTCLOUD_WSA_APIKEY")


def test_stored_keys_are_applied_before_anything_is_built_from_the_config() -> None:
    runtime = (BACKEND / "app" / "gateway" / "deps.py").read_text(encoding="utf-8")
    applied = runtime.index("ProviderKeyService.from_environ(")
    assert runtime.index("ensure_schema_tenant_binding(") < applied
    # What is built after it builds on the reloaded config, not the one read before.
    rebind = runtime.index("config = get_app_config()", applied)
    for built in ("make_stream_bridge(", "make_checkpointer(", "tool_plane_config = getattr(config"):
        assert rebind < runtime.index(built), built
    lifespan = (BACKEND / "app" / "gateway" / "app.py").read_text(encoding="utf-8")
    entered = lifespan.index("async with langgraph_runtime(app, startup_config):")
    rebound = lifespan.index('if getattr(app.state, "provider_keys_applied", False):')
    assert lifespan.index("startup_config = get_app_config()", rebound) < lifespan.index("await _ensure_admin_user(app)")
    assert entered < rebound < lifespan.index("await _ensure_admin_user(app)")
    assert rebound < lifespan.index("start_channel_service(")
