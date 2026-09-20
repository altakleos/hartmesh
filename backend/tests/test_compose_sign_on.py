"""The compose profile's sign-in mode: three outcomes from the tenant ``.env`` and no fourth.

The three sign-on keys render the identity provider as the one way in; the
explicit local-password key renders today's ``auth`` block unchanged;
anything else refuses to render and names every key involved. The rendered
callback is computable from ``HARTMESH_PUBLIC_HOST`` alone, the client
secret reaches the Gateway as a reference and nothing else, and every
refusal names keys and rules, never values. The Gateway side of the mode is
``test_sign_on_only.py`` and ``test_sign_on_only_e2e.py``.
"""

from __future__ import annotations

import importlib.util
import itertools
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
TEMPLATE = PROFILE / "config.yaml"
CATALOG = PROFILE / "providers"
README = PROFILE / "README.md"

ISSUER = "https://login.example.com/realms/tenant"
CLIENT_ID = "hartmesh-tenant-client"
SECRET = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"
HOST = "tenant.example.com"
# Every operator-typed value carries a sentinel, so a refusal that quoted any
# of them is caught by one substring check.
SENTINELS = (ISSUER, CLIENT_ID, SECRET, "SENTINEL")


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def render_config() -> Iterator[ModuleType]:
    module = _load_module("hartmesh_render_config_sign_on_test", PROFILE / "gateway" / "render_config.py")
    try:
        yield module
    finally:
        sys.modules.pop("hartmesh_render_config_sign_on_test", None)


@pytest.fixture(scope="module")
def catalog(render_config: ModuleType) -> tuple:
    return render_config.load_catalog(CATALOG)


def _environ(**extra: str) -> dict[str, str]:
    environ = {"DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow", "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0", "HARTMESH_PUBLIC_HOST": HOST}
    environ.update(extra)
    return environ


SIGN_ON = {"HARTMESH_SIGN_ON_ISSUER": ISSUER, "HARTMESH_SIGN_ON_CLIENT_ID": CLIENT_ID, "HARTMESH_SIGN_ON_CLIENT_SECRET": SECRET}
LOCAL = {"HARTMESH_LOCAL_PASSWORDS": "allowed"}


def _render(render_config: ModuleType, catalog: tuple, environ: dict[str, str]) -> tuple[dict[str, Any], str]:
    text, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), catalog, environ)
    return yaml.safe_load(text), text


def _refusal(render_config: ModuleType, catalog: tuple, environ: dict[str, str]) -> str:
    with pytest.raises(render_config.RenderError) as refused:
        _render(render_config, catalog, environ)
    message = str(refused.value)
    assert not any(sentinel in message for sentinel in SENTINELS), message
    return message


# ── 1. Three outcomes and no fourth ──────────────────────────────────────────


def test_the_sign_on_keys_render_the_provider_as_the_one_way_in(render_config: ModuleType, catalog: tuple) -> None:
    document, text = _render(render_config, catalog, _environ(**SIGN_ON))
    auth = document["auth"]
    template_local = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))["auth"]["local"]
    assert auth["local"] == {**template_local, "enabled": False, "allow_registration": False}, "the lockout policy is kept; only the mode is added"
    assert auth["oidc"] == {
        "enabled": True,
        "frontend_base_url": f"https://{HOST}",
        "providers": {
            "sso": {
                "display_name": "Single sign-on",
                "issuer": ISSUER,
                "client_id": CLIENT_ID,
                "client_secret": "$HARTMESH_SIGN_ON_CLIENT_SECRET",
                "redirect_uri": f"https://{HOST}/api/v1/auth/callback/sso",
                "scopes": ["openid", "email", "profile"],
                "token_endpoint_auth_method": "client_secret_post",
                "admin_emails": [],
            }
        },
    }
    assert "authorization" not in document, "no authorization block: the product's admin/user line is system_role at the routes, not a tool policy"
    assert SECRET not in text


def test_the_local_password_key_renders_exactly_the_previous_document(render_config: ModuleType, catalog: tuple) -> None:
    """Evidence 9: the render for a .env that adds only the local key is the previous release's render."""
    document, text = _render(render_config, catalog, _environ(**LOCAL))
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    assert document["auth"] == template["auth"], "copied through unchanged: no enabled, no allow_registration, no oidc"
    assert "authorization" not in document


def test_neither_key_refuses_and_names_both_sides(render_config: ModuleType, catalog: tuple) -> None:
    message = _refusal(render_config, catalog, _environ())
    for key in ("HARTMESH_SIGN_ON_ISSUER", "HARTMESH_SIGN_ON_CLIENT_ID", "HARTMESH_SIGN_ON_CLIENT_SECRET", "HARTMESH_LOCAL_PASSWORDS=allowed"):
        assert key in message
    assert "Nothing is assumed" in message


def test_both_keys_refuse_and_name_both_sides(render_config: ModuleType, catalog: tuple) -> None:
    message = _refusal(render_config, catalog, _environ(**SIGN_ON, **LOCAL))
    assert "HARTMESH_LOCAL_PASSWORDS" in message and all(key in message for key in SIGN_ON)


@pytest.mark.parametrize("present", [combination for size in (1, 2) for combination in itertools.combinations(sorted(SIGN_ON), size)])
def test_a_half_set_sign_on_group_refuses_and_names_the_missing_keys(render_config: ModuleType, catalog: tuple, present: tuple[str, ...]) -> None:
    environ = _environ(**{key: SIGN_ON[key] for key in present})
    message = _refusal(render_config, catalog, environ)
    missing = sorted(set(SIGN_ON) - set(present))
    assert all(key in message.split("(present:")[0] for key in missing), message
    assert all(key in message.split("(present:")[1] for key in present), message
    assert "Nothing falls back to local passwords" in message


def test_an_empty_value_is_an_absent_key(render_config: ModuleType, catalog: tuple) -> None:
    message = _refusal(render_config, catalog, _environ(**{**SIGN_ON, "HARTMESH_SIGN_ON_CLIENT_SECRET": " "}))
    assert "missing HARTMESH_SIGN_ON_CLIENT_SECRET" in message


@pytest.mark.parametrize("key", ["HARTMESH_SIGN_ON_ADMINS", "HARTMESH_SIGN_ON_SCOPES", "HARTMESH_SIGN_ON_CLIENT_AUTH", "HARTMESH_SIGN_ON_NAME"])
def test_a_sign_on_option_under_local_passwords_refuses_rather_than_being_ignored(render_config: ModuleType, catalog: tuple, key: str) -> None:
    message = _refusal(render_config, catalog, _environ(**LOCAL, **{key: "x@example.com"}))
    assert key in message and "would be ignored" in message


def test_the_local_key_takes_exactly_one_value(render_config: ModuleType, catalog: tuple) -> None:
    assert "exactly `allowed`" in _refusal(render_config, catalog, _environ(HARTMESH_LOCAL_PASSWORDS="yes"))
    assert "exactly `allowed`" in _refusal(render_config, catalog, _environ(HARTMESH_LOCAL_PASSWORDS="true"))


# ── 2. The callback and the public host ──────────────────────────────────────


def test_the_callback_is_derived_from_the_public_host_and_the_fixed_provider_name(render_config: ModuleType, catalog: tuple) -> None:
    document, _ = _render(render_config, catalog, _environ(**SIGN_ON, HARTMESH_PUBLIC_HOST="acme.hartmesh.example"))
    provider = document["auth"]["oidc"]["providers"]["sso"]
    assert provider["redirect_uri"] == "https://acme.hartmesh.example/api/v1/auth/callback/sso"
    assert document["auth"]["oidc"]["frontend_base_url"] == "https://acme.hartmesh.example"
    assert render_config.SIGN_ON_PROVIDER_ID == "sso"


def test_sign_on_mode_needs_a_usable_public_host(render_config: ModuleType, catalog: tuple) -> None:
    assert "HARTMESH_PUBLIC_HOST" in _refusal(render_config, catalog, {**_environ(**SIGN_ON), "HARTMESH_PUBLIC_HOST": ""})
    assert "HARTMESH_PUBLIC_HOST" in _refusal(render_config, catalog, {**_environ(**SIGN_ON), "HARTMESH_PUBLIC_HOST": "evil; }"})
    # Local mode never reads it.
    document, _ = _render(render_config, catalog, {**_environ(**LOCAL), "HARTMESH_PUBLIC_HOST": ""})
    assert "oidc" not in document["auth"]


def test_the_issuer_must_be_an_https_url(render_config: ModuleType, catalog: tuple) -> None:
    for issuer in ("http://login.example.com/realms/tenant", "login.example.com", "https://", "https://a b"):
        message = _refusal(render_config, catalog, _environ(**{**SIGN_ON, "HARTMESH_SIGN_ON_ISSUER": issuer}))
        assert "HARTMESH_SIGN_ON_ISSUER" in message and "https://" in message and issuer not in message.replace("https://", "")


# ── 3. The optional keys ─────────────────────────────────────────────────────


def test_the_administrators_list_becomes_admin_emails(render_config: ModuleType, catalog: tuple) -> None:
    document, _ = _render(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_ADMINS="owner@example.com, ops@example.com"))
    assert document["auth"]["oidc"]["providers"]["sso"]["admin_emails"] == ["owner@example.com", "ops@example.com"]
    message = _refusal(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_ADMINS="owner@example.com, not-an-address-SENTINEL"))
    assert "HARTMESH_SIGN_ON_ADMINS" in message and "position 1" in message


def test_extra_scopes_are_appended_to_the_defaults_once(render_config: ModuleType, catalog: tuple) -> None:
    document, _ = _render(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_SCOPES="groups openid offline_access,groups"))
    assert document["auth"]["oidc"]["providers"]["sso"]["scopes"] == ["openid", "email", "profile", "groups", "offline_access"]
    assert "HARTMESH_SIGN_ON_SCOPES" in _refusal(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_SCOPES="groups; SENTINEL"))


def test_the_client_authentication_method_defaults_to_post_and_may_select_basic(render_config: ModuleType, catalog: tuple) -> None:
    document, _ = _render(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_CLIENT_AUTH="client_secret_basic"))
    assert document["auth"]["oidc"]["providers"]["sso"]["token_endpoint_auth_method"] == "client_secret_basic"
    message = _refusal(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_CLIENT_AUTH="none"))
    assert "HARTMESH_SIGN_ON_CLIENT_AUTH" in message and "client_secret_post, client_secret_basic" in message


def test_the_display_name_is_optional_and_bounded(render_config: ModuleType, catalog: tuple) -> None:
    document, _ = _render(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_NAME="Acme Okta"))
    assert document["auth"]["oidc"]["providers"]["sso"]["display_name"] == "Acme Okta"
    assert "HARTMESH_SIGN_ON_NAME" in _refusal(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_NAME="x" * 65))


@pytest.mark.parametrize("bad", ["0", "31", "7.5", "seven", "-1"])
def test_the_session_lifetime_is_whole_days_within_the_product_bound(render_config: ModuleType, catalog: tuple, bad: str) -> None:
    message = _refusal(render_config, catalog, _environ(**SIGN_ON, AUTH_TOKEN_EXPIRY_DAYS=bad))
    assert "AUTH_TOKEN_EXPIRY_DAYS" in message and "1 to 30" in message
    # Validated in both modes: the Gateway reads it from its environment either way.
    assert "AUTH_TOKEN_EXPIRY_DAYS" in _refusal(render_config, catalog, _environ(**LOCAL, AUTH_TOKEN_EXPIRY_DAYS=bad))
    document, text = _render(render_config, catalog, _environ(**SIGN_ON, AUTH_TOKEN_EXPIRY_DAYS="14"))
    assert "AUTH_TOKEN_EXPIRY_DAYS" not in text, "not written into the config; the Gateway reads the variable"


# ── 4. What the Gateway loads ────────────────────────────────────────────────


def test_the_rendered_document_loads_as_a_sign_on_only_app_config(render_config: ModuleType, catalog: tuple, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from deerflow.config.app_config import AppConfig

    environ = _environ(**SIGN_ON, HARTMESH_SIGN_ON_ADMINS="owner@example.com")
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    _, text = _render(render_config, catalog, environ)
    path = tmp_path / "config.yaml"
    path.write_text(text, encoding="utf-8")
    config = AppConfig.from_file(str(path))
    assert config.auth.local.enabled is False and config.auth.local.allow_registration is False
    assert config.auth.local.lockout_store == "redis" and config.auth.local.source_max_failures == 600
    provider = config.auth.oidc.providers["sso"]
    assert config.auth.oidc.enabled is True and provider.client_secret == SECRET, "the reference is expanded by the Gateway, from its environment"
    assert provider.redirect_uri == f"https://{HOST}/api/v1/auth/callback/sso" and provider.admin_emails == ["owner@example.com"]
    assert provider.require_verified_email is True and provider.auto_create_users is True and provider.pkce_enabled and provider.nonce_enabled
    assert config.authorization.enabled is False


def test_the_secret_is_only_ever_a_reference(render_config: ModuleType, catalog: tuple) -> None:
    _, text = _render(render_config, catalog, _environ(**SIGN_ON))
    assert "$HARTMESH_SIGN_ON_CLIENT_SECRET" in text and SECRET not in text
    # A secret pasted where a plain value goes is still never echoed: the
    # value below fails every key's rule, and no refusal may quote it.
    pasted = f"{SECRET} pasted here!"
    for key in ("HARTMESH_SIGN_ON_ISSUER", "HARTMESH_SIGN_ON_ADMINS", "HARTMESH_SIGN_ON_SCOPES", "HARTMESH_SIGN_ON_CLIENT_AUTH", "HARTMESH_SIGN_ON_NAME", "HARTMESH_PUBLIC_HOST"):
        _refusal(render_config, catalog, _environ(**{**SIGN_ON, key: pasted if key != "HARTMESH_SIGN_ON_NAME" else pasted * 3}))


# ── 5. The template, the example and the summary line ────────────────────────


def test_the_template_carries_no_sign_in_mode_of_its_own(render_config: ModuleType, catalog: tuple) -> None:
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    assert "oidc" not in template["auth"] and "enabled" not in template["auth"]["local"] and "allow_registration" not in template["auth"]["local"]
    assert "authorization" not in template
    doctored = dict(template)
    doctored["auth"] = {"local": {**template["auth"]["local"], "enabled": True}}
    with pytest.raises(render_config.RenderError, match="auth.local.enabled"):
        render_config.render(doctored, catalog, _environ(**LOCAL))


def _example_environ() -> dict[str, str]:
    lines = (PROFILE / ".env.example").read_text(encoding="utf-8").splitlines()
    values = dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))
    return {**_environ(), **values}


def test_the_env_example_renders_sign_on_only_and_local_with_one_swap(render_config: ModuleType, catalog: tuple) -> None:
    example = _example_environ()
    document, _ = _render(render_config, catalog, example)
    assert document["auth"]["local"]["enabled"] is False
    assert document["auth"]["oidc"]["providers"]["sso"]["redirect_uri"] == f"https://{example['HARTMESH_PUBLIC_HOST']}/api/v1/auth/callback/sso"
    local = {key: value for key, value in example.items() if not key.startswith("HARTMESH_SIGN_ON_")}
    local["HARTMESH_LOCAL_PASSWORDS"] = "allowed"
    document, _ = _render(render_config, catalog, local)
    assert "oidc" not in document["auth"] and "enabled" not in document["auth"]["local"]


def test_check_mode_reports_the_mode_and_the_callback(render_config: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    for name, value in _environ(**SIGN_ON).items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("HARTMESH_LOCAL_PASSWORDS", raising=False)
    assert render_config.main(["--template", str(TEMPLATE), "--catalog", str(CATALOG), "--check"]) == 0
    out = capsys.readouterr().out
    assert f"sign-in=sign_on_only (provider sso, callback https://{HOST}/api/v1/auth/callback/sso)" in out
    assert SECRET not in out and CLIENT_ID not in out
    monkeypatch.setenv("HARTMESH_LOCAL_PASSWORDS", "allowed")
    assert render_config.main(["--template", str(TEMPLATE), "--catalog", str(CATALOG), "--check"]) == 1
    assert "both set" in capsys.readouterr().err


# ── 6. The README ────────────────────────────────────────────────────────────


def test_the_readme_states_the_keys_the_callback_the_method_and_the_upgrade_note() -> None:
    text = README.read_text(encoding="utf-8")
    section = text.split("## Sign-in", 1)[1].split("\n## ", 1)[0]
    for needle in (
        "HARTMESH_SIGN_ON_ISSUER",
        "HARTMESH_SIGN_ON_CLIENT_ID",
        "HARTMESH_SIGN_ON_CLIENT_SECRET",
        "HARTMESH_LOCAL_PASSWORDS=allowed",
        "https://<HARTMESH_PUBLIC_HOST>/api/v1/auth/callback/sso",
        "client_secret_post",
        "HARTMESH_SIGN_ON_ADMINS",
        "HARTMESH_SIGN_ON_SCOPES",
        "openid email profile",
        "AUTH_TOKEN_EXPIRY_DAYS",
        "auth_mode",
        "Upgrade note",
        "v2.1.0+hartmesh.30",
        "oauth_issuer",
        "no administrator",
    ):
        assert needle in section, needle
