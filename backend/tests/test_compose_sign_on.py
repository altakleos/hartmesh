"""The compose profile's sign-in mode: three outcomes from the tenant ``.env`` and no fourth.

The three sign-on keys render the identity provider as the one way in; the
explicit local-password key renders local passwords with the registration
choice ``HARTMESH_LOCAL_REGISTRATION`` names; anything else refuses to render
and names every key involved. The rendered
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
REGISTRATION = "HARTMESH_LOCAL_REGISTRATION"
LOCAL = {"HARTMESH_LOCAL_PASSWORDS": "allowed", REGISTRATION: "closed"}


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
        # One tolerance for this host's clock, not one per provider.
        "clock_skew_leeway_seconds": 60,
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


@pytest.mark.parametrize(("choice", "open_"), [("open", True), ("closed", False)])
def test_the_local_password_key_renders_the_registration_choice(render_config: ModuleType, catalog: tuple, choice: str, open_: bool) -> None:
    document, _ = _render(render_config, catalog, _environ(**{**LOCAL, REGISTRATION: choice}))
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    assert document["auth"] == {**template["auth"], "local": {**template["auth"]["local"], "enabled": True, "allow_registration": open_}}, "the lockout policy is kept; only the mode and the registration choice are added"
    assert "authorization" not in document


def test_local_passwords_without_a_registration_choice_refuse_and_name_the_key(render_config: ModuleType, catalog: tuple) -> None:
    for environ in (_environ(HARTMESH_LOCAL_PASSWORDS="allowed"), _environ(HARTMESH_LOCAL_PASSWORDS="allowed", **{REGISTRATION: " "})):
        message = _refusal(render_config, catalog, environ)
        assert REGISTRATION in message and "open or closed" in message and "Nothing is assumed" in message


@pytest.mark.parametrize("bad", ["yes", "true", "Open", "CLOSED", "allowed", "open,closed", "SENTINEL"])
def test_the_registration_choice_takes_exactly_one_of_two_values(render_config: ModuleType, catalog: tuple, bad: str) -> None:
    message = _refusal(render_config, catalog, _environ(**{**LOCAL, REGISTRATION: bad}))
    assert REGISTRATION in message and "exactly `open` or `closed`" in message
    assert bad not in message.replace("`open` or `closed`", ""), "the refusal names the key and the rule, never the value"


@pytest.mark.parametrize("choice", ["open", "closed"])
def test_a_registration_choice_under_sign_on_refuses_as_a_conflicting_key(render_config: ModuleType, catalog: tuple, choice: str) -> None:
    message = _refusal(render_config, catalog, _environ(**SIGN_ON, **{REGISTRATION: choice}))
    assert REGISTRATION in message and "sign-on only" in message


@pytest.mark.parametrize("value", [True, False])
@pytest.mark.parametrize("environ", [LOCAL, SIGN_ON], ids=["local", "sign_on"])
def test_a_template_naming_allow_registration_refuses_in_either_mode(render_config: ModuleType, catalog: tuple, environ: dict[str, str], value: bool) -> None:
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    doctored = {**template, "auth": {**template["auth"], "local": {**template["auth"]["local"], "allow_registration": value}}}
    with pytest.raises(render_config.RenderError, match="auth.local.allow_registration") as refused:
        render_config.render(doctored, catalog, _environ(**environ))
    assert "the sign-in keys select them" in str(refused.value)


def test_one_unmodified_template_renders_all_three_sign_in_choices(render_config: ModuleType, catalog: tuple, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """One bundle serves both modes: the same template text renders sign-on only, and local passwords open and closed, and each loads."""
    from deerflow.config.app_config import AppConfig

    text = TEMPLATE.read_text(encoding="utf-8")
    for name, environ, expected in (
        ("sign_on", _environ(**SIGN_ON), (False, False)),
        ("local_open", _environ(**{**LOCAL, REGISTRATION: "open"}), (True, True)),
        ("local_closed", _environ(**LOCAL), (True, False)),
    ):
        rendered, _ = render_config.render_text(text, catalog, environ)
        with monkeypatch.context() as scoped:
            for key, value in environ.items():
                scoped.setenv(key, value)
            path = tmp_path / f"{name}.yaml"
            path.write_text(rendered, encoding="utf-8")
            config = AppConfig.from_file(str(path))
        assert (config.auth.local.enabled, config.auth.local.allow_registration) == expected, name


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
    doctored["auth"] = {"local": {**template["auth"]["local"], "allow_registration": True}}
    with pytest.raises(render_config.RenderError, match="auth.local.allow_registration"):
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
    local[REGISTRATION] = "closed"
    document, _ = _render(render_config, catalog, local)
    assert "oidc" not in document["auth"] and document["auth"]["local"]["enabled"] is True and document["auth"]["local"]["allow_registration"] is False


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
    for key in SIGN_ON:
        monkeypatch.delenv(key)
    for choice in ("open", "closed"):
        monkeypatch.setenv(REGISTRATION, choice)
        assert render_config.main(["--template", str(TEMPLATE), "--catalog", str(CATALOG), "--check"]) == 0
        assert f"sign-in=local (registration {choice})" in capsys.readouterr().out
    monkeypatch.delenv(REGISTRATION)
    assert render_config.main(["--template", str(TEMPLATE), "--catalog", str(CATALOG), "--check"]) == 1
    assert f"refusing to render: HARTMESH_LOCAL_PASSWORDS=allowed selects local passwords, and {REGISTRATION} must" in capsys.readouterr().err


# ── 6. The README ────────────────────────────────────────────────────────────


def test_the_readme_states_the_keys_the_callback_the_method_and_the_upgrade_note() -> None:
    text = README.read_text(encoding="utf-8")
    section = text.split("## Sign-in", 1)[1].split("\n## ", 1)[0]
    for needle in (
        "HARTMESH_SIGN_ON_ISSUER",
        "HARTMESH_SIGN_ON_CLIENT_ID",
        "HARTMESH_SIGN_ON_CLIENT_SECRET",
        "HARTMESH_LOCAL_PASSWORDS=allowed",
        "HARTMESH_LOCAL_REGISTRATION=open",
        "HARTMESH_LOCAL_REGISTRATION=closed",
        "python -m app.gateway.auth.add_user",
        "/api/v1/auth/users",
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


def test_the_clock_tolerance_is_rendered_with_a_default_and_is_overridable(render_config: ModuleType, catalog: tuple) -> None:
    """The tenant is a VM, and a VM's clock drifts between NTP polls.

    With no tolerance at all a guest a second or two behind the provider
    refuses every sign-in by everyone -- measured on a released tenant, six
    attempts in thirty seconds, all `not yet valid (iat)`. The renderer owns
    the whole ``auth.oidc`` block (a template that names it is refused), so
    without a key here the tolerance would be unreachable on exactly the
    deployment shape that hit this.
    """
    document, _ = _render(render_config, catalog, _environ(**SIGN_ON))
    assert document["auth"]["oidc"]["clock_skew_leeway_seconds"] == 60

    for raw, expected in (("0", 0), ("5", 5), (" 120 ", 120), ("300", 300)):
        document, _ = _render(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_CLOCK_SKEW=raw))
        assert document["auth"]["oidc"]["clock_skew_leeway_seconds"] == expected


@pytest.mark.parametrize("bad", ["-1", "301", "1.5", "abc", "inf", "nan", "true", "+5", "5s"])
def test_the_clock_tolerance_is_a_whole_number_of_seconds_in_range(render_config: ModuleType, catalog: tuple, bad: str) -> None:
    message = _refusal(render_config, catalog, _environ(**SIGN_ON, HARTMESH_SIGN_ON_CLOCK_SKEW=bad))
    assert "HARTMESH_SIGN_ON_CLOCK_SKEW" in message and "0 to 300" in message
    assert not any(sentinel in message for sentinel in SENTINELS), "a refusal names the key and the rule, never the value"
