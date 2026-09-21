"""The compose profile's optional access keys: admission and roles follow the claim, or the render refuses and names the keys."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
TEMPLATE = PROFILE / "config.yaml"
CATALOG = PROFILE / "providers"
README = PROFILE / "README.md"

CLAIM = "urn:zitadel:iam:org:project:roles"
SENTINELS = ("SENTINEL", "https://login.example.com/realms/tenant")


@pytest.fixture(scope="module")
def render_config() -> Iterator[ModuleType]:
    spec = importlib.util.spec_from_file_location("hartmesh_render_config_membership_test", PROFILE / "gateway" / "render_config.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.modules.pop(spec.name, None)


def _environ(**extra: str) -> dict[str, str]:
    base = {
        "HARTMESH_PUBLIC_HOST": "tenant.example.com",
        "HARTMESH_SIGN_ON_ISSUER": "https://login.example.com/realms/tenant",
        "HARTMESH_SIGN_ON_CLIENT_ID": "hartmesh-SENTINEL",
        "HARTMESH_SIGN_ON_CLIENT_SECRET": "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY",
    }
    base.update(extra)
    return base


def _provider(render_config: ModuleType, environ: dict[str, str]) -> dict:
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    return render_config.sign_on_auth(template.get("auth", {}), environ)["oidc"]["providers"]["sso"]


def test_without_the_keys_the_provider_carries_no_admission_rule(render_config: ModuleType) -> None:
    provider = _provider(render_config, _environ())
    assert not {"access_claim", "access_values", "access_roles"} & set(provider), "sign-on-only mode without the keys renders exactly as before"


def test_the_claim_and_values_render_and_roles_are_optional(render_config: ModuleType) -> None:
    provider = _provider(render_config, _environ(HARTMESH_SIGN_ON_ACCESS_CLAIM=f" {CLAIM} ", HARTMESH_SIGN_ON_ACCESS_VALUES="admin, member", HARTMESH_SIGN_ON_ADMINS="owner@example.com"))
    assert provider["access_claim"] == CLAIM and provider["access_values"] == ["admin", "member"] and "access_roles" not in provider
    assert provider["admin_emails"] == ["owner@example.com"], "without the mapping, roles come from the email list"
    provider = _provider(render_config, _environ(HARTMESH_SIGN_ON_ACCESS_CLAIM=CLAIM, HARTMESH_SIGN_ON_ACCESS_VALUES="admin,member", HARTMESH_SIGN_ON_ROLES="admin=admin,member=user"))
    assert provider["access_roles"] == {"admin": "admin", "member": "user"} and provider["admin_emails"] == []


@pytest.mark.parametrize(
    ("extra", "names"),
    [
        ({"HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM}, ("HARTMESH_SIGN_ON_ACCESS_VALUES is empty",)),
        ({"HARTMESH_SIGN_ON_ACCESS_VALUES": "member"}, ("HARTMESH_SIGN_ON_ACCESS_CLAIM is not",)),
        ({"HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM, "HARTMESH_SIGN_ON_ACCESS_VALUES": "member,member"}, ("repeats a value",)),
        ({"HARTMESH_SIGN_ON_ROLES": "admin=admin"}, ("HARTMESH_SIGN_ON_ROLES is set but HARTMESH_SIGN_ON_ACCESS_CLAIM is not",)),
        (
            {"HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM, "HARTMESH_SIGN_ON_ACCESS_VALUES": "admin", "HARTMESH_SIGN_ON_ROLES": "admin=admin", "HARTMESH_SIGN_ON_ADMINS": "owner@example.com"},
            ("HARTMESH_SIGN_ON_ROLES and HARTMESH_SIGN_ON_ADMINS are both set",),
        ),
        ({"HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM, "HARTMESH_SIGN_ON_ACCESS_VALUES": "admin,member", "HARTMESH_SIGN_ON_ROLES": "admin=admin"}, ("gives no role to the admitting value(s) at position 1",)),
        ({"HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM, "HARTMESH_SIGN_ON_ACCESS_VALUES": "admin", "HARTMESH_SIGN_ON_ROLES": "admin=admin,guest=user"}, ("maps value(s) at position 1 that are not in HARTMESH_SIGN_ON_ACCESS_VALUES",)),
        ({"HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM, "HARTMESH_SIGN_ON_ACCESS_VALUES": "admin", "HARTMESH_SIGN_ON_ROLES": "admin=owner"}, ("the entry at position 0 is not",)),
        ({"HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM, "HARTMESH_SIGN_ON_ACCESS_VALUES": "admin", "HARTMESH_SIGN_ON_ROLES": "admin=admin,admin=user"}, ("twice",)),
    ],
)
def test_a_half_set_rule_refuses_and_names_the_keys_never_the_values(render_config: ModuleType, extra: dict[str, str], names: tuple[str, ...]) -> None:
    with pytest.raises(render_config.RenderError) as refused:
        _provider(render_config, _environ(**extra))
    message = str(refused.value)
    for name in names:
        assert name in message, message
    assert not any(sentinel in message for sentinel in SENTINELS) and "owner@example.com" not in message and CLAIM not in message, message


def test_values_are_split_on_commas_only_and_a_value_may_contain_a_space(render_config: ModuleType) -> None:
    provider = _provider(render_config, _environ(HARTMESH_SIGN_ON_ACCESS_CLAIM=CLAIM, HARTMESH_SIGN_ON_ACCESS_VALUES="Project Admin, member", HARTMESH_SIGN_ON_ROLES="Project Admin = admin, member=user"))
    assert provider["access_values"] == ["Project Admin", "member"] and provider["access_roles"] == {"Project Admin": "admin", "member": "user"}
    with pytest.raises(render_config.RenderError, match="HARTMESH_SIGN_ON_ACCESS_CLAIM must be one claim name without whitespace"):
        _provider(render_config, _environ(HARTMESH_SIGN_ON_ACCESS_CLAIM="two words", HARTMESH_SIGN_ON_ACCESS_VALUES="member"))


def test_the_access_keys_beside_local_passwords_refuse(render_config: ModuleType) -> None:
    environ = {"HARTMESH_LOCAL_PASSWORDS": "allowed", "HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM, "HARTMESH_SIGN_ON_ACCESS_VALUES": "member"}
    with pytest.raises(render_config.RenderError, match="HARTMESH_SIGN_ON_ACCESS_CLAIM, HARTMESH_SIGN_ON_ACCESS_VALUES"):
        render_config.select_sign_in(environ)


def test_the_rendered_provider_is_a_valid_gateway_config(render_config: ModuleType) -> None:
    from deerflow.config.auth_config import OIDCProviderConfig

    provider = _provider(render_config, _environ(HARTMESH_SIGN_ON_ACCESS_CLAIM=CLAIM, HARTMESH_SIGN_ON_ACCESS_VALUES="admin,member", HARTMESH_SIGN_ON_ROLES="admin=admin,member=user"))
    loaded = OIDCProviderConfig(**{**provider, "client_secret": "x"})
    assert loaded.access_claim == CLAIM and loaded.access_roles == {"admin": "admin", "member": "user"}


def test_the_summary_line_names_the_rule(render_config: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    import test_compose_sign_on as sign_on

    for key, value in {**sign_on._environ(**sign_on.SIGN_ON), "HARTMESH_SIGN_ON_ACCESS_CLAIM": CLAIM, "HARTMESH_SIGN_ON_ACCESS_VALUES": "admin,member", "HARTMESH_SIGN_ON_ROLES": "admin=admin,member=user"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("HARTMESH_LOCAL_PASSWORDS", raising=False)
    assert render_config.main(["--template", str(TEMPLATE), "--catalog", str(CATALOG), "--check"]) == 0
    out = capsys.readouterr().out
    assert f"sign-in=sign_on_only (provider sso, callback https://{sign_on.HOST}/api/v1/auth/callback/sso, admission by claim, roles from claim)" in out


def test_the_readme_and_env_example_name_the_keys_and_the_command() -> None:
    readme = README.read_text(encoding="utf-8")
    for key in ("HARTMESH_SIGN_ON_ACCESS_CLAIM", "HARTMESH_SIGN_ON_ACCESS_VALUES", "HARTMESH_SIGN_ON_ROLES"):
        assert f"| `{key}` |" in readme, key
    assert "### Membership follows the claim" in readme
    for phrase in ("python -m app.gateway.auth.accounts list", "end-sessions", "sso_no_access", "sso_access_off", "0040_account_access", "nothing reachable over HTTP", "PYTHONPATH=. uv run --no-sync python -m app.gateway.auth.accounts"):
        assert phrase in readme, phrase
    example = (PROFILE / ".env.example").read_text(encoding="utf-8")
    assert "#HARTMESH_SIGN_ON_ACCESS_CLAIM=" in example and "#HARTMESH_SIGN_ON_ROLES=" in example
