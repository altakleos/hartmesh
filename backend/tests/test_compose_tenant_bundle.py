"""The tenant VM profile carries the tenant bundle: one directory, two readers, no copy.

``/srv/hartmesh/operator/tenant`` is where the operator writes the company's
name, logo, colours, starter list and report profiles. The profile mounts it
read-only at ``/mnt/tenant`` in every sandbox, which is where the report
skill reads it, and names the same path in ``tenant_bundle.path``, which is
where the Gateway reads it for the header, the About page and Home. These
pin that the two are the same directory, that it sits under the operator
mount (read-only in the Gateway, outside ``home/``), that the rendered
config validates, and that ``render_config.py --check`` tells the operator
what the bundle holds and what is wrong with it before a start.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
COMPOSE = PROFILE / "compose.yaml"
TEMPLATE = PROFILE / "config.yaml"
CATALOG = PROFILE / "providers"
README = PROFILE / "README.md"
EXAMPLE = REPO_ROOT / "config.example.yaml"

BUNDLE_HOST_PATH = "/srv/hartmesh/operator/tenant"
BUNDLE_CONTAINER_PATH = "/mnt/tenant"
OPERATOR_MOUNT = "${HARTMESH_DATA_DIR}/operator"
PNG = b"\x89PNG\r\n\x1a\n" + b"\0" * 16


@pytest.fixture(scope="module")
def render_config() -> Iterator[ModuleType]:
    spec = importlib.util.spec_from_file_location("hartmesh_render_config_bundle_test", PROFILE / "gateway" / "render_config.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.modules.pop(spec.name, None)


@pytest.fixture(scope="module")
def template() -> dict:
    return yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))


def _environ() -> dict[str, str]:
    # Local-password mode: the render these tests always exercised.
    return {"DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow", "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0", "HARTMESH_LOCAL_PASSWORDS": "allowed", "HARTMESH_LOCAL_REGISTRATION": "closed"}


def test_the_gateway_and_every_sandbox_read_the_same_directory(template: dict) -> None:
    assert template["tenant_bundle"] == {"path": BUNDLE_HOST_PATH}
    mounts = [mount for mount in template["sandbox"]["mounts"] if mount["container_path"] == BUNDLE_CONTAINER_PATH]
    assert len(mounts) == 1, "one bundle mount, at the path the skill reads by default"
    assert mounts[0] == {"host_path": BUNDLE_HOST_PATH, "container_path": BUNDLE_CONTAINER_PATH, "read_only": True}
    assert mounts[0]["host_path"] == template["tenant_bundle"]["path"], "the brand is written once; the Gateway and the skill read the one file"


def test_the_bundle_sits_under_the_operator_mount_and_outside_home(template: dict) -> None:
    """Read-only in the Gateway, so nothing a chat or an agent can reach rewrites a company's name."""
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    volumes = compose["services"]["gateway"]["volumes"]
    operator = next(volume for volume in volumes if isinstance(volume, str) and volume.startswith(OPERATOR_MOUNT + ":"))
    assert operator.endswith(":ro")
    assert BUNDLE_HOST_PATH.startswith("/srv/hartmesh/operator/")
    assert not BUNDLE_HOST_PATH.startswith("/srv/hartmesh/home/"), "home/ is what sandboxes can write into"
    assert template["skills"]["path"].startswith("/srv/hartmesh/home/"), "the literal that fixes HARTMESH_DATA_DIR to /srv/hartmesh (README: Mount points)"


def test_compose_creates_the_directory_before_any_sandbox_binds_it() -> None:
    """The sandbox backend binds it with `--mount type=bind`, which refuses a missing source: without this
    entry an existing tenant that upgrades without `install -d` gets no sandbox at all. Compose's short
    volume syntax creates the host path at `up`, before the Gateway starts, so the bind always has a source."""
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    volumes = compose["services"]["gateway"]["volumes"]
    tenant = OPERATOR_MOUNT + "/tenant"
    assert f"{tenant}:{tenant}:ro" in volumes, "a short-syntax bind of the bundle directory itself, so Docker creates it"


def test_the_tenant_profile_is_the_business_profile(template: dict) -> None:
    """Someone who is not an administrator is not offered the developer screens; the bundle's starters land on a Home that shows a grid."""
    assert template["ui"] == {"profile": "business"}


def test_the_rendered_profile_validates_both_sections(render_config: ModuleType) -> None:
    from deerflow.config.sandbox_config import SandboxConfig
    from deerflow.config.tenant_bundle import TenantBundleConfig

    rendered, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), render_config.load_catalog(CATALOG), _environ())
    document = yaml.safe_load(rendered)
    assert TenantBundleConfig.model_validate(document["tenant_bundle"]).path == BUNDLE_HOST_PATH
    sandbox = SandboxConfig.model_validate(document["sandbox"])
    bundle_mounts = [mount for mount in sandbox.mounts if mount.container_path == BUNDLE_CONTAINER_PATH]
    assert len(bundle_mounts) == 1 and bundle_mounts[0].read_only and bundle_mounts[0].host_path == BUNDLE_HOST_PATH


def _template_pointing_at(tmp_path: Path, bundle: Path) -> Path:
    document = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    document["tenant_bundle"] = {"path": str(bundle)}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return path


def _check(render_config: ModuleType, template_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> tuple[int, str, str]:
    for name, value in _environ().items():
        monkeypatch.setenv(name, value)
    code = render_config.main(["--template", str(template_path), "--catalog", str(CATALOG), "--check"])
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_check_tells_the_operator_what_the_bundle_holds(render_config: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    bundle = tmp_path / "tenant"
    (bundle / "report-profiles").mkdir(parents=True)
    (bundle / "logo.png").write_bytes(PNG)
    (bundle / "brand.json").write_text(json.dumps({"company_name": "Example Services Co.", "logo": "logo.png", "colors": {"primary": "#0a6b3d"}}), encoding="utf-8")
    (bundle / "starters.json").write_text(json.dumps([{"id": "review", "title": "Monthly review", "prompt": "Build it from the export I attach."}]), encoding="utf-8")
    (bundle / "report-profiles" / "services-generic.json").write_text("{}", encoding="utf-8")

    code, out, err = _check(render_config, _template_pointing_at(tmp_path, bundle), capsys, monkeypatch)

    assert code == 0
    assert f"tenant bundle at {bundle}: company_name set; logo present; starters 1; report profiles 1" in out
    assert "warning" not in err
    assert "Example Services Co." not in out + err, "the summary counts and states; it never prints what the operator wrote"


def test_check_names_what_is_wrong_and_still_renders(render_config: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    """A brand typo is not a reason to refuse the tenant a Gateway; it is a warning with the file and the rule."""
    bundle = tmp_path / "tenant"
    bundle.mkdir()
    (bundle / "brand.json").write_text("{not json", encoding="utf-8")
    (bundle / "starters.json").write_text(json.dumps([{"id": "a", "title": "A", "prompt": "p"}, {"id": "a", "title": "B", "prompt": "q"}]), encoding="utf-8")

    code, out, err = _check(render_config, _template_pointing_at(tmp_path, bundle), capsys, monkeypatch)

    assert code == 0
    assert f"tenant bundle at {bundle}: company_name unset; logo absent; starters none; report profiles 0" in out
    assert "render_config: warning: tenant bundle: brand.json: not a JSON object" in err
    assert "render_config: warning: tenant bundle: starters.json: value_error" in err


def test_check_says_when_the_directory_is_not_there(render_config: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    """Compose creates it at `up`, so this is a Gateway run outside the profile; the render still goes through."""
    bundle = tmp_path / "tenant"

    code, out, err = _check(render_config, _template_pointing_at(tmp_path, bundle), capsys, monkeypatch)

    assert code == 0
    assert f"tenant bundle at {bundle}: unusable" in out
    assert "render_config: warning: tenant bundle: tenant bundle directory does not exist" in err


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses anything")
def test_check_says_when_the_directory_is_root_s_and_still_renders(render_config: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    """`install -d` without `-o 1000`: the Gateway starts (run.sh is `set -e`) and the operator reads why the brand is missing."""
    bundle = tmp_path / "tenant"
    bundle.mkdir()
    (bundle / "brand.json").write_text(json.dumps({"company_name": "Example Services Co."}), encoding="utf-8")
    bundle.chmod(0)
    try:
        code, out, err = _check(render_config, _template_pointing_at(tmp_path, bundle), capsys, monkeypatch)
    finally:
        bundle.chmod(0o750)

    assert code == 0
    assert f"tenant bundle at {bundle}: unusable" in out
    assert "render_config: warning: tenant bundle: tenant bundle directory cannot be read" in err


def test_the_operator_docs_describe_the_bundle() -> None:
    readme = README.read_text(encoding="utf-8")
    assert "### Tenant bundle" in readme
    for needle in (BUNDLE_HOST_PATH, "brand.json", "starters.json", "report-profiles/", "install -d -o 1000 -g 1000 -m 0750 /srv/hartmesh/operator/tenant", "--check", "readable by uid 1000", "business profile"):
        assert needle in readme, needle
    example = EXAMPLE.read_text(encoding="utf-8")
    assert "\ntenant_bundle:\n" in example and "# path: /srv/hartmesh/operator/tenant" in example
