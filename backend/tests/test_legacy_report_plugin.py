"""Historical reports use an explicitly installed, independently packaged adapter."""

import inspect
import json
import shutil
import tomllib
from pathlib import Path

import pytest
import yaml

from app.gateway.routers import artifacts
from deerflow.extensions.loader import ExtensionSpec, load_extensions
from deerflow.extensions.manager import ExtensionManager

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "backend/extensions/sources/hartmesh-legacy-report"
FIXTURE = ROOT / "frontend-hm/tests/fixtures/business-report/2026-08-business-review.report.json"
ENTRY = "hartmesh_legacy_report:install"


@pytest.mark.parametrize("provider_config", ["config.example.yaml", "deploy/compose/config.yaml"])
def test_provider_configurations_activate_the_packaged_historical_adapter(monkeypatch, provider_config):
    document = yaml.safe_load((ROOT / provider_config).read_text(encoding="utf-8"))
    declarations = [entry for entry in document.get("plugins", []) if entry.get("use") == ENTRY]
    assert len(declarations) == 1
    declaration = declarations[0]
    assert declaration["package"] == "hartmesh-legacy-report"
    assert declaration["enabled"] is True
    monkeypatch.syspath_prepend(str(PACKAGE))
    loaded, diagnostics = load_extensions([ExtensionSpec(use=declaration["use"], enabled=declaration["enabled"], required=True, config=declaration["config"])])
    assert not diagnostics
    assert loaded.plugins[0][1].artifacts[0].compat_queries == ("report_preview",)


def load_report(monkeypatch, **config):
    monkeypatch.syspath_prepend(str(PACKAGE))
    return load_extensions([ExtensionSpec(use=ENTRY, required=True, config={"enabled": True, **config})])


def test_artifact_router_has_no_builtin_report_dispatch():
    assert "report_preview" not in inspect.signature(artifacts.get_artifact).parameters
    assert not (ROOT / "backend/app/gateway/routers/_report_projection.py").exists()


def test_provider_package_loads_through_the_existing_lifecycle(monkeypatch):
    loaded, diagnostics = load_report(monkeypatch)
    assert not diagnostics
    source, plugin = loaded.plugins[0]
    assert source == ENTRY
    assert plugin.namespace == "hartmesh.legacy-report"
    declaration = plugin.artifacts[0]
    assert declaration.compat_queries == ("report_preview",)
    assert declaration.projection_marker == "business-report-v1"
    assert declaration.source_max_bytes == 16 * 1024 * 1024
    assert declaration.preview_max_bytes == 1024 * 1024
    disabled, diagnostics = load_extensions([ExtensionSpec(use=ENTRY, enabled=False)])
    assert not diagnostics and not disabled.plugins
    removed, diagnostics = load_extensions([])
    assert not diagnostics and not removed.plugins
    reloaded, diagnostics = load_report(monkeypatch)
    assert not diagnostics and reloaded.plugins[0][1].artifacts[0].id == declaration.id


def test_projection_filters_display_fields_without_changing_source(monkeypatch):
    loaded, _ = load_report(monkeypatch)
    project = loaded.plugins[0][1].artifacts[0].project
    source = json.loads(FIXTURE.read_text(encoding="utf-8"))
    source["raw_rows"] = [{"private_row": "retained"}]
    source["meta"]["build"] = {"detail": "retained"}
    original = json.dumps(source).encode("utf-8")
    preview = json.loads(project(original))
    assert "raw_rows" not in preview and "build" not in preview["meta"]
    assert preview["kpis"] == source["kpis"]
    assert preview["sections"] == source["sections"]
    assert json.loads(original) == source


@pytest.mark.parametrize("raw", [b"{bad", b"[]", b'{"meta":{},"version":NaN}'])
def test_projector_rejects_malformed_data(monkeypatch, raw):
    loaded, _ = load_report(monkeypatch)
    with pytest.raises(ValueError):
        loaded.plugins[0][1].artifacts[0].project(raw)


@pytest.mark.parametrize("enabled", ["true", 1, None])
def test_package_enablement_requires_an_operator_boolean(monkeypatch, enabled):
    from deerflow.extensions.loader import ExtensionLoadError

    with pytest.raises(ExtensionLoadError):
        load_report(monkeypatch, enabled=enabled)


@pytest.mark.parametrize("old_layout", [True, False], ids=["misnamed-layout-refused", "packaged-layout-upgrades-and-removes"])
def test_stock_package_uses_normal_manager_upgrade_and_remove(tmp_path, monkeypatch, old_layout):
    """Execute real staging/config/removal; only dependency operations are offline doubles."""
    metadata = tomllib.loads((PACKAGE / "pyproject.toml").read_text(encoding="utf-8"))
    distribution = metadata["project"]["name"]
    declared = tomllib.loads((ROOT / "backend/pyproject.toml").read_text(encoding="utf-8"))["tool"]["uv"]["sources"][distribution]["path"]
    assert Path(declared).name == distribution
    relative = "extensions/sources/legacy-report" if old_layout else declared
    root = tmp_path / "host"
    backend = root / "backend"
    backend.mkdir(parents=True)
    project = f'''[project]
name="isolated-host"
version="0.0.0"
requires-python=">=3.12"
dependencies=[]
[dependency-groups]
extensions=["{distribution}"]
[tool.uv.sources]
{distribution}={{path="{relative}"}}
'''
    (backend / "pyproject.toml").write_text(project, encoding="utf-8")
    (backend / "uv.lock").write_text(f'[[package]]\nname="{distribution}"\nversion="0.1.0"\nsource={{directory="{relative}"}}\n', encoding="utf-8")
    plugin = {"name": "legacy-report", "package": distribution, "use": ENTRY, "enabled": False, "required": True, "config": {"enabled": True}}
    config = root / "config.yaml"
    config.write_text(yaml.safe_dump({"config_version": 51, "plugins": [plugin]}, sort_keys=False), encoding="utf-8")
    managed = backend / relative
    shutil.copytree(PACKAGE, managed, ignore=shutil.ignore_patterns("__pycache__"))
    source = tmp_path / "updated-package"
    shutil.copytree(PACKAGE, source, ignore=shutil.ignore_patterns("__pycache__"))
    marker = source / "hartmesh_legacy_report/__init__.py"
    marker.write_text(marker.read_text(encoding="utf-8") + '\nPACKAGE_UPDATE_MARKER="updated"\n', encoding="utf-8")
    calls = []

    def fake_uv(command, cwd):
        calls.append(command[1])
        assert cwd == backend
        if command[1] == "remove":
            (backend / "pyproject.toml").write_text('[project]\nname="isolated-host"\nversion="0.0.0"\nrequires-python=">=3.12"\ndependencies=[]\n[dependency-groups]\nextensions=[]\n', encoding="utf-8")
            (backend / "uv.lock").write_text("version=1\n", encoding="utf-8")

    monkeypatch.setattr("deerflow.extensions.manager._run_uv", fake_uv)
    monkeypatch.setattr("deerflow.extensions.manager._sync_environment", lambda *args: calls.append("sync"))
    monkeypatch.setattr("deerflow.extensions.manager._discover_installed_entry_point", lambda *args: ("legacy-report", ENTRY))
    manager = ExtensionManager(root)
    if old_layout:
        with pytest.raises(ValueError, match="not installed"):
            manager.upgrade(str(source), yes=True)
        assert calls == [] and managed.exists()
        return
    result = manager.upgrade(str(source), yes=True)
    assert result.distribution == distribution
    assert 'PACKAGE_UPDATE_MARKER="updated"' in (managed / "hartmesh_legacy_report/__init__.py").read_text(encoding="utf-8")
    assert yaml.safe_load(config.read_text(encoding="utf-8"))["plugins"] == [plugin]
    manager.remove("legacy-report")
    assert not managed.exists()
    assert yaml.safe_load(config.read_text(encoding="utf-8")).get("plugins", []) == []
    assert calls == ["add", "sync", "remove", "sync"]
