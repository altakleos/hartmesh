"""The distributed provider explicitly installs the employee proposal form."""

from pathlib import Path

import pytest
import yaml
from test_compose_operator_models import TEMPLATE, _base_environ
from test_compose_operator_models import catalog as catalog
from test_compose_operator_models import render_config as render_config

from deerflow.agent_instances.proposal_tools import installed_forms
from deerflow.extensions.loader import ExtensionSpec, load_extensions


@pytest.mark.parametrize("enabled", [True, False])
def test_rendered_employee_storage_selects_the_packaged_form(render_config, catalog, monkeypatch, enabled):
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    (proposal,) = (plugin for plugin in template["plugins"] if plugin["name"] == "work-proposals")
    assert proposal["enabled"] is True and proposal["package"] == "hartmesh-work-proposals"
    proposal["enabled"] = enabled
    rendered, _ = render_config.render(template, catalog, _base_environ(HARTMESH_AI_EMPLOYEES="3", HARTMESH_AI_EMPLOYEE_STORAGE_GIB="1", HARTMESH_COMPANY_STORAGE_GIB="1"))
    assert rendered["agents_api"]["enabled"] is True
    assert rendered["plugins"] == template["plugins"]
    assert any(plugin["name"] == "legacy-report" for plugin in rendered["plugins"])
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "extensions/sources/hartmesh-work-proposals"))
    loaded, diagnostics = load_extensions([ExtensionSpec(use=proposal["use"], enabled=proposal["enabled"], required=True)])
    assert not diagnostics
    assert [plugin.proposal_tool.name for _, plugin in installed_forms(loaded)] == (["propose_tracked_work"] if enabled else [])
