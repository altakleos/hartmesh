"""The published gateway renderer owns the bundled connection switch."""

import pytest
import yaml
from test_compose_operator_models import TEMPLATE, _base_environ, _render
from test_compose_operator_models import catalog as catalog
from test_compose_operator_models import render_config as render_config


@pytest.mark.parametrize(("setting", "enabled"), [(None, True), ("off", False), ("on", True)])
def test_bundle_connection_switch(render_config, catalog, setting, enabled):
    env = _base_environ()
    if setting is not None:
        env["HARTMESH_AGENT_CONNECTIONS"] = setting
    assert _render(render_config, catalog, env)["agent_connections"] == {"enabled": enabled, "packages": []}


@pytest.mark.parametrize("setting", ["", "ON", "yes", "false", "off "])
def test_invalid_connection_switch_refuses_to_render(render_config, catalog, setting):
    with pytest.raises(render_config.RenderError, match="HARTMESH_AGENT_CONNECTIONS"):
        _render(render_config, catalog, _base_environ(HARTMESH_AGENT_CONNECTIONS=setting))


def test_connection_switch_preserves_operator_package_selection(render_config, catalog):
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    template["agent_connections"] = {"enabled": False, "packages": ["calendar"]}
    rendered, _ = render_config.render(template, catalog, _base_environ())
    assert rendered["agent_connections"] == {"enabled": True, "packages": ["calendar"]}
    disabled, _ = render_config.render(template, catalog, _base_environ(HARTMESH_AGENT_CONNECTIONS="off"))
    assert disabled["agent_connections"] == {"enabled": False, "packages": ["calendar"]}
