"""The distributed switch enables both kinds of jobs without changing personal limits."""

import pytest
import yaml
from pydantic import ValidationError
from test_compose_operator_models import TEMPLATE, _base_environ, _render
from test_compose_operator_models import catalog as catalog_fixture
from test_compose_operator_models import render_config as renderer_fixture

from deerflow.config.subagent_batches_config import SubagentBatchesConfig

catalog = catalog_fixture
render_config = renderer_fixture


@pytest.mark.parametrize(("setting", "expected"), [(None, False), ("off", False), ("on", True)])
def test_rendered_batch_policy(render_config, catalog, setting, expected):
    env = _base_environ()
    if setting is not None:
        env["HARTMESH_BATCHES"] = setting
    section = _render(render_config, catalog, env)["subagent_batches"]
    assert section == {"enabled": expected, "agent_max_items": 50, "agent_max_work_seconds": 900, "agent_max_start_wait_seconds": 3600}
    assert SubagentBatchesConfig.model_validate(section).max_items_per_batch == 5_000


@pytest.mark.parametrize("setting", ["", "ON", "yes", "off "])
def test_invalid_batch_choice(render_config, catalog, setting):
    with pytest.raises(render_config.RenderError, match="HARTMESH_BATCHES"):
        _render(render_config, catalog, _base_environ(HARTMESH_BATCHES=setting))


def test_worker_switch_cannot_be_set_in_template(render_config, catalog):
    template = yaml.safe_load(TEMPLATE.read_text())
    template.setdefault("subagent_batches", {})["enabled"] = False
    with pytest.raises(render_config.RenderError, match="subagent_batches.enabled"):
        render_config.render(template, catalog, _base_environ())


@pytest.mark.parametrize(("field", "maximum"), [("agent_max_items", 50), ("agent_max_work_seconds", 900), ("agent_max_start_wait_seconds", 3600)])
def test_agent_limits_are_bounded_independently_of_the_personal_worker(field, maximum):
    for invalid in (0, maximum + 1):
        with pytest.raises(ValidationError):
            SubagentBatchesConfig.model_validate({field: invalid})
    assert getattr(SubagentBatchesConfig.model_validate({field: 1}), field) == 1
