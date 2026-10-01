"""The tenant VM profile names the product from one optional .env key.

``HARTMESH_PRODUCT_NAME`` is what the sign-in page, the browser tab and the
assistant call the product. Absent, the render writes nothing and the Gateway
says HartMesh; present, it lands in ``ui.product_name`` and is checked by the
Gateway's own validator at render time, so a bad name refuses the start with
the key's name instead of the Gateway answering 503 on every route.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import yaml

from deerflow.config.ui_config import MAX_PRODUCT_NAME_CHARS, UiConfig

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
TEMPLATE = PROFILE / "config.yaml"
CATALOG = PROFILE / "providers"
ENV_EXAMPLE = PROFILE / ".env.example"
README = PROFILE / "README.md"
KEY = "HARTMESH_PRODUCT_NAME"


@pytest.fixture(scope="module")
def render_config() -> Iterator[ModuleType]:
    spec = importlib.util.spec_from_file_location("hartmesh_render_config_product_test", PROFILE / "gateway" / "render_config.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.modules.pop(spec.name, None)


def _environ(**extra: str) -> dict[str, str]:
    return {"DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow", "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0", "HARTMESH_LOCAL_PASSWORDS": "allowed", "HARTMESH_LOCAL_REGISTRATION": "closed", **extra}


def _render(render_config: ModuleType, environ: dict[str, str], template_text: str | None = None) -> dict:
    text = TEMPLATE.read_text(encoding="utf-8") if template_text is None else template_text
    rendered, _ = render_config.render_text(text, render_config.load_catalog(CATALOG), environ)
    return yaml.safe_load(rendered)


def test_an_operator_who_names_nothing_gets_hartmesh(render_config: ModuleType) -> None:
    for environ in (_environ(), _environ(**{KEY: "  "})):
        ui = _render(render_config, environ)["ui"]
        assert "product_name" not in ui
        assert UiConfig.model_validate(ui).product_name == "HartMesh"


def test_the_key_names_the_product(render_config: ModuleType) -> None:
    ui = _render(render_config, _environ(**{KEY: " Acme Assist "}))["ui"]
    assert ui["product_name"] == "Acme Assist"
    assert UiConfig.model_validate(ui).product_name == "Acme Assist"
    assert ui["profile"] == "business", "the rest of the template's ui block is kept"


def test_a_name_the_gateway_would_refuse_refuses_the_start(render_config: ModuleType) -> None:
    for bad in ("Acme\u202eAssist", "Acme\tAssist", "x" * (MAX_PRODUCT_NAME_CHARS + 1)):
        with pytest.raises(render_config.RenderError, match=KEY):
            _render(render_config, _environ(**{KEY: bad}))


def test_a_name_that_reads_as_an_environment_reference_is_refused(render_config: ModuleType) -> None:
    """The Gateway would replace `$AUTH_JWT_SECRET` with the secret and serve it before sign-in."""
    with pytest.raises(render_config.RenderError, match="must not begin with"):
        _render(render_config, _environ(**{KEY: "$AUTH_JWT_SECRET"}))


def test_the_key_is_the_only_owner(render_config: ModuleType) -> None:
    document = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    document["ui"]["product_name"] = "Template Name"
    with pytest.raises(render_config.RenderError, match="ui.product_name"):
        _render(render_config, _environ(), yaml.safe_dump(document, sort_keys=False))


def test_the_key_is_documented_where_the_operator_looks() -> None:
    assert f"#{KEY}=" in ENV_EXAMPLE.read_text(encoding="utf-8"), "optional, so shown commented out"
    assert KEY in README.read_text(encoding="utf-8")
