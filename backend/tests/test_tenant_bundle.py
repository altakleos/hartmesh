"""The tenant bundle: one directory the operator writes, read by two consumers.

A deployment's brand, starter list and report profiles live together in one
directory on the tenant's data disk. Every sandbox mounts it read-only at
``/mnt/tenant`` so the report skill picks up the company name, logo and
colours by itself, and the Gateway reads the same files so the workspace
shows the same company. Nothing is copied between the two; there is exactly
one place a company's name is written.

What these pin is the Gateway's reading of that directory. The rules mirror
the skill's own (``load_brand`` in ``business_report_common.py``): a logo is a
PNG or JPEG inside the bundle or it is nothing; a colour is ``#rrggbb`` or it
is nothing; a missing file is not a problem, a malformed one is a *named*
problem that degrades the field and never the deployment. The starter list is
validated by the same ``UiConfig`` rules a ``config.yaml`` list is, because it
is the same list arriving from a different file.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from deerflow.config.tenant_bundle import TenantBundleConfig, load_tenant_bundle
from deerflow.config.ui_config import StarterConfig

PNG = b"\x89PNG\r\n\x1a\n" + b"\0" * 16


def _bundle(tmp_path: Path, brand: dict | str | None = None, *, logo_bytes: bytes | None = PNG, logo_name: str = "logo.png") -> Path:
    root = tmp_path / "tenant"
    root.mkdir(parents=True)
    if logo_bytes is not None:
        (root / logo_name).write_bytes(logo_bytes)
    if brand is not None:
        (root / "brand.json").write_text(brand if isinstance(brand, str) else json.dumps(brand), encoding="utf-8")
    return root


FULL_BRAND = {"company_name": "Example Services Co.", "logo": "logo.png", "colors": {"primary": "#0a6b3d", "secondary": "#9ccdb4"}}


def test_no_path_is_an_empty_bundle_with_nothing_to_report() -> None:
    bundle = load_tenant_bundle(None)
    assert bundle.path is None
    assert bundle.company_name is None and bundle.logo is None
    assert bundle.primary is None and bundle.secondary is None
    assert bundle.starters is None and bundle.report_profiles == ()
    assert bundle.problems == ()


def test_a_directory_that_is_not_there_yet_is_named_and_nothing_else_breaks(tmp_path: Path) -> None:
    bundle = load_tenant_bundle(tmp_path / "absent")
    assert bundle.path == tmp_path / "absent"
    assert bundle.company_name is None and bundle.starters is None
    assert bundle.problems == ("tenant bundle directory does not exist",)


def test_a_full_brand_is_read_as_the_skill_reads_it(tmp_path: Path) -> None:
    root = _bundle(tmp_path, FULL_BRAND)
    bundle = load_tenant_bundle(root)
    assert bundle.company_name == "Example Services Co."
    assert bundle.primary == "#0a6b3d" and bundle.secondary == "#9ccdb4"
    assert bundle.logo == root / "logo.png"
    assert bundle.problems == ()


def test_a_missing_logo_degrades_to_the_name_alone(tmp_path: Path) -> None:
    """Family 10: the report and the header both carry the company; neither breaks for want of a picture."""
    root = _bundle(tmp_path, FULL_BRAND, logo_bytes=None)
    bundle = load_tenant_bundle(root)
    assert bundle.company_name == "Example Services Co."
    assert bundle.logo is None
    assert bundle.problems == ("brand.json: logo is not a PNG or JPEG file inside the bundle",)


@pytest.mark.parametrize(
    "logo",
    [
        "logo.svg",  # the skill refuses SVG (a stylesheet or an image reference can reach out); the header follows the skill
        "../logo.png",  # never outside the bundle, whatever the file is
        "/etc/hostname",
        "",
        42,
    ],
)
def test_a_logo_the_skill_would_refuse_is_refused_here_too(tmp_path: Path, logo: object) -> None:
    root = _bundle(tmp_path, {**FULL_BRAND, "logo": logo}, logo_name="logo.svg")
    (tmp_path / "logo.png").write_bytes(PNG)  # the parent holds a real PNG the traversal would reach
    bundle = load_tenant_bundle(root)
    assert bundle.logo is None
    assert bundle.company_name == "Example Services Co.", "a bad picture never costs the name"
    if logo == "":
        assert bundle.problems == (), "an empty logo is 'no logo', which is not a problem"
    else:
        assert bundle.problems == ("brand.json: logo is not a PNG or JPEG file inside the bundle",)


def test_colours_are_six_hex_digits_or_nothing(tmp_path: Path) -> None:
    root = _bundle(tmp_path, {**FULL_BRAND, "colors": {"primary": "green", "secondary": "#9ccdb4"}})
    bundle = load_tenant_bundle(root)
    assert bundle.primary is None and bundle.secondary == "#9ccdb4"
    assert bundle.problems == ("brand.json: colors.primary is not a #rrggbb colour",)


def test_the_company_name_is_one_plain_line(tmp_path: Path) -> None:
    root = _bundle(tmp_path, {**FULL_BRAND, "company_name": "  Example Services Co.  "})
    assert load_tenant_bundle(root).company_name == "Example Services Co."

    for index, bad in enumerate(["", "   ", "Two\nlines", "Ex‮ample", 12, "x" * 81]):
        root = _bundle(tmp_path / str(index), {**FULL_BRAND, "company_name": bad})
        bundle = load_tenant_bundle(root)
        assert bundle.company_name is None, bad
        assert len(bundle.problems) == 1 and bundle.problems[0].startswith("brand.json: company_name "), bundle.problems
        assert "Example" not in bundle.problems[0] and "‮" not in bundle.problems[0], "a problem names the rule, never the value"


@pytest.mark.parametrize("body", ["{not json", "[]", '"text"', "42"])
def test_an_unreadable_brand_file_is_one_named_problem(tmp_path: Path, body: str) -> None:
    root = _bundle(tmp_path, body)
    bundle = load_tenant_bundle(root)
    assert bundle.company_name is None and bundle.logo is None and bundle.primary is None
    assert bundle.problems == ("brand.json: not a JSON object",)


def test_brand_json_is_optional(tmp_path: Path) -> None:
    root = _bundle(tmp_path, None)
    bundle = load_tenant_bundle(root)
    assert bundle.company_name is None and bundle.logo is None
    assert bundle.problems == ()


def test_starters_come_from_the_bundle_under_the_config_s_own_rules(tmp_path: Path) -> None:
    root = _bundle(tmp_path, FULL_BRAND)
    (root / "starters.json").write_text(
        json.dumps(
            [
                {"id": "review", "title": "Monthly review", "prompt": "Build the monthly review from the export I attach."},
                {"id": "ask", "title": "Ask about a spreadsheet ", "prompt": "Answer questions about the spreadsheet I attach.\n"},
            ]
        ),
        encoding="utf-8",
    )
    bundle = load_tenant_bundle(root)
    assert bundle.starters == (
        StarterConfig(id="review", title="Monthly review", prompt="Build the monthly review from the export I attach."),
        StarterConfig(id="ask", title="Ask about a spreadsheet", prompt="Answer questions about the spreadsheet I attach."),
    )
    assert bundle.problems == ()


def test_an_empty_starter_list_is_a_deployment_that_wants_no_grid(tmp_path: Path) -> None:
    root = _bundle(tmp_path, FULL_BRAND)
    (root / "starters.json").write_text("[]", encoding="utf-8")
    assert load_tenant_bundle(root).starters == ()


@pytest.mark.parametrize(
    ("body", "rule"),
    [
        ("{not json", "not a JSON list"),
        ('{"id": "x"}', "not a JSON list"),
        (json.dumps([{"id": "a", "title": "A", "prompt": "p"}, {"id": "a", "title": "B", "prompt": "q"}]), "every starter needs its own id"),
        (json.dumps([{"id": "Bad Id", "title": "A", "prompt": "p"}]), "id"),
        (json.dumps([{"id": f"s{i}", "title": "A", "prompt": "p"} for i in range(7)]), "at most 6"),
        (json.dumps([{"id": "a", "title": "", "prompt": "p"}]), "title"),
        (json.dumps([{"id": "a", "title": "A", "prompt": "p", "extra": 1}]), "extra"),
    ],
)
def test_a_starter_list_that_breaks_the_rules_is_a_named_problem_and_no_list(tmp_path: Path, body: str, rule: str) -> None:
    """The grid falls back to what the config says; a typo in the bundle never empties Home."""
    root = _bundle(tmp_path, FULL_BRAND)
    (root / "starters.json").write_text(body, encoding="utf-8")
    bundle = load_tenant_bundle(root)
    assert bundle.starters is None
    assert len(bundle.problems) == 1 and bundle.problems[0].startswith("starters.json: ")
    assert rule in bundle.problems[0], bundle.problems[0]


def test_report_profiles_are_listed_by_name_and_left_to_the_skill(tmp_path: Path) -> None:
    root = _bundle(tmp_path, FULL_BRAND)
    profiles = root / "report-profiles"
    profiles.mkdir()
    (profiles / "services-generic.json").write_text("{}", encoding="utf-8")
    (profiles / "clinic.json").write_text("not even json", encoding="utf-8")
    (profiles / "notes.txt").write_text("", encoding="utf-8")
    bundle = load_tenant_bundle(root)
    assert bundle.report_profiles == ("clinic", "services-generic"), "names only, sorted; the skill validates a profile when it loads one"
    assert bundle.problems == ()


def test_the_config_key_is_one_optional_path() -> None:
    assert TenantBundleConfig().path is None
    assert TenantBundleConfig(path="/srv/hartmesh/operator/tenant").path == "/srv/hartmesh/operator/tenant"
    with pytest.raises(ValueError):
        TenantBundleConfig(path="/x", company_name="inline names are not a second place to write the brand")


def test_the_app_config_carries_the_section(tmp_path: Path) -> None:
    from deerflow.config.app_config import AppConfig

    config = AppConfig.model_validate({"sandbox": {"use": "test"}, "tenant_bundle": {"path": str(tmp_path)}})
    assert config.tenant_bundle.path == str(tmp_path)
    assert AppConfig.model_validate({"sandbox": {"use": "test"}}).tenant_bundle.path is None
