"""The tenant bundle: what a deployment says about the company it serves.

One directory on the deployment's own disk, written by the operator and read
by two consumers that never talk to each other: every sandbox mounts it
read-only at ``/mnt/tenant`` so the report skill picks up the company name,
logo and colours by itself, and the Gateway reads the same files so the
workspace header, the About page and Home's starter grid show the same
company. There is no second place a brand is written and nothing that copies
one into the other; the config carries one optional path and nothing else.

Its layout is the skill's contract (``skills/public/business-report``):

* ``brand.json`` -- ``company_name``, ``logo`` (a PNG or JPEG next to it),
  ``colors.primary`` and ``colors.secondary`` as ``#rrggbb``;
* ``starters.json`` -- Home's starter list, the same shape ``ui.starters``
  takes in ``config.yaml`` and validated by the same rules, because it is the
  same list arriving from a different file;
* ``report-profiles/*.json`` -- report profiles the skill loads by name ahead
  of its own; listed here, validated there.

Every file is optional and a missing one is not a problem. A malformed one is
a *named* problem -- the file, the field and the rule, never the value, since
an operator's paste can put anything in any field and the problem line is
journalled -- and it degrades that field alone: a bad logo still leaves the
name, a bad starter list leaves the grid the config would have shown. Nothing
here can take a deployment down; ``render_config.py --check`` is where an
operator sees the problems before a start.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from deerflow.config.ui_config import StarterConfig, UiConfig, first_control_or_reordering_character

logger = logging.getLogger(__name__)

#: The suffixes the report skill accepts for a logo, and so the only ones the
#: header shows: an SVG can carry a stylesheet or an image reference that
#: reaches out, which is why the skill refuses it.
LOGO_SUFFIXES = frozenset({".png", ".jpg", ".jpeg"})

MAX_COMPANY_NAME_CHARS = 80


class TenantBundleConfig(BaseModel):
    """Where the bundle is. Nothing about the brand is written in config."""

    model_config = ConfigDict(extra="forbid")

    path: str | None = Field(
        default=None,
        description=(
            "Directory holding brand.json, an optional logo, starters.json and report-profiles/. "
            "Sandboxes mount the same directory read-only at /mnt/tenant (a sandbox.mounts entry); "
            "the Gateway reads it from this path. Unset means no bundle."
        ),
    )


@dataclass(frozen=True)
class TenantBundle:
    """What the bundle directory said, field by field, with every problem named."""

    path: Path | None
    company_name: str | None
    primary: str | None
    secondary: str | None
    #: Absolute path of a PNG or JPEG inside the bundle; None when there is no usable logo.
    logo: Path | None
    #: None when there is no usable starters.json; an empty tuple is a deliberate empty grid.
    starters: tuple[StarterConfig, ...] | None
    report_profiles: tuple[str, ...]
    problems: tuple[str, ...]


EMPTY_BUNDLE = TenantBundle(path=None, company_name=None, primary=None, secondary=None, logo=None, starters=None, report_profiles=(), problems=())


def load_tenant_bundle(path: str | Path | None) -> TenantBundle:
    """Read the bundle at ``path``; every field degrades on its own and every problem is named."""

    if path is None:
        return EMPTY_BUNDLE
    root = Path(path)
    if not root.is_dir():
        return TenantBundle(path=root, company_name=None, primary=None, secondary=None, logo=None, starters=None, report_profiles=(), problems=("tenant bundle directory does not exist",))

    problems: list[str] = []
    company_name, primary, secondary, logo = _read_brand(root, problems)
    starters = _read_starters(root, problems)
    profiles_dir = root / "report-profiles"
    report_profiles = tuple(sorted(entry.stem for entry in profiles_dir.glob("*.json") if entry.is_file())) if profiles_dir.is_dir() else ()
    return TenantBundle(
        path=root,
        company_name=company_name,
        primary=primary,
        secondary=secondary,
        logo=logo,
        starters=starters,
        report_profiles=report_profiles,
        problems=tuple(problems),
    )


def _read_json(path: Path, problems: list[str]) -> object | None:
    """The parsed file, or None with the failure named; a file that is not there is silently None."""
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        problems.append(f"{path.name}: cannot be read")
        return None
    except (ValueError, UnicodeDecodeError):
        return _MALFORMED


_MALFORMED = object()


def _read_brand(root: Path, problems: list[str]) -> tuple[str | None, str | None, str | None, Path | None]:
    data = _read_json(root / "brand.json", problems)
    if data is None:
        return None, None, None, None
    if data is _MALFORMED or not isinstance(data, dict):
        problems.append("brand.json: not a JSON object")
        return None, None, None, None

    company_name = _company_name(data.get("company_name"), problems)
    colors = data.get("colors")
    colors = colors if isinstance(colors, dict) else {}
    primary = _color("primary", colors.get("primary"), problems)
    secondary = _color("secondary", colors.get("secondary"), problems)
    logo = _logo(root, data.get("logo"), problems)
    return company_name, primary, secondary, logo


def _company_name(value: object, problems: list[str]) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        problems.append("brand.json: company_name must be text")
        return None
    name = value.strip()
    if not name:
        problems.append("brand.json: company_name is blank")
        return None
    if len(name) > MAX_COMPANY_NAME_CHARS:
        problems.append(f"brand.json: company_name is longer than {MAX_COMPANY_NAME_CHARS} characters")
        return None
    if first_control_or_reordering_character(name, allow_newlines=False) is not None:
        problems.append("brand.json: company_name must be one line without control or reordering characters")
        return None
    return name


def _color(field: str, value: object, problems: list[str]) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and len(value) == 7 and value[0] == "#" and all(char in "0123456789abcdefABCDEF" for char in value[1:]):
        return value
    problems.append(f"brand.json: colors.{field} is not a #rrggbb colour")
    return None


def _logo(root: Path, value: object, problems: list[str]) -> Path | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        candidate = (root / value).resolve()
        if _inside(candidate, root.resolve()) and candidate.is_file() and candidate.suffix.lower() in LOGO_SUFFIXES:
            return candidate
    problems.append("brand.json: logo is not a PNG or JPEG file inside the bundle")
    return None


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _read_starters(root: Path, problems: list[str]) -> tuple[StarterConfig, ...] | None:
    data = _read_json(root / "starters.json", problems)
    if data is None:
        return None
    if data is _MALFORMED or not isinstance(data, list):
        problems.append("starters.json: not a JSON list")
        return None
    try:
        # The config's own rules -- ids, lengths, plain text, the cap, distinct
        # ids -- applied to the same list from a different file.
        resolved = UiConfig.model_validate({"starters": data}).starters
    except ValidationError as error:
        # Location and rule only; pydantic's `input` is the operator's text.
        rules = "; ".join(f"{'.'.join(str(part) for part in item['loc'])}: {item['msg']}" for item in error.errors(include_input=False, include_url=False))
        problems.append(f"starters.json: {rules}")
        return None
    return tuple(resolved)


_last_reported_problems: tuple[str, ...] | None = None


def configured_tenant_bundle(config: object) -> TenantBundle:
    """The bundle ``config.tenant_bundle.path`` names, its problems journalled once per change rather than per request.

    Both readers on the Gateway -- the features report and the logo route --
    go through here, so an operator's edit is read the same way by both.
    """
    global _last_reported_problems
    section = getattr(config, "tenant_bundle", None)
    bundle = load_tenant_bundle(getattr(section, "path", None))
    if bundle.problems != _last_reported_problems:
        _last_reported_problems = bundle.problems
        for problem in bundle.problems:
            logger.warning("tenant bundle: %s", problem)
    return bundle
