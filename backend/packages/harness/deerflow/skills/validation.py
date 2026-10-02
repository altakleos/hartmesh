"""Skill frontmatter validation utilities.

Pure-logic validation of SKILL.md frontmatter — no FastAPI or HTTP dependencies.
"""

import re
from collections.abc import Callable
from pathlib import Path

from deerflow.skills.first_command import FIRST_COMMAND_PROPERTY, is_package_file, parse_first_command
from deerflow.skills.frontmatter import ALLOWED_FRONTMATTER_PROPERTIES, split_skill_markdown
from deerflow.skills.parser import parse_allowed_tools
from deerflow.skills.types import SKILL_MD_FILE


def _validate_skill_frontmatter(skill_dir: Path, *, package: bool = True) -> tuple[bool, str, str | None]:
    """Validate a skill directory's SKILL.md frontmatter.

    Args:
        skill_dir: Path to the skill directory containing SKILL.md.
        package: Whether *skill_dir* holds the whole package. When it holds
            only ``SKILL.md`` (validating content before it is written), a
            ``first-command`` is checked for its shape alone; loading refuses
            one whose script the package does not have.

    Returns:
        Tuple of (is_valid, message, skill_name).
    """
    skill_md = skill_dir / SKILL_MD_FILE
    if not skill_md.exists():
        return False, f"{SKILL_MD_FILE} not found", None

    content = skill_md.read_text(encoding="utf-8")
    return validate_skill_frontmatter_text(content, is_package_script=(lambda script: is_package_file(skill_dir, script)) if package else None)


def validate_skill_frontmatter_text(content: str, *, is_package_script: Callable[[str], bool] | None = None) -> tuple[bool, str, str | None]:
    """Validate captured text using the same rules as installation.

    ``is_package_script`` says whether a ``first-command`` script is a regular
    file of the package; without it the command is checked for its shape alone.
    """
    skill_md = Path(SKILL_MD_FILE)
    parts, error = split_skill_markdown(content)
    if error:
        return False, error, None
    if parts is None:
        return False, "Invalid frontmatter format", None
    frontmatter = parts.metadata

    # Check for unexpected properties
    unexpected_keys = set(frontmatter.keys()) - ALLOWED_FRONTMATTER_PROPERTIES
    if unexpected_keys:
        return False, f"Unexpected key(s) in SKILL.md frontmatter: {', '.join(sorted(unexpected_keys))}", None

    # Check required fields
    if "name" not in frontmatter:
        return False, "Missing 'name' in frontmatter", None
    if "description" not in frontmatter:
        return False, "Missing 'description' in frontmatter", None

    # Validate name
    name = frontmatter.get("name", "")
    if not isinstance(name, str):
        return False, f"Name must be a string, got {type(name).__name__}", None
    name = name.strip()
    if not name:
        return False, "Name cannot be empty", None

    # Check naming convention (hyphen-case: lowercase with hyphens)
    if not re.match(r"^[a-z0-9-]+$", name):
        return False, f"Name '{name}' should be hyphen-case (lowercase letters, digits, and hyphens only)", None
    if name.startswith("-") or name.endswith("-") or "--" in name:
        return False, f"Name '{name}' cannot start/end with hyphen or contain consecutive hyphens", None
    if len(name) > 64:
        return False, f"Name is too long ({len(name)} characters). Maximum is 64 characters.", None

    # Validate description
    description = frontmatter.get("description", "")
    if not isinstance(description, str):
        return False, f"Description must be a string, got {type(description).__name__}", None
    description = description.strip()
    # The loader (parse_skill_file) drops a skill whose description is blank, so
    # accepting one here would write a SKILL.md that never loads again.
    if not description:
        return False, "Description cannot be empty", None
    if "<" in description or ">" in description:
        return False, "Description cannot contain angle brackets (< or >)", None
    if len(description) > 1024:
        return False, f"Description is too long ({len(description)} characters). Maximum is 1024 characters.", None

    try:
        parse_allowed_tools(frontmatter.get("allowed-tools"), skill_md)
    except ValueError as e:
        return False, str(e).replace(str(skill_md), SKILL_MD_FILE), None

    required_secrets = frontmatter.get("required-secrets")
    if required_secrets is not None and not isinstance(required_secrets, list):
        return False, f"required-secrets in {SKILL_MD_FILE} must be a list", None
    if required_secrets is not None:
        for item in required_secrets:
            if isinstance(item, dict) and not isinstance(item.get("optional", False), bool):
                if item.get("name") is None:
                    return False, "required-secrets entry without a name has an optional field that must be a boolean", None
                return False, f"required-secrets entry {item.get('name')!r} optional must be a boolean", None

    secrets_autonomous = frontmatter.get("secrets-autonomous")
    if secrets_autonomous is not None and not isinstance(secrets_autonomous, bool):
        return False, f"secrets-autonomous in {SKILL_MD_FILE} must be a boolean", None

    try:
        parse_first_command(frontmatter.get(FIRST_COMMAND_PROPERTY), is_package_script or (lambda script: True))
    except ValueError as e:
        return False, str(e), None

    return True, "Skill is valid!", name
