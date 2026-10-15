"""Deployment-owned brief templates, independent of enrolled agent resources."""

import logging

from deerflow.config.app_config import get_app_config
from deerflow.config.tenant_bundle import configured_tenant_bundle
from deerflow.skills.types import SkillCategory

logger = logging.getLogger(__name__)
_last_skill_problems: tuple[str, ...] | None = None


def role_templates() -> list[dict]:
    """Read current operator data; advertise only enabled public skill references."""
    global _last_skill_problems
    config = get_app_config()
    bundle = configured_tenant_bundle(config.tenant_bundle.path)
    roles = bundle.roles if bundle.roles is not None else config.ui.roles
    source = "tenant bundle: roles.json" if bundle.roles is not None else "ui.roles"
    public = set()
    if any(role.skills for role in roles):
        from deerflow.agents.lead_agent.prompt import get_enabled_skills_for_config

        public = {skill.name for skill in get_enabled_skills_for_config(config, user_id=None) if skill.category == SkillCategory.PUBLIC}
    result, problems = [], []
    for index, role in enumerate(roles):
        row = role.model_dump(mode="json")
        row["skills"] = [name for name in role.skills if name in public]
        if len(row["skills"]) != len(role.skills):
            problems.append(f"{source}: {index}.skills: unknown skill")
        result.append(row)
    current = tuple(problems)
    if current != _last_skill_problems:
        _last_skill_problems = current
        for problem in current:
            logger.warning("%s", problem)
    return result
