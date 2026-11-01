"""Whether a slash word in a chat names a skill, read as the run that follows reads it."""

from __future__ import annotations

import logging
from typing import Literal

from deerflow.config.agents_config import load_agent_config
from deerflow.skills.slash import parse_slash_skill_reference
from deerflow.skills.storage import get_or_new_user_skill_storage

logger = logging.getLogger(__name__)

SkillOutcome = Literal["run", "off", "unavailable", "failed"]


def slash_skill_outcome(link, text: str) -> SkillOutcome | None:
    """None for a word that names no skill. Reads the skill files, so call it off the loop."""
    reference = parse_slash_skill_reference(text)
    if reference is None or link.agent_kind == "instance":
        return None
    try:
        skills = get_or_new_user_skill_storage(link.owner_user_id).load_skills(enabled_only=False)
        skill = next((candidate for candidate in skills if candidate.name == reference.name), None)
        if skill is None:
            return None
        if not skill.enabled:
            return "off"
        if link.agent_kind == "custom_agent":
            agent = load_agent_config(link.agent_ref, user_id=link.owner_user_id)
            if agent is not None and agent.skills is not None and reference.name not in agent.skills:
                return "unavailable"
        return "run"
    except Exception as error:
        logger.warning("telegram_bots skill_lookup_failed link=%s error=%s", link.id, type(error).__name__)
        return "failed"
