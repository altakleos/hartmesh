"""Counts and role templates are validated configuration, never runtime authority."""

import pytest
from pydantic import ValidationError

from deerflow.config.agents_api_config import AgentsApiConfig
from deerflow.config.storage_spaces_config import StorageSpacesConfig
from deerflow.config.ui_config import UiConfig

PLAN = dict(users=10, user_gib=1, ai_assistants_per_user=1, ai_assistant_gib=1, ai_employees=10, ai_employee_gib=1, company_gib=1)
ROLE = dict(id="researcher", name="Researcher", description="Researches a topic.", soul="Cite sources.\nState uncertainties.")


def test_storage_plan_and_agent_defaults():
    from deerflow.config.storage_spaces_config import StoragePlan

    config = StorageSpacesConfig()
    assert config.user_spaces is True and config.plan is None
    plan = StoragePlan(**PLAN)
    assert plan.model_dump() == PLAN
    assert StorageSpacesConfig(plan=plan).plan == plan
    with pytest.raises(ValidationError):
        plan.users = 5
    with pytest.raises(ValidationError):
        StoragePlan(**PLAN, unexpected=True)
    assert AgentsApiConfig().ai_assistants_per_user == 1
    assert AgentsApiConfig().ai_employees == 10


@pytest.mark.parametrize("field,low,high", [("users", 1, 100), ("user_gib", 1, 1024), ("ai_assistants_per_user", 0, 10), ("ai_assistant_gib", 1, 1024), ("ai_employees", 0, 100), ("ai_employee_gib", 1, 1024), ("company_gib", 1, 1024)])
def test_storage_plan_bounds(field, low, high):
    from deerflow.config.storage_spaces_config import StoragePlan

    for value in (low, high):
        assert getattr(StoragePlan(**{**PLAN, field: value}), field) == value
    for value in (low - 1, high + 1, True, 1.5):
        with pytest.raises(ValidationError):
            StoragePlan(**{**PLAN, field: value})


@pytest.mark.parametrize("field,maximum", [("ai_assistants_per_user", 10), ("ai_employees", 100)])
def test_agent_count_bounds(field, maximum):
    for value in (0, maximum):
        assert getattr(AgentsApiConfig(**{field: value}), field) == value
    for value in (-1, maximum + 1):
        with pytest.raises(ValidationError):
            AgentsApiConfig(**{field: value})


def test_roles_are_optional_unique_bounded_and_frozen():
    assert UiConfig().roles == UiConfig(roles=None).roles == []
    role = UiConfig(roles=[ROLE]).roles[0]
    assert role.skills == [] and role.work_policy is None
    with pytest.raises(ValidationError):
        role.name = "Changed"
    assert len(UiConfig(roles=[{**ROLE, "id": f"role-{i}"} for i in range(8)]).roles) == 8
    for roles in ([ROLE, ROLE], [{**ROLE, "id": f"role-{i}"} for i in range(9)], [{**ROLE, "unknown": True}]):
        with pytest.raises(ValidationError):
            UiConfig(roles=roles)


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "Bad_ID"),
        ("id", "a" * 41),
        ("name", ""),
        ("name", "a" * 61),
        ("name", "two\nlines"),
        ("name", "two\u0085lines"),
        ("name", "two\u2028lines"),
        ("description", "two\u2029lines"),
        ("description", "hidden\u009bcontrol"),
        ("soul", "hidden\u009bcontrol"),
        ("description", "a" * 201),
        ("description", "hidden\u202e"),
        ("soul", "a" * 16001),
        ("soul", "hidden\x01"),
        ("skills", ["deep-research"] * 17),
    ],
)
def test_invalid_role_text_and_skills_are_rejected(field, value):
    with pytest.raises(ValidationError):
        UiConfig(roles=[{**ROLE, field: value}])


def test_released_seat_timestamp_is_read_as_utc():
    from datetime import UTC, datetime
    from uuid import uuid4

    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from deerflow.persistence.user.model import UserRow

    released = datetime(2026, 10, 1)
    row = UserRow(id=str(uuid4()), email="person@example.com", system_role="user", created_at=released, released_at=released, needs_setup=False, token_version=0)
    assert SQLiteUserRepository._row_to_user(row).released_at == released.replace(tzinfo=UTC)
