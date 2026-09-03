"""Regression anchors for governed validator event-loop safety.

Validator source identities are prepared once at import/startup. Validation and
promotion compare those cached values and must never rediscover or read source
files on the asyncio event loop.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from test_tool_plane_service import _POLICY, _TENANT, _actor, _base_candidate

from deerflow.tool_plane import (
    GovernedSkillArtifactStore,
    GovernedToolPlaneValidator,
    InMemoryToolPlaneProjection,
    InMemoryToolPlaneRevisionRepository,
    ScopedStageRevisionRequest,
    ToolPlaneRevisionScopeV1,
    ToolPlaneRevisionService,
)

pytestmark = pytest.mark.asyncio


async def test_validation_and_promotion_use_cached_validator_identities(
    tmp_path: Path,
) -> None:
    validator = GovernedToolPlaneValidator(
        policy_digest=_POLICY,
        artifact_store=GovernedSkillArtifactStore(tmp_path / "artifacts"),
        durable=True,
    )
    projection = InMemoryToolPlaneProjection()
    service = ToolPlaneRevisionService(
        repository=InMemoryToolPlaneRevisionRepository(tenant=_TENANT),
        projection=projection,
        validator=validator,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_base_candidate(),
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)
    promoted = await service.promote(staged.revision_id, admin)

    assert report.validator_versions == validator.validator_versions
    assert promoted.state == "promoted"
    assert projection.project_count == 1
