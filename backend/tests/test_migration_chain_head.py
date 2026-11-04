"""The migration chain ends in this distribution's own revisions, in one line.

Published ancestry is immutable. New work, including imported upstream DDL,
must be reachable from the published head as well as on a fresh database.
The release-39 fixture comes from its tag; never regenerate it from HEAD to
accommodate an ancestry change. Later releases add separate historical fixtures.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from alembic.script import ScriptDirectory

from deerflow.persistence import bootstrap

LAST_UPSTREAM_REVISION = "0026_mcp_task_lease_tokens"
DISTRIBUTION_REVISIONS = (
    "0027_account_access",
    "0028_provider_keys",
    "0029_shared_publications",
    "0030_storage_spaces",
    "0031_storage_files",
    "0032_storage_lifecycle",
    "0033_storage_features",
    "0034_agent_instances",
    "0035_agent_conversations",
    "0036_agent_instance_memory",
    "0037_agent_lifecycle",
    "0038_agent_work",
    "0039_human_input",
    "0040_work_execution",
    "0041_storage_host_replacements",
    "0042_telegram_bots",
    "0043_telegram_inbox_chat_scope",
    "0044_telegram_assistant_ids",
)
# Upstream DeerFlow's revisions through b537d9c16, appended after the last
# published distribution head (plan §5.3: append-import, never insert).
IMPORTED_UPSTREAM_REVISIONS = (
    "0027_notification_deliveries",
    "0028_parked_attempts",
    "0029_scheduler_agent_tasks",
    "0030_notification_claim_tokens",
    "0031_scheduled_streak_boundary",
    "0032_activity_and_task_events",
    "0033_batch_result_artifact",
    "0034_run_event_seq_watermark",
    "0035_login_throttle",
    "0036_run_idempotency_request",
    "0037_project_document_summaries",
    "0038_channel_thread_bindings",
    "0039_user_disabled",
    "0040_scheduled_group_notice",
    "0041_im_notice_preferences",
    "0042_channel_leases",
    "0043_runtime_im_notices",
    "0044_channel_runtime_configs",
    "0045_completed_run_evidence",
    "0046_skill_mutations",
    "0047_mcp_oauth_tokens",
    "0048_extension_execution_leases",
)
CHAIN_AFTER_UPSTREAM_BASE = DISTRIBUTION_REVISIONS + IMPORTED_UPSTREAM_REVISIONS
HEAD = CHAIN_AFTER_UPSTREAM_BASE[-1]
PUBLISHED_FIXTURES = ["release_39_ancestry.json", "release_43_ancestry.json", "release_44_ancestry.json", "release_45_ancestry.json", "release_46_ancestry.json", "release_50_ancestry.json"]


def test_the_chain_has_one_head_and_it_is_the_last_appended_revision():
    script = ScriptDirectory(str(bootstrap._MIGRATIONS_DIR))
    assert script.get_heads() == [HEAD]
    assert bootstrap._get_head_revision() == HEAD


def test_each_distribution_revision_follows_the_one_before():
    """One line: HartMesh's published revisions, then the imported upstream ones."""
    script = ScriptDirectory(str(bootstrap._MIGRATIONS_DIR))
    previous = LAST_UPSTREAM_REVISION
    for revision in CHAIN_AFTER_UPSTREAM_BASE:
        assert script.get_revision(revision).down_revision == previous
        assert len(revision) <= 32, "alembic_version.version_num is VARCHAR(32)"
        previous = revision


def _assert_published_upgrade(script: ScriptDirectory, fixture_name: str = "release_39_ancestry.json") -> None:
    published = json.loads((Path(__file__).parent / "fixtures/migrations" / fixture_name).read_text(encoding="utf-8"))
    for revision, ancestry in published["revisions"].items():
        actual = script.get_revision(revision)
        assert actual.down_revision == ancestry["down_revision"], f"Published parent changed: {revision}"
        assert actual.dependencies == ancestry["depends_on"], f"Published dependencies changed: {revision}"
    fresh = [step.revision.revision for step in script._upgrade_revs("head", "base")]
    upgrade = [step.revision.revision for step in script._upgrade_revs("head", published["head"])]
    assert upgrade == [revision for revision in fresh if revision not in published["revisions"]]


@pytest.mark.parametrize("fixture_name", PUBLISHED_FIXTURES)
def test_published_ancestry_and_previous_release_upgrade_plan(fixture_name):
    _assert_published_upgrade(ScriptDirectory(str(bootstrap._MIGRATIONS_DIR)), fixture_name)


@pytest.mark.parametrize("reparent_published", [False, True], ids=["append-import", "reject-inserted-ancestor"])
@pytest.mark.parametrize("fixture_name", PUBLISHED_FIXTURES)
def test_future_import_is_executed_from_published_head(tmp_path, reparent_published, fixture_name):
    """An inserted ancestor can pass fresh/head checks but be skipped on upgrade."""
    versions = tmp_path / "versions"
    shutil.copytree(bootstrap._MIGRATIONS_DIR / "versions", versions, ignore=shutil.ignore_patterns("__pycache__"))
    imported = "0033_synthetic_import"
    parent = LAST_UPSTREAM_REVISION if reparent_published else HEAD
    (versions / "0033_synthetic_import.py").write_text(f"revision = {imported!r}\ndown_revision = {parent!r}\ndef upgrade():\n    pass\ndef downgrade():\n    pass\n", encoding="utf-8")
    if reparent_published:
        first = versions / "0027_account_access.py"
        first.write_text(first.read_text(encoding="utf-8").replace(f'"{LAST_UPSTREAM_REVISION}"', f'"{imported}"'), encoding="utf-8")
    script = ScriptDirectory(str(tmp_path))
    assert len(script.get_heads()) == 1
    assert imported in [step.revision.revision for step in script._upgrade_revs("head", "base")]
    if reparent_published:
        assert script._upgrade_revs("head", HEAD) == []
        with pytest.raises(AssertionError, match="Published parent changed"):
            _assert_published_upgrade(script, fixture_name)
    else:
        assert [step.revision.revision for step in script._upgrade_revs("head", HEAD)] == [imported]
        _assert_published_upgrade(script, fixture_name)
