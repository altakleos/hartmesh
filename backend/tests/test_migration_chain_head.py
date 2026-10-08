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
)


def test_the_chain_has_one_head_and_it_is_the_last_distribution_revision():
    script = ScriptDirectory(str(bootstrap._MIGRATIONS_DIR))
    assert script.get_heads() == [DISTRIBUTION_REVISIONS[-1]]
    assert bootstrap._get_head_revision() == DISTRIBUTION_REVISIONS[-1]


def test_each_distribution_revision_follows_the_one_before():
    script = ScriptDirectory(str(bootstrap._MIGRATIONS_DIR))
    previous = LAST_UPSTREAM_REVISION
    for revision in DISTRIBUTION_REVISIONS:
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


@pytest.mark.parametrize("fixture_name", ["release_39_ancestry.json", "release_43_ancestry.json", "release_44_ancestry.json"])
def test_published_ancestry_and_previous_release_upgrade_plan(fixture_name):
    _assert_published_upgrade(ScriptDirectory(str(bootstrap._MIGRATIONS_DIR)), fixture_name)


@pytest.mark.parametrize("reparent_published", [False, True], ids=["append-import", "reject-inserted-ancestor"])
@pytest.mark.parametrize("fixture_name", ["release_39_ancestry.json", "release_43_ancestry.json", "release_44_ancestry.json"])
def test_future_import_is_executed_from_published_head(tmp_path, reparent_published, fixture_name):
    """An inserted ancestor can pass fresh/head checks but be skipped on upgrade."""
    versions = tmp_path / "versions"
    shutil.copytree(bootstrap._MIGRATIONS_DIR / "versions", versions, ignore=shutil.ignore_patterns("__pycache__"))
    imported = "0033_synthetic_import"
    parent = LAST_UPSTREAM_REVISION if reparent_published else DISTRIBUTION_REVISIONS[-1]
    (versions / "0033_synthetic_import.py").write_text(f"revision = {imported!r}\ndown_revision = {parent!r}\ndef upgrade():\n    pass\ndef downgrade():\n    pass\n", encoding="utf-8")
    if reparent_published:
        first = versions / "0027_account_access.py"
        first.write_text(first.read_text(encoding="utf-8").replace(f'"{LAST_UPSTREAM_REVISION}"', f'"{imported}"'), encoding="utf-8")
    script = ScriptDirectory(str(tmp_path))
    assert len(script.get_heads()) == 1
    assert imported in [step.revision.revision for step in script._upgrade_revs("head", "base")]
    if reparent_published:
        assert script._upgrade_revs("head", DISTRIBUTION_REVISIONS[-1]) == []
        with pytest.raises(AssertionError, match="Published parent changed"):
            _assert_published_upgrade(script, fixture_name)
    else:
        assert [step.revision.revision for step in script._upgrade_revs("head", DISTRIBUTION_REVISIONS[-1])] == [imported]
        _assert_published_upgrade(script, fixture_name)
