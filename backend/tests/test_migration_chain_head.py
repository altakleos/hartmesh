"""The migration chain ends in this distribution's own revisions, in one line.

Upstream's revisions come first and stay as upstream wrote them; the
revisions below follow its newest one, each on the one before. A new
revision is added at the end of ``DISTRIBUTION_REVISIONS``. When a merge
brings a new upstream revision, that revision is re-pointed to follow the
last one here, so a database that already carries these keeps one line of
history.
"""

from __future__ import annotations

from alembic.script import ScriptDirectory

from deerflow.persistence import bootstrap

LAST_UPSTREAM_REVISION = "0026_mcp_task_lease_tokens"
DISTRIBUTION_REVISIONS = (
    "0027_account_access",
    "0028_provider_keys",
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
