"""Release notes must disclose database schema changes for the selected version."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / "scripts" / "release_notes.py"
_VERSION = "2.2.0+hartmesh.38"


def _run(
    tmp_path: Path, changelog: str, version: str = _VERSION
) -> subprocess.CompletedProcess[str]:
    path = tmp_path / "CHANGELOG.md"
    path.write_text(changelog, encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(_SCRIPT), version, "--changelog", str(path)],
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    "schema",
    [
        "No database schema changes since v2.1.0+hartmesh.37.",
        "Adds user_preferences; existing deployments upgrade automatically.",
    ],
)
def test_publishes_only_the_selected_release_with_its_schema_statement(
    tmp_path: Path, schema: str
) -> None:
    entry = f"## [{_VERSION}] — 2026-10-02\n\n### Schema changes\n\n{schema}\n\n### Fixed\n\n- Keeps café and 日本語 readable."
    changelog = f"# Changelog\n\n## [Unreleased]\n\nFuture changes.\n\n{entry}\n\n## [2.1.0+hartmesh.37]\n\nOlder changes.\n\n[{_VERSION}]: https://example.com/release\n"

    result = _run(tmp_path, changelog)

    assert result.returncode == 0, result.stderr
    assert result.stdout == entry + f"\n\n[{_VERSION}]: https://example.com/release\n"


@pytest.mark.parametrize(
    ("changelog", "message"),
    [
        (
            "## [Unreleased]\n\n### Schema changes\n\nNo database schema changes.\n",
            "No changelog entry",
        ),
        (f"## [{_VERSION}]\n\n### Fixed\n\n- A bug.\n", "Schema changes"),
        (f"## [{_VERSION}]\n\n### Schema changes\n\n### Fixed\n\n- A bug.\n", "empty"),
        (
            f"## [{_VERSION}]\n\n### Schema changes\n\nNo database schema changes.\n\n## [{_VERSION}]\n\nDuplicate.\n",
            "Duplicate changelog entries",
        ),
    ],
)
def test_refuses_missing_ambiguous_or_incomplete_release_notes(
    tmp_path: Path, changelog: str, message: str
) -> None:
    result = _run(tmp_path, changelog)

    assert result.returncode == 1
    assert message in result.stderr
    assert result.stdout == ""


def test_fenced_example_headings_do_not_select_or_truncate_a_release(
    tmp_path: Path,
) -> None:
    changelog = f"""# Changelog

```markdown
## [{_VERSION}]
### Schema changes
Example only.
```

## [{_VERSION}]

### Schema changes

Adds a table. SQL example:

~~~sql
## [another-version]
~~~

### Fixed

- A bug.
"""

    result = _run(tmp_path, changelog)

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith(f"## [{_VERSION}]\n")
    assert "- A bug." in result.stdout
    assert "Example only." not in result.stdout


def test_workflow_checks_notes_before_publishing_and_uses_them_for_new_and_existing_releases() -> (
    None
):
    workflow = (
        _REPO_ROOT / ".github" / "workflows" / "release-manifest.yaml"
    ).read_text(encoding="utf-8")

    assert workflow.index(
        'python3 scripts/release_notes.py "$VERSION"'
    ) < workflow.index("Authenticate registry clients")
    assert 'gh release create "$TAG"' in workflow
    assert 'gh release edit "$TAG"' in workflow
    assert workflow.count('--notes-file "$RUNNER_TEMP/release-notes.md"') == 2
    assert '--notes "see CHANGELOG.md"' not in workflow
