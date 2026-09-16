"""Public skill text may not hardcode the tree a deployment happens to mount.

A skill package does not know where it is mounted. A durable accepted
invocation mounts exactly one immutable snapshot at
``{skills_root}/.accepted/{snapshot}/`` and nothing else, so a command example
written against ``{skills_root}/public/<skill>/scripts/x.py`` names a path that
does not exist for the invocation reading it.

That is not a hypothetical. In the hartmesh-tenancy ``.16`` tenant-class run the
model read ``business-report``'s ``SKILL.md`` at its correct snapshot path, ran
the very first command the file showed it, was refused, and — with every listing
root above the snapshot refused by the same fence — fell back to
``find / -name "report.py"``. That search took 341.3 s of a 522.2 s turn inside a
512 MiB sandbox, against a script that was mounted and readable throughout.

So the rule is the text's, not the runtime's: a skill addresses its own files
relative to its own directory, and the runtime is what reports where that is.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from deerflow.constants import DEFAULT_SKILLS_CONTAINER_PATH

REPO_ROOT = Path(__file__).resolve().parents[3]
PUBLIC_SKILLS = REPO_ROOT / "skills" / "public"

#: Skill text that still names an absolute skills path. Each of these packages
#: already fails deterministic skill review on ``main`` for an unrelated reason
#: — a secret-like assignment, a ``subprocess`` call without ``shell=True``, a
#: declared sensitive capability — and CI reviews a package only when it
#: changes, so editing this text would fail the gate on debt this repair did not
#: create and has not evaluated. The harness half of the fix still covers them:
#: a stale path now costs one redirected tool call instead of a filesystem walk.
#: Clearing a package's review debt is what removes its entry, and ``strict``
#: xfail is what makes this test say so once the entry is stale.
SKILL_TEXT_AWAITING_REVIEW_DEBT = frozenset(
    {
        "image-generation/SKILL.md",
        "music-generation/SKILL.md",
        "skill-creator/SKILL.md",
        "vercel-deploy-claimable/SKILL.md",
        "video-generation/SKILL.md",
    }
)

#: Every textual resource a model can be told to follow, not just ``SKILL.md``:
#: ``podcast-generation`` carries the same command examples in a template.
SKILL_TEXT_FILES = sorted(PUBLIC_SKILLS.rglob("*.md"))


def _case(path: Path):
    relative = path.relative_to(PUBLIC_SKILLS).as_posix()
    if relative in SKILL_TEXT_AWAITING_REVIEW_DEBT:
        return pytest.param(
            path,
            marks=pytest.mark.xfail(
                strict=True,
                reason=f"{relative} is held by unrelated skill-review debt; see SKILL_TEXT_AWAITING_REVIEW_DEBT",
            ),
        )
    return pytest.param(path)


SKILL_TEXT_CASES = [_case(path) for path in SKILL_TEXT_FILES]


def test_review_debt_entries_all_exist() -> None:
    """A stale entry would silently exempt a file that no longer needs it."""
    known = {path.relative_to(PUBLIC_SKILLS).as_posix() for path in SKILL_TEXT_FILES}
    assert SKILL_TEXT_AWAITING_REVIEW_DEBT <= known


#: ``$SKILL_DIR``/``$IMAGE_SKILL_DIR`` is the convention the files establish:
#: the directory the reader loaded the file from, which ``describe_skill``
#: reports as ``Location``.
_SKILL_DIR_VARIABLE = re.compile(r"\$[A-Z][A-Z0-9_]*SKILL_DIR\b")


def test_public_skills_exist() -> None:
    """Guard the glob itself, so a bad root cannot make this file vacuous."""
    assert SKILL_TEXT_FILES, f"no skill text found under {PUBLIC_SKILLS}"


@pytest.mark.parametrize("path", SKILL_TEXT_CASES, ids=lambda p: str(p.relative_to(PUBLIC_SKILLS)))
def test_public_skill_text_names_no_absolute_skills_path(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    offenders = [line for line in text.splitlines() if DEFAULT_SKILLS_CONTAINER_PATH in line]
    assert not offenders, (
        f"{path.relative_to(REPO_ROOT)} writes an absolute skills path, which an accepted "
        f"invocation does not mount: {offenders}. Address the skill's own files through "
        f"$SKILL_DIR, and another skill's through the directory describe_skill reports."
    )


@pytest.mark.parametrize("path", SKILL_TEXT_FILES, ids=lambda p: str(p.relative_to(PUBLIC_SKILLS)))
def test_public_skill_text_defines_every_directory_variable_it_uses(path: Path) -> None:
    """A variable no one defines is the same dead end by another name."""
    text = path.read_text(encoding="utf-8")
    used = set(_SKILL_DIR_VARIABLE.findall(text))
    if not used:
        return
    for variable in sorted(used):
        name = variable.lstrip("$")
        # The definition is prose, not an assignment: the file has to say what
        # the directory is, because the reader is the one who substitutes it.
        assert f"`{variable}` is" in text or f"{name} is that" in text or f"is `{variable}`" in text, f"{path.relative_to(REPO_ROOT)} uses {variable} without saying which directory it is"
