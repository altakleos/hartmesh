"""Public skill text may not hardcode the tree a deployment happens to mount.

A skill package does not know where it is mounted. A durable accepted
invocation mounts exactly one immutable snapshot at
``{skills_root}/.accepted/{snapshot}/`` and nothing else, so a command example
written against ``{skills_root}/public/<skill>/scripts/x.py`` names a path that
does not exist for the invocation reading it.

That is not a hypothetical. In a released-profile qualification run the
model read ``business-report``'s ``SKILL.md`` at its correct snapshot path, ran
the very first command the file showed it, was refused, and — with every listing
root above the snapshot refused by the same fence — fell back to
``find / -name "report.py"``. That search took 341.3 s of a 522.2 s turn inside a
512 MiB sandbox, against a script that was mounted and readable throughout.

So the rule is the text's, not the runtime's: a skill addresses its own files
relative to its own directory, and the runtime is what reports where that is.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from deerflow.constants import DEFAULT_SKILLS_CONTAINER_PATH

REPO_ROOT = Path(__file__).resolve().parents[3]
PUBLIC_SKILLS = REPO_ROOT / "skills" / "public"

#: Skill text that still names an absolute skills path. Each of these four
#: packages fails the deterministic skill-review gate on ``main`` — a
#: secret-like assignment in ``image``/``music``/``video``'s ``generate.py``, a
#: declared sensitive capability in ``vercel-deploy-claimable`` — with no
#: trusted waiver, and CI reviews a package only when it changes, so editing
#: this text would fail the gate on debt this repair did not create and has not
#: evaluated. (``skill-creator`` looked like a fifth until the gate was actually
#: run with the trusted manifest: its two findings are waived, so it is fixed
#: here like the rest.) The harness half of the fix still covers these four: a
#: stale path now costs one redirected tool call instead of a filesystem walk.
#: Clearing a package's review debt is what removes its entry, and ``strict``
#: xfail is what makes this test say so once the entry is stale.
SKILL_TEXT_AWAITING_REVIEW_DEBT = frozenset(
    {
        "image-generation/SKILL.md",
        "music-generation/SKILL.md",
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
#: a skill's own directory, which ``describe_skill`` reports as ``Directory``.
#: Matches the bare name as well as a prefixed one, braced or not — an earlier
#: spelling required a character between ``$`` and ``SKILL_DIR`` and so could
#: only ever see ``$IMAGE_SKILL_DIR``, which is how an undefined ``$SKILL_DIR``
#: shipped in a template.
_SKILL_DIR_VARIABLE = re.compile(r"\$\{?((?:[A-Z][A-Z0-9_]*_)?SKILL_DIR)\b")


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
    """A variable no one defines is the same dead end by another name.

    Unset, ``"$SKILL_DIR/scripts/x.py"`` expands to ``/scripts/x.py`` — not a
    skills path, so the accepted-snapshot fence never sees it and the model gets
    a bare "no such file" with no redirect. That is strictly worse than the
    absolute path this convention replaced, so every file that uses one of these
    variables has to say, in prose, which directory it is.
    """
    text = path.read_text(encoding="utf-8")
    for name in sorted(set(_SKILL_DIR_VARIABLE.findall(text))):
        # The reader is the one who substitutes it, so the definition is prose.
        assert f"`${name}` is" in text, f"{path.relative_to(REPO_ROOT)} uses ${name} without saying which directory it is"


@pytest.mark.parametrize("path", SKILL_TEXT_FILES, ids=lambda p: str(p.relative_to(PUBLIC_SKILLS)))
def test_every_command_example_fails_loudly_when_the_variable_is_unset(path: Path) -> None:
    """A copied-verbatim command must say what is missing, not resolve to ``/``."""
    offenders = [line for line in path.read_text(encoding="utf-8").splitlines() if re.search(r"\$\{?(?:[A-Z][A-Z0-9_]*_)?SKILL_DIR\}?/", line)]
    assert not offenders, f"{path.relative_to(REPO_ROOT)} uses an unguarded expansion; write ${{SKILL_DIR:?…}}: {offenders}"


#: A ``${VAR:?message}`` guard, as the command examples write it.
_GUARD = re.compile(r"\$\{[A-Za-z_][A-Za-z0-9_]*:\?[^}]*\}")


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is the sandbox shell; without it there is nothing to parse with")
@pytest.mark.parametrize("path", SKILL_TEXT_FILES, ids=lambda path: path.relative_to(PUBLIC_SKILLS).as_posix())
def test_every_path_guard_parses_in_bash(path: Path) -> None:
    """The guard's message was once ``set it to this skill's directory``.

    Inside a double-quoted ``${VAR:?word}`` bash still pairs single quotes, so
    that apostrophe opened a quote nothing closed: a copied command was a
    syntax error, and in the AIO sandbox's persistent shell it waited for the
    closing quote until the call's own timeout, with no output. zsh and dash
    accept it, which is why a local sandbox never showed it. Every guard is
    parsed here the way the sandbox's shell parses it.
    """
    guards = sorted(set(_GUARD.findall(path.read_text(encoding="utf-8"))))
    for guard in guards:
        parsed = subprocess.run(["bash", "-n", "-c", f'python "{guard}/scripts/x.py"'], capture_output=True, text=True, timeout=10)
        assert parsed.returncode == 0, f"{guard}: {parsed.stderr.strip()}"


#: A script run through a skill-directory variable: ``python "${SKILL_DIR:?…}/…"``,
#: also as a JSON-escaped ``command`` string (``\"``). Group 1 is the variable.
_GUARDED_RUN = re.compile(r'\b(?:python3?|bash|sh)\s+\\?"\$\{((?:[A-Z][A-Z0-9_]*_)?SKILL_DIR):\?')

#: The assignment written as a prefix of the command that expands it:
#: ``SKILL_DIR=… python "${SKILL_DIR}/…"``. The shell expands the command's
#: own words before the prefix takes effect, so the path comes out as
#: ``/scripts/…`` (or, guarded, an "unset" error for a variable just set).
_PREFIX_ASSIGNMENT = re.compile(r'\b((?:[A-Z][A-Z0-9_]*_)?SKILL_DIR)=(?:\\?"[^"\\]*\\?"|[^\s;"\\]+)[ \t]+(?:python3?|bash|sh)\b')

#: Any path through a skill-directory variable, guarded or not, whatever runs it.
#: Group 1 is the variable.
_PATH_THROUGH_VARIABLE = re.compile(r"\$\{?((?:[A-Z][A-Z0-9_]*_)?SKILL_DIR)(?::\?[^}]*)?\}?/")

#: What must come just before such a path: the variable assigned as its own
#: statement, then one command word and the path's opening quote.
_OWN_STATEMENT = r'\b{name}=\\?"<[^"\\>]+>\\?";\s*[\w.]+\s+\\?"$'


@pytest.mark.parametrize("path", SKILL_TEXT_FILES, ids=lambda p: str(p.relative_to(PUBLIC_SKILLS)))
def test_every_command_example_assigns_its_directory_as_a_statement_of_its_own(path: Path) -> None:
    """The examples, not the prose, are what a model copies.

    The text used to say "set ``SKILL_DIR`` … at the start of each command"
    while no example showed the assignment, so a model composed it itself --
    as ``SKILL_DIR="…" python "${SKILL_DIR}/scripts/report.py" …``, which bash
    runs as ``python /scripts/report.py``: a released report turn failed on
    exactly that and spent three more calls finding out why. Each example now
    carries ``NAME="<…>"; `` ahead of the run, the one shape that expands; any
    path through the variable, whatever command takes it, is held to the same.
    """
    text = path.read_text(encoding="utf-8")
    missing = []
    for line in text.splitlines():
        for match in _PATH_THROUGH_VARIABLE.finditer(line):
            if not re.search(_OWN_STATEMENT.format(name=re.escape(match.group(1))), line[: match.start()]):
                missing.append(line.strip())
    assert not missing, f"{path.relative_to(REPO_ROOT)}: an example runs a script without assigning its directory first: {missing}"
    prefixed = [match.group(0) for match in _PREFIX_ASSIGNMENT.finditer(text)]
    assert not prefixed, f"{path.relative_to(REPO_ROOT)} shows the assignment as a prefix, which bash expands too late: {prefixed}"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is the sandbox shell; without it there is nothing to run with")
@pytest.mark.parametrize("path", SKILL_TEXT_FILES, ids=lambda p: str(p.relative_to(PUBLIC_SKILLS)))
def test_every_command_example_resolves_its_script_under_the_directory_it_assigns(path: Path, tmp_path: Path) -> None:
    """Run each example's assignment and script path in bash, with the placeholder filled in as a model fills it."""
    shape = re.compile(r'\b((?:[A-Z][A-Z0-9_]*_)?SKILL_DIR)="<[^">]+>";\s*(?:python3?|bash|sh)\s+("\$\{\1:\?[^}]*\}/[^"]+")')
    text = path.read_text(encoding="utf-8").replace('\\"', '"')
    runs = shape.findall(text)
    for name, script in sorted(set(runs)):
        command = f'{name}="{tmp_path}"; printf "%s" {script}'
        ran = subprocess.run(["bash", "-c", command], capture_output=True, text=True, timeout=10)
        assert ran.returncode == 0, f"{command}: {ran.stderr.strip()}"
        assert ran.stdout.startswith(f"{tmp_path}/scripts/"), f"{command} resolved to {ran.stdout!r}"
    assert len(runs) >= len(_GUARDED_RUN.findall(text)), f"{path.relative_to(REPO_ROOT)}: an example did not match the runnable shape"


@pytest.mark.parametrize("path", SKILL_TEXT_FILES, ids=lambda p: str(p.relative_to(PUBLIC_SKILLS)))
def test_every_json_example_that_runs_a_script_still_parses(path: Path) -> None:
    """A tool-call example escapes its quotes; one left unescaped is a call a model cannot copy."""
    blocks = re.findall(r"```json\n(.*?)```", path.read_text(encoding="utf-8"), flags=re.DOTALL)
    for block in blocks:
        if "SKILL_DIR" in block:
            json.loads(block)
