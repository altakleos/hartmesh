"""A skill's declared ``first-command``: parsing the declaration and recognising a run of it."""

from pathlib import Path

import pytest

from deerflow.skills.first_command import FirstCommand, parse_first_command, runs_first_command

_DIRECTORY = "/mnt/skills/.accepted/snap-1/public/business-report"
_BUILD = FirstCommand("scripts/report.py", ("build",))
_GUARD = "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}"


@pytest.fixture
def skill_dir(tmp_path: Path) -> Path:
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "report.py").write_text("print('report')\n", encoding="utf-8")
    return tmp_path


# ── The declaration ──────────────────────────────────────────────────────────


def _in(skill_dir: Path):
    return lambda script: (skill_dir / script).is_file()


def test_a_declaration_names_a_script_and_the_words_after_it(skill_dir):
    assert parse_first_command("scripts/report.py build", _in(skill_dir)) == _BUILD
    assert parse_first_command("scripts/report.py", _in(skill_dir)) == FirstCommand("scripts/report.py", ())
    assert str(_BUILD) == "scripts/report.py build"


def test_no_declaration_is_none(skill_dir):
    assert parse_first_command(None, _in(skill_dir)) is None


@pytest.mark.parametrize(
    ("raw", "fragment"),
    [
        (["scripts/report.py", "build"], "must be a string"),
        ("", "cannot be empty"),
        ("   ", "cannot be empty"),
        ("/mnt/skills/public/business-report/scripts/report.py build", "relative to the skill's directory"),
        ("../other/scripts/report.py build", "relative to the skill's directory"),
        ("./scripts/report.py build", "relative to the skill's directory"),
        ("scripts/../scripts/report.py build", "relative to the skill's directory"),
        ("scripts/missing.py build", "not a file in the skill's directory"),
        ("scripts build", "not a Python script"),
        ("scripts/report.sh build", "not a Python script"),
        ("scripts/report.py build; rm x", "plain words"),
        ("scripts/report.py $PERIOD", "plain words"),
        ("scripts/report.py 'build'", "plain words"),
    ],
)
def test_a_malformed_declaration_is_refused(skill_dir, raw, fragment):
    with pytest.raises(ValueError, match="first-command") as refused:
        parse_first_command(raw, _in(skill_dir))

    assert fragment in str(refused.value)


# ── Recognising a run of it ──────────────────────────────────────────────────


def _runs(command: str) -> bool:
    return runs_first_command(command, directory=_DIRECTORY, first_command=_BUILD)


@pytest.mark.parametrize(
    "command",
    [
        # The form every example in business-report's SKILL.md uses, as the released model sent it.
        f'SKILL_DIR="{_DIRECTORY}"; python "{_GUARD}/scripts/report.py" build /mnt/user-data/uploads/input.xlsx --period 2026-08 --out /mnt/user-data/outputs/reports/2026-08-business-review --render pdf,docx,xlsx',
        f'SKILL_DIR="{_DIRECTORY}"\npython3 "$SKILL_DIR/scripts/report.py" build /mnt/user-data/uploads/input.xlsx',
        f"SKILL_DIR={_DIRECTORY} && python ${{SKILL_DIR}}/scripts/report.py build input.xlsx",
        f"python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"python3.12 -u {_DIRECTORY}/scripts/report.py build input.xlsx 2>&1 | tail -n 40",
        f"cd {_DIRECTORY} && python scripts/report.py build /mnt/user-data/uploads/input.xlsx",
        f'D="{_DIRECTORY}"; cd "$D/scripts"; python ./report.py build input.xlsx',
        f"mkdir -p /mnt/user-data/outputs/reports && python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"python {_DIRECTORY}/./scripts//report.py build input.xlsx",
        f"python {_DIRECTORY}/scripts/report.py build input.xlsx  # August review",
        f'export SKILL_DIR="{_DIRECTORY}"; python "$SKILL_DIR/scripts/report.py" build input.xlsx',
        f'declare -x SKILL_DIR="{_DIRECTORY}"; python "$SKILL_DIR/scripts/report.py" build input.xlsx',
        f"python {_DIRECTORY}/scripts/report.py \\\n  build \\\n  input.xlsx --period 2026-08",
        f"timeout 300 python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"timeout -k 10 5m python3 {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"env PYTHONUNBUFFERED=1 python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"nice -n 10 python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"time python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"nohup python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"command python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"exec python {_DIRECTORY}/scripts/report.py build input.xlsx",
        f"python -W ignore -X utf8 {_DIRECTORY}/scripts/report.py build input.xlsx",
    ],
)
def test_the_first_command_is_recognised_in_the_forms_bash_would_run(command):
    assert _runs(command)


@pytest.mark.parametrize(
    "command",
    [
        # The inspections the released model chose before building.
        "cd /mnt/user-data && python3 -c \"\nimport openpyxl\nwb = openpyxl.load_workbook('/mnt/user-data/uploads/input.xlsx', data_only=True)\nprint(wb.sheetnames)\n\"\n",
        "ls -la /mnt/user-data/uploads/input.xlsx",
        f"ls {_DIRECTORY}/scripts",
        f"cat {_DIRECTORY}/scripts/report.py",
        "ls -R /mnt/user-data/outputs",
        # The skill's own script, but not its first command.
        f"python {_DIRECTORY}/scripts/report.py inspect /mnt/user-data/uploads/input.xlsx",
        f"python {_DIRECTORY}/scripts/report.py --help",
        f"python {_DIRECTORY}/scripts/report.py",
        # An assignment in front of the command is not set yet when bash expands its words.
        f'SKILL_DIR="{_DIRECTORY}" python "{_GUARD}/scripts/report.py" build input.xlsx',
        # A variable nothing in the command assigns.
        f'python "{_GUARD}/scripts/report.py" build input.xlsx',
        'python "$HOME/scripts/report.py" build input.xlsx',
        # A relative script with no known working directory, and another skill's script.
        "python scripts/report.py build input.xlsx",
        "python /mnt/skills/.accepted/snap-1/public/other-skill/scripts/report.py build",
        f"python {_DIRECTORY}/../other-skill/scripts/report.py build",
        # Not Python running the script.
        f"bash {_DIRECTORY}/scripts/report.py build",
        f"python -c 'import runpy' {_DIRECTORY}/scripts/report.py build",
        f"python -m report {_DIRECTORY}/scripts/report.py build",
        # ``-c`` and ``-m`` take the next word as code or a module, never as a script file.
        f"python -c {_DIRECTORY}/scripts/report.py build",
        f"python -m {_DIRECTORY}/scripts/report.py build",
        # A relative ``cd`` from a directory nothing established.
        "cd scripts && python report.py build input.xlsx",
        # An unset guard in front of a relative path stops bash before Python starts.
        f'cd {_DIRECTORY} && python "${{SKILL_DIR:?assign it}}scripts/report.py" build',
        # The declared words commented out.
        f"python {_DIRECTORY}/scripts/report.py # build",
        # An environment variable passed through env is not set for the words bash expands.
        f'env SKILL_DIR="{_DIRECTORY}" python "{_GUARD}/scripts/report.py" build',
        f"echo python {_DIRECTORY}/scripts/report.py build",
        f"python $(echo {_DIRECTORY})/scripts/report.py build",
        f'python "{_DIRECTORY}/scripts/report.py build',
        "",
    ],
)
def test_anything_else_is_not_the_first_command(command):
    assert not _runs(command)


def test_a_bare_script_declaration_accepts_any_arguments():
    bare = FirstCommand("scripts/report.py", ())

    assert runs_first_command(f"python {_DIRECTORY}/scripts/report.py show r.json", directory=_DIRECTORY, first_command=bare)
    assert not runs_first_command(f"ls {_DIRECTORY}/scripts/report.py", directory=_DIRECTORY, first_command=bare)


# ── Where a declaration is read ──────────────────────────────────────────────


def _write_skill(skill_dir: Path, first_command: str) -> Path:
    skill_md = skill_dir / "SKILL.md"
    skill_md.write_text(f"---\nname: report-kit\ndescription: Builds a report.\nfirst-command: {first_command}\n---\n# Report kit\n", encoding="utf-8")
    return skill_md


def test_a_loaded_skill_carries_its_first_command(skill_dir):
    from deerflow.skills.parser import parse_skill_file
    from deerflow.skills.types import SkillCategory

    skill = parse_skill_file(_write_skill(skill_dir, "scripts/report.py build"), SkillCategory.CUSTOM)

    assert skill is not None
    assert skill.first_command == _BUILD


def test_a_skill_with_a_malformed_first_command_does_not_load(skill_dir, caplog):
    from deerflow.skills.parser import parse_skill_file
    from deerflow.skills.types import SkillCategory

    assert parse_skill_file(_write_skill(skill_dir, "scripts/missing.py build"), SkillCategory.CUSTOM) is None
    assert "Invalid first-command" in caplog.text


def test_install_validation_names_a_malformed_first_command(skill_dir):
    from deerflow.skills.validation import _validate_skill_frontmatter

    _write_skill(skill_dir, "scripts/report.py build")
    assert _validate_skill_frontmatter(skill_dir) == (True, "Skill is valid!", "report-kit")

    _write_skill(skill_dir, "/usr/bin/python build")
    valid, message, _name = _validate_skill_frontmatter(skill_dir)
    assert not valid
    assert "first-command" in message
    assert str(skill_dir) not in message


def test_a_first_command_that_is_a_link_is_not_a_package_file(skill_dir, tmp_path_factory):
    from deerflow.skills.first_command import is_package_file
    from deerflow.skills.validation import _validate_skill_frontmatter

    outside = tmp_path_factory.mktemp("outside") / "report.py"
    outside.write_text("print('elsewhere')\n", encoding="utf-8")
    (skill_dir / "scripts" / "linked.py").symlink_to(outside)
    (skill_dir / "scripts" / "sibling.py").symlink_to(skill_dir / "scripts" / "report.py")

    assert is_package_file(skill_dir, "scripts/report.py")
    assert not is_package_file(skill_dir, "scripts/linked.py")
    assert not is_package_file(skill_dir, "scripts/sibling.py")
    assert not is_package_file(skill_dir, "scripts")

    _write_skill(skill_dir, "scripts/linked.py build")
    valid, message, _name = _validate_skill_frontmatter(skill_dir)
    assert not valid
    assert "not a file in the skill's directory" in message


def test_skill_content_checked_before_its_package_is_written_checks_the_shape_alone():
    """Creating or editing SKILL.md validates it alone; loading checks the script exists."""
    from deerflow.skills.storage.skill_storage import SkillStorage

    content = "---\nname: report-kit\ndescription: Builds a report.\nfirst-command: {}\n---\n# Report kit\n"

    SkillStorage.validate_skill_markdown_content("report-kit", content.format("scripts/report.py build"))
    for malformed in ("/usr/bin/python build", "scripts/report.py build; rm x", "scripts/report.sh build"):
        with pytest.raises(ValueError, match="first-command"):
            SkillStorage.validate_skill_markdown_content("report-kit", content.format(malformed))


def test_business_report_starts_with_its_build():
    from deerflow.skills.parser import parse_skill_file
    from deerflow.skills.types import SkillCategory

    skill_md = Path(__file__).resolve().parents[2] / "skills" / "public" / "business-report" / "SKILL.md"
    skill = parse_skill_file(skill_md, SkillCategory.PUBLIC)

    assert skill is not None
    assert skill.first_command == FirstCommand("scripts/report.py", ())
