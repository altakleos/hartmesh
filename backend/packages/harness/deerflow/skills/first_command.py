"""A skill's ``first-command``: the command its work starts with.

A skill may declare, in its ``SKILL.md`` frontmatter, a script in its own
directory and the words that follow it::

    first-command: scripts/report.py build

``SkillToolPolicyMiddleware`` then runs nothing else in the sandbox between the
message that first loads the skill and a run of that command. A skill declares
one only when its instructions start every piece of work with that command,
because the command itself reads what it needs (for business-report, the build
reads the upload and prints what it found).

The script is a Python file, run with ``python``. A run is recognised in the
``command`` the model gave ``bash``, read the way bash would read the forms a
skill's own examples use: variables assigned as a statement of their own
(``export`` included), ``${VAR:?message}`` guards, ``cd`` into the skill's
directory, line continuations, and the ``env``, ``timeout``, ``nice``,
``time``, ``nohup``, ``command`` and ``exec`` wrappers. Anything that cannot be
read that way is not a run of it, so the model is told the command instead of
having something else pass for it. A run is judged by what the command says,
not by whether bash reaches it: ``false && python …`` counts, as its result
(an ordinary failure) would show the model.
"""

from __future__ import annotations

import posixpath
import re
import shlex
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

FIRST_COMMAND_PROPERTY = "first-command"

# Words a declaration may hold: a path and plain arguments, nothing a shell
# would expand, quote or treat as an operator.
_PLAIN_WORD = re.compile(r"[A-Za-z0-9_@%+=:,./-]+")
_SHELL_OPERATORS = ";&|\n()<>"
_ASSIGNMENT = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=(.*)", re.DOTALL)
_EXPANSION = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?:(:?[-=?+])([^}]*))?\}|\$([A-Za-z_][A-Za-z0-9_]*)")
_PYTHON = re.compile(r"python(?:3(?:\.\d+)?)?")
# Python options whose value is the next word.
_PYTHON_OPTIONS_WITH_VALUE = frozenset({"-W", "-X", "--check-hash-based-pycs"})
# Commands that run the rest of their words as a command, and their options
# that take the next word as a value.
_WRAPPERS = {
    "command": frozenset(),
    "exec": frozenset({"-a"}),
    "env": frozenset({"-u", "--unset", "-C", "--chdir", "-S", "--split-string"}),
    "nice": frozenset({"-n", "--adjustment"}),
    "nohup": frozenset(),
    "time": frozenset({"-f", "--format", "-o", "--output"}),
    "timeout": frozenset({"-s", "--signal", "-k", "--kill-after"}),
}
# Builtins that assign variables for the rest of the command, and declare's
# flags are skipped.
_ASSIGNING_BUILTINS = frozenset({"export", "readonly", "declare", "typeset", "local"})


@dataclass(frozen=True)
class FirstCommand:
    """A script relative to the skill's directory, and the words that must follow it."""

    script: str
    arguments: tuple[str, ...] = ()

    def __str__(self) -> str:
        return " ".join((self.script, *self.arguments))


def parse_first_command(raw: object, is_file: Callable[[str], bool]) -> FirstCommand | None:
    """Parse the ``first-command`` frontmatter value; ``None`` when it is absent.

    *is_file* answers whether a path relative to the skill's directory is a
    file in the package. Raises ``ValueError`` for a value that does not name
    one followed by plain words. The messages carry no host path.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError(f"{FIRST_COMMAND_PROPERTY} must be a string: a script in the skill's directory and the words that follow it")
    words = raw.split()
    if not words:
        raise ValueError(f"{FIRST_COMMAND_PROPERTY} cannot be empty")
    for word in words:
        if not _PLAIN_WORD.fullmatch(word):
            raise ValueError(f"{FIRST_COMMAND_PROPERTY} holds plain words only, without shell syntax: {word!r}")
    script = words[0]
    if script.startswith("/") or posixpath.normpath(script) != script or script == ".." or script.startswith("../"):
        raise ValueError(f"{FIRST_COMMAND_PROPERTY} must start with a script path relative to the skill's directory, without . or .. segments: {script!r}")
    if not script.endswith(".py"):
        raise ValueError(f"{FIRST_COMMAND_PROPERTY} names {script}, which is not a Python script; the command is run with python")
    if not is_file(script):
        raise ValueError(f"{FIRST_COMMAND_PROPERTY} names {script}, which is not a file in the skill's directory")
    return FirstCommand(script, tuple(words[1:]))


def is_package_file(skill_dir: Path, script: str) -> bool:
    """Whether *script* is a regular file of the package in *skill_dir*, not a link out of it."""
    path = skill_dir / script
    return path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(skill_dir.resolve())


def runs_first_command(command: str, *, directory: str, first_command: FirstCommand) -> bool:
    """Whether the shell *command* runs *first_command* from the skill *directory*.

    True when one of its simple commands is Python running the declared
    script, with the declared words right after it.
    """
    simple_commands = _simple_commands(command)
    if simple_commands is None:
        return False
    script = posixpath.normpath(posixpath.join(directory, first_command.script))
    variables: dict[str, str] = {}
    cwd: str | None = None
    for words in simple_commands:
        if words[0] in _ASSIGNING_BUILTINS:
            # ``export NAME=value`` sets NAME for every later command, like a bare assignment.
            words = [word for word in words[1:] if not word.startswith(("-", "+"))]
            if not words:
                continue
        assignments = [_ASSIGNMENT.fullmatch(word) for word in words]
        if all(assignments):
            # A statement of assignments only: set for every later command.
            for match in assignments:
                name, value = match.groups()
                expanded = _expand(value, variables)
                if expanded is None:
                    variables.pop(name, None)
                else:
                    variables[name] = expanded
            continue
        words = _unwrapped(words)
        if not words:
            continue
        if words[0] == "cd" and len(words) == 2:
            cwd = _resolve(_expand(words[1], variables), cwd)
            continue
        if _python_runs(words, script, first_command.arguments, variables, cwd):
            return True
    return False


def _unwrapped(words: list[str]) -> list[str]:
    """The command a simple command runs, past assignments in front of it and wrappers.

    Assignments in front of a command reach its process, after bash has
    already expanded the command's own words, so they set nothing here.
    """
    while True:
        while words and _ASSIGNMENT.fullmatch(words[0]):
            words = words[1:]
        if not words or words[0] not in _WRAPPERS:
            return words
        wrapper, takes_value = words[0], _WRAPPERS[words[0]]
        words = words[1:]
        while words and words[0].startswith("-") and words[0] != "--":
            option = words[0]
            words = words[2:] if option in takes_value else words[1:]
        if words and words[0] == "--":
            words = words[1:]
        if wrapper == "timeout" and words:
            # The duration comes before the command.
            words = words[1:]


def _simple_commands(command: str) -> list[list[str]] | None:
    """Split *command* into its simple commands' words; ``None`` for malformed shell."""
    # A backslash before a newline joins the lines, as bash does.
    lexer = shlex.shlex(command.replace("\\\n", ""), posix=True, punctuation_chars=_SHELL_OPERATORS)
    # A newline separates commands, as it does in bash, unless it is quoted.
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    lexer.commenters = "#"
    try:
        tokens = list(lexer)
    except ValueError:
        return None
    commands: list[list[str]] = [[]]
    for token in tokens:
        if all(char in _SHELL_OPERATORS for char in token):
            commands.append([])
        else:
            commands[-1].append(token)
    return [words for words in commands if words]


def _expand(word: str, variables: dict[str, str]) -> str | None:
    """*word* after parameter expansion; ``None`` when bash would not reach a known value."""
    unknown = False

    def substitute(match: re.Match[str]) -> str:
        nonlocal unknown
        name = match.group(1) or match.group(4)
        operator, alternative = match.group(2), match.group(3)
        value = variables.get(name)
        # With a colon, an empty value counts as unset.
        unset = value is None or (value == "" and operator is not None and operator.startswith(":"))
        if operator in (":-", "-", ":=", "="):
            return alternative if unset else value
        if operator in (":+", "+") or unset:
            # ``${VAR:?message}`` stops the command when VAR is unset, and a
            # plain ``$VAR`` nothing assigned is not a path this can know.
            unknown = True
            return ""
        return value

    expanded = _EXPANSION.sub(substitute, word)
    if unknown or "$" in expanded or "`" in expanded:
        return None
    return expanded


def _resolve(path: str | None, cwd: str | None) -> str | None:
    if path is None:
        return None
    if path.startswith("/"):
        return posixpath.normpath(path)
    if cwd is None:
        return None
    return posixpath.normpath(posixpath.join(cwd, path))


def _python_runs(words: list[str], script: str, arguments: tuple[str, ...], variables: dict[str, str], cwd: str | None) -> bool:
    if not _PYTHON.fullmatch(posixpath.basename(words[0])):
        return False
    rest = words[1:]
    while rest and rest[0].startswith("-"):
        # ``-c`` runs the string after it and ``-m`` a module: neither runs a script file.
        if rest[0].startswith(("-c", "-m")):
            return False
        rest = rest[2:] if rest[0] in _PYTHON_OPTIONS_WITH_VALUE else rest[1:]
    if not rest:
        return False
    return _resolve(_expand(rest[0], variables), cwd) == script and tuple(rest[1 : 1 + len(arguments)]) == arguments
