"""Shared execution policy for operator-supplied stdio MCP launchers."""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from typing import Literal, NamedTuple

MCP_STDIO_COMMAND_ALLOWLIST_ENV = "DEER_FLOW_MCP_STDIO_COMMAND_ALLOWLIST"
DEFAULT_MCP_STDIO_COMMAND_ALLOWLIST = frozenset({"npx", "uvx"})
_SHELL_METACHARS = frozenset(";|&`$<>\n\r")
_ARBITRARY_EXEC_ARGS = frozenset(
    {
        "-c",
        "--call",
        "-e",
        "--eval",
        "--print",
        "--shell",
        "--node-arg",
        "--node-options",
    }
)


class _LauncherGrammar(NamedTuple):
    exec_args: frozenset[str]
    known_args: frozenset[str]
    unknown_consumes_value: bool

    def consumes_value(self, flag: str) -> bool:
        """Return whether this launcher option consumes the next token."""

        if self.unknown_consumes_value:
            return flag not in self.known_args
        return flag in self.known_args


# npm boolean options generated from @npmcli/config 10.9.4. The `-p`
# shorthand is removed below because npm exec overrides it as --package.
_NPM_BOOLEAN_ARGS = frozenset(
    {
        "--all",
        "--allow-same-version",
        "--audit",
        "--bin-links",
        "--commit-hooks",
        "--description",
        "--dev",
        "--diff-ignore-all-space",
        "--diff-name-only",
        "--diff-no-prefix",
        "--diff-text",
        "--dry-run",
        "--engine-strict",
        "--expect-results",
        "--force",
        "--foreground-scripts",
        "--format-package-lock",
        "--fund",
        "--git-tag-version",
        "--global",
        "--global-style",
        "--if-present",
        "--ignore-scripts",
        "--include-staged",
        "--include-workspace-root",
        "--install-links",
        "--json",
        "--legacy-bundling",
        "--legacy-peer-deps",
        "--link",
        "--long",
        "--offline",
        "--omit-lockfile-registry-resolved",
        "--optional",
        "--package-lock",
        "--package-lock-only",
        "--parseable",
        "--prefer-dedupe",
        "--prefer-offline",
        "--prefer-online",
        "--production",
        "--progress",
        "--provenance",
        "--read-only",
        "--rebuild-bundle",
        "--save",
        "--save-bundle",
        "--save-dev",
        "--save-exact",
        "--save-optional",
        "--save-peer",
        "--save-prod",
        "--shrinkwrap",
        "--sign-git-commit",
        "--sign-git-tag",
        "--strict-peer-deps",
        "--strict-ssl",
        "--timing",
        "--unicode",
        "--update-notifier",
        "--usage",
        "--version",
        "--versions",
        "--workspaces",
        "--workspaces-update",
        "--yes",
        "-?",
        "-B",
        "-D",
        "-E",
        "-H",
        "-O",
        "-P",
        "-S",
        "-a",
        "-d",
        "-dd",
        "-ddd",
        "-desc",
        "-f",
        "-g",
        "-h",
        "-help",
        "-iwr",
        "-l",
        "-local",
        "-n",
        "-no",
        "-porcelain",
        "-q",
        "-quiet",
        "-readonly",
        "-s",
        "-silent",
        "-v",
        "-verbose",
        "-ws",
        "-y",
    }
)
_NPX_BOOLEAN_ARGS = _NPM_BOOLEAN_ARGS - {"-p"}

# Value-taking uvx options generated from uvx 0.11.1 help output.
_UVX_VALUE_ARGS = frozenset(
    {
        "--allow-insecure-host",
        "--build-constraints",
        "--cache-dir",
        "--color",
        "--config-file",
        "--config-setting",
        "--config-settings-package",
        "--constraints",
        "--default-index",
        "--directory",
        "--env-file",
        "--exclude-newer",
        "--exclude-newer-package",
        "--extra-index-url",
        "--find-links",
        "--fork-strategy",
        "--from",
        "--index",
        "--index-strategy",
        "--index-url",
        "--keyring-provider",
        "--link-mode",
        "--no-binary-package",
        "--no-build-isolation-package",
        "--no-build-package",
        "--no-sources-package",
        "--overrides",
        "--prerelease",
        "--project",
        "--python",
        "--python-platform",
        "--refresh-package",
        "--reinstall-package",
        "--resolution",
        "--torch-backend",
        "--upgrade-package",
        "--with",
        "--with-editable",
        "--with-requirements",
        "-C",
        "-P",
        "-b",
        "-c",
        "-f",
        "-i",
        "-p",
        "-w",
    }
)

_PACKAGE_LAUNCHERS: dict[str, _LauncherGrammar] = {
    "npx": _LauncherGrammar(
        exec_args=_ARBITRARY_EXEC_ARGS,
        known_args=_NPX_BOOLEAN_ARGS,
        unknown_consumes_value=True,
    ),
    "uvx": _LauncherGrammar(
        exec_args=frozenset(flag for flag in _ARBITRARY_EXEC_ARGS if flag.startswith("--")),
        known_args=_UVX_VALUE_ARGS,
        unknown_consumes_value=False,
    ),
}
_EXEC_ARGS_OUTSIDE_PACKAGE_LAUNCHERS = frozenset({"-p"})
_CLUSTERED_EXEC_LETTERS = frozenset(flag[1] for flag in _ARBITRARY_EXEC_ARGS | _EXEC_ARGS_OUTSIDE_PACKAGE_LAUNCHERS if len(flag) == 2 and flag.startswith("-"))
_CODE_INJECTING_ENV_VARS = frozenset(
    {
        "BASH_ENV",
        "DYLD_INSERT_LIBRARIES",
        "ENV",
        "LD_AUDIT",
        "LD_PRELOAD",
        "NODE_OPTIONS",
        "PERL5OPT",
        "PYTHONHOME",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        "RUBYOPT",
    }
)


class McpStdioLaunchPolicyViolation(ValueError):
    """Safe typed rejection from the shared stdio launch policy."""

    def __init__(
        self,
        code: Literal[
            "command_required",
            "command_not_bare",
            "command_not_allowed",
            "argument_not_allowed",
            "environment_not_allowed",
        ],
        *,
        value: str | None = None,
        allowed_commands: tuple[str, ...] = (),
    ) -> None:
        self.code = code
        self.value = value
        self.allowed_commands = allowed_commands
        super().__init__(code)


def allowed_stdio_commands() -> frozenset[str]:
    """Return bare executable names allowed by the deployment environment."""

    raw = os.environ.get(MCP_STDIO_COMMAND_ALLOWLIST_ENV)
    extra = () if raw is None else (item.strip() for item in raw.split(","))
    return DEFAULT_MCP_STDIO_COMMAND_ALLOWLIST | frozenset(item for item in extra if item)


def _stdio_command_name(command: object) -> str:
    if not isinstance(command, str) or not command.strip():
        raise McpStdioLaunchPolicyViolation("command_required")
    stripped = command.strip()
    if stripped != command or "/" in stripped or "\\" in stripped or any(character.isspace() for character in stripped) or any(character in stripped for character in _SHELL_METACHARS):
        raise McpStdioLaunchPolicyViolation("command_not_bare")
    return stripped


def _launcher_option_region(
    args: Sequence[object],
    *,
    grammar: _LauncherGrammar,
) -> tuple[str, ...]:
    region: list[str] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if not isinstance(arg, str):
            break
        token = arg.strip()
        if token in {"--", "-"} or not token.startswith("-"):
            break
        region.append(token)
        index += 1
        if "=" not in token and grammar.consumes_value(token):
            index += 1
    return tuple(region)


def _arbitrary_exec_arg(
    args: Sequence[object],
    *,
    command: str,
) -> str | None:
    grammar = _PACKAGE_LAUNCHERS.get(command.lower())
    if grammar is not None:
        for token in _launcher_option_region(args, grammar=grammar):
            flag = token.split("=", 1)[0]
            normalized = flag.lower() if flag.startswith("--") else flag
            if normalized in grammar.exec_args:
                return normalized
        return None

    denied = _ARBITRARY_EXEC_ARGS | _EXEC_ARGS_OUTSIDE_PACKAGE_LAUNCHERS
    for arg in args:
        if not isinstance(arg, str):
            continue
        flag = arg.split("=", 1)[0].strip().lower()
        if flag in denied:
            return flag
        if not flag.startswith("-") or flag.startswith("--"):
            continue
        for letter in flag[1:]:
            if letter in _CLUSTERED_EXEC_LETTERS:
                return f"-{letter}"
    return None


def validate_mcp_stdio_launch(
    *,
    command: object,
    args: Sequence[object],
    env_names: Iterable[object],
    allowed_commands: Iterable[str] | None = None,
    enforce_execution_policy: bool = True,
) -> str:
    """Validate a stdio MCP launch and return its normalized bare command.

    This is defense in depth for authenticated operator input, not a sandbox:
    allowed package launchers can still fetch and execute operator-selected
    code. The same function is used by both the legacy HTTP mutation boundary
    and governed revision validation so those paths cannot diverge.
    """

    command_name = _stdio_command_name(command)
    allowed = frozenset(allowed_stdio_commands() if allowed_commands is None else allowed_commands)
    if enforce_execution_policy and command_name not in allowed:
        raise McpStdioLaunchPolicyViolation(
            "command_not_allowed",
            value=command_name,
            allowed_commands=tuple(sorted(allowed)),
        )
    if enforce_execution_policy:
        exec_flag = _arbitrary_exec_arg(args, command=command_name)
        if exec_flag is not None:
            raise McpStdioLaunchPolicyViolation(
                "argument_not_allowed",
                value=exec_flag,
            )
    for raw_name in env_names:
        if not isinstance(raw_name, str):
            continue
        env_name = raw_name.strip().upper()
        if env_name in _CODE_INJECTING_ENV_VARS:
            raise McpStdioLaunchPolicyViolation(
                "environment_not_allowed",
                value=raw_name,
            )
    return command_name


__all__ = [
    "DEFAULT_MCP_STDIO_COMMAND_ALLOWLIST",
    "MCP_STDIO_COMMAND_ALLOWLIST_ENV",
    "McpStdioLaunchPolicyViolation",
    "allowed_stdio_commands",
    "validate_mcp_stdio_launch",
]
