"""A tool that makes files can present them in the same call.

Why
---
Files reach the user only through a presentation: an ``artifacts`` state
update, which ``present_files`` produces from a separate model call made after
the files exist. That call is one more model round trip on every deliverable,
and it is a judgement the model can reason itself out of. On the tenant class
it did (hartmesh-tenancy/DF17): after a revision that rewrote the same four
files as the turn before, the model wrote "Done" and presented nothing, and
the delivery fence correctly failed the run.

This module lets a tool that produces files present them as part of the same
call: the model names them under a ``present`` argument when it asks for the
work, and the tool attaches whichever of them exist once the work is done.
The decision stays the model's, exactly as with ``present_files``; what
changes is when it is made (while composing the command, with the paths in
front of it) and what it costs (nothing). Nothing is read from the command's
output: text a command prints is data, never a presentation.

What is checked
---------------
Each named path, against filesystem facts rather than the model's word: it
resolves inside this conversation's outputs directory through the resolver
``present_files`` uses (which follows a symlink to its target and so refuses
one that leaves the directory); it names a regular file that exists once the
work is done; and, when the caller says when its work started, the file was
modified at or after that moment. The last rule is what makes ``present`` on
a producing tool mean "what this call produced": a build that failed leaves
last turn's render where it was, and that file is refused by name rather
than delivered as if it were new. A refused path is reported with its reason
and never written to ``artifacts``; the model can still hand it over with
``present_files`` if it means that file. A delegated task (a subagent's
shell scope) may not present, as ``present_files`` is withheld from it.

How a tool adopts it
--------------------
Accept ``present: list[str] | None``, note ``time.time()`` before doing the
work, and return :func:`with_presentation` in place of the text. The result
is then the same ``Command`` ``present_files`` returns -- the ``artifacts``
update the journal records as a presentation whatever tool made it, and a
``ToolMessage`` tagged ``additional_kwargs[PRESENTED_FILES_KEY]``
(``deerflow.runtime.presented_files``). The tag is the presentation: the
journal records tagged paths as what the run presented, and the fence, the
archive and the evidence bundle read that record, so no registry of
presenting tools exists downstream and a file that lands in ``artifacts``
as a side effect (a browser tool's screenshot) counts for none of them.
``present_files`` tags its result the same way; the browser draws that call
from the call itself and skips its result, so nothing is listed twice.

What the result says is bounded: at most a few refused paths are echoed,
each clipped and stripped of control characters, because the path is the
model's text and the result is read back into its context.
"""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.runtime.presented_files import PRESENTED_FILES_KEY
from deerflow.sandbox.lease import sandbox_command_scope
from deerflow.tools.builtins.present_file_tool import resolve_presented_filepath

__all__ = ["PRESENTED_FILES_KEY", "Presentation", "describe_presentation", "validate_presentation", "with_presentation"]

#: Filesystems round modification times; a file written in the first moments
#: after the work started must not be refused for a rounding.
MTIME_TOLERANCE_SECONDS = 2.0

#: Bounds on what one call may present, so a runaway argument cannot turn the
#: result into an unbounded list: a report is four files.
MAX_PRESENTED_PATHS = 64
MAX_PRESENTED_PATH_LENGTH = 4096

#: How many refusals the result names before summarizing the rest, and how
#: much of a refused path it echoes: the path is the model's own text.
MAX_DESCRIBED_REFUSALS = 8
MAX_ECHOED_PATH_LENGTH = 160
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")

#: What the result says when files were attached; the skill docs and the
#: lead-agent prompt refer to it, so it is owned here.
PRESENTED_PHRASE = "Presented to the user"
NOT_ATTACHED_PHRASE = "Not attached"


@dataclass(frozen=True)
class Presentation:
    """What the named paths resolved to, path by path."""

    presented: list[str] = field(default_factory=list)
    refused: list[tuple[str, str]] = field(default_factory=list)

    @property
    def requested(self) -> bool:
        return bool(self.presented or self.refused)


def _requested_paths(paths: Sequence[Any] | None) -> tuple[list[str], list[tuple[str, str]]]:
    """The paths the model named, in order, without repeats, bounded; and the entries set aside, with why.

    Nothing is dropped silently: an entry that is not a path, too long, or
    past the bound is refused by name, so a call that asked for files is
    always answered.
    """
    if not isinstance(paths, (list, tuple)):
        return [], []
    requested: list[str] = []
    refused: list[tuple[str, str]] = []
    for path in paths:
        if not isinstance(path, str) or not path:
            refused.append((repr(path), "not a path"))
        elif len(path) > MAX_PRESENTED_PATH_LENGTH:
            refused.append((path, "path too long"))
        elif path in requested:
            continue
        elif len(requested) >= MAX_PRESENTED_PATHS:
            refused.append((path, f"more than {MAX_PRESENTED_PATHS} paths in one call"))
        else:
            requested.append(path)
    return requested, refused


def validate_presentation(runtime: Any, paths: Sequence[Any] | None, *, written_after: float | None = None) -> Presentation:
    """Which of *paths* may be presented now, and why each of the others may not.

    *written_after* is the moment the caller's work started (``time.time()``);
    a file last modified before it was not produced by this call and is refused.
    ``None`` asks only that the file exist.
    """
    requested, refused = _requested_paths(paths)
    if not requested:
        return Presentation(refused=refused)
    if sandbox_command_scope(getattr(runtime, "context", None)) is not None:
        return Presentation(refused=[(path, "a delegated task cannot present files; report the paths to the agent that delegated") for path in requested] + refused)

    presented: list[str] = []
    for path in requested:
        try:
            virtual_path, actual_path = resolve_presented_filepath(runtime, path)
        except ValueError:
            refused.append((path, "outside this conversation's outputs"))
            continue
        try:
            metadata = os.stat(actual_path)
        except OSError:
            refused.append((path, "does not exist"))
            continue
        if not stat.S_ISREG(metadata.st_mode):
            refused.append((path, "not a regular file"))
            continue
        # mtime for a write; ctime for a file moved or copied with its stamps
        # kept (``mv``, ``cp -p``), which userland cannot backdate. The
        # sandbox and the Gateway share the host clock (the local Docker
        # backend); a provider whose files are not on this host is refused
        # earlier as "does not exist".
        if written_after is not None and max(metadata.st_mtime, metadata.st_ctime) < written_after - MTIME_TOLERANCE_SECONDS:
            refused.append((path, "not written by this call, so it may be from an earlier run; present_files can hand it over if you mean that file as it stands"))
            continue
        if virtual_path not in presented:
            presented.append(virtual_path)
    return Presentation(presented=presented, refused=refused)


def _echo(path: str) -> str:
    """A refused path as the result may repeat it: no control characters, clipped."""
    clean = _CONTROL_CHARS.sub("?", path)
    return clean if len(clean) <= MAX_ECHOED_PATH_LENGTH else clean[: MAX_ECHOED_PATH_LENGTH - 1] + "…"


def describe_presentation(presentation: Presentation) -> str:
    """The lines appended to the tool result so the model knows what happened.

    Bounded: presented paths are the validated virtual paths; refused ones are
    the model's text, echoed clean and clipped, and at most
    :data:`MAX_DESCRIBED_REFUSALS` of them by name.
    """
    lines: list[str] = []
    if presentation.presented:
        count = len(presentation.presented)
        lines.append(f"{PRESENTED_PHRASE}: {count} file{'s' if count != 1 else ''}, delivered with this turn. Do not call present_files for them: that would attach them a second time.")
        lines.extend(f"  {path}" for path in presentation.presented)
    elif presentation.refused:
        lines.append(f"Nothing was presented: the user has not received {'this file' if len(presentation.refused) == 1 else 'these files'}.")
    for path, reason in presentation.refused[:MAX_DESCRIBED_REFUSALS]:
        lines.append(f"{NOT_ATTACHED_PHRASE}: {_echo(path)} ({reason}).")
    remaining = len(presentation.refused) - MAX_DESCRIBED_REFUSALS
    if remaining > 0:
        lines.append(f"{NOT_ATTACHED_PHRASE}: {remaining} more path{'s' if remaining != 1 else ''}, not listed.")
    return "\n".join(lines)


def with_presentation(
    runtime: Any,
    text: str,
    *,
    present: Sequence[Any] | None,
    tool_call_id: str,
    written_after: float | None = None,
    max_chars: int = 0,
    truncate: Callable[[str, int], str] | None = None,
) -> str | Command:
    """The tool result for *text*: the text itself, or a state update that presents.

    Returns *text* (truncated to *max_chars* by *truncate* when both are given)
    unchanged when nothing was asked for. Otherwise the text gains a
    description of what was attached and what was refused, and when at least
    one file survived validation the result is the ``Command`` described in
    the module docstring. The description takes its room out of the budget,
    so the result stays within *max_chars* rather than over it by the size of
    the trailer.
    """
    presentation = validate_presentation(runtime, present, written_after=written_after)
    if not presentation.requested:
        return truncate(text, max_chars) if truncate is not None and max_chars else text
    if presentation.presented and not tool_call_id:
        # Without a tool call to answer, no state update can carry the files:
        # say so rather than describe a delivery that did not happen.
        presentation = Presentation(refused=[(path, "no tool call to attach it to") for path in presentation.presented] + presentation.refused)
    trailer = describe_presentation(presentation)
    body = text.rstrip()
    if truncate is not None and max_chars:
        # Never zero: to the bash truncator zero means "no limit", and the
        # trailer must not switch the budget off.
        body = truncate(body, max(max_chars - len(trailer) - 2, 1))
    combined = f"{body}\n\n{trailer}" if body else trailer
    if not presentation.presented:
        return combined
    return Command(
        update={
            "artifacts": list(presentation.presented),
            "messages": [
                ToolMessage(
                    combined,
                    tool_call_id=tool_call_id,
                    additional_kwargs={PRESENTED_FILES_KEY: list(presentation.presented)},
                )
            ],
        },
    )
