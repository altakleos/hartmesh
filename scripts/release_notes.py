#!/usr/bin/env python3
"""Extract one release's changelog entry, requiring its schema disclosure."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_HEADING = re.compile(r"^(#{2,6})[ \t]+(.+?)(?:[ \t]+#+)?[ \t]*$")
_RELEASE = re.compile(r"\[([^\]]+)\](?:[ \t].*)?$")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_REFERENCE = re.compile(r"^ {0,3}\[[^\]]+\]:[ \t]*\S")


def extract_release_notes(changelog: str, version: str) -> str:
    lines = changelog.splitlines()
    headings: list[tuple[int, int, str]] = []
    references: list[int] = []
    fence = ""
    for index, line in enumerate(lines):
        marker = _FENCE.match(line)
        if fence:
            if (
                marker
                and marker[1][0] == fence[0]
                and len(marker[1]) >= len(fence)
                and not marker[2].strip()
            ):
                fence = ""
            continue
        if marker:
            fence = marker[1]
            continue
        if heading := _HEADING.match(line):
            headings.append((index, len(heading[1]), heading[2]))
        elif _REFERENCE.match(line):
            references.append(index)

    matches = [
        index
        for index, level, title in headings
        if level == 2
        and (release := _RELEASE.fullmatch(title))
        and release[1] == version
    ]
    if not matches:
        raise ValueError(f"No changelog entry for {version}")
    if len(matches) != 1:
        raise ValueError(f"Duplicate changelog entries for {version}")
    start = matches[0]
    end = min(
        [index for index, level, _ in headings if index > start and level == 2]
        + [len(lines)]
    )
    # Keep link definitions out of the last release's body, but retain definitions
    # it actually uses so Markdown links still resolve on the GitHub Release.
    entry_references = [index for index in references if start < index < end]
    if entry_references:
        end = min(entry_references)
    schema_sections = [
        index
        for index, level, title in headings
        if start < index < end and level == 3 and title.casefold() == "schema changes"
    ]
    if len(schema_sections) != 1:
        raise ValueError(
            f"Release {version} must have exactly one '### Schema changes' section"
        )
    schema_start = schema_sections[0]
    schema_end = min(
        [
            index
            for index, level, _ in headings
            if schema_start < index < end and level <= 3
        ]
        + [end]
    )
    if not "\n".join(lines[schema_start + 1 : schema_end]).strip():
        raise ValueError(f"Release {version} has an empty '### Schema changes' section")
    notes = "\n".join(lines[start:end]).strip()
    used_references = [
        lines[index]
        for index in references
        if lines[index].lstrip().split("]:", 1)[0] + "]" in notes
    ]
    if used_references:
        notes += "\n\n" + "\n".join(used_references)
    return notes + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="release version without the leading v")
    parser.add_argument(
        "--changelog",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "CHANGELOG.md",
    )
    args = parser.parse_args()
    try:
        notes = extract_release_notes(
            args.changelog.read_text(encoding="utf-8"), args.version
        )
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    sys.stdout.write(notes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
